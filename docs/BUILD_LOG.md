# Build Log

Resume point for the staged build. Times are UTC wall clock.

## Stage 1 — Pipeline (restart under simplicity rules)

**Restarted under simplicity rules; earlier phases 0–3 (commits 1b5840e..3889644) are superseded.** The user deleted
the earlier, more complex files; the phase 0 commit of this restart records those deletions together with the
CLAUDE.md and agent-file edits that add the simplicity rules. Verified facts from the earlier log were re-checked
before reuse (noted per phase).

- **Start:** 2026-10-20 21:51:05 UTC

### Checklist

- [x] Phase 0 — Scaffold + preflight
- [x] Phase 1 — LAUS ingest -> raw
- [x] Phase 2 — LAUS staging + tests
- [x] Phase 3 — County reference + month spine + geography gaps
- [x] Phase 4 — FRED
- [x] Phase 5 — Panel + target + leakage check
- [x] Phase 6 — Wrap-up

### Phase 0 — Scaffold + preflight

- uv 0.13.4, Python 3.12 (`.python-version`), non-packaged uv project (`[tool.uv] package = false`), imports as `src.*`.
- Stage 1 dependencies only: duckdb 1.5.6, prefect 3.8.8, requests, pyyaml, python-dotenv, pytest.
- Re-verified: Prefect 3.8.8's temporary local server crashes with fastapi 0.156 (`Body(min_items=...)` removed).
  Pinned `fastapi>=0.139,<0.150` (resolves 0.149.0); a test flow then runs.
- One helper module `src/utils/helpers.py` (config via `yaml.safe_load`, MotherDuck connect, retrying GET, temp download,
  SQL-file runner, state FIPS table). No custom config loader, no retention/pruning, no DB-size warnings.
- Config deviations from CLAUDE.md section 5 (simplicity rules): dropped the `storage` block and
  `fred_use_initial_release`; renamed `fred_fallback_lag_months` to `fred_lag_months` (it is now the only FRED timing rule).
- No `src/features/` package: all feature logic is plain SQL in `sql/marts/`.
- Preflight: MotherDuck authenticated, FRED key accepted, BLS reachable with contact User-Agent.
  `PREFECT_API_KEY`/`PREFECT_API_URL` not set -> warning; flows run locally.
- Elapsed at end of phase 0: ~4 min.

### Phase 1 — LAUS ingest -> raw

Verified facts (re-checked 2026-10-20 from `la.txt`, `la.series`, `la.measure`, `la.period`, `la.area_type`, `la.footnote`):
- County series ID = `LA` + `U` + `CN` + 5-digit county FIPS + `00000000` + 2-digit measure, e.g. `LAUCN010010000000003`.
  `la.series` has 3,225 county areas (area_type `F` = "Counties and equivalents"), each with measures 03-06, all seasonal code `U`
  (not seasonally adjusted). 78 of them are Puerto Rico (state 72), excluded by design (state table = 50 states + DC).
- Measures: 03 unemployment rate, 04 unemployment, 05 employment, 06 labor force. Periods `M01`-`M12` months, `M13` annual average.
- Data file: tab-delimited, space-padded, CRLF, header row. Missing value marker `-` with footnote `X` (2025 lapse in
  appropriations): 12,572 rows, all 2025 M10. Footnote `P` = preliminary (latest month, 2026 M08). Footnote `G` on 2025 annual averages.
- `src/ingest/laus.py`: stream to temp file (deleted in `finally`), filter in DuckDB `read_csv`, one `INSERT ... SELECT`.
  Filter documented: county series, chosen states, measures 03-06, `year >= laus_min_year` (2012).
- Smoke WV/VA/OH: 209,760 rows. Full: 2,388,276 rows, 12,576 series (3,144 counties), 51 states, years 2012-2026, ~27 s.
- Idempotency: the smoke and full runs were both today's snapshot; after the full run the table holds exactly one copy
  (2,388,276 rows, 0 duplicate series x year x period x snapshot keys). Same-day rerun replaces the whole day's snapshot
  (simplest rule; a smoke run must therefore be followed by a full run on the same day).
- Deviation: the earlier build's raw table (same columns) was reused; its same-day snapshot was replaced.
- Elapsed at end of phase 1: ~6 min.

### Phase 2 — LAUS staging + tests

- `sql/staging/laus_county_month.sql` -> `staging.laus_county_month`: 553,075 rows, 3,144 counties, 2012-01..2026-09.
  Latest snapshot only; `M13` excluded; `month` = first of month; measures pivoted to columns; `-` -> NULL via
  `CAST(nullif(value, '-') AS DOUBLE)` (any other non-number fails loudly); footnotes kept; `available_date = month + 2 months`.
- Months 2026-07 and 2026-08 have 3,143 counties; 2026-09 has only 3 (Los Angeles CA, DC, Miami-Dade FL publish early
  as sub-state areas). Handled in phase 3 (spine end) — not dropped here.
- Only NULL rates: 2025-10 for all 3,143 active counties (BLS footnote X, shutdown). Kept as NULL; documented.
- SQL files use `{name}` placeholders filled from config by `run_sql_file` (plain `str.format`).
- Tests: unit `test_laus_staging_parsing` (FIPS keeps leading zero, month parsing, M13 dropped, `-` -> NULL, lag);
  integration unique grain (raw, staging), 5-char FIPS, rate in [0, 100], labor force >= employed, NULLs only in 2025-10. 5 passed.
- Elapsed at end of phase 2: ~7 min.

### Phase 3 — County reference + month spine + geography gaps

- `ingest_fips` flow (`src/ingest/fips.py`): `la.area` -> `raw.laus_area` (8,351 rows, same-day snapshot replacement) and the
  static 51-row `ref.state`. Verified `la.area_type`: `F` = "Counties and equivalents" (3,225 areas incl. 78 Puerto Rico).
- `sql/ref/county.sql` -> `ref.county`: 3,147 counties, 51 states (inner join to `ref.state` drops Puerto Rico by design).
  County name = `area_text` without the ", XX" suffix (DC has no suffix and is kept as "District of Columbia").
- `sql/ref/month_spine.sql` -> `ref.month_spine`: 3,147 counties x 176 months (2012-01..2026-08) = 553,872 rows. Spine end =
  newest month with rates for >= 50% of counties (2026-08); the 3 counties with 2026-09 do not extend it.
- `sql/ref/geography_gap_report.sql` -> `ref.geography_gap_report` (one row per county, `status`). National gap months
  (< 50% of counties have a rate) are ignored: only 2025-10 (shutdown, BLS footnote X/G).
- **6 counties flagged, all Alaska** (same as the earlier build, re-verified):
  - no data in window: 02201 Prince of Wales-Outer Ketchikan, 02232 Skagway-Hoonah-Angoon, 02280 Wrangell-Petersburg (discontinued pre-2012)
  - disappears early: 02261 Valdez-Cordova (last month 2019-12)
  - appears late: 02063 Chugach, 02066 Copper River (first month 2020-01; Valdez-Cordova split)
- **Excluded share:** 3 of the 3,144 counties with data (0.095%); their average labor force ~10.1k of ~162.2M summed
  average labor force (~0.006%). Excluded via `status <> 'ok'`; nothing dropped silently.
- Connecticut already published as 9 planning regions back to 1990 and Virginia shows no appear/disappear in the window,
  so neither creates a gap here.
- `src/flows/build_staging.py` runs the SQL files in order and prints row counts.
- Tests: unique grain on ref.state/ref.county/ref.month_spine/ref.geography_gap_report; 51 states; every staging county is in
  ref.county; spine consecutive and rows = counties x months; gap report covers every county. 11 passed.
- Elapsed at end of phase 3: ~10 min.

### Phase 4 — FRED

- Verified with real `fred/series` calls (2026-10-20): UNRATE M SA; FEDFUNDS M NSA; CPIAUCSL M SA; T10Y2Y **D** NSA;
  INDPRO M SA; PAYEMS M SA; ICSA **W** SA (week-ending dates). All exist. Missing marker is `.`: UNRATE and CPIAUCSL have
  no 2025-10 value (shutdown); T10Y2Y has 171 `.` days (market holidays).
- `ingest_fred` (`src/ingest/fred.py`): observations from 2011-01-01 (one year before `laus_min_year`, for YoY), one temp CSV,
  one `INSERT ... SELECT`, same-day snapshot replacement. 5,891 rows.
- **Simple timing rule (simplicity rules; replaces CLAUDE.md section 10.3):** the value for reference month M counts as known in
  month `M + fred_lag_months[series]`. No `output_type=4` / initial-release logic; FRED's revised values are used (limitation).
- `staging.fred_monthly` (1,323 rows; series x ref_month): daily/weekly -> monthly mean; current incomplete month dropped;
  change columns via `LAG` per series (test confirms consecutive months); `available_month`.
- `staging.fred_by_available_month` (190 rows; 2011-02..2026-11): pivot to one row per `available_month` with the 9 FRED features
  of CLAUDE.md section 11, plus `fred_latest_ref_month` for the leakage check.
- Tests: unique grain on the three FRED tables, 7 series, consecutive months per series. 15 passed.
- Elapsed at end of phase 4: ~12 min.

### Phase 5 — Panel + target + leakage check

- `sql/marts/county_month_panel.sql` -> `marts.county_month_panel`: 439,740 rows = 3,141 counties x 140 months (2015-01..2026-08).
  **Equality joins only** (simplicity rules; replaces the ASOF JOIN of CLAUDE.md section 10.2): staging JOIN `ref.county` on
  `county_fips`, and one LEFT JOIN to `staging.fred_by_available_month` on `decision_date = available_month`. Exclusions and the
  spine end are `WHERE ... IN (subquery)` / scalar-subquery filters, not joins. A grain comment sits above every join.
- History features via `LAG` and trailing `ROWS BETWEEN k PRECEDING AND CURRENT ROW` windows per county (computed on 2012+ history,
  then filtered to `panel_start`); target via `LEAD(rate, 3)`. Cross-county context = window medians `PARTITION BY month`
  and `PARTITION BY state_fips, month` (same month t only, so no future data and no extra join).
- Target: `y_delta_3m = round(rate_{t+3} - rate_t, 1)` (percentage points), `y_label = 1` if `>= 0.5`, NULL when the target is unknown.
- `decision_date = month + 2 months`; prediction assumed made at the end of that month (after the LAUS release early in the month
  and the FRED releases implied by the lags).
- Rate changes rounded to 1 decimal (inputs are published to 1 decimal; removes float noise).
- Tests (22 passed): unit `test_target_is_rounded_pp_change`; unit **leakage perturbation** `test_no_feature_uses_future_data`
  (runs the real FRED + panel SQL on synthetic data, scrambles LAUS after t and FRED rows not yet available at d_t, asserts
  all features at t identical and that the target did change). Mutation-checked: joining FRED one month later, or a
  12-month window reaching 1 month ahead, both make it fail. Integration: panel unique grain, row count unchanged after each
  join, panel = included counties x months, panel counties in ref.county, leakage dates (`month < decision_date` and
  `fred_latest_ref_month < decision_date` on every row), no NULLs outside documented windows.
- Documented NULL windows: 2025-10 (no BLS county data, no UNRATE/CPI) makes rate-based features NULL at 2025-10 and for
  lags that reach it (e.g. 3-month changes at 2026-01), targets NULL for 2025-07 and 2025-10, FRED UNRATE NULL for
  decision month 2025-11; the last 3 months (2026-06..08) have NULL targets (kept for live scoring). Trailing mean/std/min
  skip the single NULL month.
- Elapsed at end of phase 5: ~16 min.

### Phase 6 — Wrap-up

- `src/flows/monthly_pipeline.py`: ingest_laus -> ingest_fips -> ingest_fred -> build_staging -> build_panel -> `pytest -q`
  (non-zero exit fails the flow) -> row count per table. **Full-state run end to end: ~42 s**, 22 tests passed inside the flow;
  row counts identical to the previous same-day runs (idempotent).
- `.github/workflows/monthly.yml`: cron `0 14 28 * *` + `workflow_dispatch`, `astral-sh/setup-uv`, `uv sync --locked`, preflight,
  then the pipeline; secrets from `${{ secrets.* }}`. Not executed on GitHub (no push); locally verified `uv sync --locked`
  and that Prefect runs with empty `PREFECT_API_*` variables (what Actions passes when those secrets are absent).
- Release timing (verified 2026-10-20 on bls.gov schedules): Metropolitan Area release for Oct 2026 on Dec 2, 2026 and for
  Nov 2026 on Dec 30, 2026; State release for Oct 2026 on Nov 20, 2026. The live file had county data through Aug 2026 on
  Oct 20, consistent with county data arriving with the metro release (~t+2). Lag 2 kept; cron day 28 kept.
  Not verified: that county rows appear in the LAUS flat files on the exact metro release day.
- README (setup, secrets, run, schedule, point-in-time rules, scope and limitations), `docs/data_dictionary.md`,
  `scripts/panel_summary.py`. No scope-cut ladder items were needed.
- Final `uv run pytest`: 22 passed (5 unit incl. leakage perturbation and target tests, 17 integration) in ~1.4 s.
- Elapsed at end of phase 6: ~16 min. **Total Stage 1 elapsed (restart): ~16 min** (21:51:05 -> ~22:07 UTC).

### Panel summary (descriptive only, 2026-10-20 snapshot, default threshold 0.5 pp)

- Rows: 439,740; counties: 3,141; months: 2015-01-01 to 2026-08-01
- Excluded for geography: 3 of 3,144 counties with data (0.095%), 0.0062% of summed average labor force; plus 3 reference counties with no data
- Positive-label rate (y_delta_3m >= 0.5 pp): 27.0% of 424,035 labeled rows

| Year | Labeled rows | Positive rate |
|---|---|---|
| 2015 | 37,692 | 25.5% |
| 2016 | 37,692 | 24.2% |
| 2017 | 37,692 | 23.7% |
| 2018 | 37,692 | 27.3% |
| 2019 | 37,692 | 31.3% |
| 2020 | 37,692 | 37.1% |
| 2021 | 37,692 | 17.1% |
| 2022 | 37,692 | 27.2% |
| 2023 | 37,692 | 28.9% |
| 2024 | 37,692 | 31.0% |
| 2025 | 31,410 | 26.0% |
| 2026 | 15,705 | 21.9% |

| Column | Missing rows | Missing % |
|---|---|---|
| rate | 3,141 | 0.71% |
| unemployed | 3,141 | 0.71% |
| employed | 3,141 | 0.71% |
| labor_force | 3,141 | 0.71% |
| rate_change_1m | 6,282 | 1.43% |
| rate_change_3m | 6,282 | 1.43% |
| rate_change_6m | 6,282 | 1.43% |
| rate_change_12m | 3,141 | 0.71% |
| rate_minus_min_36m | 3,141 | 0.71% |
| labor_force_pct_change_3m | 6,282 | 1.43% |
| labor_force_pct_change_12m | 3,141 | 0.71% |
| employed_pct_change_3m | 6,282 | 1.43% |
| employed_pct_change_12m | 3,141 | 0.71% |
| unemployed_pct_change_3m | 6,282 | 1.43% |
| y_delta_3m | 15,705 | 3.57% |
| national_median_change_3m | 6,282 | 1.43% |
| state_median_change_3m | 6,282 | 1.43% |
| y_label | 15,705 | 3.57% |
| fred_unrate | 3,141 | 0.71% |
| fred_unrate_change_3m | 6,282 | 1.43% |
| fred_cpi_yoy_pct | 3,141 | 0.71% |

(Columns not listed have no missing values.)

Positive-label rate by calendar month of t (strong seasonality from unadjusted county rates): Jan 9.4%, Feb 9.8%, Mar 27.5%,
Apr 51.2%, May 30.4%, Jun 1.5%, Jul 2.2%, Aug 6.4%, Sep 24.4%, Oct 63.1%, Nov 59.8%, Dec 40.0%. COVID: t = 2019-12..2020-03 are
92.3%, 96.5%, 97.3%, 87.5% positive; t = 2020-04..06 are 9.2%, 1.1%, 0.3%.
The 2025 and 2026 labeled-row counts are lower because of the 2025-10 gap and the unlabeled final 3 months.

### Deviations from CLAUDE.md (all from the simplicity rules)

- FRED: fixed per-series lag (`available_month = ref_month + lag`), no `output_type=4`; equality join instead of ASOF JOIN.
- Config: no `storage` block, no `fred_use_initial_release`, `fred_fallback_lag_months` renamed `fred_lag_months`.
- No raw snapshot retention/pruning and no `PRAGMA database_size` logging/warnings.
- Extra tables not in section 9: `raw.laus_area`, `ref.state`, `ref.geography_gap_report`, `staging.fred_by_available_month`.
- Same-day raw replacement deletes the whole day's snapshot (not per state).

### Limitations / unverified

- Revised vintages only (LAUS and FRED); real vintage history starts at the first snapshot (2026-10-20).
- The GitHub Actions workflow has not been run on GitHub.
- Prefect Cloud not configured (no `PREFECT_API_*`); flows ran on Prefect's temporary local server.
