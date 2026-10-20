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
- [ ] Phase 3 — County reference + month spine + geography gaps
- [ ] Phase 4 — FRED
- [ ] Phase 5 — Panel + target + leakage check
- [ ] Phase 6 — Wrap-up

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
