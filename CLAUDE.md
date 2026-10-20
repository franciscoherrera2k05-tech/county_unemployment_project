# CLAUDE.md — County Unemployment Early-Warning System

> **Spec for a staged, autonomous build.** One agent (`.claude/agents/project-builder.md`) builds the
> project one **stage** at a time. The user's prompt names the stage; do only that stage, then stop.
> All design decisions are made below. Do not ask the user about resolved decisions. If reality contradicts
> this spec (an API changed, a variable doesn't exist), adapt, log it in `docs/BUILD_LOG.md`, and continue.

## 1. Mission

Build (A) a monthly batch data pipeline and (B) a notebook-based ML study that predicts whether a US
county's unemployment rate rises by at least 0.5 percentage points over the next three months.

Research questions (answered in the final stage, with numbers):
1. Can historical labor-market and economic data help predict future increases in county unemployment?
2. Which indicators provide the most useful early-warning signals?
3. How much earlier can the model identify economic distress than a simple historical benchmark?
4. How should a policymaker allocate limited resources when predictions are uncertain?

## 2. Stages (how the work is split across days)

| Stage | Scope | Detail lives in |
|---|---|---|
| **1. Pipeline** | Scaffold, preflight, LAUS / county reference / FRED ingest, staging, panel + target, tests, monthly flow, schedule, docs | This file |
| **2. Baselines + logistic** | `src/ml/` helpers; notebooks 00 (target/EDA), 01 (baseline), 02 (logistic regression) | `docs/ML_SPEC.md` |
| **3. Tree models** | Notebooks 03 (random forest), 04 (gradient boosting) | `docs/ML_SPEC.md` |
| **4. Kernel + dimensionality reduction** | Notebooks 05 (RBF SVM), 06 (PCA/UMAP + logistic) | `docs/ML_SPEC.md` |
| **5. Comparison, policy, report** | Notebooks 07, 08, 09 and `reports/findings.md` | `docs/ML_SPEC.md` |

Optional extension, not part of Stage 1: ACS county demographics (section 16). Add it later only if the user asks.

Rules for stages:
- Do **only** the stage the user names ("Run Stage N", or "Run Stage N, notebook X only").
- Before starting Stage N, confirm in `docs/BUILD_LOG.md` that earlier stages are complete and
  `uv run pytest` passes. If not, say what's missing and stop.
- Read `docs/ML_SPEC.md` only for Stages 2–5. Do not install ML dependencies in Stage 1.
- A stage is done when its exit criteria are met, `docs/BUILD_LOG.md` is updated, and the work is committed.
  Then stop and report. Don't start the next stage.

## 2a. Time budget (Stage 1)

**Target: Stage 1 finishes in about 75 minutes of wall-clock time (hard ceiling: 90).** Run `date` at the start, log the start time
in `docs/BUILD_LOG.md`, and record elapsed time at the end of every phase.

Milestones: phases 0–2 done by ~30 min; phases 3–4 by ~50 min; phases 5–6 by ~75 min.

Speed rules:
- Bulk-load only. Read each download in a local in-memory DuckDB (`read_csv` on the temp file), filter there,
  then do one `INSERT ... SELECT` into MotherDuck. Never insert row by row.
- Keep MotherDuck round trips few: batch SQL, build tables with single statements.
- Run the 3-state smoke test for LAUS only. Later sources go straight to the full state set.
- Keep the non-pipeline test suite fast (under ~2 minutes).
- If a problem takes more than ~10 minutes, stop debugging and use the documented fallback (for example:
  exclude the mismatched counties and report them; fall back to assumed FRED lags). Log it.

**Scope-cut ladder.** If you are behind a milestone, apply cuts in this order, logging each one as a limitation:
1. Shorten docs: README concise, data dictionary as one short table.
2. Write the GitHub Actions workflow but don't test it.
3. Reduce FRED to `UNRATE`, `FEDFUNDS`, `T10Y2Y`, `PAYEMS`.
4. Shorten history: `panel_start: 2018-01-01`, `laus_min_year: 2015`.

**Never cut:** the point-in-time rules, the leakage perturbation test, grain/uniqueness tests, county reference validation,
idempotent ingestion, secret handling.

## 3. Environment and secrets

Environment variables (Codespaces secrets, or a gitignored `.env` loaded with python-dotenv):

| Variable | Required | Used for |
|---|---|---|
| `MOTHERDUCK_TOKEN` | yes (read/write token) | Warehouse |
| `FRED_API_KEY` | yes (free) | FRED API |
| `BLS_CONTACT_EMAIL` | yes | Goes in the User-Agent for BLS downloads. BLS needs no key. |
| `PREFECT_API_KEY`, `PREFECT_API_URL` | no (warn if missing) | Prefect Cloud run tracking; without them flows run locally |

**Preflight (first action of Stage 1):** `scripts/preflight.py` checks every variable and makes one cheap
authenticated call to each service, reporting all problems at once. If anything required is missing or
rejected, stop and ask the user once. Never print, log, or commit secret values.

**Free only.** Use only free data and free tiers. Never sign up for, call, or recommend a paid API or plan.

## 4. Stack (fixed)

- Python 3.12 via **uv** (`uv add`, `uv run`; commit `pyproject.toml` and `uv.lock`).
- **DuckDB on MotherDuck** (`duckdb.connect("md:")`). No local database file.
- **Prefect 3** (Prefect Cloud when keys exist). **GitHub** + **Codespaces**; GitHub Actions cron for the schedule.
- **pytest** with SQL assertions for data tests. No pandera.
- Stage 1 dependencies only: `duckdb`, `prefect`, `requests`, `pyyaml`, `python-dotenv`, `pytest`.
- Not allowed without asking: Docker, Spark, Airflow, dbt, other warehouses, neural networks.

**Little disk:** never leave large files on disk. Stream downloads to a temp file and delete it. Never
commit data. Select only needed columns and rows from MotherDuck.

## 5. Configuration (`config/settings.yaml`) — create exactly these defaults

```yaml
project:
  database: unemployment
  random_seed: 42
data:
  states: all                  # or a list like [WV, VA, OH] for smoke runs
  laus_min_year: 2012          # history for 36-month lookback features
  panel_start: 2015-01-01      # ~11 years: expansion, 2015-16 energy bust, COVID, inflation/rate cycle
timing:
  laus_release_lag_months: 2   # conservative; verify against the BLS release schedule
  fred_use_initial_release: true
  fred_fallback_lag_months: {UNRATE: 1, FEDFUNDS: 1, CPIAUCSL: 2, T10Y2Y: 1, INDPRO: 2, PAYEMS: 1, ICSA: 1}
target:
  horizon_months: 3
  threshold_pp: 0.5
  sensitivity_thresholds: [0.3, 0.5, 0.75, 1.0]
storage:
  raw_snapshot_retention: 12
  warn_db_size_gb: 7           # MotherDuck free tier is ~10 GB
```

All code reads from config. No hardcoded thresholds, dates, or state lists.

**Why 2015:** enough history to learn current-regime dynamics without pre-2015 data (the Great Recession
recovery, which has different dynamics). Trade-off to state in the docs: there are few distress episodes
besides COVID, so results will lean heavily on 2020. Missing pre-2015 years (and any
years cut by the scope-cut ladder) are a deliberate scope decision for a time-boxed portfolio project, and
must be listed under limitations in the README (see section 14).

## 6. Repository layout

```
.
├── CLAUDE.md
├── README.md                      # setup, run, schedule, config
├── pyproject.toml / uv.lock
├── .gitignore                     # .env, data/, __pycache__, .ipynb_checkpoints
├── .env.example                   # variable names only
├── .claude/agents/project-builder.md
├── .github/workflows/monthly.yml  # cron + workflow_dispatch; secrets via ${{ secrets.* }}
├── config/settings.yaml
├── docs/
│   ├── BUILD_LOG.md               # stage + phase checklist, verified facts, deviations (the resume point)
│   ├── ML_SPEC.md                 # Stages 2-5 spec (read only when running those stages)
│   └── data_dictionary.md
├── scripts/preflight.py
├── sql/{staging,marts}/           # one .sql per table
├── src/{utils,ingest,flows,features}/    # (src/ml/ added in Stage 2)
├── tests/{unit,integration}/      # integration marked @pytest.mark.integration
└── data/                          # gitignored scratch only
```

## 7. Data sources and facts to verify (not assume)

**BLS LAUS** — flat files at `https://download.bls.gov/pub/time.series/la/`: `la.data.64.County` (~330 MB,
tab-delimited, county series only) plus `la.series`, `la.area`, `la.area_type`, `la.measure`, `la.period`,
and docs in `la.txt`. Send a User-Agent with contact email.
*Hypotheses to confirm from the files first:* series IDs look like `LAU` + `CN` + 5-digit county FIPS +
zeros + 2-digit measure code; measures 03 = unemployment rate, 04 = unemployment, 05 = employment,
06 = labor force; periods `M01`–`M12` are months and `M13` is the annual average (exclude); county data is
**not seasonally adjusted**; missing values are marked with a dash; a footnote code may flag preliminary
data (keep it). Stream to temp file, delete it, keep only `year >= laus_min_year`, and document that filter.

**County reference** — a reference dimension, not a feed. Derive it from LAUS's own `la.area` and
`la.area_type` files (area codes and names; no Census dependency). Build `ref.county` (`county_fips` 5-char,
`state_fips` = first two digits, `county_name`, `state_abbr` from a static 51-row state FIPS table in code) and
validate `staging.laus_county_month` and the panel against it. Report anomalies; never drop silently.

**FRED** — `UNRATE`, `FEDFUNDS`, `CPIAUCSL`, `T10Y2Y`, `INDPRO`, `PAYEMS`, `ICSA` (verify each exists and
check its frequency). Do not use `USREC` or any ex-post recession indicator.

## 8. Known data issues (handle and document each)

- **Geography changes:** county definitions change over time (e.g., Connecticut's move toward planning
  regions, Alaska and Virginia county-equivalent changes). Check every county for a continuous series across the
  spine. Produce a report of counties with gaps or that appear/disappear, exclude them via a documented list,
  and quantify the share of counties and labor force excluded.
- **Seasonality:** county rates aren't seasonally adjusted. Include month-of-year sin/cos features.
- **Rounding:** rates are published to one decimal. Compute `y_delta_3m = round(rate_{t+3} - rate_t, 1)` and
  compare to the threshold on that rounded value.
- **COVID:** March–June 2020 dominates the target. Document it in the data dictionary.
- **Revisions:** the flat files hold only the latest benchmarked estimates, so history reflects revisions not
  known in real time. State this as a limitation. Monthly raw snapshots build real vintage history going forward.

## 9. Warehouse design

Schemas: `raw`, `ref`, `staging`, `marts`. Every table has a documented grain and a uniqueness test.

| Table | Grain | Notes |
|---|---|---|
| `raw.laus_county` | series × year × period × `_snapshot_date` | All text, plus `_ingested_at`, `_snapshot_date`, `_source`. Same-day rerun replaces that day's snapshot (idempotent). Prune to `raw_snapshot_retention`. |
| `raw.fred_observations` | series × ref date × `_snapshot_date` | Includes `realtime_start`. |
| `ref.county` | county_fips | Dimension, from `la.area`. |
| `ref.month_spine` | county_fips × month | Complete months from `laus_min_year` to the latest LAUS month, no gaps. |
| `staging.laus_county_month` | county_fips × month | rate, unemployed, employment, labor_force, footnote, `available_date`. From the latest snapshot. |
| `staging.fred_monthly` | series × ref_month | Monthly aggregates + change features, `available_date`. |
| `marts.county_month_panel` | county_fips × month (t) | Features, `decision_date`, `y_delta_3m`, `y_label`. Rows with null target (final 3 months) are kept for live scoring. |

Log DB size via `PRAGMA database_size` after each run; warn above `warn_db_size_gb`.

## 10. Point-in-time rules (the central methodological rule)

- **t** = reference month (first of month). **decision_date d_t** = `t + laus_release_lag_months`: when
  month-t LAUS data could be known and a prediction is made. Every source table carries `available_date`.
1. LAUS features use only reference months `<= t`.
2. FRED features use only rows with `available_date <= d_t`, via `ASOF JOIN`, never a same-month join.
3. FRED: when `fred_use_initial_release`, fetch with `output_type=4` (initial release only, full real-time
   window); use `realtime_start` as `available_date`. Verify with a real call; if it doesn't behave as
   expected, fall back to reference-month-end + `fred_fallback_lag_months` and log it. Daily/weekly series
   become monthly means whose `available_date` is the max of their constituents'. Compute change features
   on the series' own monthly table, then as-of join.
4. Cross-county aggregates (section 11) are computed only from LAUS rows with reference month `<= t`.
5. Rolling windows are trailing only. The target uses months `t+1..t+3`; no feature touches them.
6. `y_label = 1` if `y_delta_3m >= threshold_pp`. **A change in percentage points, not a level.**

## 11. Features

County LAUS: current rate; rate change over 1, 3, 6, 12 months; trailing 12-month mean and std; rate minus
trailing 36-month minimum; labor-force and employment % change over 3 and 12 months; unemployed-count %
change over 3 months; month-of-year sin/cos.
FRED (as-of): UNRATE level and 3-month change; FEDFUNDS level and 6-month change; CPI year-over-year; T10Y2Y
monthly mean; INDPRO year-over-year; PAYEMS 3-month % change; ICSA monthly mean 3-month % change.
Cross-county context (computed from LAUS only, reference months `<= t`): the national median of the county
3-month rate change at t, and the same median within the county's state. These give macro and regional signal
without extra data sources.
Metadata: `county_fips`, `state_fips`, `month`, `decision_date`, labor force.

## 12. Pipeline (Prefect flows) and schedule

Flows: `ingest_laus`, `ingest_fips`, `ingest_fred`, `build_staging`, `build_panel`, and parent
`monthly_pipeline` (runs them in order, then the test suite, then logs row counts and DB size).
Network tasks retry with backoff and log row counts. A failing data test fails the flow. Every ingest is
idempotent and accepts a states override for smoke runs.
`.github/workflows/monthly.yml`: monthly cron (default day 28, 14:00 UTC) + `workflow_dispatch`, uses
`astral-sh/setup-uv`, runs `uv run python -m src.flows.monthly_pipeline`, secrets from repo secrets.
Verify BLS LAUS county release timing and adjust the day if needed, noting it in the log.

## 13. Tests

Unit (in-memory DuckDB, synthetic data): FIPS padding; month parsing and M13 exclusion; delta rounding and
threshold comparison; trailing-window features; as-of join picks the latest row with `available_date <= d_t`.

**Leakage perturbation test (required):** build features for a slice, then alter every LAUS value for months
`> t` and every FRED row with `available_date > d_t`, rebuild, and assert features at `t` are identical.

Integration (marked): unique grain on every table; 5-character FIPS; rate in [0, 100]; every panel county
exists in `ref.county`; no gaps in the month spine; panel row count consistent with counties × months; no
unexplained nulls after documented warm-up periods. Run with `uv run pytest`.

## 14. Stage 1 exit criteria

1. Phases 0–6 in `docs/BUILD_LOG.md` checked off with verified facts and deviations recorded.
2. `uv run pytest` passes (including the leakage test) against the live warehouse.
3. `monthly_pipeline` runs end to end on all states.
4. `docs/BUILD_LOG.md` contains a **panel summary**: row count, county count, date range, the share of
   counties/labor force excluded, positive-label rate overall and by year (default threshold), and
   missingness by feature. (Descriptive only. No modeling.)
5. `README.md`, `docs/data_dictionary.md`, and the Actions workflow exist. Committed one commit per phase.
6. `README.md` has a **Scope and limitations** section stating factually, as deliberate scope decisions:
   history starts in 2015 (earlier years, including the Great Recession and early recovery, are not covered;
   plus any years cut by the ladder); LAUS reflects only the latest revised vintage, with real vintage history
   starting at the first snapshot; counties excluded for geography problems (with counts); no county structural features
   (poverty, education, industry mix) because ACS is not included; a small FRED indicator set; county data not seasonally adjusted; COVID dominating the
   distress signal; and every item cut under the scope-cut ladder.
7. `docs/BUILD_LOG.md` records start time, elapsed time per phase, and total elapsed time.

## 15. Rules of conduct

- **Honesty over polish.** Never fabricate numbers, citations, or verification. Say what couldn't be verified.
- Verify external facts (series-ID layout, release timing, variable labels, API behavior) against official
  docs or real responses.
- Side effects: write only to the configured MotherDuck database. Never drop other databases. `CREATE OR REPLACE`
  only for staging and marts. In `raw`, the only deletions are same-day snapshot replacement and retention
  pruning. Don't modify GitHub secrets or repo settings. Don't push unless the user asks.
- Ask the user only when blocked. Otherwise decide, log it, continue.
- Update `docs/BUILD_LOG.md` and commit at the end of every phase so work is resumable.

## 16. Optional extension (not Stage 1): ACS county demographics

Only if the user asks. Needs `CENSUS_API_KEY` (free). Add config `acs_min_vintage: 2013` and
`acs_release: {vintage_offset_years: 1, month: 12, day: 31}`. ACS 5-year, vintages from `acs_min_vintage` to the
latest (probe the API), one call per vintage with `for=county:*`. Verify each variable against
`https://api.census.gov/data/{year}/acs/acs5/variables.json` and assert the label matches before use:
`B01003_001E` population; `B01002_001E` median age; `B19013_001E` median household income; `B17001_001E`,
`B17001_002E` poverty; `B15003_001E` and `B15003_022E`–`025E` bachelor's+; `C24050_001E`–`C24050_014E`
employed by industry. Add `raw.acs_county` and `staging.acs_county`; `available_date = (vintage +
vintage_offset_years)-{month}-{day}`; as-of join on `available_date <= d_t`; derive poverty rate, bachelor's+
share, log income, log population, and industry shares; extend the leakage test to cover it. Expect county
FIPS mismatches between LAUS and ACS (Connecticut, Alaska, Virginia) and handle them with an exclusion report.
