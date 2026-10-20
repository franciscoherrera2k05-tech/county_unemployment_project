# County Unemployment Early-Warning System

A monthly batch pipeline (Stage 1) that builds a county x month panel for predicting whether a US county's
unemployment rate rises by **at least 0.5 percentage points over the next 3 months**. The ML study (Stages 2-5)
lives in notebooks and is specified in `docs/ML_SPEC.md`.

- **Sources:** BLS Local Area Unemployment Statistics (county files, no key) and FRED (free API key).
- **Warehouse:** DuckDB on MotherDuck, database `unemployment`, layers `raw` -> `ref`/`staging` -> `marts`.
- **Orchestration:** Prefect 3 flows; GitHub Actions cron for the monthly schedule.
- **Docs:** `docs/WALKTHROUGH.md` (every file and join explained), `docs/data_dictionary.md`, `docs/BUILD_LOG.md`.

## Setup

```bash
uv sync                                   # Python 3.12 + locked dependencies
cp .env.example .env                      # then fill in values (or use Codespaces secrets)
uv run python scripts/preflight.py        # checks every secret with one cheap real call
```

| Variable | Required | Used for |
|---|---|---|
| `MOTHERDUCK_TOKEN` | yes | Warehouse (read/write token) |
| `FRED_API_KEY` | yes | FRED API (free) |
| `BLS_CONTACT_EMAIL` | yes | User-Agent for BLS downloads |
| `PREFECT_API_KEY`, `PREFECT_API_URL` | no | Prefect Cloud tracking; without them flows run locally |

## Run

```bash
uv run python -m src.flows.monthly_pipeline   # everything: ingest, staging, panel, tests, row counts (~1 min)
uv run python -m src.ingest.laus              # single steps: ingest_laus, ingest_fips, ingest_fred,
uv run python -m src.flows.build_staging      #   build_staging, build_panel
uv run python -m src.flows.build_panel
uv run pytest                                 # unit tests (synthetic, in-memory) + integration tests (live warehouse)
uv run python scripts/panel_summary.py        # descriptive panel summary
```

Smoke run for a few states: `uv run python -c "from src.ingest.laus import ingest_laus; ingest_laus(['WV','VA','OH'])"`,
or set `data.states` in `config/settings.yaml`. A same-day rerun replaces that day's raw snapshot, so follow a smoke run
with a full run. All settings (states, history start, lags, target horizon/threshold) are in `config/settings.yaml`.

## Schedule

`.github/workflows/monthly.yml` runs on the 28th of each month at 14:00 UTC (and on demand via `workflow_dispatch`),
using repository secrets with the same names as above. BLS posts county data for month t with the Metropolitan Area
release, usually early in month t+2, so the run picks up one new month each time.

## How point-in-time correctness works

- Month t's county data is treated as known in the **decision month** `t + 2` (`laus_release_lag_months`).
- A FRED value for month M is treated as known in month `M + lag` (fixed per-series lag in the config; 1 or 2 months).
  The panel attaches FRED with one equality join, `decision_date = available_month`.
- County history features use only months `<= t` (trailing window functions); the target uses month t+3.
- Tests prove it: scrambling all future data leaves every feature unchanged.

## Scope and limitations

These are deliberate scope decisions for a time-boxed portfolio project:

- **History starts in 2015** (features look back to 2012). Earlier years, including the Great Recession and the early
  recovery, are not covered. No years were cut by the scope-cut ladder.
- **Revised data only.** The BLS flat files hold only the latest benchmarked estimates, and FRED is used at its current
  (revised) vintage with fixed release lags rather than initial-release vintages. History therefore reflects revisions
  not known in real time. Real vintage history starts with the first raw snapshot (2026-10-20).
- **Geography exclusions:** 6 Alaska county-equivalents fail the continuity check (3 have no data since 2012;
  Valdez-Cordova ends in 2019 and its successors Chugach and Copper River start in 2020). The 3 with data in the window
  are 0.095% of counties and ~0.006% of labor force. Puerto Rico is out of scope (50 states + DC only).
- **No county structural features** (poverty, education, industry mix): ACS is not included.
- **Small FRED indicator set:** UNRATE, FEDFUNDS, CPIAUCSL, T10Y2Y, INDPRO, PAYEMS, ICSA.
- **County data are not seasonally adjusted.** The target therefore has a strong seasonal pattern (positive rate ~2% for
  June-July starts vs. ~60% for October-November starts); month-of-year sin/cos features are included.
- **COVID dominates the distress signal** (spring 2020); there are few other distress episodes after 2015.
- **October 2025 is missing** (federal government shutdown: no BLS county data, no UNRATE/CPI), leaving documented
  NULLs in features and targets near that month.
- **Simplified design** (by choice, so every file is explainable): no raw-snapshot pruning, no DB-size monitoring,
  and a small, focused test suite.
