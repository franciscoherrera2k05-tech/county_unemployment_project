# Walkthrough

Plain-English tour of every file, every join, and likely interview questions. Updated at the end of each phase.

## The story in one paragraph

Every month a pipeline downloads county unemployment data from the Bureau of Labor Statistics (LAUS) and national
indicators from FRED, stores them untouched in a `raw` layer, cleans them into a `staging` layer, and builds one
modeling table in `marts`: one row per county per month with history features and a target. The target asks
"will this county's unemployment rate rise by at least 0.5 percentage points over the next 3 months?". Every
feature only uses data that would actually have been published by the time the prediction is made
(point-in-time correctness), and rerunning the pipeline on the same day gives the same result (idempotent).

## Files

### Setup and config

**`pyproject.toml` / `uv.lock` / `.python-version`** — Declare Python 3.12 and the six Stage 1 libraries, locked
by uv so every machine installs identical versions. `fastapi` is pinned below 0.150 because Prefect 3.8.8's local
server breaks on newer fastapi. Pytest settings live here too (the `integration` marker).

**`config/settings.yaml`** — The single place for settings: database name, which states, history start, release
lags, target horizon and threshold. Code reads it with `yaml.safe_load`, so changing the threshold or the state list
never requires a code change.

**`.gitignore` / `.env.example`** — `.gitignore` keeps secrets (`.env`) and scratch data out of git.
`.env.example` lists the variable names (never values) a new developer must set.

**`src/utils/helpers.py`** — The only shared module. It loads the config, opens a MotherDuck connection (and creates
the four schemas), downloads files with retries, and runs SQL files with config values filled in. It also holds the
static table of 51 state FIPS codes (50 states + DC).

**`scripts/preflight.py`** — Run first. It checks the required secrets exist and makes one cheap real call to
MotherDuck, FRED and BLS, listing every problem at once so setup fails fast instead of halfway through a load.
It never prints secret values.

**`tests/conftest.py` / `tests/unit/test_config.py`** — `conftest.py` skips warehouse (integration) tests when no
MotherDuck token exists and shares one connection. `test_config.py` checks the config loads and the state table has 51 rows.

### Ingest (raw layer)

**`src/ingest/laus.py`** — The `ingest_laus` Prefect flow. It streams the ~340 MB BLS county file to a temp file,
lets DuckDB read and filter it (county series, chosen states, the four measures, years from 2012), and inserts the
result into `raw.laus_county` in a single statement, then deletes the temp file. Values stay as text in raw so we keep
exactly what BLS published; typing happens in staging. Each load is stamped with `_snapshot_date`; rerunning on the same
day first deletes that day's snapshot, so reruns never create duplicates.

### Staging layer

**`sql/staging/laus_county_month.sql`** — Turns raw LAUS text into one clean row per county per month. It reads only
the latest snapshot, drops the annual average (`M13`), converts `'M04'` + `'2020'` into the date 2020-04-01, turns the
BLS missing marker `-` into NULL, and pivots the four measures into columns. It also adds `available_date` (month + 2):
the earliest month this number could have been known, which is what point-in-time correctness is built on.

**`tests/unit/test_laus_staging.py`** — Runs the real staging SQL on three fake rows in an in-memory DuckDB, so the
parsing rules (leading-zero FIPS, month dates, M13 dropped, `-` -> NULL, release lag) are tested without the warehouse.

**`tests/integration/test_warehouse.py`** — SQL assertions on the live tables: unique grain for every table, 5-character
FIPS, rates between 0 and 100, labor force >= employed, and NULL rates only in the October 2025 shutdown month.

## Interview questions

**Q: Why a separate preflight script?** A: Loads take minutes; a bad token should fail in seconds with a clear
message listing every problem, not mid-pipeline.

**Q: Why put settings in YAML instead of constants?** A: Thresholds, dates and state lists change between experiments
and smoke runs; keeping them in one file means no code edits and an auditable record of what was run.

**Q: Why keep raw data as text with a snapshot date?** A: Raw is an untouched copy of the source, so any cleaning bug
can be fixed by rebuilding staging. Snapshot dates build a history of what BLS published each month, since BLS revises past values.

**Q: What makes the ingest idempotent?** A: Before inserting, it deletes rows with today's `_snapshot_date` inside one
transaction, so running twice on the same day leaves exactly one copy.

**Q: How do you handle the October 2025 gap?** A: BLS did not collect October 2025 data (government shutdown), and marks it
with `-`. Staging keeps the row with a NULL rate rather than inventing a value; features and targets that need that month
are NULL too, and the model stage decides how to treat them.
