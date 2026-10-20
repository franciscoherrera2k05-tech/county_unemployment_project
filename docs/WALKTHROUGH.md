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

## Interview questions

**Q: Why a separate preflight script?** A: Loads take minutes; a bad token should fail in seconds with a clear
message listing every problem, not mid-pipeline.

**Q: Why put settings in YAML instead of constants?** A: Thresholds, dates and state lists change between experiments
and smoke runs; keeping them in one file means no code edits and an auditable record of what was run.
