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

**`src/ingest/fips.py`** — The `ingest_fips` flow. It downloads BLS's small area list (`la.area`: every area code and
name) into `raw.laus_area`, with the same same-day replacement rule as LAUS. It also writes `ref.state`, the 51 state FIPS
codes and abbreviations kept in code. The county table itself is built in SQL from these two inputs.

**`src/ingest/fred.py`** — The `ingest_fred` flow. It calls the FRED API once per series listed in the config, writes all
observations to one temp CSV, and loads it into `raw.fred_observations` with one insert, replacing any snapshot from the
same day. It starts one year before the LAUS history so year-over-year changes exist from the first month.

### Staging layer

**`sql/staging/laus_county_month.sql`** — Turns raw LAUS text into one clean row per county per month. It reads only
the latest snapshot, drops the annual average (`M13`), converts `'M04'` + `'2020'` into the date 2020-04-01, turns the
BLS missing marker `-` into NULL, and pivots the four measures into columns. It also adds `available_date` (month + 2):
the earliest month this number could have been known, which is what point-in-time correctness is built on.

**`tests/unit/test_laus_staging.py`** — Runs the real staging SQL on three fake rows in an in-memory DuckDB, so the
parsing rules (leading-zero FIPS, month dates, M13 dropped, `-` -> NULL, release lag) are tested without the warehouse.

**`tests/integration/test_warehouse.py`** — SQL assertions on the live tables: unique grain for every table, 5-character
FIPS, rates between 0 and 100, labor force >= employed, and NULL rates only in the October 2025 shutdown month.

**`sql/staging/fred_monthly.sql`** — Turns raw FRED observations into one value per series per month. Daily (T10Y2Y) and
weekly (ICSA) series are averaged over the month; the current month is dropped because it is not finished. It adds simple
changes versus 3, 6 and 12 months earlier, and `available_month` = reference month + a fixed per-series lag from the config
(e.g. September CPI counts as known in November). That one line is the whole FRED point-in-time rule.

**`sql/staging/fred_by_available_month.sql`** — Pivots FRED to one row per `available_month` with one column per feature
(UNRATE level and 3-month change, CPI year-over-year, and so on). Each row answers "what did we know about the national
economy in this month?". Because lags differ, one row mixes reference months (September UNRATE, August CPI), and
`fred_latest_ref_month` records the newest one for the leakage check.

### Reference layer

**`sql/ref/county.sql`** — Builds `ref.county`, the county dimension: one row per 5-digit county FIPS with its state and
name. It keeps only BLS area type `F` (counties and equivalents) and takes the FIPS from the area code. Joining to
`ref.state` adds the state abbreviation and drops Puerto Rico, which is outside the 50 states + DC scope.

**`sql/ref/month_spine.sql`** — Builds `ref.month_spine`: every county paired with every month from January 2012 to
the latest published month, with no gaps. It is the "expected" grid we compare the real data against, so missing
months become visible rows instead of silently absent ones. The end month is the newest month that most counties have
published, so three early-reporting counties don't stretch it.

**`sql/ref/geography_gap_report.sql`** — For each county, counts how many spine months actually have data, finds its
first and last month, and labels it `ok`, `appears late`, `disappears early`, `internal gaps`, or `no data in window`.
The October 2025 shutdown month is ignored because it is missing for everyone. Counties not `ok` (6 in Alaska, due to
census-area splits) are excluded from the panel, and the report quantifies what that costs.

**`src/flows/build_staging.py`** — The `build_staging` flow: runs the staging and reference SQL files in a fixed order and
prints each table's row count. Each file is one `CREATE OR REPLACE TABLE`, so the flow can be rerun safely.

## Joins

**ref.county: counties JOIN ref.state ON state_fips.** Left: one row per county area from BLS (3,225, incl. Puerto Rico).
Right: one row per state (51). Many counties to one state, so the result is still one row per county. Without it we'd have
no state abbreviation; as an inner join it also removes Puerto Rico on purpose.

**ref.month_spine: ref.county CROSS JOIN months.** Left: one row per county. Right: one row per month. Result: every
county x month combination. Without it, a county that silently stops reporting would just have fewer rows and we
wouldn't notice.

**geography gap report: month_spine LEFT JOIN staging.laus_county_month ON (county_fips, month).** Both sides are one row
per county x month, so the result is too. LEFT keeps spine rows with no data (they show up as NULL), which is exactly
what we want to count. An inner join would hide the gaps we're looking for.

**staging.fred_monthly: complete_months JOIN lags ON series_id.** Left: one row per series x month. Right: one row per
series (the lag table from config, 7 rows). Many-to-one, so still one row per series x month. Without it there would be no
available_month; as an inner join it also drops any series not listed in the config.

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

**Q: Why build a month spine instead of just looking at the data?** A: Missing data is invisible in the data itself.
The spine says what *should* exist, and a LEFT JOIN against it turns each missing month into a countable NULL.

**Q: Why exclude the Alaska counties rather than patch them?** A: Valdez-Cordova split into two new census areas in 2020,
so no single FIPS has a continuous series. Stitching them would invent a geography; excluding them costs 3 counties
(~0.006% of the labor force), which is documented in the gap report.

**Q: How do you stop FRED data from leaking the future?** A: Each series gets a fixed publication lag from the config, and
a value is only attached to a decision in the month it would have been published (`available_month = ref_month + lag`).
The lags are conservative (CPI and industrial production: 2 months, others: 1).

**Q: What doesn't the FRED rule handle?** A: Revisions. FRED serves today's revised numbers (e.g. payrolls get revised),
not what was first published. Using initial releases (ALFRED vintages) would fix that; it is listed as a limitation.
