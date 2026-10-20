"""Prefect flow `build_staging`: rebuild the staging and reference tables from raw, in order.

What it does: runs each SQL file below, one after another, against MotherDuck. Every file is a single
CREATE OR REPLACE TABLE statement, so rerunning gives the same result (idempotent).
In: raw.laus_county, raw.laus_area, ref.state, raw.fred_observations. Out: the staging.* and ref.* tables listed.
"""
from prefect import flow, task

from src.utils.helpers import connect, load_config, run_sql_file

# Order matters: each file reads tables built by the files above it.
SQL_FILES = [
    "staging/laus_county_month.sql",
    "ref/county.sql",
    "ref/month_spine.sql",
    "ref/geography_gap_report.sql",
    "staging/fred_monthly.sql",
    "staging/fred_by_available_month.sql",
]


@task
def run_file(con, path, cfg):
    run_sql_file(con, path, cfg)
    table = path.replace("/", ".").removesuffix(".sql")   # 'ref/county.sql' -> 'ref.county'
    row_count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    print(f"{table}: {row_count:,} rows")


@flow(name="build_staging")
def build_staging():
    cfg = load_config()
    con = connect()
    for path in SQL_FILES:
        run_file(con, path, cfg)
    con.close()


if __name__ == "__main__":
    build_staging()
