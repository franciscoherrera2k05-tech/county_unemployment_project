"""Prefect flow `build_panel`: rebuild marts.county_month_panel from the staging and reference tables.

What it does: runs sql/marts/county_month_panel.sql (one CREATE OR REPLACE TABLE, so reruns are idempotent) and
prints the row count, county count and month range.
In: staging.laus_county_month, staging.fred_by_available_month, ref.county, ref.geography_gap_report, ref.month_spine.
Out: marts.county_month_panel (one row per county x month).
"""
from prefect import flow

from src.utils.helpers import connect, load_config, run_sql_file


@flow(name="build_panel")
def build_panel():
    con = connect()
    run_sql_file(con, "marts/county_month_panel.sql", load_config())
    rows, counties, first, last = con.execute("""SELECT count(*), count(DISTINCT county_fips), min(month), max(month)
        FROM marts.county_month_panel""").fetchone()
    con.close()
    print(f"marts.county_month_panel: {rows:,} rows, {counties:,} counties, {first} to {last}")
    return rows


if __name__ == "__main__":
    build_panel()
