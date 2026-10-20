"""Prefect flow `ingest_fips`: BLS area list -> raw.laus_area, plus the static state table -> ref.state.

What it does: downloads la.area (the list of every LAUS area code and name, ~0.4 MB) to a temp file and loads it into
raw.laus_area with a same-day snapshot replacement (idempotent). It also writes ref.state, the 51-row table of state
FIPS codes and abbreviations (50 states + DC) kept in src/utils/helpers.py.
In: nothing but the network. Out: raw.laus_area (text columns + _snapshot_date) and ref.state (state_fips, state_abbr).
The county reference table itself is built in SQL (sql/ref/county.sql).
"""
import datetime as dt
import os

from prefect import flow, task

from src.utils.helpers import STATE_FIPS, bls_headers, connect, download_to_temp

URL = "https://download.bls.gov/pub/time.series/la/la.area"


@task(retries=3, retry_delay_seconds=30)
def load_area_file(snapshot_date):
    path = download_to_temp(URL, headers=bls_headers())
    try:
        con = connect()
        con.execute("""CREATE TABLE IF NOT EXISTS raw.laus_area (
            area_type_code VARCHAR, area_code VARCHAR, area_text VARCHAR, _snapshot_date DATE)""")
        con.execute("BEGIN")
        con.execute("DELETE FROM raw.laus_area WHERE _snapshot_date = ?", [snapshot_date])
        # la.area is tab-delimited with a header; we keep the three columns we need
        con.execute(f"""INSERT INTO raw.laus_area
            SELECT trim(area_type_code), trim(area_code), trim(area_text), ?::DATE
            FROM read_csv('{path}', delim='\t', header=true, all_varchar=true, quote='')""", [snapshot_date])
        con.execute("COMMIT")
        row_count = con.execute("SELECT count(*) FROM raw.laus_area WHERE _snapshot_date = ?",
                                [snapshot_date]).fetchone()[0]
        con.close()
    finally:
        os.remove(path)
    print(f"raw.laus_area: loaded {row_count:,} rows for snapshot {snapshot_date}")
    return row_count


@task
def write_state_table():
    values = ", ".join(f"('{fips}', '{abbr}')" for fips, abbr in sorted(STATE_FIPS.items()))
    con = connect()
    con.execute(f"CREATE OR REPLACE TABLE ref.state AS SELECT * FROM (VALUES {values}) t(state_fips, state_abbr)")
    con.close()


@flow(name="ingest_fips")
def ingest_fips():
    write_state_table()
    return load_area_file(dt.date.today())


if __name__ == "__main__":
    ingest_fips()
