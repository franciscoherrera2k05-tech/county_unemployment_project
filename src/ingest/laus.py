"""Prefect flow `ingest_laus`: BLS LAUS county file -> raw.laus_county.

What it does: streams la.data.64.County (~340 MB, tab-delimited) to a temp file, keeps only county series for the
chosen states, measures 03-06 and years >= laus_min_year, and loads them in one INSERT ... SELECT. The temp file is
always deleted. Idempotent: a same-day rerun deletes that day's snapshot first, so it replaces rather than duplicates.
In: config (states, laus_min_year). Out: rows in raw.laus_county (all text + _ingested_at, _snapshot_date, _source).

Series ID layout (verified in la.txt and la.series): LA + U (not seasonally adjusted) + CN + 5-digit county FIPS
+ 8 zeros + 2-digit measure code, e.g. LAUCN010010000000003 = Autauga County AL, unemployment rate.
"""
import datetime as dt
import os

from prefect import flow, task

from src.utils.helpers import bls_headers, connect, download_to_temp, load_config, state_fips_list

URL = "https://download.bls.gov/pub/time.series/la/la.data.64.County"

CREATE_RAW = """
CREATE TABLE IF NOT EXISTS raw.laus_county (
    series_id VARCHAR, year VARCHAR, period VARCHAR, value VARCHAR, footnote_codes VARCHAR,
    _ingested_at TIMESTAMP, _snapshot_date DATE, _source VARCHAR
)"""

# Read the temp file as text and keep only the rows we need. The file pads columns with spaces, hence trim().
SELECT_FROM_FILE = """
SELECT trim(series_id) AS series_id, trim(year) AS year, trim(period) AS period,
       trim(value) AS value, nullif(trim(footnote_codes), '') AS footnote_codes
FROM read_csv('{path}', delim='\t', header=true, all_varchar=true, quote='',
              names=['series_id', 'year', 'period', 'value', 'footnote_codes'])
WHERE trim(series_id) LIKE 'LAUCN%'                      -- county series only
  AND substr(trim(series_id), 6, 2) IN ({states})        -- characters 6-7 = state FIPS
  AND right(trim(series_id), 2) IN ('03', '04', '05', '06')  -- rate, unemployed, employed, labor force
  AND CAST(trim(year) AS INTEGER) >= {min_year}
"""


@task(retries=3, retry_delay_seconds=30)
def download_laus():
    return download_to_temp(URL, headers=bls_headers())


@task
def load_laus(path, states, snapshot_date):
    cfg = load_config()
    state_list = ", ".join(f"'{s}'" for s in state_fips_list(states))
    con = connect()
    con.execute(CREATE_RAW)
    con.execute("CREATE OR REPLACE TEMP TABLE laus_load AS "
                + SELECT_FROM_FILE.format(path=path, states=state_list, min_year=cfg["data"]["laus_min_year"]))
    row_count = con.execute("SELECT count(*) FROM laus_load").fetchone()[0]
    if row_count == 0:
        raise RuntimeError("LAUS file produced 0 rows; not replacing the snapshot")
    # Same-day rerun: replace today's snapshot instead of adding a duplicate copy.
    con.execute("BEGIN")
    con.execute("DELETE FROM raw.laus_county WHERE _snapshot_date = ?", [snapshot_date])
    con.execute("INSERT INTO raw.laus_county SELECT *, now()::TIMESTAMP, ?::DATE, ? FROM laus_load",
                [snapshot_date, URL])
    con.execute("COMMIT")
    con.close()
    print(f"raw.laus_county: loaded {row_count:,} rows for snapshot {snapshot_date}")
    return row_count


@flow(name="ingest_laus")
def ingest_laus(states=None):
    """states: None = use config; or a list like ['WV', 'VA', 'OH'] for a smoke run."""
    states = states or load_config()["data"]["states"]
    path = download_laus()
    try:
        return load_laus(path, states, dt.date.today())
    finally:
        os.remove(path)


if __name__ == "__main__":
    ingest_laus()
