"""BLS LAUS county ingest -> raw.laus_county.

Verified layout (la.txt + la.series, 2026-10-20): county series IDs are
LA + U (not seasonally adjusted) + area_code CN<5-digit FIPS>00000000 + 2-digit measure code
(e.g. LAUCN540010000000003). Measures: 03 rate, 04 unemployment, 05 employment, 06 labor force.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import time

from prefect import flow, get_run_logger, task

from src.utils.config import load_config, resolve_states
from src.utils.db import connect, log_db_size
from src.utils.http import bls_headers, download_to_temp, get_with_retry
from src.utils.states import state_fips_for

BLS_BASE = "https://download.bls.gov/pub/time.series/la/"
COUNTY_FILE = "la.data.64.County"
MEASURES = ("03", "04", "05", "06")

RAW_DDL = """
CREATE TABLE IF NOT EXISTS raw.laus_county (
    series_id VARCHAR, year VARCHAR, period VARCHAR, value VARCHAR, footnote_codes VARCHAR,
    _ingested_at TIMESTAMP, _snapshot_date DATE, _source VARCHAR
)"""


def _logger():
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


def filter_sql(src: str, state_fips: list[str], min_year: int) -> str:
    """SELECT over a LAUS county data file (tab-delimited, padded, CRLF) keeping only needed rows.

    Keeps county series (LAUCN) for the requested states, measures 03-06, year >= min_year.
    Values stay text in raw; whitespace is trimmed.
    """
    states = ", ".join(f"'{s}'" for s in state_fips)
    measures = ", ".join(f"'{m}'" for m in MEASURES)
    return f"""
    SELECT trim(series_id) AS series_id, trim(year) AS year, trim(period) AS period,
           trim(value) AS value, nullif(trim(footnote_codes), '') AS footnote_codes
    FROM read_csv('{src}', delim='\t', header=true, all_varchar=true, quote='',
                  names=['series_id','year','period','value','footnote_codes'],
                  null_padding=true, strict_mode=false)
    WHERE starts_with(trim(series_id), 'LAUCN')
      AND substr(trim(series_id), 6, 2) IN ({states})
      AND right(trim(series_id), 2) IN ({measures})
      AND try_cast(trim(year) AS INTEGER) >= {int(min_year)}
    """


@task(retries=3, retry_delay_seconds=[10, 30, 90])
def download_laus_file(name: str) -> str:
    log = _logger()
    t0 = time.time()
    path = download_to_temp(BLS_BASE + name, headers=bls_headers())
    log.info("Downloaded %s (%.1f MB) in %.0fs", name, os.path.getsize(path) / 1e6, time.time() - t0)
    return path


@task
def load_raw_laus(path: str, states: list[str] | None, snapshot_date: dt.date) -> int:
    log = _logger()
    cfg = load_config()
    fips = state_fips_for(states)
    con = connect()
    try:
        con.execute(RAW_DDL)
        con.execute(f"CREATE OR REPLACE TEMP TABLE _laus_load AS {filter_sql(path, fips, cfg['data']['laus_min_year'])}")
        n = con.execute("SELECT count(*) FROM _laus_load").fetchone()[0]
        if n == 0:
            raise RuntimeError("LAUS load produced 0 rows; refusing to replace snapshot")
        states_sql = ", ".join(f"'{s}'" for s in fips)
        con.execute("BEGIN")
        # idempotent: same-day rerun replaces that day's snapshot for the loaded states
        con.execute(f"DELETE FROM raw.laus_county WHERE _snapshot_date = ? AND substr(series_id, 6, 2) IN ({states_sql})",
                    [snapshot_date])
        con.execute("""INSERT INTO raw.laus_county
                       SELECT *, now()::TIMESTAMP, ?::DATE, ? FROM _laus_load""",
                    [snapshot_date, BLS_BASE + COUNTY_FILE])
        con.execute("COMMIT")
        prune_snapshots(con, "raw.laus_county", cfg["storage"]["raw_snapshot_retention"])
        log.info("raw.laus_county: inserted %d rows for snapshot %s (%d states)", n, snapshot_date, len(fips))
        log_db_size(con, log)
        return n
    finally:
        con.close()


def prune_snapshots(con, table: str, keep: int) -> None:
    con.execute(f"""DELETE FROM {table} WHERE _snapshot_date NOT IN (
                      SELECT DISTINCT _snapshot_date FROM {table} ORDER BY _snapshot_date DESC LIMIT {int(keep)})""")


@flow(name="ingest_laus")
def ingest_laus(states: list[str] | str | None = None, snapshot_date: str | None = None) -> int:
    st = resolve_states(states)
    snap = dt.date.fromisoformat(snapshot_date) if snapshot_date else dt.date.today()
    path = download_laus_file(COUNTY_FILE)
    try:
        return load_raw_laus(path, st, snap)
    finally:
        os.remove(path)


@task(retries=3, retry_delay_seconds=[5, 15, 45])
def fetch_text(name: str) -> str:
    return get_with_retry(BLS_BASE + name, headers=bls_headers()).text


if __name__ == "__main__":
    import sys
    ingest_laus(sys.argv[1] if len(sys.argv) > 1 else None)
