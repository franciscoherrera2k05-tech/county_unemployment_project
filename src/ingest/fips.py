"""ingest_fips flow: build ref.county from BLS LAUS la.area / la.area_type (no Census dependency)."""
from __future__ import annotations

import logging
import os

from prefect import flow, get_run_logger, task

from src.ingest.laus import BLS_BASE
from src.utils.config import resolve_states
from src.utils.db import connect
from src.utils.http import bls_headers, download_to_temp, get_with_retry
from src.utils.sql import SQL_DIR
from src.utils.states import state_fips_for, values_sql

DDL = """CREATE TABLE IF NOT EXISTS ref.county (
    county_fips VARCHAR PRIMARY KEY, state_fips VARCHAR, state_abbr VARCHAR, county_name VARCHAR,
    area_code VARCHAR, area_text VARCHAR, _loaded_at TIMESTAMP)"""


def _logger():
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


def county_type_code(area_type_text: str) -> str:
    """Find the area_type_code for 'Counties and equivalents' in la.area_type (verified, not assumed)."""
    for line in area_type_text.splitlines()[1:]:
        parts = line.strip("\r").split("\t")
        if len(parts) >= 2 and parts[1].strip().lower() == "counties and equivalents":
            return parts[0].strip()
    raise RuntimeError("Could not find 'Counties and equivalents' in la.area_type")


def county_select_sql(src: str, state_fips: list[str], county_type: str) -> str:
    return (SQL_DIR / "ref" / "county.sql").read_text().format(
        src=src, state_values=values_sql(), county_type=county_type,
        state_list=", ".join(f"'{s}'" for s in state_fips))


@task(retries=3, retry_delay_seconds=[5, 15, 45])
def fetch_area_type() -> str:
    return get_with_retry(BLS_BASE + "la.area_type", headers=bls_headers()).text


@task(retries=3, retry_delay_seconds=[5, 15, 45])
def download_area() -> str:
    return download_to_temp(BLS_BASE + "la.area", headers=bls_headers())


@task
def load_ref_county(path: str, states: list[str] | None, county_type: str) -> int:
    log = _logger()
    fips = state_fips_for(states)
    con = connect()
    try:
        con.execute(DDL)
        con.execute(f"CREATE OR REPLACE TEMP TABLE _county AS {county_select_sql(path, fips, county_type)}")
        n = con.execute("SELECT count(*) FROM _county").fetchone()[0]
        if n == 0:
            raise RuntimeError("ref.county load produced 0 rows")
        states_sql = ", ".join(f"'{s}'" for s in fips)
        con.execute("BEGIN")
        con.execute(f"DELETE FROM ref.county WHERE state_fips IN ({states_sql})")
        con.execute("INSERT INTO ref.county SELECT * FROM _county")
        con.execute("COMMIT")
        log.info("ref.county: %d counties loaded for %d states", n, len(fips))
        return n
    finally:
        con.close()


@flow(name="ingest_fips")
def ingest_fips(states: list[str] | str | None = None) -> int:
    st = resolve_states(states)
    code = county_type_code(fetch_area_type())
    path = download_area()
    try:
        return load_ref_county(path, st, code)
    finally:
        os.remove(path)


if __name__ == "__main__":
    import sys
    ingest_fips(sys.argv[1] if len(sys.argv) > 1 else None)
