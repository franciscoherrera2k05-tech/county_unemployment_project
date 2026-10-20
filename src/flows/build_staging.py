"""build_staging flow: raw -> staging tables (CREATE OR REPLACE, single statements)."""
from __future__ import annotations

import logging

from prefect import flow, get_run_logger, task

from src.features.ref_tables import build_ref_derived
from src.utils.db import connect
from src.utils.sql import render

STAGING = ["staging/laus_county_month.sql"]


def _logger():
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


def run_sql_files(con, files: list[str], logger=None) -> dict[str, int]:
    logger = logger or _logger()
    counts = {}
    for f in files:
        con.execute(render(f))
        table = f.split("/")[0] + "." + f.split("/")[1].removesuffix(".sql")
        counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        logger.info("%s: %d rows", table, counts[table])
    return counts


@task
def build_staging_tables(files: list[str]) -> dict[str, int]:
    con = connect()
    try:
        counts = run_sql_files(con, files)
        counts.update(build_ref_derived(con, _logger()))
        return counts
    finally:
        con.close()


@flow(name="build_staging")
def build_staging(files: list[str] | None = None) -> dict[str, int]:
    return build_staging_tables(files or STAGING)


if __name__ == "__main__":
    build_staging()
