"""MotherDuck connection helpers. Only the configured database is ever touched."""
from __future__ import annotations

import logging

import duckdb

from src.utils.config import env, load_config

log = logging.getLogger(__name__)
SCHEMAS = ("raw", "ref", "staging", "marts")


def connect(create: bool = True) -> duckdb.DuckDBPyConnection:
    env("MOTHERDUCK_TOKEN")  # duckdb reads it from the environment; never logged
    db = load_config()["project"]["database"]
    con = duckdb.connect("md:")
    if create:
        con.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    con.execute(f"USE {db}")
    for s in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    return con


def db_size(con: duckdb.DuckDBPyConnection) -> dict:
    db = load_config()["project"]["database"]
    rows = con.execute("PRAGMA database_size").fetchall()
    cols = [d[0] for d in con.description]
    for r in rows:
        rec = dict(zip(cols, r))
        if rec.get("database_name") == db:
            return rec
    return {}


def log_db_size(con: duckdb.DuckDBPyConnection, logger=None) -> dict:
    logger = logger or log
    rec = db_size(con)
    warn_gb = load_config()["storage"]["warn_db_size_gb"]
    size = str(rec.get("database_size", "unknown"))
    logger.info("MotherDuck database size: %s", size)
    gb = _parse_size_gb(size)
    if gb is not None and gb > warn_gb:
        logger.warning("Database size %.2f GB exceeds warn threshold %s GB", gb, warn_gb)
    return rec


def _parse_size_gb(size: str) -> float | None:
    units = {"bytes": 1e-9, "KiB": 2**10 / 1e9, "MiB": 2**20 / 1e9, "GiB": 2**30 / 1e9, "TiB": 2**40 / 1e9,
             "KB": 1e-6, "MB": 1e-3, "GB": 1.0, "TB": 1e3}
    try:
        num, unit = size.split()
        return float(num) * units[unit]
    except Exception:
        return None
