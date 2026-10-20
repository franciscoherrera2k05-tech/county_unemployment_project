"""Shared helpers for unit tests: local in-memory DuckDB with project schemas."""
import duckdb

from src.ingest.laus import RAW_DDL


def mem_db():
    con = duckdb.connect()
    for s in ("raw", "ref", "staging", "marts"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    con.execute(RAW_DDL)
    return con


def insert_raw_laus(con, rows, snapshot="2026-10-20"):
    """rows: (series_id, year, period, value, footnote)."""
    con.executemany(
        "INSERT INTO raw.laus_county VALUES (?, ?, ?, ?, ?, now()::TIMESTAMP, ?::DATE, 'test')",
        [(*r, snapshot) for r in rows],
    )
