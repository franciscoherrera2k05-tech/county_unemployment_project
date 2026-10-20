"""Derived ref tables: month spine, geography gap report, county exclusion list.

ref tables are rebuilt by DELETE + INSERT in one transaction (CREATE OR REPLACE is reserved for staging/marts).
"""
from __future__ import annotations

from src.utils.sql import render

REF_DERIVED = ["ref/month_spine.sql", "ref/geography_gap_report.sql"]


def replace_ref_table(con, table: str, select_sql: str) -> int:
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM ({select_sql}) LIMIT 0")
    con.execute("BEGIN")
    con.execute(f"DELETE FROM {table}")
    con.execute(f"INSERT INTO {table} {select_sql}")
    con.execute("COMMIT")
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def build_ref_derived(con, logger=None) -> dict[str, int]:
    counts = {}
    for f in REF_DERIVED:
        table = "ref." + f.split("/")[1].removesuffix(".sql")
        counts[table] = replace_ref_table(con, table, render(f))
    counts["ref.county_exclusions"] = replace_ref_table(
        con, "ref.county_exclusions",
        "SELECT county_fips, county_name, state_abbr, status AS reason, first_month, last_month, "
        "n_months_missing, avg_labor_force FROM ref.geography_gap_report WHERE status <> 'ok'")
    if logger:
        for t, n in counts.items():
            logger.info("%s: %d rows", t, n)
    return counts
