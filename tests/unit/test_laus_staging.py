"""Unit test for sql/staging/laus_county_month.sql on a tiny synthetic raw table (in-memory DuckDB).

In: three fake raw rows for one county. Out: pass/fail on FIPS padding, month parsing, M13 exclusion,
the '-' missing marker, and available_date = month + release lag.
"""
import datetime as dt

import duckdb

from src.utils.helpers import load_config, run_sql_file


def test_laus_staging_parsing():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw; CREATE SCHEMA staging;")
    con.execute("""CREATE TABLE raw.laus_county AS SELECT * FROM (VALUES
        ('LAUCN010010000000003', '2020', 'M04', '12.5', NULL, now()::TIMESTAMP, DATE '2026-01-01', 'x'),
        ('LAUCN010010000000003', '2020', 'M13', '5.0',  NULL, now()::TIMESTAMP, DATE '2026-01-01', 'x'),
        ('LAUCN010010000000003', '2025', 'M10', '-',    'X',  now()::TIMESTAMP, DATE '2026-01-01', 'x')
    ) t(series_id, year, period, value, footnote_codes, _ingested_at, _snapshot_date, _source)""")
    cfg = load_config()
    run_sql_file(con, "staging/laus_county_month.sql", cfg)
    rows = con.execute("SELECT county_fips, state_fips, month, unemployment_rate, available_date "
                       "FROM staging.laus_county_month ORDER BY month").fetchall()
    lag = cfg["timing"]["laus_release_lag_months"]
    assert len(rows) == 2                                   # M13 dropped
    assert rows[0][:4] == ("01001", "01", dt.date(2020, 4, 1), 12.5)
    assert rows[0][4] == dt.date(2020, 4 + lag, 1)
    assert rows[1][3] is None                               # '-' became NULL
