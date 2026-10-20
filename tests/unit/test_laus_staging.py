import datetime as dt

from src.utils.sql import pad_fips, render
from tests.helpers import insert_raw_laus, mem_db


def sid(fips, measure):
    return f"LAUCN{fips}00000000{measure}"


def test_pad_fips():
    assert pad_fips("1001") == "01001"
    assert pad_fips(" 54001 ") == "54001"
    assert pad_fips(6037) == "06037"
    assert pad_fips("1", 2) == "01"


def test_staging_parsing_m13_missing_and_lag():
    con = mem_db()
    rows = []
    for m, v in (("03", "6.4"), ("04", "100"), ("05", "900"), ("06", "1000")):
        rows += [(sid("54001", m), "2015", "M01", v, None),
                 (sid("54001", m), "2015", "M13", v, None),
                 (sid("54001", m), "2025", "M10", "-", "X"),
                 (sid("54001", m), "2026", "M08", v, "P"),
                 (sid("54001", m), "2011", "M01", v, None)]  # below laus_min_year
    insert_raw_laus(con, rows)
    con.execute(render("staging/laus_county_month.sql"))
    out = con.execute("""SELECT county_fips, state_fips, month, unemployment_rate, unemployed, employment,
                                labor_force, footnote, is_missing_marker, is_preliminary, available_date
                         FROM staging.laus_county_month ORDER BY month""").fetchall()
    assert len(out) == 3  # M13 and pre-2012 excluded
    assert out[0] == ("54001", "54", dt.date(2015, 1, 1), 6.4, 100, 900, 1000, None, False, False, dt.date(2015, 3, 1))
    assert out[1][2] == dt.date(2025, 10, 1) and out[1][3] is None and out[1][7] == "X" and out[1][8] is True
    assert out[2][9] is True and out[2][10] == dt.date(2026, 10, 1)


def test_staging_uses_latest_snapshot():
    con = mem_db()
    insert_raw_laus(con, [(sid("01001", "03"), "2015", "M01", "5.0", None)], snapshot="2026-09-20")
    insert_raw_laus(con, [(sid("01001", "03"), "2015", "M01", "5.2", None)], snapshot="2026-10-20")
    con.execute(render("staging/laus_county_month.sql"))
    assert con.execute("SELECT unemployment_rate FROM staging.laus_county_month").fetchall() == [(5.2,)]
