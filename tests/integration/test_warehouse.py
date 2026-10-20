"""Integration tests: SQL assertions against the live MotherDuck tables.

In: the built raw/ref/staging/marts tables. Out: pass/fail on unique grain, 5-character FIPS, valid rates,
county reference coverage, spine completeness, and row counts after joins.
"""
import pytest

pytestmark = pytest.mark.integration

GRAINS = {
    "raw.laus_county": "series_id, year, period, _snapshot_date",
    "staging.laus_county_month": "county_fips, month",
    "ref.state": "state_fips",
    "ref.county": "county_fips",
    "ref.month_spine": "county_fips, month",
    "ref.geography_gap_report": "county_fips",
}


@pytest.mark.parametrize("table", list(GRAINS))
def test_unique_grain(con, table):
    dupes = con.execute(f"SELECT count(*) FROM (SELECT {GRAINS[table]} FROM {table} "
                        f"GROUP BY ALL HAVING count(*) > 1)").fetchone()[0]
    assert dupes == 0


def test_laus_values_valid(con):
    bad = con.execute("""SELECT count(*) FROM staging.laus_county_month
        WHERE length(county_fips) <> 5 OR unemployment_rate NOT BETWEEN 0 AND 100
           OR labor_force < employed""").fetchone()[0]
    assert bad == 0
    # the only missing rates are the October 2025 shutdown month (BLS footnote X)
    other_nulls = con.execute("""SELECT count(*) FROM staging.laus_county_month
        WHERE unemployment_rate IS NULL AND month <> DATE '2025-10-01'""").fetchone()[0]
    assert other_nulls == 0


def test_county_reference(con):
    assert con.execute("SELECT count(DISTINCT state_fips) FROM ref.county").fetchone()[0] == 51
    # every county in the LAUS data must exist in the reference table
    orphans = con.execute("""SELECT count(*) FROM staging.laus_county_month
        WHERE county_fips NOT IN (SELECT county_fips FROM ref.county)""").fetchone()[0]
    assert orphans == 0


def test_month_spine_has_no_gaps(con):
    counties, months, rows, span = con.execute("""SELECT count(DISTINCT county_fips), count(DISTINCT month), count(*),
        date_diff('month', min(month), max(month)) + 1 FROM ref.month_spine""").fetchone()
    assert months == span                      # consecutive months
    assert rows == counties * months           # every county has every month
    report_rows = con.execute("SELECT count(*) FROM ref.geography_gap_report").fetchone()[0]
    assert report_rows == counties             # every county gets a gap-report row
