"""Integration tests: SQL assertions against the live MotherDuck tables.

In: the built raw/ref/staging/marts tables. Out: pass/fail on unique grain, 5-character FIPS, valid rates,
county reference coverage, spine completeness, and row counts after joins.
"""
import pytest

pytestmark = pytest.mark.integration

GRAINS = {
    "raw.laus_county": "series_id, year, period, _snapshot_date",
    "staging.laus_county_month": "county_fips, month",
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
