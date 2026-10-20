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
    "raw.fred_observations": "series_id, date, _snapshot_date",
    "staging.fred_monthly": "series_id, ref_month",
    "staging.fred_by_available_month": "available_month",
    "marts.county_month_panel": "county_fips, month",
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


def test_fred_months_are_consecutive(con):
    # LAG(value, 3) means "3 months earlier" only if each series has no missing months
    broken = con.execute("""SELECT series_id FROM staging.fred_monthly GROUP BY series_id
        HAVING count(*) <> date_diff('month', min(ref_month), max(ref_month)) + 1""").fetchall()
    assert broken == []
    assert con.execute("SELECT count(DISTINCT series_id) FROM staging.fred_monthly").fetchone()[0] == 7


def test_panel_joins_keep_row_counts(con):
    # join 1: staging x ref.county must not add or drop rows (every staging county is in ref.county exactly once)
    staging_rows = con.execute("SELECT count(*) FROM staging.laus_county_month").fetchone()[0]
    joined_rows = con.execute("""SELECT count(*) FROM staging.laus_county_month l
        JOIN ref.county c ON c.county_fips = l.county_fips""").fetchone()[0]
    assert joined_rows == staging_rows
    # join 2: panel x FRED on decision_date = available_month must not duplicate rows
    panel_rows = con.execute("SELECT count(*) FROM marts.county_month_panel").fetchone()[0]
    with_fred = con.execute("""SELECT count(*) FROM marts.county_month_panel p
        LEFT JOIN staging.fred_by_available_month f ON f.available_month = p.decision_date""").fetchone()[0]
    assert with_fred == panel_rows
    # overall: panel = included counties x panel months (no row lost or invented)
    counties, months = con.execute("SELECT count(DISTINCT county_fips), count(DISTINCT month) "
                                   "FROM marts.county_month_panel").fetchone()
    assert panel_rows == counties * months
    included = con.execute("SELECT count(*) FROM ref.geography_gap_report WHERE status = 'ok'").fetchone()[0]
    assert counties == included


def test_panel_counties_in_reference(con):
    orphans = con.execute("""SELECT count(*) FROM marts.county_month_panel
        WHERE county_fips NOT IN (SELECT county_fips FROM ref.county)""").fetchone()[0]
    assert orphans == 0


def test_leakage_dates(con):
    # simplest leakage check: no row feeding a feature is dated at or after the decision (prediction) month
    bad = con.execute("""SELECT count(*) FROM marts.county_month_panel
        WHERE month >= decision_date OR fred_latest_ref_month >= decision_date
           OR fred_latest_ref_month IS NULL""").fetchone()[0]
    assert bad == 0


def test_no_unexplained_nulls(con):
    # NULLs are expected only near the October 2025 shutdown month (no BLS/FRED data) and in the last 3 months'
    # target. Outside those documented windows every column must be filled.
    columns = [r[0] for r in con.execute("DESCRIBE marts.county_month_panel").fetchall()]
    null_checks = " OR ".join(f'"{c}" IS NULL' for c in columns)
    bad = con.execute(f"""SELECT count(*) FROM marts.county_month_panel
        WHERE month NOT BETWEEN DATE '2025-07-01' AND DATE '2026-10-01'
          AND month <= (SELECT max(month) - INTERVAL 3 MONTH FROM marts.county_month_panel)
          AND ({null_checks})""").fetchone()[0]
    assert bad == 0
