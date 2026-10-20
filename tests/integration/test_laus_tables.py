import pytest

pytestmark = pytest.mark.integration


def test_raw_laus_grain(md):
    assert md.execute("""SELECT count(*) FROM (SELECT series_id, year, period, _snapshot_date
                         FROM raw.laus_county GROUP BY ALL HAVING count(*) > 1)""").fetchone()[0] == 0


def test_staging_laus_grain(md):
    assert md.execute("""SELECT count(*) FROM (SELECT county_fips, month FROM staging.laus_county_month
                         GROUP BY ALL HAVING count(*) > 1)""").fetchone()[0] == 0


def test_staging_laus_values(md):
    bad = md.execute("""SELECT
        count(*) FILTER (WHERE length(county_fips) <> 5 OR NOT regexp_matches(county_fips, '^[0-9]{5}$')),
        count(*) FILTER (WHERE unemployment_rate < 0 OR unemployment_rate > 100),
        count(*) FILTER (WHERE day(month) <> 1),
        count(*) FILTER (WHERE available_date <= month),
        count(*) FILTER (WHERE unemployment_rate IS NULL AND NOT is_missing_marker),
        count(*) FILTER (WHERE labor_force < employment)
        FROM staging.laus_county_month""").fetchone()
    assert bad == (0, 0, 0, 0, 0, 0)


def test_staging_missing_only_documented_months(md):
    # the only documented missing month is Oct 2025 (federal shutdown, BLS footnote G)
    months = md.execute("""SELECT DISTINCT month FROM staging.laus_county_month
                           WHERE unemployment_rate IS NULL""").fetchall()
    assert {str(m[0]) for m in months} <= {"2025-10-01"}
