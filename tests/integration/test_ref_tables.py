import pytest

pytestmark = pytest.mark.integration


def test_ref_county_grain_and_fips(md):
    n, n_distinct, bad, bad_state = md.execute("""SELECT count(*), count(DISTINCT county_fips),
        count(*) FILTER (WHERE NOT regexp_matches(county_fips, '^[0-9]{5}$')),
        count(*) FILTER (WHERE state_fips <> substr(county_fips, 1, 2) OR state_abbr IS NULL)
        FROM ref.county""").fetchone()
    assert n == n_distinct and n > 3000
    assert bad == 0 and bad_state == 0
    assert md.execute("SELECT count(DISTINCT state_fips) FROM ref.county").fetchone()[0] == 51


def test_staging_counties_in_ref(md):
    assert md.execute("""SELECT count(*) FROM (SELECT DISTINCT county_fips FROM staging.laus_county_month)
                         ANTI JOIN ref.county USING (county_fips)""").fetchone()[0] == 0


def test_month_spine_grain_and_no_gaps(md):
    dup = md.execute("""SELECT count(*) FROM (SELECT county_fips, month FROM ref.month_spine
                        GROUP BY ALL HAVING count(*) > 1)""").fetchone()[0]
    assert dup == 0
    # every county has every month between global min and max (no gaps)
    n_months, n_counties, n_rows, n_gaps = md.execute("""
        WITH b AS (SELECT min(month) s, max(month) e FROM ref.month_spine)
        SELECT date_diff('month', s, e) + 1, (SELECT count(DISTINCT county_fips) FROM ref.month_spine),
               (SELECT count(*) FROM ref.month_spine),
               (SELECT count(*) FROM (SELECT county_fips, count(*) n FROM ref.month_spine GROUP BY 1)
                 WHERE n <> date_diff('month', s, e) + 1)
        FROM b""").fetchone()
    assert n_rows == n_months * n_counties and n_gaps == 0


def test_gap_report_covers_all_and_exclusions_consistent(md):
    n_ref = md.execute("SELECT count(*) FROM ref.county").fetchone()[0]
    n_rep, n_dup = md.execute("SELECT count(*), count(*) - count(DISTINCT county_fips) FROM ref.geography_gap_report").fetchone()
    assert n_rep >= n_ref and n_dup == 0
    n_exc = md.execute("SELECT count(*) FROM ref.county_exclusions").fetchone()[0]
    n_bad = md.execute("SELECT count(*) FROM ref.geography_gap_report WHERE status <> 'ok'").fetchone()[0]
    assert n_exc == n_bad
    # every non-excluded county has a non-null rate in every non-national-gap spine month
    missing = md.execute("""SELECT count(*) FROM ref.month_spine s
        LEFT JOIN staging.laus_county_month l USING (county_fips, month)
        WHERE NOT s.is_national_gap_month AND l.unemployment_rate IS NULL
          AND s.county_fips NOT IN (SELECT county_fips FROM ref.county_exclusions)""").fetchone()[0]
    assert missing == 0
