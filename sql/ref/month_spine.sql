-- ref.month_spine: every county paired with every month, with no gaps. Grain: one row per county_fips x month.
-- In:  ref.county and staging.laus_county_month. Out: (county_fips, month) from Jan {laus_min_year} to the latest month.
-- The latest month is the newest month that at least half of all counties have published; a handful of
-- counties (e.g. Los Angeles) publish one month early and should not extend the spine.
CREATE OR REPLACE TABLE ref.month_spine AS
WITH last_month AS (
    SELECT max(month) AS month
    FROM (
        SELECT month
        FROM staging.laus_county_month
        GROUP BY month
        HAVING count(unemployment_rate) >= 0.5 * (SELECT count(*) FROM ref.county)
    )
),
months AS (
    SELECT CAST(generate_series AS DATE) AS month
    FROM generate_series(make_date({laus_min_year}, 1, 1), (SELECT month FROM last_month), INTERVAL 1 MONTH)
)
SELECT c.county_fips, m.month
-- CROSS JOIN grain: ref.county = 1 row per county; months = 1 row per month -> 1 row per county x month.
FROM ref.county c
CROSS JOIN months m;
