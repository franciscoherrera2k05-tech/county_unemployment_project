-- ref.geography_gap_report: does each county have a complete series? Grain: one row per county_fips.
-- In:  ref.month_spine and staging.laus_county_month. Out: months expected vs. with data, first/last month,
--      average labor force, and a status. Counties with status <> 'ok' are excluded from the panel.
-- National gap months (fewer than half of counties published, i.e. October 2025 shutdown) are ignored here,
-- because they are missing for every county and are not a geography problem.
CREATE OR REPLACE TABLE ref.geography_gap_report AS
WITH national_gap_months AS (
    SELECT month
    FROM staging.laus_county_month
    GROUP BY month
    HAVING count(unemployment_rate) < 0.5 * (SELECT count(*) FROM ref.county)
),
coverage AS (
    SELECT s.county_fips, s.month, l.unemployment_rate, l.labor_force
    -- LEFT JOIN grain: spine = 1 row per county x month; staging = 1 row per county x month (unique).
    -- Key (county_fips, month) -> result is still 1 row per county x month; missing data shows up as NULL.
    FROM ref.month_spine s
    LEFT JOIN staging.laus_county_month l ON l.county_fips = s.county_fips AND l.month = s.month
    WHERE s.month NOT IN (SELECT month FROM national_gap_months)
),
per_county AS (
    SELECT
        county_fips,
        count(*) AS months_expected,
        count(unemployment_rate) AS months_with_data,
        min(month) FILTER (WHERE unemployment_rate IS NOT NULL) AS first_month,
        max(month) FILTER (WHERE unemployment_rate IS NOT NULL) AS last_month,
        avg(labor_force) AS avg_labor_force
    FROM coverage
    GROUP BY county_fips
)
SELECT
    *,
    CASE
        WHEN months_with_data = months_expected THEN 'ok'
        WHEN months_with_data = 0 THEN 'no data in window'
        WHEN first_month > (SELECT min(month) FROM ref.month_spine) THEN 'appears late'
        WHEN last_month < (SELECT max(month) FROM ref.month_spine) THEN 'disappears early'
        ELSE 'internal gaps'
    END AS status
FROM per_county;
