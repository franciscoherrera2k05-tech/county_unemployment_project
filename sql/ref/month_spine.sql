-- ref.month_spine. Grain: county_fips x month.
-- Every month from Jan {laus_min_year} to the latest LAUS month with data for >= 50% of counties
-- (a few large counties publish one month early; that month is not yet complete).
-- National gap months (< 50% coverage, e.g. 2025-10 shutdown) stay in the spine, flagged.
WITH cov AS (
    SELECT month, count(*) FILTER (WHERE unemployment_rate IS NOT NULL) AS n_ok
    FROM staging.laus_county_month GROUP BY month
),
tot AS (SELECT count(DISTINCT county_fips) AS n FROM staging.laus_county_month),
bounds AS (
    SELECT make_date({laus_min_year}, 1, 1) AS start_month,
           max(month) FILTER (WHERE n_ok >= 0.5 * (SELECT n FROM tot)) AS end_month
    FROM cov
),
months AS (
    SELECT CAST(generate_series AS DATE) AS month
    FROM bounds, generate_series(bounds.start_month, bounds.end_month, INTERVAL 1 MONTH)
),
counties AS (
    SELECT county_fips FROM ref.county
    WHERE county_fips IN (SELECT DISTINCT county_fips FROM staging.laus_county_month)
)
SELECT c.county_fips, m.month,
       coalesce(cov.n_ok < 0.5 * (SELECT n FROM tot), true) AS is_national_gap_month
FROM counties c CROSS JOIN months m LEFT JOIN cov USING (month)
