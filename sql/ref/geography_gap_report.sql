-- ref.geography_gap_report. Grain: county_fips (union of ref.county and staging counties).
-- A county is 'ok' only if it has a non-null rate in every spine month that is not a national gap month.
WITH spine_bounds AS (SELECT min(month) AS s, max(month) AS e FROM ref.month_spine),
all_counties AS (
    SELECT county_fips FROM ref.county
    UNION SELECT DISTINCT county_fips FROM staging.laus_county_month
),
nat_gap AS (SELECT DISTINCT month FROM ref.month_spine WHERE is_national_gap_month),
spine_months AS (
    SELECT CAST(generate_series AS DATE) AS month FROM spine_bounds, generate_series(s, e, INTERVAL 1 MONTH)
),
obs AS (
    SELECT l.county_fips, l.month, l.unemployment_rate, l.labor_force
    FROM staging.laus_county_month l, spine_bounds b
    WHERE l.month BETWEEN b.s AND b.e AND l.unemployment_rate IS NOT NULL
),
per AS (
    SELECT a.county_fips,
           min(o.month) AS first_month, max(o.month) AS last_month,
           count(o.month) AS n_months_present,
           avg(o.labor_force) AS avg_labor_force
    FROM all_counties a LEFT JOIN obs o USING (county_fips)
    GROUP BY a.county_fips
),
expected AS (
    SELECT count(*) AS n_expected FROM spine_months WHERE month NOT IN (SELECT month FROM nat_gap)
)
SELECT p.county_fips,
       c.county_name, c.state_abbr,
       c.county_fips IS NOT NULL AS in_ref_county,
       p.first_month, p.last_month, p.n_months_present, e.n_expected,
       e.n_expected - p.n_months_present AS n_months_missing,
       p.avg_labor_force,
       CASE
         WHEN c.county_fips IS NULL THEN 'not_in_ref_county'
         WHEN p.n_months_present = 0 THEN 'no_data_in_window'
         WHEN p.first_month > (SELECT s FROM spine_bounds) THEN 'appears_late'
         WHEN p.last_month < (SELECT e FROM spine_bounds) THEN 'disappears_early'
         WHEN p.n_months_present < e.n_expected THEN 'internal_gap'
         ELSE 'ok'
       END AS status
FROM per p CROSS JOIN expected e LEFT JOIN ref.county c USING (county_fips)
