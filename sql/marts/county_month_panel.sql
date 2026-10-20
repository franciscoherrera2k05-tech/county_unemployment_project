-- marts.county_month_panel: the modeling table. Grain: one row per county_fips x month (t).
-- In:  staging.laus_county_month, ref.county, ref.geography_gap_report, ref.month_spine, staging.fred_by_available_month.
-- Out: LAUS history features (only months <= t), cross-county context at t, FRED features known by the decision date,
--      decision_date, and the target y_delta_3m / y_label (rate change from t to t+3). Rows from {panel_start} on.
--      The last 3 months have a NULL target and are kept for live scoring.
-- Only equality joins are used; all history comes from window functions over each county's own rows.
CREATE OR REPLACE TABLE marts.county_month_panel AS
WITH laus AS (
    SELECT l.county_fips, l.state_fips, c.state_abbr, c.county_name, l.month,
           l.unemployment_rate AS rate, l.unemployed, l.employed, l.labor_force
    -- JOIN grain: staging.laus_county_month = 1 row per county x month; ref.county = 1 row per county.
    -- Many-to-one on county_fips -> result is 1 row per county x month (row count unchanged; tested).
    FROM staging.laus_county_month l
    JOIN ref.county c ON c.county_fips = l.county_fips
    WHERE l.county_fips IN (SELECT county_fips FROM ref.geography_gap_report WHERE status = 'ok')  -- geography exclusions
      AND l.month <= (SELECT max(month) FROM ref.month_spine)   -- ignore the 3 counties that publish a month early
),
features AS (
    SELECT
        *,
        -- the month in which month-t county data is published and the prediction is made
        CAST(month + INTERVAL ({laus_lag}) MONTH AS DATE) AS decision_date,
        -- LAG(x, k) = the value k months earlier (each county has one row for every month)
        round(rate - LAG(rate, 1) OVER w, 1) AS rate_change_1m,
        round(rate - LAG(rate, 3) OVER w, 1) AS rate_change_3m,
        round(rate - LAG(rate, 6) OVER w, 1) AS rate_change_6m,
        round(rate - LAG(rate, 12) OVER w, 1) AS rate_change_12m,
        avg(rate) OVER w12 AS rate_mean_12m,                    -- trailing: months t-11..t
        stddev_samp(rate) OVER w12 AS rate_std_12m,
        round(rate - min(rate) OVER w36, 1) AS rate_minus_min_36m,       -- trailing: months t-35..t
        100 * (labor_force / nullif(LAG(labor_force, 3) OVER w, 0) - 1) AS labor_force_pct_change_3m,
        100 * (labor_force / nullif(LAG(labor_force, 12) OVER w, 0) - 1) AS labor_force_pct_change_12m,
        100 * (employed / nullif(LAG(employed, 3) OVER w, 0) - 1) AS employed_pct_change_3m,
        100 * (employed / nullif(LAG(employed, 12) OVER w, 0) - 1) AS employed_pct_change_12m,
        100 * (unemployed / nullif(LAG(unemployed, 3) OVER w, 0) - 1) AS unemployed_pct_change_3m,
        -- seasonality: month-of-year on a circle, so December and January are neighbours
        sin(2 * pi() * month(month) / 12) AS month_sin,
        cos(2 * pi() * month(month) / 12) AS month_cos,
        -- TARGET: rate {horizon} months ahead minus rate now, in percentage points, rounded to the published 1 decimal
        round(LEAD(rate, {horizon}) OVER w - rate, 1) AS y_delta_3m
    FROM laus
    WINDOW w AS (PARTITION BY county_fips ORDER BY month),
           w12 AS (PARTITION BY county_fips ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
           w36 AS (PARTITION BY county_fips ORDER BY month ROWS BETWEEN 35 PRECEDING AND CURRENT ROW)
),
with_context AS (
    SELECT
        *,
        -- cross-county context: the median 3-month change across all counties (and within the state) in the SAME month t
        median(rate_change_3m) OVER (PARTITION BY month) AS national_median_change_3m,
        median(rate_change_3m) OVER (PARTITION BY state_fips, month) AS state_median_change_3m,
        CASE WHEN y_delta_3m >= {threshold} THEN 1 WHEN y_delta_3m IS NOT NULL THEN 0 END AS y_label
    FROM features
)
SELECT p.*, f.* EXCLUDE (available_month)
-- LEFT JOIN grain: with_context = 1 row per county x month; fred_by_available_month = 1 row per available_month.
-- Many-to-one on decision_date = available_month -> still 1 row per county x month (row count unchanged; tested).
-- LEFT keeps a county-month even if no FRED row exists for its decision month.
FROM with_context p
LEFT JOIN staging.fred_by_available_month f ON f.available_month = p.decision_date
WHERE p.month >= DATE '{panel_start}';
