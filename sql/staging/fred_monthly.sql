-- staging.fred_monthly: FRED indicators as complete monthly values. Grain: one row per series_id x ref_month.
-- In:  raw.fred_observations (latest snapshot; daily, weekly and monthly series).
-- Out: monthly value (daily/weekly series averaged over the month), simple change columns, and available_month.
-- Timing rule (kept simple on purpose): the value for month M counts as known in month M + lag, where the lag per
-- series comes from config (timing.fred_lag_months). FRED serves revised values, which is a stated limitation.
CREATE OR REPLACE TABLE staging.fred_monthly AS
WITH monthly AS (
    SELECT
        series_id,
        CAST(date_trunc('month', CAST(date AS DATE)) AS DATE) AS ref_month,
        avg(CAST(nullif(value, '.') AS DOUBLE)) AS value    -- '.' = missing in FRED; avg() skips NULLs
    FROM raw.fred_observations
    WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM raw.fred_observations)
    GROUP BY series_id, ref_month
),
complete_months AS (
    SELECT *
    FROM monthly
    WHERE ref_month < date_trunc('month', current_date)    -- drop the current, still-incomplete month
),
lags AS (
    SELECT * FROM (VALUES {fred_lag_values}) t(series_id, lag_months)
)
SELECT
    m.series_id,
    m.ref_month,
    m.value,
    -- changes compared with the same series k months earlier (rows are consecutive months per series)
    m.value - LAG(m.value, 3) OVER w AS change_3m,
    m.value - LAG(m.value, 6) OVER w AS change_6m,
    100 * (m.value / LAG(m.value, 3) OVER w - 1) AS pct_change_3m,
    100 * (m.value / LAG(m.value, 12) OVER w - 1) AS pct_change_12m,
    CAST(m.ref_month + to_months(l.lag_months) AS DATE) AS available_month
-- JOIN grain: complete_months = 1 row per series x month; lags = 1 row per series (7 rows).
-- Many-to-one on series_id -> still 1 row per series x month. Inner join keeps only configured series.
FROM complete_months m
JOIN lags l ON l.series_id = m.series_id
WINDOW w AS (PARTITION BY m.series_id ORDER BY m.ref_month);
