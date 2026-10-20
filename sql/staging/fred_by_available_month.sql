-- staging.fred_by_available_month: the FRED features known in each month. Grain: one row per available_month.
-- In:  staging.fred_monthly. Out: one column per FRED feature, plus fred_latest_ref_month (the newest reference
--      month used in the row, kept for the leakage check).
-- Pivot: for each available_month, take each series' value whose available_month equals it. Because each
-- series has a fixed lag, this is exactly one reference month per series (e.g. Sep UNRATE and Aug CPI in Oct).
CREATE OR REPLACE TABLE staging.fred_by_available_month AS
SELECT
    available_month,
    max(CASE WHEN series_id = 'UNRATE'   THEN value END)          AS fred_unrate,
    max(CASE WHEN series_id = 'UNRATE'   THEN change_3m END)      AS fred_unrate_change_3m,
    max(CASE WHEN series_id = 'FEDFUNDS' THEN value END)          AS fred_fedfunds,
    max(CASE WHEN series_id = 'FEDFUNDS' THEN change_6m END)      AS fred_fedfunds_change_6m,
    max(CASE WHEN series_id = 'CPIAUCSL' THEN pct_change_12m END) AS fred_cpi_yoy_pct,
    max(CASE WHEN series_id = 'T10Y2Y'   THEN value END)          AS fred_t10y2y_mean,
    max(CASE WHEN series_id = 'INDPRO'   THEN pct_change_12m END) AS fred_indpro_yoy_pct,
    max(CASE WHEN series_id = 'PAYEMS'   THEN pct_change_3m END)  AS fred_payems_pct_change_3m,
    max(CASE WHEN series_id = 'ICSA'     THEN pct_change_3m END)  AS fred_icsa_pct_change_3m,
    max(ref_month) AS fred_latest_ref_month
FROM staging.fred_monthly
GROUP BY available_month;
