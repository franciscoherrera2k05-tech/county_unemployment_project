-- staging.laus_county_month
-- Grain: county_fips x month. Source: raw.laus_county, latest snapshot per series.
-- month = first day of reference month; M13 (annual average) excluded.
-- Missing-value marker '-' (footnote X) -> NULL. available_date = month + laus_release_lag_months.
CREATE OR REPLACE TABLE staging.laus_county_month AS
WITH latest AS (
    SELECT * FROM raw.laus_county
    QUALIFY _snapshot_date = max(_snapshot_date) OVER (PARTITION BY series_id)
),
parsed AS (
    SELECT
        lpad(trim(substr(series_id, 6, 5)), 5, '0')                        AS county_fips,
        right(series_id, 2)                                                AS measure,
        make_date(CAST(year AS INTEGER), CAST(substr(period, 2, 2) AS INTEGER), 1) AS month,
        CASE WHEN trim(value) IN ('-', '', '(n)', 'N/A') THEN NULL ELSE try_cast(trim(value) AS DOUBLE) END AS val,
        trim(value) AS raw_value,
        footnote_codes,
        _snapshot_date
    FROM latest
    WHERE regexp_matches(period, '^M(0[1-9]|1[0-2])$')
      AND CAST(year AS INTEGER) >= {laus_min_year}
)
SELECT
    county_fips,
    substr(county_fips, 1, 2)                                     AS state_fips,
    month,
    max(val) FILTER (WHERE measure = '03')                        AS unemployment_rate,
    CAST(max(val) FILTER (WHERE measure = '04') AS BIGINT)        AS unemployed,
    CAST(max(val) FILTER (WHERE measure = '05') AS BIGINT)        AS employment,
    CAST(max(val) FILTER (WHERE measure = '06') AS BIGINT)        AS labor_force,
    nullif(array_to_string(list_sort(list_distinct(list(footnote_codes) FILTER (WHERE footnote_codes IS NOT NULL))), ','), '') AS footnote,
    bool_or(raw_value = '-')                                      AS is_missing_marker,
    coalesce(bool_or(contains(footnote_codes, 'P')), false)       AS is_preliminary,
    CAST(month + INTERVAL ({laus_lag}) MONTH AS DATE)             AS available_date,
    max(_snapshot_date)                                           AS _snapshot_date
FROM parsed
GROUP BY county_fips, month;
