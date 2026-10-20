-- staging.laus_county_month: clean, typed LAUS data. Grain: one row per county_fips x month.
-- In:  raw.laus_county (all text, one row per series x year x period x snapshot).
-- Out: rate, unemployed, employed, labor force as numbers; month as a real date; footnotes; available_date.
-- Only the latest snapshot is used (the most recent, fully revised BLS file).
CREATE OR REPLACE TABLE staging.laus_county_month AS
WITH latest_snapshot AS (
    SELECT *
    FROM raw.laus_county
    WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM raw.laus_county)
      AND period <> 'M13'                      -- M13 is the annual average, not a month
),
typed AS (
    SELECT
        substr(series_id, 6, 5) AS county_fips,   -- characters 6-10 of LAUCN01001... = '01001' (keeps leading zero)
        right(series_id, 2) AS measure_code,
        -- 'M03' -> 3; year 2020 + month 3 -> 2020-03-01
        make_date(CAST(year AS INTEGER), CAST(substr(period, 2, 2) AS INTEGER), 1) AS month,
        -- BLS marks missing values with '-'; anything else that is not a number makes this fail loudly
        CAST(nullif(value, '-') AS DOUBLE) AS value,
        footnote_codes
    FROM latest_snapshot
)
SELECT
    county_fips,
    substr(county_fips, 1, 2) AS state_fips,
    month,
    -- pivot: one column per measure (each county-month has one row per measure in raw)
    max(CASE WHEN measure_code = '03' THEN value END) AS unemployment_rate,
    max(CASE WHEN measure_code = '04' THEN value END) AS unemployed,
    max(CASE WHEN measure_code = '05' THEN value END) AS employed,
    max(CASE WHEN measure_code = '06' THEN value END) AS labor_force,
    string_agg(DISTINCT footnote_codes, ',' ORDER BY footnote_codes) AS footnote_codes,  -- e.g. 'P' = preliminary
    -- month t is published about laus_lag months later; this is when it could first be used
    CAST(month + INTERVAL ({laus_lag}) MONTH AS DATE) AS available_date
FROM typed
GROUP BY county_fips, month;
