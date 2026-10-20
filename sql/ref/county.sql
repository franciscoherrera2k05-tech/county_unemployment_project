-- ref.county: the county reference (dimension) table. Grain: one row per county_fips.
-- In:  raw.laus_area (latest snapshot) and ref.state (51 rows: 50 states + DC).
-- Out: county_fips (5 chars), state_fips, state_abbr, county_name.
CREATE OR REPLACE TABLE ref.county AS
WITH counties AS (
    SELECT
        substr(area_code, 3, 5) AS county_fips,      -- 'CN0100100000000' -> '01001'
        regexp_replace(area_text, ', [A-Z]{{2}}$', '') AS county_name,   -- 'Autauga County, AL' -> 'Autauga County'
        area_text
    FROM raw.laus_area
    WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM raw.laus_area)
      AND area_type_code = 'F'                       -- 'F' = "Counties and equivalents" (la.area_type)
)
SELECT
    c.county_fips,
    s.state_fips,
    s.state_abbr,
    c.county_name
-- JOIN grain: counties = 1 row per county (3,225 incl. Puerto Rico); ref.state = 1 row per state (51).
-- Many-to-one on state_fips -> result is 1 row per county. Inner join drops Puerto Rico (72) on purpose.
FROM counties c
JOIN ref.state s ON substr(c.county_fips, 1, 2) = s.state_fips;
