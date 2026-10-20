-- ref.county rows from la.area (+ la.area_type check). Grain: county_fips.
-- {src} = local path of la.area. Static 51-row state table restricts to 50 states + DC.
WITH area AS (
    SELECT trim(area_type_code) AS area_type_code, trim(area_code) AS area_code, trim(area_text) AS area_text
    FROM read_csv('{src}', delim='\t', header=true, all_varchar=true, quote='')
),
states(state_fips, state_abbr) AS (VALUES {state_values})
SELECT
    lpad(substr(a.area_code, 3, 5), 5, '0')                       AS county_fips,
    s.state_fips,
    s.state_abbr,
    CASE WHEN a.area_text LIKE '%, __' THEN left(a.area_text, length(a.area_text) - 4) ELSE a.area_text END AS county_name,
    a.area_code,
    a.area_text,
    now()::TIMESTAMP                                              AS _loaded_at
FROM area a
JOIN states s ON s.state_fips = substr(a.area_code, 3, 2)
WHERE a.area_type_code = '{county_type}' AND starts_with(a.area_code, 'CN')
  AND s.state_fips IN ({state_list})
