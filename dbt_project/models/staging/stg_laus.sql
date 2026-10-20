-- Newest LAUS snapshot (raw text) -> one row per county x month with rate, unemployed, employed, labor force.
with latest_data as (
    -- newest load wins (a --dev and a full load can share a snapshot_date)
    select snapshot_hash from {{ source('raw', 'laus_vintages') }}
    order by snapshot_date desc, loaded_at desc limit 1
),

latest_area as (
    select snapshot_hash from {{ source('raw', 'laus_area') }}
    order by snapshot_date desc, loaded_at desc limit 1
),

-- one row per series x month (series = county x measure); '-' (not collected, e.g. Oct 2025) becomes null
long as (
    select
        substr(series_id, 6, 5)                              as county_fips,
        right(series_id, 2)                                  as measure_code,
        make_date(year::int, substr(period, 2)::int, 1)      as month,
        try_cast(nullif(value, '-') as double)               as value
    from {{ source('raw', 'laus_vintages') }}
    where snapshot_hash = (select snapshot_hash from latest_data)
),

-- one row per county x month (pivot the 4 measures into columns)
wide as (
    select
        county_fips,
        month,
        max(case when measure_code = '03' then value end) as rate,
        max(case when measure_code = '04' then value end) as unemployed,
        max(case when measure_code = '05' then value end) as employed,
        max(case when measure_code = '06' then value end) as labor_force
    from long
    group by county_fips, month
),

-- one row per county (area_type F = county); text looks like "Autauga County, AL"
area as (
    select
        substr(area_code, 3, 5)                                   as county_fips,
        case when substr(area_code, 3, 2) = '11' then 'DC'
             else regexp_extract(area_text, ', ([A-Z]{2})$', 1) end as state_abbr,
        regexp_replace(area_text, ', [A-Z]{2}$', '')              as county_name
    from {{ source('raw', 'laus_area') }}
    where snapshot_hash = (select snapshot_hash from latest_area)
      and area_type_code = 'F'
)

-- join: wide (one row per county x month) LEFT JOIN area (one row per county) on county_fips
-- result: one row per county x month, now with state_abbr and county_name
select
    wide.county_fips,
    left(wide.county_fips, 2) as state_fips,
    area.state_abbr,
    area.county_name,
    wide.month,
    wide.rate,
    wide.unemployed,
    wide.employed,
    wide.labor_force
from wide
left join area on wide.county_fips = area.county_fips
where left(wide.county_fips, 2) not in ('72', '78')
