-- Fails (returns a row) if county_month has a different row count than stg_laus under the same panel filter.
select m.n as mart_rows, s.n as stg_rows
from (select count(*) as n from {{ ref('county_month') }}) m
cross join (select count(*) as n from {{ ref('stg_laus') }} where month >= date '{{ var("panel_start") }}') s
where m.n <> s.n
