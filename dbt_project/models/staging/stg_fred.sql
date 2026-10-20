-- Newest FRED snapshot (raw text) -> one row per available_month with the f_fred_* features (fixed publication lags).
with latest as (
    select snapshot_hash from {{ source('raw', 'fred_vintages') }}
    order by snapshot_date desc, loaded_at desc limit 1
),

-- one row per series x reference month; daily (T10Y2Y) and weekly (ICSA) values are averaged within the month
monthly as (
    select
        series_id,
        date_trunc('month', date::date)::date             as reference_month,
        avg(try_cast(nullif(value, '.') as double))       as value
    from {{ source('raw', 'fred_vintages') }}
    where snapshot_hash = (select snapshot_hash from latest)
    group by series_id, date_trunc('month', date::date)::date
),

-- one row per series x reference month, plus change features computed on that series' own monthly history.
-- windows: partition = one series, ordered by reference month; lag() looks only backward.
features as (
    select
        series_id,
        reference_month,
        cast(reference_month + interval (case series_id
            when 'CPIAUCSL' then 2 when 'INDPRO' then 2 else 1 end) month as date)  as available_month,
        value,
        value - lag(value, 3) over w                                as chg_3m,
        value - lag(value, 6) over w                                as chg_6m,
        100 * (value / nullif(lag(value, 3) over w, 0) - 1)         as pct_chg_3m,
        100 * (value / nullif(lag(value, 12) over w, 0) - 1)        as yoy_pct
    from monthly
    window w as (partition by series_id order by reference_month)
)

-- pivot: one row per available_month (all 7 series side by side)
select
    available_month,
    max(reference_month)                                                        as fred_latest_ref_month,
    max(case when series_id = 'UNRATE'   then value end)                        as f_fred_unrate,
    max(case when series_id = 'UNRATE'   then round(chg_3m, 2) end)             as f_fred_unrate_chg_3m,
    max(case when series_id = 'FEDFUNDS' then value end)                        as f_fred_fedfunds,
    max(case when series_id = 'FEDFUNDS' then round(chg_6m, 2) end)             as f_fred_fedfunds_chg_6m,
    max(case when series_id = 'CPIAUCSL' then yoy_pct end)                      as f_fred_cpi_yoy_pct,
    max(case when series_id = 'T10Y2Y'   then value end)                        as f_fred_t10y2y_mean,
    max(case when series_id = 'INDPRO'   then yoy_pct end)                      as f_fred_indpro_yoy_pct,
    max(case when series_id = 'PAYEMS'   then pct_chg_3m end)                   as f_fred_payems_pct_chg_3m,
    max(case when series_id = 'ICSA'     then pct_chg_3m end)                   as f_fred_icsa_pct_chg_3m
from features
group by available_month
