-- stg_laus (county x month) + stg_fred (available month) -> county_month: LAUS + FRED features, 3-month-ahead target.
with base as (
    select
        *,
        -- windows below: partition = one county, ordered by month; each row looks only at its own past
        -- (lag / N preceding) except the target, which uses lead(). stg_laus has every calendar month per county.
        round(rate - lag(rate, 1) over w, 1) as f_rate_chg_1m,
        round(rate - lag(rate, 3) over w, 1) as f_rate_chg_3m,
        round(rate - lag(rate, 6) over w, 1) as f_rate_chg_6m,
        round(rate - lag(rate, 12) over w, 1) as f_rate_chg_12m,
        avg(rate)          over (w rows between 11 preceding and current row) as f_rate_mean_12m,
        stddev_samp(rate)  over (w rows between 11 preceding and current row) as f_rate_std_12m,
        round(rate - min(rate) over (w rows between 35 preceding and current row), 1) as f_rate_minus_min_36m,
        100 * (labor_force / nullif(lag(labor_force, 3) over w, 0)  - 1) as f_lf_pct_3m,
        100 * (labor_force / nullif(lag(labor_force, 12) over w, 0) - 1) as f_lf_pct_12m,
        100 * (employed    / nullif(lag(employed, 3) over w, 0)     - 1) as f_emp_pct_3m,
        100 * (employed    / nullif(lag(employed, 12) over w, 0)    - 1) as f_emp_pct_12m,
        100 * (unemployed  / nullif(lag(unemployed, 3) over w, 0)   - 1) as f_unemp_pct_3m,
        round(lead(rate, 3) over w - rate, 1) as y_delta_3m
    from {{ ref('stg_laus') }}
    window w as (partition by county_fips order by month)
),

with_medians as (
    select
        *,
        -- window: partition = all counties in the same month -> national median 3-month change (one value per month)
        median(f_rate_chg_3m) over (partition by month)              as f_nat_med_chg_3m,
        -- window: partition = all counties in the same state and month -> state median 3-month change
        median(f_rate_chg_3m) over (partition by state_fips, month)  as f_state_med_chg_3m
    from base
)

select
    county_fips || '-' || strftime(month, '%Y-%m')                          as county_month_id,
    county_fips,
    state_fips,
    state_abbr,
    county_name,
    month,
    cast(month + interval {{ var('laus_lag_months') }} month as date)     as decision_date,
    rate                                                                    as f_rate,
    f_rate_chg_1m, f_rate_chg_3m, f_rate_chg_6m, f_rate_chg_12m,
    f_rate_mean_12m, f_rate_std_12m, f_rate_minus_min_36m,
    f_lf_pct_3m, f_lf_pct_12m, f_emp_pct_3m, f_emp_pct_12m, f_unemp_pct_3m,
    sin(2 * pi() * month(month) / 12)                                       as f_month_sin,
    cos(2 * pi() * month(month) / 12)                                       as f_month_cos,
    f_nat_med_chg_3m,
    f_state_med_chg_3m,
    fred.fred_latest_ref_month,
    fred.f_fred_unrate, fred.f_fred_unrate_chg_3m, fred.f_fred_fedfunds, fred.f_fred_fedfunds_chg_6m,
    fred.f_fred_cpi_yoy_pct, fred.f_fred_t10y2y_mean, fred.f_fred_indpro_yoy_pct,
    fred.f_fred_payems_pct_chg_3m, fred.f_fred_icsa_pct_chg_3m,
    y_delta_3m,
    y_delta_3m >= {{ var('threshold_pp') }}                                 as y_label,
    y_delta_3m is not null                                                  as is_labeled,
    labor_force
from with_medians as laus
-- THE join: laus (one row per county x month) LEFT JOIN fred (one row per available_month) on
-- decision_date = available_month (equality only). Result: still one row per county x month.
left join {{ ref('stg_fred') }} as fred
    on cast(laus.month + interval {{ var('laus_lag_months') }} month as date) = fred.available_month
where month >= date '{{ var("panel_start") }}'
