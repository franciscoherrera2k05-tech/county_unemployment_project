# Data dictionary — `marts.county_month_panel`

Grain: one row per `county_fips` x `month` (t), 2015-01 to the latest published month. Rates are in percent; "pp" =
percentage points. All LAUS features use only months `<= t`; FRED features use only values known in `decision_date`.

| Column | Meaning |
|---|---|
| `county_fips`, `state_fips`, `state_abbr`, `county_name` | 5-digit county FIPS, 2-digit state FIPS, state abbreviation, name (from `ref.county`) |
| `month` | Reference month t (first of month) |
| `rate`, `unemployed`, `employed`, `labor_force` | LAUS values for month t (not seasonally adjusted) |
| `decision_date` | `month + 2 months`: when month-t data is published and the prediction is made |
| `rate_change_1m/3m/6m/12m` | `rate(t) - rate(t-k)`, pp, rounded to 0.1 |
| `rate_mean_12m`, `rate_std_12m` | Mean and sample std of the rate over months t-11..t |
| `rate_minus_min_36m` | `rate(t)` minus the lowest rate over months t-35..t |
| `labor_force_pct_change_3m/12m`, `employed_pct_change_3m/12m`, `unemployed_pct_change_3m` | % change vs. k months earlier |
| `month_sin`, `month_cos` | Month of year on a circle (seasonality) |
| `national_median_change_3m`, `state_median_change_3m` | Median `rate_change_3m` across all included counties (or the county's state) in month t |
| `fred_unrate`, `fred_unrate_change_3m` | US unemployment rate and its 3-month change |
| `fred_fedfunds`, `fred_fedfunds_change_6m` | Effective fed funds rate and its 6-month change |
| `fred_cpi_yoy_pct` | CPI (CPIAUCSL) % change vs. 12 months earlier |
| `fred_t10y2y_mean` | Monthly mean of the daily 10y-2y Treasury spread |
| `fred_indpro_yoy_pct` | Industrial production % change vs. 12 months earlier |
| `fred_payems_pct_change_3m` | Nonfarm payrolls % change over 3 months |
| `fred_icsa_pct_change_3m` | Monthly mean of weekly initial claims, % change over 3 months |
| `fred_latest_ref_month` | Newest FRED reference month used in the row (leakage check: always before `decision_date`) |
| `y_delta_3m` | **Target:** `round(rate(t+3) - rate(t), 1)`, pp. NULL for the last 3 months (kept for live scoring) |
| `y_label` | 1 if `y_delta_3m >= 0.5` (config `threshold_pp`), 0 if lower, NULL if `y_delta_3m` is NULL |

FRED lags (month M known in month M + lag): UNRATE, FEDFUNDS, T10Y2Y, PAYEMS, ICSA 1; CPIAUCSL, INDPRO 2.

Notes:
- **COVID:** March-June 2020 dominates the target. Rows for Dec 2019-Mar 2020 are ~88-97% positive (rates jumped in April
  2020); rows for Apr-Jun 2020 are almost all negative (the recovery).
- **Seasonality:** the target is computed on unadjusted rates, so it is strongly seasonal (e.g. ~2% positive for June
  starts, ~63% for October starts).
- **October 2025:** no BLS data (shutdown). `rate` and dependent features are NULL at 2025-10 and where a lag reaches it
  (e.g. 3-month changes at 2026-01); targets are NULL for 2025-07 and 2025-10; `fred_unrate` is NULL for decision month 2025-11.
  Trailing mean/std/min skip that single month.
- **Warm-up:** none inside the panel; features use LAUS history from 2012, so the 36-month window is full by 2015-01.
