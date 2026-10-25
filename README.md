# County Unemployment Early-Warning

**Can we tell, from data available today, which US counties are about to see unemployment rise?** This project builds a
monthly data pipeline (BLS county unemployment + FRED national indicators → MotherDuck → dbt) and three notebooks that
explore the data, compare models, and pick a winner.

**Headline result.** The best model that can be built from the pipeline's table alone is a **logistic regression
(elastic net, ρ = 0.25, C = 0.1) on 26 features, trained without 2020–21**. On a held-out future year (2025-04 to
2026-05) it reaches **ROC AUC 0.827, PR AUC 0.668, F1 0.633**, and its probabilities are well calibrated. It still does
**not** beat a simple "same window last year" rule on hard yes/no flags (F1 0.667). The single change that does beat that
rule, adding each county's own seasonal history as features, is the top next step (ROC AUC 0.861, F1 0.68 in notebook 02).

---

## Contents
1. [Research questions](#1-research-questions)
2. [Repository layout](#2-repository-layout)
3. [Where the data comes from](#3-where-the-data-comes-from)
4. [The pipeline](#4-the-pipeline)
5. [Data dictionary](#5-data-dictionary)
6. [Research methods](#6-research-methods)
7. [The notebooks](#7-the-notebooks)
8. [Results and metrics](#8-results-and-metrics)
9. [Conclusions](#9-conclusions)
10. [Limitations and caveats](#10-limitations-and-caveats)
11. [Where to improve next](#11-where-to-improve-next)
12. [How to reproduce](#12-how-to-reproduce)
13. [Deploy the monthly run (Prefect Cloud)](#13-deploy-the-monthly-run-prefect-cloud)
14. [Glossary](#14-glossary)

---

## 1. Research questions

**Main question.** For a county in month *t*, will its unemployment rate rise by **at least 0.5 percentage points over the
next 3 months**? The intended use is an early-warning map that colors at-risk counties red each month (the map itself is
future work).

**Questions asked along the way**

| # | Question | Where it's answered | Short answer |
|---|---|---|---|
| Q1 | What does the target look like: how common, how seasonal, how much does COVID distort it? | Notebook 01 §2 | ~27% positive, strongly seasonal, 2020 is an extreme outlier |
| Q2 | How do you validate honestly on a county × month panel? | Notebook 01 §3, §3.1 | Out-of-time test year + 3-month gap + quarter-grouped, stratified CV |
| Q3 | Does the type of regularization (L1 / L2 / elastic net) matter for logistic regression? | Notebook 01 §4 | No: all land at C = 0.1, within noise |
| Q4 | Can a model beat a "seasonal-naive" rule? | Notebooks 01 §5, 02 §7, 03 §3 | Not with the mart's current features; yes once county seasonal history is added |
| Q5 | Does compressing features (PCA) or a nonlinear embedding (UMAP) help? | Notebook 01 §6–7 | No, both hurt |
| Q6 | Do support vector machines (linear, RBF, polynomial, sigmoid) beat logistic regression? | Notebook 02 | No: all significantly below LR on the test year |
| Q7 | Should the COVID years (2020–21) be used for training? | Notebooks 01 §5.5, 03 §3 | No: dropping them improves every metric |
| Q8 | Should the tuning sample be removed from the training data? | Notebook 01 §3.1, 02 §1.0 | It buys less than it looks; the real issue is time, not row overlap |
| Q9 | Which single model is best, and how much should we trust it? | Notebook 03 | LR without 2020–21; see caveats |

---

## 2. Repository layout

```
README.md                          this file
01_eda_logistic_regression.ipynb   EDA, validation design, regularized logistic regression, PCA, UMAP
02_svm.ipynb                       support vector machines vs. logistic regression
03_final_model.ipynb               finalists head-to-head, the decision, caveats, conclusions
pipeline/
  ingest/laus.py                   BLS LAUS download → raw.laus_vintages, raw.laus_area
  ingest/fred.py                   FRED API → raw.fred_vintages
  dbt_project/
    dbt_project.yml, profiles.yml  dbt config (vars: laus_lag_months=2, threshold_pp=0.5, panel_start=2015-01-01)
    models/staging/stg_laus.sql    newest LAUS snapshot → county × month
    models/staging/stg_fred.sql    newest FRED snapshot → one row per available month, f_fred_* features
    models/marts/county_month.sql  the mart: features + target, one row per county × month
    models/schema.yml              sources + unique/not_null tests on county_month_id
    tests/county_month_rows_match_laus.sql   row-count test (no join may add or drop rows)
  flows/monthly.py                 Prefect flow: ingest LAUS → ingest FRED → dbt build
.gitignore
```

Local only, **not in git** (see `.gitignore`): `.venv/`, `data/` (the parquet export), `__pycache__/`,
`.python-version`, `pyproject.toml`, `uv.lock`, `CLAUDE.md`, the `.claude/` folder (Claude Code agent settings), and a `models/` folder if you create one for saved models. Because the dependency files aren't committed, §12 lists the
packages to install.

---

## 3. Where the data comes from

### 3.1 BLS Local Area Unemployment Statistics (LAUS), county level
- **Source:** U.S. Bureau of Labor Statistics, `https://download.bls.gov/pub/time.series/la/`. Public, no key; BLS asks
  for a contact email in the User-Agent (`BLS_CONTACT_EMAIL`).
- **Files used:** `la.data.64.County` (all county series, ~330 MB, tab-delimited) and `la.area` (area codes → names).
- **Series ID layout:** e.g. `LAUCN510030000000003` = `LA` (LAUS) + `U` (**not seasonally adjusted**) + `CN` (county) +
  `51003` (5-digit county FIPS: state 51 = Virginia, county 003 = Albemarle) + `0000000` + `03` (measure).
- **Measures kept:** `03` unemployment rate (%), `04` unemployed (persons), `05` employed (persons), `06` labor force (persons).
- **Filters at load:** years ≥ 2012; period `M13` (annual average) dropped; a `-` value means "not collected" and becomes
  null. **October 2025 has no county values** (the BLS collection gap during the 2025 federal shutdown).
- **Geography:** 3,144 counties and county-equivalents in 50 states + DC. Puerto Rico (72) and the Virgin Islands (78) are
  excluded.
- **Publication lag:** county data for month *t* is released roughly 1–2 months later; the pipeline assumes month *t* is
  usable at `decision_date = t + 2 months`.

### 3.2 FRED (Federal Reserve Bank of St. Louis), national indicators
- **Source:** FRED API `https://api.stlouisfed.org/fred/series/observations` (free key, `FRED_API_KEY`), all observations
  since 2010.

| Series | What it is | Native frequency | Assumed publication lag | Feature(s) built |
|---|---|---|---|---|
| `UNRATE` | U.S. civilian unemployment rate, seasonally adjusted (%) | monthly | 1 month | level, 3-month change |
| `FEDFUNDS` | Effective federal funds rate (%) | monthly | 1 month | level, 6-month change |
| `CPIAUCSL` | Consumer Price Index, all urban consumers, SA (index) | monthly | 2 months | year-over-year % change |
| `T10Y2Y` | 10-year minus 2-year Treasury yield spread (pp) | daily → monthly mean | 1 month | monthly mean |
| `INDPRO` | Industrial production index, SA | monthly | 2 months | year-over-year % change |
| `PAYEMS` | Total nonfarm payrolls, SA (thousands) | monthly | 1 month | 3-month % change |
| `ICSA` | Initial unemployment insurance claims, SA | weekly → monthly mean | 1 month | 3-month % change |

### 3.3 What's in the warehouse right now (snapshot of 2026-10-20)

| Table | Rows | Notes |
|---|---|---|
| `raw.laus_vintages` | 2,461,516 | one full snapshot (2,267,212 rows) + one 3-state dev snapshot |
| `raw.laus_area` | 8,351 | all LAUS area codes |
| `raw.fred_vintages` | 6,264 | one snapshot of 7 series |
| `marts.stg_laus` | 553,072 | county × month, 2012-01 to 2026-08 |
| `marts.stg_fred` | 202 | available months 2010-02 to 2026-11 |
| `marts.county_month` | 439,960 | 3,144 counties × 2015-01 to 2026-08; 424,242 labeled rows |

Database size: about 127 MiB.

---

## 4. The pipeline

```
 BLS LAUS files ─┐                     ┌─ stg_laus  (newest LAUS snapshot → county × month) ─┐
                 ├─► raw.* (append-only├                                                     ├─► county_month (mart)
 FRED API ───────┘   vintages, text)   └─ stg_fred  (newest FRED snapshot → available month) ─┘
     ingest/*.py                              dbt staging                                        dbt mart + tests
                       └──────────── run monthly by pipeline/flows/monthly.py (Prefect) ────────────┘
```

**1. Ingest (`pipeline/ingest/`).** Each script downloads the full source, computes a SHA-256 hash of the downloaded bytes,
and **appends** the whole download to a `raw` table with `snapshot_date`, `snapshot_hash`, and `loaded_at`. If that hash is
already loaded, it skips the append, so reruns are idempotent. Values stay as text, exactly as published. Raw tables are
**append-only forever**: nothing is ever updated or deleted, so every monthly vintage is kept. `laus.py --dev` loads only
WV, VA, and OH (its hash gets a `-dev` suffix so it never blocks a full load).

**2. Staging (`models/staging/`, rebuilt every run).**
- `stg_laus` reads **only the newest** LAUS snapshot, parses the series ID into county FIPS and measure, pivots the four
  measures into columns, attaches county name and state from `la.area`, drops territories, and **caps the panel at the last
  month published for ≥ 90% of counties** (BLS releases a few large counties a month early; without the cap the "newest
  month" would hold only those).
- `stg_fred` reads only the newest FRED snapshot, averages daily/weekly series to months, computes each series' change
  features on its own monthly history (`lag()` only), shifts each series by its publication lag
  (`available_month = reference_month + lag`), and pivots to one row per `available_month`.

**3. Mart (`models/marts/county_month.sql`, rebuilt every run).** One row per county × month from 2015-01-01. LAUS
history features use `lag()` and trailing windows only (never the future); national and state medians use windows
partitioned by month; the target uses `lead()`. **The one join** is LAUS ↔ FRED on `decision_date = available_month`
(equality only, no as-of or range joins): each county-month gets the FRED values that were actually published by the time
that county-month's own data was available.

**4. Tests.** `unique` and `not_null` on `county_month_id`, plus one singular test: the mart has exactly as many rows as
`stg_laus` under the same panel filter, so no join can duplicate or drop rows. All pass (6 of 6 including the models).

**5. Orchestration (`pipeline/flows/monthly.py`).** A Prefect flow: ingest LAUS (2 retries) → ingest FRED (2 retries) →
`dbt build`. Locally it reads keys from environment variables; on a Prefect Cloud managed work pool it reads them from
Secret blocks. See §13.

**Monthly lifecycle.** Download everything → append to raw only if the content changed → dbt rebuilds staging and the mart
from the newest snapshot. Because each month's full download is kept, true real-time "vintages" accumulate from the first
run onward.

---

## 5. Data dictionary

### 5.1 `marts.county_month` (the table the notebooks use)
Grain: **one row per county × month *t***. 40 columns. Null rates are over all 439,960 rows. "pp" = percentage points.

**Identifiers and dates (never used as features)**

| Column | Type | Definition | Null % |
|---|---|---|---|
| `county_month_id` | text | `<county_fips>-<YYYY-MM>`, e.g. `51003-2024-06`. Primary key (tested unique, not null) | 0 |
| `county_fips` | text | 5-digit county FIPS code | 0 |
| `state_fips` | text | first 2 digits of `county_fips` | 0 |
| `state_abbr` | text | 2-letter state code (from `la.area`; DC hard-coded) | 0 |
| `county_name` | text | e.g. `Albemarle County` | 0 |
| `month` | date | month *t* (first day of month) | 0 |
| `decision_date` | date | `month + 2 months`: when month *t*'s county data is assumed published; the FRED join key | 0 |
| `fred_latest_ref_month` | date | newest FRED reference month included in that row's FRED values | 0 |

**LAUS features (county's own history; only past and current months)**

| Column | Definition | Null % |
|---|---|---|
| `f_rate` | unemployment rate in month *t* (%) | 0.71 |
| `f_rate_chg_1m` | `rate(t) − rate(t−1)`, pp, rounded to 0.1 | 1.43 |
| `f_rate_chg_3m` | `rate(t) − rate(t−3)`, pp | 1.43 |
| `f_rate_chg_6m` | `rate(t) − rate(t−6)`, pp | 1.43 |
| `f_rate_chg_12m` | `rate(t) − rate(t−12)`, pp | 0.72 |
| `f_rate_mean_12m` | mean rate over months *t−11 … t* | 0 |
| `f_rate_std_12m` | sample standard deviation of the rate over *t−11 … t* | 0 |
| `f_rate_minus_min_36m` | `rate(t) −` the minimum rate over *t−35 … t*, pp (how far above its recent low) | 0.71 |
| `f_lf_pct_3m` | labor force % change vs. 3 months earlier | 1.43 |
| `f_lf_pct_12m` | labor force % change vs. 12 months earlier | 0.72 |
| `f_emp_pct_3m` | employed persons % change vs. 3 months earlier | 1.43 |
| `f_emp_pct_12m` | employed persons % change vs. 12 months earlier | 0.72 |
| `f_unemp_pct_3m` | unemployed persons % change vs. 3 months earlier | 1.43 |
| `f_month_sin`, `f_month_cos` | calendar month of *t* on a circle: `sin/cos(2π · month / 12)` | 0 |
| `f_nat_med_chg_3m` | median `f_rate_chg_3m` across all counties in month *t* | 1.43 |
| `f_state_med_chg_3m` | median `f_rate_chg_3m` across counties in the same state and month | 1.43 |

**FRED features (national; values published by `decision_date`)**

| Column | Definition | Null % |
|---|---|---|
| `f_fred_unrate` | U.S. unemployment rate (%) | 0.71 |
| `f_fred_unrate_chg_3m` | 3-month change in U.S. unemployment rate, pp | 1.43 |
| `f_fred_fedfunds` | effective federal funds rate (%) | 0 |
| `f_fred_fedfunds_chg_6m` | 6-month change in the fed funds rate, pp | 0 |
| `f_fred_cpi_yoy_pct` | CPI year-over-year % change (inflation) | 0.71 |
| `f_fred_t10y2y_mean` | monthly mean of the 10y − 2y Treasury spread, pp | 0 |
| `f_fred_indpro_yoy_pct` | industrial production year-over-year % change | 0 |
| `f_fred_payems_pct_chg_3m` | nonfarm payrolls 3-month % change | 0 |
| `f_fred_icsa_pct_chg_3m` | initial jobless claims (monthly mean) 3-month % change | 0 |

**Target and context**

| Column | Type | Definition | Null % |
|---|---|---|---|
| `y_delta_3m` | double | `round(rate(t+3) − rate(t), 1)`: the 3-month change, pp (a change, not a level) | 3.57 |
| `y_label` | boolean | `y_delta_3m ≥ 0.5` (dbt var `threshold_pp`). **The classification target** | 3.57 |
| `is_labeled` | boolean | `y_delta_3m is not null` (false for the newest 3 months and around the Oct 2025 gap) | 0 |
| `labor_force` | double | labor force in month *t* (persons); context, not a feature | 0.71 |

Why the nulls: 0.71% = one missing month (October 2025) for every county; 1.43% = that month plus the month whose lag
lands on it; `y_delta_3m` is also null for the newest 3 months (2026-06 to 2026-08) and July 2025 (its *t+3* is October 2025).
The notebooks impute feature nulls with the training median inside the model pipeline.

**Label rate by year (labeled rows):** 2015 0.255 · 2016 0.242 · 2017 0.237 · 2018 0.273 · 2019 0.313 · **2020 0.371** ·
**2021 0.171** · 2022 0.272 · 2023 0.289 · 2024 0.310 · 2025 0.260 · 2026 (Jan–May) 0.219. Overall 0.270.

### 5.2 Staging tables (schema `marts`)

| Table | Grain | Columns |
|---|---|---|
| `stg_laus` | county × month (2012-01 onward, capped at the last ≥ 90%-complete month) | `county_fips`, `state_fips`, `state_abbr`, `county_name`, `month`, `rate` (%), `unemployed`, `employed`, `labor_force` (persons) |
| `stg_fred` | available month | `available_month`, `fred_latest_ref_month`, and the nine `f_fred_*` columns above |

### 5.3 Raw tables (schema `raw`, append-only, all values text)

| Table | Columns | Meaning |
|---|---|---|
| `laus_vintages` | `series_id`, `year`, `period` (`M01`–`M12`), `value`, `footnote_codes`, `snapshot_date`, `snapshot_hash`, `loaded_at` | every downloaded LAUS county observation (measures 03–06, years ≥ 2012) |
| `laus_area` | `area_type_code` (`F` = county), `area_code`, `area_text` (e.g. `Albemarle County, VA`), `snapshot_date`, `snapshot_hash`, `loaded_at` | area names |
| `fred_vintages` | `series_id`, `date`, `value` (`.` = missing), `realtime_start`, `snapshot_date`, `snapshot_hash`, `loaded_at` | every downloaded FRED observation |

---

## 6. Research methods

### 6.1 Target
A county-month is **positive** if its unemployment rate rises by ≥ 0.5 pp from *t* to *t+3* (`y_label`). About 27% of
labeled rows are positive. Rates aren't seasonally adjusted, so the same counties rise at the same time every year
(tourism, farming, construction); a seasonal pattern is a large part of the signal.

### 6.2 Preventing leakage (information from the future)
- **History features look only backward** (`lag()`, trailing windows). Only the target uses `lead()`.
- **Publication lag:** each row is "decided" at `decision_date = t + 2 months`, and FRED values are joined only if they
  were published by then (fixed lags per series, equality join).
- **Out-of-time test year with a gap:** the newest 12 labeled months (2025-04 to 2026-05) are the test set. The 3 months
  before it are dropped, because a training label built from *t+3* would otherwise reach into the test period.
- **Imputation and scaling happen inside the model pipeline**, so they're learned from training rows only.
- **The seasonal-naive baseline** uses `rate(t−9) − rate(t−12)` (last year's same window), which only looks back.

### 6.3 Validation design
| Set | Rows | Months | Purpose |
|---|---|---|---|
| Training | 377,097 | 2015-01 to 2024-12 | fit models |
| Tuning sample | 60,000 (stratified sample *of* the training rows) | same | choose hyperparameters and the F1 threshold |
| Gap | dropped | 2025-01 to 2025-03 | keeps training labels out of the test year |
| Test | 37,716 | 2025-04 to 2026-05 (12 labeled months) | scored once at the end |

- **Cross-validation:** 5-fold `StratifiedGroupKFold` on the tuning sample, **grouped by calendar quarter** (a quarter is
  never split between a fold's fit and validation sides) and **stratified** (each fold has about the same positive rate).
  Each fold fits on 4/5 and scores the held-out 1/5.
- **Why the tuning sample overlaps training** (notebook 01 §3.1): it exists for speed; CV does the validating, and the test
  year is never touched, so there's no leakage. Removing those rows from training would separate *rows* but not
  *information* (a county's neighboring months are near-copies), and would cost 16% of the training data. The real
  weakness is that CV folds mix years, which makes CV optimistic (CV ROC AUC 0.842 vs. 0.799 on the test year). Only an
  out-of-time tuning period would fix that.
- **Thresholds:** treated as a hyperparameter. LR's F1-optimal threshold comes from out-of-fold predictions on the tuning
  sample. SVMs get probabilities by Platt scaling on a **calibration year** (the last 12 training months, after a 3-month
  gap from their fit set), and their thresholds are chosen there.
- **Significance:** a **county-clustered bootstrap** (resample whole counties 300 times) gives 95% intervals for metric
  differences on the test year, because each county appears 12 times and rows aren't independent.

### 6.4 Metrics
| Metric | What it measures | Better | Note |
|---|---|---|---|
| **ROC AUC** | chance a random positive scores above a random negative | higher (0.5 = random) | threshold-free; main ranking metric |
| **PR AUC** (average precision) | precision across all recall levels | higher (chance = positive rate ≈ 0.29) | focuses on the positive class |
| **Log loss** (binary cross-entropy) | quality of the probabilities | lower | what LR optimizes; used for LR tuning |
| **Brier score** | mean squared error of probabilities | lower | |
| **Precision** | of flagged counties, share that actually rose | higher | at the chosen threshold |
| **Recall** | of counties that rose, share that were flagged | higher | at the chosen threshold |
| **F1** | harmonic mean of precision and recall | higher | weighs misses and false alarms equally |
| **Accuracy** | share of correct yes/no calls | higher | misleading alone: "never flag" scores ~71–73% |
| **Hinge loss** | the SVM's own training loss | lower | SVMs only; not comparable to LR |
| **Calibration** | do "30%" counties rise ~30% of the time? | on the diagonal | matters because a map shows scores |

### 6.5 Models compared
- **Baselines:** prior-only dummy; **seasonal-naive rule** ("flag if this county rose ≥ 0.5 pp over the same window last year").
- **Logistic regression:** L2, L1, elastic net (ρ ∈ {0, 0.25, 0.5, 0.75, 1}) × C ∈ {10⁻⁴ … 10}, `saga` solver, tuned on log
  loss; plus unpenalized LR as a reference.
- **Dimensionality reduction:** PCA (k tuned) + LR; UMAP-10 + LR.
- **Support vector machines:** linear SVM (L1/L2, hinge/squared hinge, class weights; all training rows) and exact kernel
  SVMs (RBF, polynomial, sigmoid; tuned on 20k rows, fit on 40k because kernel cost grows with the square of the row count),
  tuned on ROC AUC.
- **Data variants:** training without 2020–21; two county seasonal-history features computed in notebook 02
  (`rate(t−9) − rate(t−12)` and `rate(t−21) − rate(t−24)`).

---

## 7. The notebooks

All three read `marts.county_month` from MotherDuck (or `data/county_month.parquet` if `MOTHERDUCK_TOKEN` isn't set),
use the same split with the same seeds, and **write nothing**. Run them from the repo root in order.

**`01_eda_logistic_regression.ipynb` — EDA, validation, logistic regression, PCA, UMAP.** Class balance, the seasonal
sawtooth (label rate by year × calendar month), the COVID shock, feature distributions, Spearman correlations, univariate
ROC AUC per feature, label rate by state, and the seasonal-naive baseline. Then the validation design (§3) and the case for
the tuning sample (§3.1), a grid search over every LR penalty with per-fold error bars, the L1 regularization path,
standardized coefficients, the F1 threshold, a full test-year evaluation (ROC/PR curves, confusion matrices, calibration,
per-month AUC, training without COVID), PCA (scree, loadings, CV over k), and UMAP (2-D views and a 10-D embedding as
features).

**`02_svm.ipynb` — support vector machines vs. logistic regression.** Reproduces notebook 01's data and LR benchmark,
explains Platt scaling, then tunes a linear SVM, an RBF SVM (with a cost/learning curve showing why kernels use a
subsample), and polynomial and sigmoid kernels. Looks at support vectors, the margin, hinge loss, and permutation
importance. Applies notebook 01's lessons (seasonal features, no COVID) to LR and the SVMs, and compares everything on the
test year with a county-clustered bootstrap.

**`03_final_model.ipynb` — the decision and conclusions.** Lists every model tried and which ones can be built from the mart
alone, re-runs the two finalists (LR on all years vs. LR without 2020–21) head-to-head with a county-clustered bootstrap,
per-month ROC AUC, and calibration, then declares the best model, its caveats, the next improvements, and the conclusions
across all three notebooks.

---

## 8. Results and metrics

All numbers are on the **test year 2025-04 to 2026-05** (37,716 county-months, positive rate 0.288).

### 8.1 The final comparison (notebook 03)

| Model | Threshold | Log loss ↓ | Brier ↓ | ROC AUC ↑ | PR AUC ↑ | Accuracy | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|---|---|
| Dummy (prior) | 0.500 | 0.601 | 0.206 | 0.500 | 0.288 | 0.712 | 0.000 | 0.000 | 0.000 |
| Seasonal-naive rule | – | – | – | – | – | **0.794** | 0.625 | **0.716** | **0.667** |
| LR, all training years | 0.297 | 0.498 | 0.163 | 0.799 | 0.643 | 0.766 | 0.596 | 0.586 | 0.591 |
| **LR, without 2020–21 (best)** | **0.365** | **0.464** | **0.145** | **0.827** | **0.668** | 0.786 | **0.627** | 0.640 | 0.633 |

County-clustered bootstrap, best minus all-years LR: ROC AUC **+0.028** (95% CI +0.026 to +0.031), F1 **+0.043**
(+0.037 to +0.048), log loss **−0.034** (−0.040 to −0.027). The best model has the higher ROC AUC in 11 of 12 test months
and tracks the calibration diagonal; the all-years model under-predicts its riskiest counties.

### 8.2 Everything tried

| Model (notebook) | Test ROC AUC | Test F1 | Note |
|---|---|---|---|
| LR, L2 / L1 / elastic net, best C = 0.1 each (01) | 0.7985–0.7991 | ≈ 0.59 | penalties tie; CV ROC AUC ≈ 0.842 |
| LR unpenalized (01) | 0.799 | 0.59 | same as penalized |
| PCA (11 components, 90% variance) + LR (01) | 0.65 | – | CV picked all 26 components = no reduction |
| UMAP-10 + LR (01) | 0.41 | – | below chance; LR on the same 20k rows: 0.78 |
| Linear SVM (02) | 0.782 | 0.57 | significantly below LR |
| RBF SVM, 40k rows (02) | 0.784 | 0.53 | CV 0.860 → test 0.784; −0.014 vs. LR (CI −0.018 to −0.011) |
| Polynomial SVM, degree 3 (02) | 0.774 | 0.56 | biggest CV → test drop (0.861 → 0.774) |
| Sigmoid SVM (02) | 0.725 | 0.52 | worst kernel |
| LR without 2020–21 (01, 03) | 0.827 | 0.63 | **best usable as-is** |
| LR + county seasonal features, without 2020–21 (02) | **0.861** | **0.68** | best overall; features not in the mart yet |
| Linear SVM + seasonal features, without 2020–21 (02) | 0.856 | 0.66 | |
| Polynomial SVM + seasonal features, without 2020–21 (02) | 0.827 | 0.64 | |

Other diagnostics: the F1-optimal threshold is ≈ 0.30–0.37, not 0.5. The RBF SVM keeps 39% of its training rows as support
vectors (97% of them at their bound) and 30% of test rows fall inside its margin: the classes overlap heavily, so the limit
is the information in the features, not the shape of the boundary.

---

## 9. Conclusions

1. **The best model usable today is logistic regression (elastic net, ρ = 0.25, C = 0.1) on the 26 mart features,
   trained without 2020–21, flagging counties with score ≥ 0.365.** It wins every metric among usable models, consistently
   across months, with well-calibrated probabilities.
2. **Data choices beat model choices.** Dropping the COVID years and adding county seasonal history improved every model by
   0.03–0.07 ROC AUC. Switching from LR to SVMs, PCA, or UMAP never helped.
3. **Seasonality is the core signal and the core difficulty.** A rule that only asks "did this county rise over the same
   window last year?" gets F1 0.667, and only a model given each county's own seasonal history beats it.
4. **Validate out of time.** Cross-validation inside the training years flattered every model (most of all flexible ones:
   kernels won CV and lost the test year). The out-of-time test year is the number to trust.
5. **Regularization type doesn't matter here.** With 26 features and hundreds of thousands of rows, L1, L2, and elastic net
   are indistinguishable; L1 is useful only to see which features matter most.

---

## 10. Limitations and caveats

- **The best usable model doesn't beat the seasonal-naive rule on hard flags** (F1 0.633 vs. 0.667). Its advantage is a
  calibrated, ranked probability for every county (the rule is missing for ~9% of rows and gives only yes/no).
- **No pandemic memory.** Training without 2020–21 helps normal years but means the model has never seen a nationwide shock.
- **One test year, one regime.** The bootstrap measures noise within 2025–26, not how another year would go.
- **Revised vs. preliminary data.** Training labels use revised LAUS history; anyone using the model in real time would see
  first-published (preliminary) numbers. True vintages only began accumulating at the pipeline's first run (2026-10-20).
- **Not seasonally adjusted.** County rates are NSA, which is why seasonality dominates.
- **Fixed FRED lags.** Publication lags are assumed constant per series, not looked up per release.
- **Publication lag eats part of the window.** Month *t*'s data appears around *t + 2*, so part of the 3-month window has
  passed by the time a county could be flagged.
- **Missing October 2025.** No county rates for that month (the 2025 shutdown), so October and July 2025 have no label.
- **No county structural features** (industry mix, population, etc.), by design.
- **Hyperparameters weren't retuned for the no-COVID rows**, and kernel SVMs saw only 20–40k rows (the learning curve
  suggests that cost little).
- **The F1 threshold weighs misses and false alarms equally**; a different cost balance would move it.

---

## 11. Where to improve next

1. **Add county seasonal history to the mart** as two `f_` columns in `county_month.sql`:
   `rate(t−9) − rate(t−12)` and `rate(t−21) − rate(t−24)` (two `lag()` expressions). Expected: ROC AUC ≈ 0.86, F1 ≈ 0.68,
   beating the seasonal-naive rule. Then rerun notebook 03.
2. **Try gradient-boosted trees**, which can learn county × season interactions; judge them on the out-of-time test year.
3. **Tune on a later period** (an out-of-time tuning year) instead of a sample of the training years.
4. **Down-weight 2020–21 instead of dropping it.**
5. **Re-check on preliminary data** once a few monthly snapshots have accumulated in the raw tables.
6. **Build the map**: color counties by score, red at or above the threshold.

---

## 12. How to reproduce

### 12.1 Accounts and keys (once)
1. Accounts: GitHub, MotherDuck (app.motherduck.com), FRED (fred.stlouisfed.org), Prefect Cloud (app.prefect.cloud, free).
2. Keys: FRED (My Account → API Keys), MotherDuck (Settings → Access Tokens, read/write). BLS needs no key, only an email.
3. GitHub → Settings → Codespaces → Secrets: `MOTHERDUCK_TOKEN`, `FRED_API_KEY`, `BLS_CONTACT_EMAIL`, with access to this
   repo. Do this before creating the Codespace. Keys are read from environment variables and never printed or committed.
4. Create a Codespace and check: `for v in MOTHERDUCK_TOKEN FRED_API_KEY BLS_CONTACT_EMAIL; do echo "$v: ${!v:+set}"; done`

### 12.2 Python environment (Python 3.12)
`pyproject.toml` and `uv.lock` are kept local, not in git, so on a fresh clone install the packages directly:
```bash
uv venv --python 3.12
uv pip install requests "duckdb==1.4.1" dbt-duckdb prefect scikit-learn pandas pyarrow seaborn matplotlib umap-learn ipykernel nbconvert
```
(Python 3.12 matters: newer versions can break Prefect or dbt.)

### 12.3 Run the pipeline by hand (from the repo root)
```bash
uv run python pipeline/ingest/laus.py            # add --dev for WV, VA, OH only
uv run python pipeline/ingest/fred.py
uv run dbt build --project-dir pipeline/dbt_project --profiles-dir pipeline/dbt_project
mkdir -p data && uv run python -c "import duckdb; duckdb.connect('md:unemployment').execute(\"copy (select * from marts.county_month) to 'data/county_month.parquet'\")"
```
Or all three steps as the Prefect flow: `uv run python pipeline/flows/monthly.py` (after `prefect cloud login`; in a
Codespace choose **Paste an API key**, created at app.prefect.cloud → profile → API Keys).

### 12.4 Run the notebooks
Open them from the repo root with the `.venv` kernel and run 01 → 02 → 03. Notebook 01 takes about 20 minutes, notebook
02 about 2 hours (kernel SVM grid searches), notebook 03 about 6 minutes on a 2-core Codespace.

### 12.5 Rebuilding the pipeline with Claude Code (optional)
The pipeline was built by a Claude Code agent from a local spec (`CLAUDE.md`) and
`.claude/agents/pipeline-builder.md`, both kept out of git. With both in place, "Use the pipeline-builder agent to build everything" runs slices
1–4 (LAUS, FRED, full run, Prefect) and resumes from `git log` if interrupted; "do Slice N" runs one slice.

---

## 13. Deploy the monthly run (Prefect Cloud)
The work pool `county-managed` (type `prefect:managed`) already exists and passed a test run. A managed run has no
Codespace env vars, so the flow reads three Prefect Secret blocks. Create them from the Codespace (values come from your env
vars and are never printed):
```bash
uv run python -c "import os; from prefect.blocks.system import Secret
for env, block in [('MOTHERDUCK_TOKEN','motherduck-token'),('FRED_API_KEY','fred-api-key'),('BLS_CONTACT_EMAIL','bls-contact-email')]:
    Secret(value=os.environ[env]).save(block, overwrite=True)"
```
The managed worker pulls code from GitHub, so push the repo first (`git push`). If the repo is private, also save a
GitHub token (fine-grained, read-only Contents) as a Secret block named `github-token` in the Prefect UI
(Blocks → Secret), then deploy (runs on the 10th of each month, 14:00 UTC):
```bash
uv run python -c "
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
repo = GitRepository(url='https://github.com/<you>/county_unemployment_project.git', branch='main',
                     credentials={'access_token': Secret.load('github-token')})
flow.from_source(source=repo, entrypoint='pipeline/flows/monthly.py:monthly').deploy(
    name='monthly', work_pool_name='county-managed', cron='0 14 10 * *',
    job_variables={'pip_packages': ['requests', 'duckdb==1.4.1', 'dbt-duckdb']})"
uv run prefect deployment run 'county-unemployment-monthly/monthly'   # one test run now
```
For a public repo, pass `source='https://github.com/<you>/county_unemployment_project.git'` instead of `repo`.

---

## 14. Glossary
- **LAUS:** BLS Local Area Unemployment Statistics, monthly labor-force estimates for states, counties, and metro areas.
- **FIPS code:** 5-digit county identifier (2-digit state + 3-digit county).
- **NSA / SA:** not seasonally adjusted / seasonally adjusted.
- **pp (percentage points):** the difference between two percentages (5.0% → 5.5% is +0.5 pp).
- **Vintage / snapshot:** one full download of a source on a given date; values can be revised between vintages.
- **Mart:** the final, analysis-ready table (`county_month`).
- **Out-of-time test:** evaluating on months after all training data, as the model would be used.
- **Stratified / grouped k-fold:** CV folds that keep the positive rate similar (stratified) and keep related rows together (grouped).
- **Elastic net:** a mix of L1 (sets some coefficients to zero) and L2 (shrinks all coefficients) penalties; `ρ` is the L1 share.
- **Platt scaling:** fitting a small logistic regression on a model's scores to turn them into probabilities.
- **Seasonal-naive rule:** predict this year's change from the same window last year.
- **County-clustered bootstrap:** resampling whole counties to get honest uncertainty when each county appears many times.
