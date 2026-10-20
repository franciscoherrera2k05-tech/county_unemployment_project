# ML_SPEC.md — Stages 2–5 (read only when the user starts one of these stages)

Prerequisite: Stage 1 complete, `uv run pytest` green, panel summary in `docs/BUILD_LOG.md`.

## Setup (Stage 2, first thing)
- `uv add pandas numpy scikit-learn seaborn matplotlib umap-learn pyarrow jupyter jupytext nbconvert ipykernel`
- Add to `config/settings.yaml`:
  ```yaml
  ml:
    test_start: 2023-01-01
    cv_folds: 5
    gap_months: 3
    policy_budget_k: 50
  ```
- Create `src/ml/` (`data.py`, `splits.py`, `baselines.py`, `metrics.py`, `plotting.py`) and `artifacts/preds/`.
- Author each notebook as a jupytext percent-format `.py` in `notebooks/`, then
  `uv run jupytext --to ipynb --execute`. Commit both `.py` and the executed `.ipynb`; keep outputs small.

## Notebook conventions
Notebook style with **a markdown cell before each code cell explaining what and why, and one after each result
interpreting it.** Write interpretation only after seeing real outputs, citing the actual numbers. Never
pre-write or invent results. Each model notebook saves test-period and out-of-fold predictions to
`artifacts/preds/<model>.parquet`. Shared logic lives in `src/ml/` so every notebook evaluates identically.
Pull the panel with a column-selected query; drop rows with null target for training.

## Notebooks by stage
| Stage | Notebook | Contents |
|---|---|---|
| 2 | `00_target_and_eda` | Target (rise ≥ threshold pp from t to t+3; information at t predicts whether unemployment rises meaningfully over the next 3 months). Class balance over time and by state, sensitivity to `sensitivity_thresholds`, seasonality, COVID effect, missingness, leakage audit. |
| 2 | `01_baseline` | Scored baselines (so PR-AUC works): (a) trend continuation = trailing 3-month change; (b) seasonal naive = change over the same window a year earlier; plus a base-rate model. If models can't beat these, added complexity isn't justified. |
| 2 | `02_logistic_regression` | Standardize inside a Pipeline. L1, L2, elastic-net (tune `C`, `l1_ratio`; solver `saga`). Coefficient paths. Why it's a useful statistical baseline: convex loss, interpretable coefficients, well-calibrated probabilities. |
| 3 | `03_random_forest` | Tune `n_estimators`, `max_depth`, `min_samples_leaf`, `max_features` via time-respecting RandomizedSearchCV (≤ 30 iterations, subsampled tuning set). Impurity and permutation importance; partial dependence. |
| 3 | `04_gradient_boosting` | `HistGradientBoostingClassifier` first (learning rate, leaf nodes/depth, iterations, L2, early stopping on a time-ordered validation split). Optionally compare `GradientBoostingClassifier` on a subsample. |
| 4 | `05_svm_rbf` | RBF `SVC` on a time-respecting subsample (≤ ~30k rows): scaling, tune `C` and `gamma`, calibrated probabilities. Compare linear and polynomial kernels. Compare to `Nystroem` + linear classifier on the full dev set. Explain why exact SVC doesn't scale. |
| 4 | `06_pca_umap` | PCA + logistic (scree plot, tune components). Kernel PCA (RBF) on a subsample. UMAP (tune `n_neighbors`, `n_components`) fit on training data only, `.transform` on test; 2-D embeddings colored by label. Does reduction help logistic regression, and what does that imply for later models? |
| 5 | `07_model_comparison` | All models and baselines on identical folds and the held-out test period: PR-AUC, ROC-AUC, log loss, Brier, calibration curves, top-k precision/recall, lead time, fold variance, with and without the COVID window (2020-03 to 2021-06), and across alternative thresholds. Strengths and weaknesses of each. A clear recommendation. Answer research questions 1–3 with numbers (permutation importance and logistic coefficients for question 2). |
| 5 | `08_policy_allocation` | Budget `k` counties per month. Compare: highest predicted risk; risk × labor force (expected affected residents); baseline ranking; random. Events captured, residents in captured events, bootstrap intervals over months, and cost-ratio analysis (miss vs. false alarm). State plainly that risk ranking isn't social need. Answers question 4. |
| 5 | `09_next_steps` | Limitations (revisions, geography changes, COVID dominance, spatial correlation, short history) and concrete areas to improve. |
| 5 | `reports/findings.md` | Concise, honest, policy-oriented; answers all four questions with real numbers; states limitations and negative results. |

## Validation and metrics (non-negotiable)
- Time-based only: development = months before `test_start`; test = after. No random splits.
- Expanding-window CV by month blocks (`cv_folds`) with a **gap ≥ `gap_months`** between train and validation.
  Target windows overlap and counties are correlated: state the limitation, report fold variance, and use a
  month-block bootstrap for intervals.
- All preprocessing (imputation, scaling, PCA, UMAP) is fit inside a `Pipeline` on training folds only. No
  tuning on the test period.
- Primary metrics: **PR-AUC** and **log loss**. Also ROC-AUC, Brier, calibration, top-k precision/recall, lead time.
- **Lead time:** for onset events (first month a county's forward 3-month rise crosses the threshold after at
  least 6 quiet months), measure how many months before onset each scorer first alerts at a matched alert
  volume (same alerts per month), versus the baseline. Define it precisely in the notebook.
- Handle imbalance deliberately (class weights and/or threshold) and justify.
- Fix seeds from config. Aim for < ~20 minutes per notebook by subsampling for tuning.

## Practical constraints
- RBF SVC scales roughly quadratically or worse: subsample and use Nystroem.
- UMAP is not in scikit-learn, is stochastic, and must be fit on training data only; be skeptical of it for prediction.
- Codespaces has limited RAM/CPU: downcast dtypes, select columns in SQL, subsample for tuning, time long steps.
- If a complex model fails to beat the baseline, say so and say what it implies.
