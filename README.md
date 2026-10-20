# County Unemployment Early-Warning

BLS LAUS + FRED → append-only raw vintages in MotherDuck → dbt builds one mart (`county_month`) → I train models in
`notebooks/` → a monthly Prefect flow scores the newest month and appends to `predictions`.

## Setup (once)
1. Accounts: GitHub, MotherDuck (app.motherduck.com), FRED (fred.stlouisfed.org), Prefect Cloud (app.prefect.cloud, free).
2. Keys: FRED (My Account → API Keys), MotherDuck (Settings → Access Tokens, read/write). BLS needs no key, only an email.
3. GitHub → Settings → Codespaces → Secrets: `MOTHERDUCK_TOKEN`, `FRED_API_KEY`, `BLS_CONTACT_EMAIL`, with access to this
   repo. Do this before creating the Codespace.
4. Create a Codespace. Check secrets: `for v in MOTHERDUCK_TOKEN FRED_API_KEY BLS_CONTACT_EMAIL; do echo "$v: ${!v:+set}"; done`
5. Put `CLAUDE.md` in the repo root and `pipeline-builder.md` in `.claude/agents/`. Commit.
6. Install Claude Code (`curl -fsSL https://claude.ai/install.sh | bash`), run `claude`, then `/login`.
7. Optional, before the run: in the Codespace terminal run `prefect cloud login` and choose **Paste an API key** (create the
   key at app.prefect.cloud → profile → API Keys). The browser option can crash in a Codespace. If it fails, skip it: the
   agent builds everything else and reports what's left for Prefect.

## Build everything in one prompt
Finish setup steps 1–6 (step 7 is optional), then paste:
> Use the pipeline-builder agent to build everything.

It runs all six slices in order and stops only if blocked. Afterward it lists what only you can do: save a model,
enter Prefect secrets, and run the deploy commands. If interrupted, send the same prompt; it resumes from `git log`.

## Or build one slice at a time
Run `/clear` between slices, then paste:
> Use the pipeline-builder agent to do Slice 1.

(then 2, 3, 4; Slice 5 after you've saved a model; then 6). If the agent isn't found: "Read
.claude/agents/pipeline-builder.md and follow it. Do Slice 1." Review each slice's files and joins before the next.

## Monthly lifecycle (Prefect)
Download the whole LAUS file and FRED → append to raw vintages if new → dbt rebuilds staging and the mart from the
newest snapshot → `score.py` scores only the newest unlabeled rows and appends to `predictions` → `outcomes` appended
once the answer exists (~3 runs later).

## Notebooks
Load `data/county_month.parquet` or query `marts.county_month`. Features: `df.filter(like="f_")`. Targets:
`y_delta_3m`, `y_label`. Train on `is_labeled`. Never use ID columns as features. Retrain every 6 months, hold out the
most recent months to compare old vs. new model, and save `models/<version>.joblib` per the contract in CLAUDE.md.

## Deploy (monthly run on a Prefect Cloud managed work pool)
The work pool `county-managed` (type `prefect:managed`) already exists and passed a test run. A managed run has no
Codespace env vars, so the flow reads three Prefect Secret blocks. Create them yourself from the Codespace (the values
come from your env vars and are never printed):
```bash
uv run python -c "import os; from prefect.blocks.system import Secret
for env, block in [('MOTHERDUCK_TOKEN','motherduck-token'),('FRED_API_KEY','fred-api-key'),('BLS_CONTACT_EMAIL','bls-contact-email')]:
    Secret(value=os.environ[env]).save(block, overwrite=True)"
```
The managed worker pulls code from GitHub, so push the repo first (`git push`). If the repo is private, also save a
GitHub token (fine-grained, read-only Contents) as a Secret block named `github-token` in the Prefect UI
(Blocks -> Secret), then deploy (runs on the 10th of each month, 14:00 UTC):
```bash
uv run python -c "
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
repo = GitRepository(url='https://github.com/<you>/county_unemployment_project.git', branch='main',
                     credentials={'access_token': Secret.load('github-token')})
flow.from_source(source=repo, entrypoint='flows/monthly.py:monthly').deploy(
    name='monthly', work_pool_name='county-managed', cron='0 14 10 * *',
    job_variables={'pip_packages': ['requests', 'duckdb==1.4.1', 'dbt-duckdb', 'joblib', 'scikit-learn', 'pandas', 'pyarrow']})"
uv run prefect deployment run 'county-unemployment-monthly/monthly'   # one test run now
```
For a public repo, pass `source='https://github.com/<you>/county_unemployment_project.git'` instead of `repo`.
Saved models live in `models/`; commit `models/current.txt` and the `.joblib` it names so the managed run can score.
