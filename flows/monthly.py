# Monthly Prefect flow: BLS LAUS + FRED downloads -> raw vintages -> dbt build -> score.py (predictions, outcomes).
import os, subprocess, sys
from pathlib import Path
from prefect import flow, task, get_run_logger

ROOT = Path(__file__).resolve().parents[1]
# env var -> Prefect Secret block name (used only when the env var is missing, e.g. on a managed work pool)
SECRETS = {"MOTHERDUCK_TOKEN": "motherduck-token", "FRED_API_KEY": "fred-api-key",
           "BLS_CONTACT_EMAIL": "bls-contact-email"}


def load_secrets():
    """Fill missing env vars from Prefect Secret blocks (values are never logged)."""
    from prefect.blocks.system import Secret
    for env, block in SECRETS.items():
        if not os.environ.get(env):
            os.environ[env] = Secret.load(block).get()


def run(args):
    """Run one command from the repo root; raise if it fails."""
    out = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    get_run_logger().info(out.stdout[-3000:])
    if out.returncode != 0:
        raise RuntimeError(f"{args[:3]} failed:\n{out.stdout[-2000:]}\n{out.stderr[-2000:]}")
    for line in out.stdout.splitlines():
        if line.startswith("WARNING"):
            get_run_logger().warning(line)


@task(retries=2, retry_delay_seconds=300)
def ingest_laus():
    run([sys.executable, "ingest/laus.py"])


@task(retries=2, retry_delay_seconds=60)
def ingest_fred():
    run([sys.executable, "ingest/fred.py"])


@task
def dbt_build():
    dbt = str(Path(sys.executable).with_name("dbt"))
    run([dbt, "build", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"])


@task
def score_newest():
    if not (ROOT / "models" / "current.txt").exists():
        get_run_logger().warning("models/current.txt is missing; skipping scoring until a model is saved")
        return
    run([sys.executable, "score.py"])


@task
def append_outcomes():
    run([sys.executable, "score.py", "--outcomes"])


@flow(name="county-unemployment-monthly")
def monthly():
    load_secrets()
    ingest_laus()
    ingest_fred()
    dbt_build()
    score_newest()
    append_outcomes()


if __name__ == "__main__":
    monthly()
