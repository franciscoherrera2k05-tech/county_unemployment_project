"""Prefect flow `monthly_pipeline`: the whole monthly batch run, in order.

What it does: ingest LAUS, the BLS area list and FRED into raw; rebuild staging/ref tables; rebuild the panel; then run
the full test suite (a failing data test fails the flow) and print the row count of every table.
In: secrets from the environment and config/settings.yaml. Out: refreshed raw/ref/staging/marts tables in MotherDuck.
Run with: uv run python -m src.flows.monthly_pipeline   (GitHub Actions runs exactly this every month)
"""
import subprocess
import sys

from prefect import flow

from src.flows.build_panel import build_panel
from src.flows.build_staging import build_staging
from src.ingest.fips import ingest_fips
from src.ingest.fred import ingest_fred
from src.ingest.laus import ingest_laus
from src.utils.helpers import connect

TABLES = ["raw.laus_county", "raw.laus_area", "raw.fred_observations", "ref.state", "ref.county", "ref.month_spine",
          "ref.geography_gap_report", "staging.laus_county_month", "staging.fred_monthly",
          "staging.fred_by_available_month", "marts.county_month_panel"]


@flow(name="monthly_pipeline")
def monthly_pipeline(states=None):
    ingest_laus(states)
    ingest_fips()
    ingest_fred()
    build_staging()
    build_panel()
    tests = subprocess.run([sys.executable, "-m", "pytest", "-q"], capture_output=True, text=True)
    print(tests.stdout[-2000:])
    if tests.returncode != 0:
        raise RuntimeError("Data tests failed; see output above")
    con = connect()
    for table in TABLES:
        print(f"{table}: {con.execute(f'SELECT count(*) FROM {table}').fetchone()[0]:,} rows")
    con.close()


if __name__ == "__main__":
    monthly_pipeline()
