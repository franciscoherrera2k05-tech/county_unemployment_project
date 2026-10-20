"""Prefect flow `ingest_fred`: FRED API -> raw.fred_observations.

What it does: for each series in config (timing.fred_lag_months), downloads all observations from the year before
laus_min_year onward (the extra year feeds year-over-year changes), writes them to one temp CSV, and loads that file
with a single INSERT ... SELECT. Same-day rerun replaces that day's snapshot (idempotent). The temp file is deleted.
In: FRED_API_KEY, config. Out: raw.fred_observations (all text + realtime_start, _ingested_at, _snapshot_date, _source).
Values are the current (revised) FRED vintage; FRED marks missing values with '.'.
"""
import csv
import datetime as dt
import os
import tempfile

from prefect import flow, task

from src.utils.helpers import connect, get_with_retry, load_config

URL = "https://api.stlouisfed.org/fred/series/observations"


@task(retries=3, retry_delay_seconds=10)
def fetch_series(series_id, start_date):
    params = {"series_id": series_id, "api_key": os.environ["FRED_API_KEY"], "file_type": "json",
              "observation_start": start_date}
    observations = get_with_retry(URL, params=params).json()["observations"]
    print(f"FRED {series_id}: {len(observations):,} observations")
    return [(series_id, o["date"], o["value"], o["realtime_start"]) for o in observations]


@task
def load_fred(rows, snapshot_date):
    fd, path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            csv.writer(f).writerows([("series_id", "date", "value", "realtime_start"), *rows])
        con = connect()
        con.execute("""CREATE TABLE IF NOT EXISTS raw.fred_observations (
            series_id VARCHAR, date VARCHAR, value VARCHAR, realtime_start VARCHAR,
            _ingested_at TIMESTAMP, _snapshot_date DATE, _source VARCHAR)""")
        con.execute("BEGIN")
        con.execute("DELETE FROM raw.fred_observations WHERE _snapshot_date = ?", [snapshot_date])
        con.execute(f"""INSERT INTO raw.fred_observations
            SELECT *, now()::TIMESTAMP, ?::DATE, ? FROM read_csv('{path}', header=true, all_varchar=true)""",
                    [snapshot_date, URL])
        con.execute("COMMIT")
        con.close()
    finally:
        os.remove(path)
    print(f"raw.fred_observations: loaded {len(rows):,} rows for snapshot {snapshot_date}")
    return len(rows)


@flow(name="ingest_fred")
def ingest_fred():
    cfg = load_config()
    start_date = f"{cfg['data']['laus_min_year'] - 1}-01-01"
    rows = []
    for series_id in cfg["timing"]["fred_lag_months"]:
        rows += fetch_series(series_id, start_date)
    return load_fred(rows, dt.date.today())


if __name__ == "__main__":
    ingest_fred()
