"""Shared helpers used by every pipeline step.

What it does: reads config/settings.yaml, opens a MotherDuck connection, downloads a URL to a temp file
(with retries), runs a .sql file with config values filled in, and holds the static 51-row state FIPS table.
In: environment variables (.env or Codespaces/GitHub secrets) and the config file.
Out: plain Python values (dicts, a DuckDB connection, a temp-file path). Secrets are never printed.
"""
import os
import tempfile
import time
from pathlib import Path

import duckdb
import requests
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

# 50 states + DC. Puerto Rico (72) is deliberately left out.
STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT",
    "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL",
    "18": "IN", "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
    "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO", "30": "MT", "31": "NE",
    "32": "NV", "33": "NH", "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA", "54": "WV",
    "55": "WI", "56": "WY",
}


def load_config():
    """Return config/settings.yaml as a dict."""
    with open(ROOT / "config" / "settings.yaml") as f:
        return yaml.safe_load(f)


def state_fips_list(states):
    """Turn 'all' or a list like ['WV', 'VA'] into a sorted list of 2-digit state FIPS codes."""
    if states == "all" or states is None:
        return sorted(STATE_FIPS)
    abbr_to_fips = {abbr: fips for fips, abbr in STATE_FIPS.items()}
    return sorted(abbr_to_fips[s] for s in states)


def connect():
    """Open MotherDuck and switch to the configured database (created if it does not exist)."""
    db = load_config()["project"]["database"]
    con = duckdb.connect("md:")
    con.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    con.execute(f"USE {db}")
    for schema in ["raw", "ref", "staging", "marts"]:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return con


def bls_headers():
    """BLS asks for a User-Agent with a contact email; it needs no API key."""
    return {"User-Agent": f"county-unemployment-research/0.1 ({os.environ['BLS_CONTACT_EMAIL']})"}


def get_with_retry(url, params=None, headers=None, tries=4):
    """GET a URL, retrying with backoff (1s, 2s, 4s). Params are never logged (they may hold an API key)."""
    for attempt in range(tries):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=120)
            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(2 ** attempt)


def download_to_temp(url, headers=None):
    """Stream a (possibly large) file to a temp file and return its path. The caller deletes it."""
    fd, path = tempfile.mkstemp(suffix=".txt")
    with os.fdopen(fd, "wb") as f, requests.get(url, headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1 << 20):
            f.write(chunk)
    return path


def sql_params(cfg):
    """The config values that SQL files may reference as {name}."""
    return {
        "laus_min_year": cfg["data"]["laus_min_year"],
        "panel_start": cfg["data"]["panel_start"],
        "laus_lag": cfg["timing"]["laus_release_lag_months"],
        "horizon": cfg["target"]["horizon_months"],
        "threshold": cfg["target"]["threshold_pp"],
    }


def run_sql_file(con, relative_path, cfg):
    """Read sql/<relative_path>, fill in {name} placeholders from config, and run it."""
    sql = (ROOT / "sql" / relative_path).read_text()
    con.execute(sql.format(**sql_params(cfg)))
