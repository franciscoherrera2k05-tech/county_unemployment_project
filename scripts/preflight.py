"""Preflight check, run before anything else.

What it does: checks that each required environment variable is set, then makes one cheap call to
MotherDuck, FRED and BLS to prove the credentials work. Reports every problem at once.
In: environment variables. Out: OK/WARN/PROBLEM lines and exit code 1 if anything required fails.
Secret values are never printed.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb  # noqa: E402
import requests  # noqa: E402

from src.utils.helpers import bls_headers  # noqa: E402  (importing also loads .env)

problems, warnings, ok = [], [], []

for name in ["MOTHERDUCK_TOKEN", "FRED_API_KEY", "BLS_CONTACT_EMAIL"]:
    if not os.environ.get(name):
        problems.append(f"{name} is missing")
for name in ["PREFECT_API_KEY", "PREFECT_API_URL"]:
    if not os.environ.get(name):
        warnings.append(f"{name} not set: flows will run locally without Prefect Cloud tracking")

if os.environ.get("MOTHERDUCK_TOKEN"):
    try:
        duckdb.connect("md:").execute("SELECT 1").fetchone()
        ok.append("MotherDuck: authenticated")
    except Exception as e:
        problems.append(f"MotherDuck connection failed ({type(e).__name__})")

if os.environ.get("FRED_API_KEY"):
    r = requests.get("https://api.stlouisfed.org/fred/series", timeout=30,
                     params={"series_id": "UNRATE", "api_key": os.environ["FRED_API_KEY"], "file_type": "json"})
    if r.status_code == 200:
        ok.append("FRED: API key accepted")
    else:
        problems.append(f"FRED_API_KEY rejected (HTTP {r.status_code})")

if os.environ.get("BLS_CONTACT_EMAIL"):
    r = requests.get("https://download.bls.gov/pub/time.series/la/la.measure", headers=bls_headers(), timeout=30)
    if r.status_code == 200 and "measure_code" in r.text:
        ok.append("BLS: download.bls.gov reachable with contact User-Agent")
    else:
        problems.append(f"BLS download failed (HTTP {r.status_code})")

for line in ok:
    print("OK     ", line)
for line in warnings:
    print("WARN   ", line)
for line in problems:
    print("PROBLEM", line)
print("Preflight", "FAILED" if problems else "PASSED")
sys.exit(1 if problems else 0)
