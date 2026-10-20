"""Preflight: check env vars and make one cheap authenticated call per service.

Reports all problems at once. Never prints secret values. Exit code 1 if anything required fails.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests  # noqa: E402

from src.utils.config import load_config  # noqa: E402,F401  (loads .env)

REQUIRED = ["MOTHERDUCK_TOKEN", "FRED_API_KEY", "BLS_CONTACT_EMAIL"]
OPTIONAL = ["PREFECT_API_KEY", "PREFECT_API_URL"]


def main() -> int:
    problems, warnings, ok = [], [], []
    for v in REQUIRED:
        if not os.environ.get(v):
            problems.append(f"{v}: missing")
    for v in OPTIONAL:
        if not os.environ.get(v):
            warnings.append(f"{v}: not set (Prefect flows will run locally, no Cloud tracking)")

    if os.environ.get("MOTHERDUCK_TOKEN"):
        try:
            import duckdb
            con = duckdb.connect("md:")
            con.execute("SELECT 1").fetchone()
            dbs = [r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()]
            ok.append(f"MotherDuck: authenticated ({len(dbs)} databases visible)")
            con.close()
        except Exception as e:
            problems.append(f"MOTHERDUCK_TOKEN: connection failed ({type(e).__name__}: {str(e)[:120]})")

    if os.environ.get("FRED_API_KEY"):
        try:
            r = requests.get("https://api.stlouisfed.org/fred/series",
                             params={"series_id": "UNRATE", "api_key": os.environ["FRED_API_KEY"], "file_type": "json"},
                             timeout=30)
            if r.status_code == 200 and r.json().get("seriess"):
                ok.append("FRED: API key accepted")
            else:
                problems.append(f"FRED_API_KEY: rejected (HTTP {r.status_code})")
        except Exception as e:
            problems.append(f"FRED: request failed ({type(e).__name__})")

    if os.environ.get("BLS_CONTACT_EMAIL"):
        try:
            ua = f"county-unemployment-research/0.1 ({os.environ['BLS_CONTACT_EMAIL']})"
            r = requests.get("https://download.bls.gov/pub/time.series/la/la.measure",
                             headers={"User-Agent": ua}, timeout=30)
            if r.status_code == 200 and "measure_code" in r.text:
                ok.append("BLS: download.bls.gov reachable with contact User-Agent")
            else:
                problems.append(f"BLS: download failed (HTTP {r.status_code})")
        except Exception as e:
            problems.append(f"BLS: request failed ({type(e).__name__})")

    if os.environ.get("PREFECT_API_KEY") and os.environ.get("PREFECT_API_URL"):
        try:
            r = requests.get(os.environ["PREFECT_API_URL"].rstrip("/") + "/health",
                             headers={"Authorization": f"Bearer {os.environ['PREFECT_API_KEY']}"}, timeout=30)
            (ok if r.status_code == 200 else warnings).append(f"Prefect Cloud: health HTTP {r.status_code}")
        except Exception as e:
            warnings.append(f"Prefect Cloud: request failed ({type(e).__name__})")

    for m in ok:
        print("OK     ", m)
    for m in warnings:
        print("WARN   ", m)
    for m in problems:
        print("PROBLEM", m)
    print("Preflight", "FAILED" if problems else "PASSED")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
