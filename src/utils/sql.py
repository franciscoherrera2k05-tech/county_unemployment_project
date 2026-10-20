"""Render SQL files in sql/ with config-driven parameters."""
from __future__ import annotations

from pathlib import Path

from src.utils.config import ROOT, load_config

SQL_DIR = ROOT / "sql"


def params(cfg: dict | None = None) -> dict:
    cfg = cfg or load_config()
    t = cfg["timing"]
    return {
        "laus_min_year": int(cfg["data"]["laus_min_year"]),
        "panel_start": str(cfg["data"]["panel_start"]),
        "laus_lag": int(t["laus_release_lag_months"]),
        "horizon": int(cfg["target"]["horizon_months"]),
        "threshold": float(cfg["target"]["threshold_pp"]),
    }


def render(rel_path: str, **overrides) -> str:
    p = {**params(), **overrides}
    return (SQL_DIR / rel_path).read_text().format(**p)


def pad_fips(code, width: int = 5) -> str:
    """Python twin of the SQL lpad rule: strip, left-pad with zeros."""
    return str(code).strip().zfill(width)
