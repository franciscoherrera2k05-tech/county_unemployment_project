"""Configuration loading. All thresholds, dates and state lists come from config/settings.yaml."""
from __future__ import annotations

import datetime as dt
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config" / "settings.yaml"

load_dotenv(ROOT / ".env", override=False)


@lru_cache(maxsize=1)
def load_config(path: str | None = None) -> dict[str, Any]:
    with open(path or CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    ps = cfg["data"]["panel_start"]
    if isinstance(ps, str):
        cfg["data"]["panel_start"] = dt.date.fromisoformat(ps)
    return cfg


def env(name: str, required: bool = True) -> str | None:
    val = os.environ.get(name)
    if required and not val:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return val


def resolve_states(override: list[str] | str | None = None) -> list[str] | None:
    """Return a list of state abbreviations, or None meaning all states."""
    states = override if override is not None else load_config()["data"]["states"]
    if states in (None, "all", "ALL"):
        return None
    if isinstance(states, str):
        states = [s.strip() for s in states.split(",") if s.strip()]
    return [s.upper() for s in states]
