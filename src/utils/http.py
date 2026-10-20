"""HTTP helpers: BLS User-Agent, retry with backoff, streaming download to a temp file."""
from __future__ import annotations

import logging
import os
import tempfile
import time

import requests

from src.utils.config import env

log = logging.getLogger(__name__)


def bls_headers() -> dict[str, str]:
    return {"User-Agent": f"county-unemployment-research/0.1 ({env('BLS_CONTACT_EMAIL')})"}


def get_with_retry(url: str, *, params=None, headers=None, tries: int = 4, backoff: float = 2.0,
                   timeout: int = 60, stream: bool = False) -> requests.Response:
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout, stream=stream)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except requests.RequestException as e:  # noqa: PERF203
            last = e
            wait = backoff ** i
            # never log params (may contain api keys)
            log.warning("GET %s failed (%s); retry %d/%d in %.0fs", url, type(e).__name__, i + 1, tries, wait)
            time.sleep(wait)
    raise RuntimeError(f"GET {url} failed after {tries} tries") from last


def download_to_temp(url: str, headers=None, suffix: str = ".txt") -> str:
    """Stream a URL to a temp file; caller must delete it (use try/finally)."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            r = get_with_retry(url, headers=headers, stream=True, timeout=300)
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    except Exception:
        os.remove(path)
        raise
    return path
