# Build Log

Resume point for the staged build. Times are UTC wall clock.

## Stage 1 — Pipeline

- **Start:** 2026-10-20 21:22:40 UTC

### Checklist

- [x] Phase 0 — Scaffold + preflight
- [ ] Phase 1 — LAUS ingest -> raw
- [ ] Phase 2 — LAUS staging + tests
- [ ] Phase 3 — County reference + month spine + geography gaps
- [ ] Phase 4 — FRED
- [ ] Phase 5 — Panel + target + leakage test
- [ ] Phase 6 — Wrap-up

### Phase 0 — Scaffold + preflight

- `uv` was not installed in the Codespace; installed via the official astral installer (uv 0.13.4). Python 3.12 pinned in `.python-version`.
- Project is a non-packaged uv project (`[tool.uv] package = false`); code imported as `src.*` (pytest `pythonpath = ["."]`).
- Stage 1 dependencies only: duckdb 1.5.6, prefect 3.8.8, requests, pyyaml, python-dotenv, pytest.
- Preflight (`uv run python scripts/preflight.py`): MotherDuck authenticated, FRED key accepted, BLS download reachable with
  contact User-Agent. `PREFECT_API_KEY` / `PREFECT_API_URL` not set -> warning only; flows run locally (ephemeral Prefect).
- Integration tests are auto-skipped if `MOTHERDUCK_TOKEN` is absent (conftest).
- Elapsed at end of phase 0: ~6 min.
