# Build Log

Resume point for the staged build. Times are UTC wall clock.

## Stage 1 — Pipeline (restart under simplicity rules)

**Restarted under simplicity rules; earlier phases 0–3 (commits 1b5840e..3889644) are superseded.** The user deleted
the earlier, more complex files; the phase 0 commit of this restart records those deletions together with the
CLAUDE.md and agent-file edits that add the simplicity rules. Verified facts from the earlier log were re-checked
before reuse (noted per phase).

- **Start:** 2026-10-20 21:51:05 UTC

### Checklist

- [x] Phase 0 — Scaffold + preflight
- [ ] Phase 1 — LAUS ingest -> raw
- [ ] Phase 2 — LAUS staging + tests
- [ ] Phase 3 — County reference + month spine + geography gaps
- [ ] Phase 4 — FRED
- [ ] Phase 5 — Panel + target + leakage check
- [ ] Phase 6 — Wrap-up

### Phase 0 — Scaffold + preflight

- uv 0.13.4, Python 3.12 (`.python-version`), non-packaged uv project (`[tool.uv] package = false`), imports as `src.*`.
- Stage 1 dependencies only: duckdb 1.5.6, prefect 3.8.8, requests, pyyaml, python-dotenv, pytest.
- Re-verified: Prefect 3.8.8's temporary local server crashes with fastapi 0.156 (`Body(min_items=...)` removed).
  Pinned `fastapi>=0.139,<0.150` (resolves 0.149.0); a test flow then runs.
- One helper module `src/utils/helpers.py` (config via `yaml.safe_load`, MotherDuck connect, retrying GET, temp download,
  SQL-file runner, state FIPS table). No custom config loader, no retention/pruning, no DB-size warnings.
- Config deviations from CLAUDE.md section 5 (simplicity rules): dropped the `storage` block and
  `fred_use_initial_release`; renamed `fred_fallback_lag_months` to `fred_lag_months` (it is now the only FRED timing rule).
- No `src/features/` package: all feature logic is plain SQL in `sql/marts/`.
- Preflight: MotherDuck authenticated, FRED key accepted, BLS reachable with contact User-Agent.
  `PREFECT_API_KEY`/`PREFECT_API_URL` not set -> warning; flows run locally.
- Elapsed at end of phase 0: ~4 min.
