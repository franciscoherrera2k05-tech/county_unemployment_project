# Build Log

Resume point for the staged build. Times are UTC wall clock.

## Stage 1 — Pipeline

- **Start:** 2026-10-20 21:22:40 UTC

### Checklist

- [x] Phase 0 — Scaffold + preflight
- [x] Phase 1 — LAUS ingest -> raw
- [x] Phase 2 — LAUS staging + tests
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

### Phase 1 — LAUS ingest -> raw

Verified facts (from `la.txt`, `la.series`, `la.measure`, `la.period`, `la.footnote`, and real rows, 2026-10-20):
- County series ID = `LA` + seasonal `U` + area_code `CN` + 5-digit county FIPS + `00000000` + 2-digit measure,
  e.g. `LAUCN540010000000003` (20 chars). FIPS = chars 6-10; state FIPS = chars 6-7; measure = last 2.
- Measures confirmed: 03 unemployment rate, 04 unemployment, 05 employment, 06 labor force (07-09 exist but not for county files we use).
- `M01`-`M12` months, `M13` annual average (kept in raw, excluded in staging). All county series are `U` (not seasonally adjusted).
- Data file is tab-delimited, fixed-width padded, CRLF line endings, header row. `la.data.64.County` = 337 MB.
- Missing value marker is `-` with footnote `X`. In the 2026-10-20 file it occurs only for **2025 M10** in every county:
  footnote G says October 2025 data were not collected due to the federal government shutdown.
- Footnote `P` (preliminary) appears on the latest month (2026 M08). Footnote codes kept.
- Latest county month in the file: 2026 M08 for nearly all counties; 3 counties (Los Angeles CA, DC, Miami-Dade FL) already have 2026 M09
  (they double as sub-state areas with earlier releases).
- `la.series` lists 78 Puerto Rico (72) county-equivalents; excluded because the spec's state table is 50 states + DC.
- Connecticut is already published as 9 planning regions (FIPS 09110-09190) back to 1990, so no CT discontinuity inside our window.
- Alaska: Valdez-Cordova (02261) ends 2019; Chugach (02063) and Copper River (02066) start 2020 -> geography gap (handled in phase 3).

Implementation:
- `src/ingest/laus.py` (`ingest_laus` flow): stream file to a temp file (deleted in `finally`), filter in DuckDB (`read_csv` all varchar,
  county series, requested states, measures 03-06, `year >= laus_min_year` = 2012), single `INSERT ... SELECT` into `raw.laus_county`.
- Idempotency: same-day rerun deletes that day's snapshot rows **for the loaded states only**, then inserts (one transaction);
  retention pruning keeps the latest `raw_snapshot_retention` snapshot dates.
- Smoke test WV/VA/OH: 209,760 rows (276 counties x 4 measures x 190 periods). Full run: 2,388,276 rows, 12,576 series (3,144 counties), 51 states.
  Rerunning WV on the same day left the total unchanged (idempotent). Full download+load ~25 s.
- Deviation: Prefect 3.8.8's local ephemeral server fails to import with fastapi 0.156 (`Body(min_items=...)` removed);
  pinned `fastapi>=0.139,<0.150` (resolved 0.149.0).
- Elapsed at end of phase 1: ~7 min.

### Phase 2 — LAUS staging + tests

- `sql/staging/laus_county_month.sql` -> `staging.laus_county_month` (553,075 rows). Latest snapshot **per series** (so a later
  partial smoke snapshot never hides other states). Pivots measures 03-06; `month` = first of month from `M01`-`M12`; `M13` excluded;
  `-` -> NULL with `is_missing_marker`; footnotes kept as a sorted comma list; `is_preliminary` from footnote `P`;
  `available_date = month + laus_release_lag_months` (2).
- SQL files are rendered with config parameters by `src/utils/sql.py` (no hardcoded thresholds/dates).
- Tests: unit (FIPS padding, month parsing, M13 exclusion, missing marker, preliminary flag, lag, latest snapshot) and
  integration (raw/staging grain, 5-char FIPS, rate in [0,100], available_date > month, nulls only where marker, LF >= employment,
  nulls only in 2025-10). 10 passed.
- Elapsed at end of phase 2: ~8 min.
