"""Unit tests for sql/marts/county_month_panel.sql on synthetic data (in-memory DuckDB, real SQL files).

In: 3 fake counties (2 in one state) with monthly LAUS values 2012-2016, and fake monthly FRED raw data for all 7 series.
Out: pass/fail on (1) the target definition (rounded pp change vs. threshold) and (2) the leakage check: changing every
LAUS value after month t and every FRED value not yet available at decision date d_t leaves all features at t unchanged.
"""
import duckdb

from src.utils.helpers import load_config, run_sql_file

T = "2015-06-01"   # the prediction month we protect


def build(con, cfg):
    run_sql_file(con, "staging/fred_monthly.sql", cfg)
    run_sql_file(con, "staging/fred_by_available_month.sql", cfg)
    run_sql_file(con, "marts/county_month_panel.sql", cfg)


def make_synthetic(cfg):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw; CREATE SCHEMA ref; CREATE SCHEMA staging; CREATE SCHEMA marts;")
    con.execute("""CREATE TABLE ref.county AS SELECT * FROM (VALUES
        ('01001', '01', 'AL', 'A'), ('01003', '01', 'AL', 'B'), ('54001', '54', 'WV', 'C')
        ) t(county_fips, state_fips, state_abbr, county_name)""")
    con.execute("CREATE TABLE ref.geography_gap_report AS SELECT county_fips, 'ok' AS status FROM ref.county")
    con.execute("""CREATE TABLE ref.month_spine AS SELECT county_fips, CAST(m AS DATE) AS month
        FROM ref.county, generate_series(DATE '2012-01-01', DATE '2016-12-01', INTERVAL 1 MONTH) g(m)""")
    # rates to one decimal that wiggle by county and month; counts consistent with the rate
    con.execute("""CREATE TABLE staging.laus_county_month AS
        SELECT county_fips, state_fips, month,
               round(5 + 2 * sin(date_diff('month', DATE '2012-01-01', month) + hash(county_fips) % 7), 1)
                   AS unemployment_rate,
               10000.0 AS labor_force
        FROM ref.month_spine JOIN ref.county USING (county_fips)""")
    con.execute("""ALTER TABLE staging.laus_county_month ADD COLUMN unemployed DOUBLE;
        ALTER TABLE staging.laus_county_month ADD COLUMN employed DOUBLE;
        UPDATE staging.laus_county_month SET unemployed = labor_force * unemployment_rate / 100,
                                             employed = labor_force * (1 - unemployment_rate / 100);""")
    con.execute(f"""CREATE TABLE raw.fred_observations AS
        SELECT s AS series_id, strftime(m, '%Y-%m-%d') AS date, CAST(100 + i + len(s) AS VARCHAR) AS value,
               '2026-01-01' AS realtime_start, now()::TIMESTAMP AS _ingested_at, DATE '2026-01-01' AS _snapshot_date,
               'synthetic' AS _source
        FROM unnest({list(cfg['timing']['fred_lag_months'])}) u(s),
             generate_series(DATE '2011-01-01', DATE '2016-12-01', INTERVAL 1 MONTH) WITH ORDINALITY g(m, i)""")
    return con


def panel_rows_at_t(con):
    return con.execute(f"SELECT * EXCLUDE (y_delta_3m, y_label) FROM marts.county_month_panel "
                       f"WHERE month = DATE '{T}' ORDER BY county_fips").fetchall()


def test_target_is_rounded_pp_change():
    cfg = load_config()
    con = make_synthetic(cfg)
    build(con, cfg)
    bad = con.execute(f"""SELECT count(*) FROM marts.county_month_panel p
        JOIN staging.laus_county_month f ON f.county_fips = p.county_fips AND f.month = p.month + INTERVAL 3 MONTH
        WHERE p.y_delta_3m <> round(f.unemployment_rate - p.rate, 1)
           OR p.y_label <> CAST(p.y_delta_3m >= {cfg['target']['threshold_pp']} AS INTEGER)""").fetchone()[0]
    assert bad == 0
    assert con.execute("SELECT count(*) FROM marts.county_month_panel WHERE month > '2016-09-01' "
                       "AND y_label IS NOT NULL").fetchone()[0] == 0     # last 3 months have no target yet


def test_no_feature_uses_future_data():
    cfg = load_config()
    con = make_synthetic(cfg)
    build(con, cfg)
    before = panel_rows_at_t(con)
    target_before = con.execute(f"SELECT y_delta_3m FROM marts.county_month_panel WHERE month = '{T}'").fetchall()
    decision = f"DATE '{T}' + INTERVAL {cfg['timing']['laus_release_lag_months']} MONTH"
    # scramble the future: LAUS after t, and FRED rows whose available month is after the decision date
    con.execute(f"UPDATE staging.laus_county_month SET unemployment_rate = 99 - unemployment_rate, "
                f"labor_force = 3 * labor_force WHERE month > DATE '{T}'")
    lags = " ".join(f"WHEN '{s}' THEN {k}" for s, k in cfg["timing"]["fred_lag_months"].items())
    con.execute(f"""UPDATE raw.fred_observations SET value = CAST(CAST(value AS DOUBLE) * 7 AS VARCHAR)
        WHERE CAST(date AS DATE) + to_months(CASE series_id {lags} END) > {decision}""")
    build(con, cfg)
    assert panel_rows_at_t(con) == before
    # sanity: the perturbation was real, because the target (which looks ahead by design) did change
    assert con.execute(f"SELECT y_delta_3m FROM marts.county_month_panel WHERE month = '{T}'").fetchall() != target_before
