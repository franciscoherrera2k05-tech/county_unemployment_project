# Newest unlabeled county_month rows + models/current.txt -> <schema>.predictions; --outcomes: predictions -> outcomes.
import argparse, datetime
from pathlib import Path
import duckdb, joblib, pandas as pd, pyarrow as pa

ROOT = Path(__file__).resolve().parent
LATEST_LAUS = "select snapshot_hash from raw.laus_vintages order by snapshot_date desc, loaded_at desc limit 1"
THRESHOLD_PP = 0.5  # same as dbt var threshold_pp

OUTCOMES_SQL = """
insert into {schema}.outcomes
-- one row per full (non --dev) snapshot x county x month: the unemployment rate as published in that snapshot
with rates as (
    select snapshot_hash, loaded_at, substr(series_id, 6, 5) as county_fips,
           make_date(year::int, substr(period, 2)::int, 1) as month, try_cast(nullif(value, '-') as double) as rate
    from raw.laus_vintages
    where right(series_id, 2) = '03' and snapshot_hash not like '%-dev'
),
-- one row per county x month: the FIRST snapshot in which that month's rate exists
first_snap as (
    select county_fips, month, arg_min(snapshot_hash, loaded_at) as snapshot_hash
    from rates where rate is not null group by county_fips, month
)
select p.month, p.county_fips, p.model_version, p.score, p.flagged, p.snapshot_hash as prediction_snapshot_hash,
       f.snapshot_hash as outcome_snapshot_hash, r0.rate as rate_t, r3.rate as rate_t3,
       round(r3.rate - r0.rate, 1) as actual_delta_3m, round(r3.rate - r0.rate, 1) >= {threshold} as actual_label,
       current_date as run_date
from {schema}.predictions p
-- join: predictions (one row per month x county x model x snapshot) to first_snap (one row per county x month)
-- on county and month = t + 3. Result: one row per prediction whose answer exists (others drop out until later).
join first_snap f on f.county_fips = p.county_fips and f.month = cast(p.month + interval 3 month as date)
-- join: + rates (one row per snapshot x county x month): rate at t+3 from that first snapshot. Still one per prediction.
join rates r3 on r3.snapshot_hash = f.snapshot_hash and r3.county_fips = p.county_fips and r3.month = f.month
-- join: + rates again: rate at t from the SAME snapshot. Still one row per prediction.
join rates r0 on r0.snapshot_hash = f.snapshot_hash and r0.county_fips = p.county_fips and r0.month = p.month
-- append once: skip predictions that already have an outcome row (anti-join on the prediction key)
where not exists (select 1 from {schema}.outcomes o where o.month = p.month and o.county_fips = p.county_fips
                  and o.model_version = p.model_version and o.prediction_snapshot_hash = p.snapshot_hash)
"""


def load_bundle():
    """Return the active model bundle, or None if models/current.txt is missing."""
    current = ROOT / "models" / "current.txt"
    if not current.exists():
        return None
    return joblib.load(ROOT / "models" / f"{current.read_text().strip()}.joblib")


def model_age_months(con, bundle):
    """Months between the model's trained_through and the newest labeled month in the mart."""
    newest = pd.Timestamp(con.execute("select max(month) from marts.county_month where is_labeled").fetchone()[0])
    trained = pd.Timestamp(str(bundle["trained_through"]))
    return (newest.year - trained.year) * 12 + newest.month - trained.month


def score(con, bundle, schema="main", dry_run=False):
    rows = con.execute("""select * from marts.county_month
        where month = (select max(month) from marts.county_month) and not is_labeled""").df()
    scores = bundle["model"].predict_proba(rows[bundle["features"]])[:, 1]
    out = pd.DataFrame({"month": rows["month"].dt.date, "county_fips": rows["county_fips"],
                        "model_version": bundle["version"], "score": scores,
                        "flagged": scores >= bundle["threshold"],
                        "snapshot_hash": con.execute(LATEST_LAUS).fetchone()[0],
                        "run_date": datetime.date.today()})
    print(f"scored {len(out)} rows for month {out['month'].max()}, flagged {int(out['flagged'].sum())}")
    if dry_run:
        print(out.sort_values("score", ascending=False).head(5).to_string(index=False))
        return out
    con.execute(f"create schema if not exists {schema}")
    con.execute(f"""create table if not exists {schema}.predictions (month date, county_fips text, model_version text,
        score double, flagged boolean, snapshot_hash text, run_date date)""")
    key = [out["month"].iloc[0], bundle["version"], out["snapshot_hash"].iloc[0]]
    if con.execute(f"""select count(*) from {schema}.predictions
            where month = ? and model_version = ? and snapshot_hash = ?""", key).fetchone()[0]:
        print("predictions already appended for this month, model and snapshot; skipping")
        return out
    con.register("out_df", pa.Table.from_pandas(out, preserve_index=False))
    con.execute(f"insert into {schema}.predictions select * from out_df")
    print(f"appended {len(out)} rows to {schema}.predictions")
    return out


def append_outcomes(con, schema="main"):
    if not con.execute("select count(*) from information_schema.tables where table_catalog = 'unemployment' "
                       "and table_schema = ? and table_name = 'predictions'", [schema]).fetchone()[0]:
        print("no predictions table yet; nothing to compare")
        return
    con.execute(f"""create table if not exists {schema}.outcomes (month date, county_fips text, model_version text,
        score double, flagged boolean, prediction_snapshot_hash text, outcome_snapshot_hash text, rate_t double,
        rate_t3 double, actual_delta_3m double, actual_label boolean, run_date date)""")
    n = con.execute(OUTCOMES_SQL.format(schema=schema, threshold=THRESHOLD_PP)).fetchone()[0]
    print(f"appended {n} rows to {schema}.outcomes")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="score and print, write nothing")
    p.add_argument("--outcomes", action="store_true", help="append outcomes for predictions whose t+3 now exists")
    args = p.parse_args()
    con = duckdb.connect("md:unemployment")
    bundle = load_bundle()
    if args.outcomes:
        append_outcomes(con)
    elif bundle is None:
        print("WARNING: models/current.txt missing; nothing to score")
    else:
        age = model_age_months(con, bundle)
        if age > 6:
            print(f"WARNING: active model {bundle['version']} is {age} months old (retrain every 6 months)")
        score(con, bundle, dry_run=args.dry_run)
