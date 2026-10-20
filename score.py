# marts.county_month newest unlabeled rows + models/current.txt model -> appended to <schema>.predictions.
import argparse, datetime
from pathlib import Path
import duckdb, joblib, pandas as pd, pyarrow as pa

ROOT = Path(__file__).resolve().parent
LATEST_LAUS = "select snapshot_hash from raw.laus_vintages order by snapshot_date desc, loaded_at desc limit 1"


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


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="score and print, write nothing")
    args = p.parse_args()
    con = duckdb.connect("md:unemployment")
    bundle = load_bundle()
    if bundle is None:
        print("WARNING: models/current.txt missing; nothing to score")
    else:
        age = model_age_months(con, bundle)
        if age > 6:
            print(f"WARNING: active model {bundle['version']} is {age} months old (retrain every 6 months)")
        score(con, bundle, dry_run=args.dry_run)
