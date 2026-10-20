# FRED API (7 national series, all observations since 2010) -> appended to raw.fred_vintages in MotherDuck (text values).
import datetime, hashlib, json, os
import duckdb, requests

SERIES = ["UNRATE", "FEDFUNDS", "CPIAUCSL", "T10Y2Y", "INDPRO", "PAYEMS", "ICSA"]
URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch(series_id):
    """Return the raw JSON body for one series."""
    params = {"series_id": series_id, "api_key": os.environ["FRED_API_KEY"], "file_type": "json",
              "observation_start": "2010-01-01"}
    r = requests.get(URL, params=params, timeout=60)
    r.raise_for_status()
    return r.content


def main():
    bodies = {s: fetch(s) for s in SERIES}
    snapshot_hash = hashlib.sha256(b"".join(bodies[s] for s in SERIES)).hexdigest()
    rows = [(s, o["date"], o["value"], o["realtime_start"])
            for s in SERIES for o in json.loads(bodies[s])["observations"]]

    con = duckdb.connect("md:")
    con.execute("create database if not exists unemployment")
    con.execute("use unemployment")
    con.execute("create schema if not exists raw")
    con.execute("""create table if not exists raw.fred_vintages (series_id text, date text, value text,
        realtime_start text, snapshot_date date, snapshot_hash text, loaded_at timestamp)""")
    if con.execute("select count(*) from raw.fred_vintages where snapshot_hash = ?", [snapshot_hash]).fetchone()[0]:
        print("fred: hash already loaded, skipping")
        return
    today, now = datetime.date.today(), datetime.datetime.now()
    con.execute("create temp table new_rows (series_id text, date text, value text, realtime_start text)")
    con.executemany("insert into new_rows values (?, ?, ?, ?)", rows)
    con.execute("insert into raw.fred_vintages select *, ?, ?, ? from new_rows", [today, snapshot_hash, now])
    print(f"fred: appended {len(rows)} rows, hash {snapshot_hash[:12]}")


if __name__ == "__main__":
    main()
