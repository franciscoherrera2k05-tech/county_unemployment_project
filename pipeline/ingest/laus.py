# BLS LAUS county file + la.area (download) -> appended to raw.laus_vintages / raw.laus_area in MotherDuck (text values).
import argparse, datetime, hashlib, os, tempfile
import duckdb, requests

BASE = "https://download.bls.gov/pub/time.series/la/"
DEV_STATES = ["54", "51", "39"]  # WV, VA, OH


def download(name, path):
    """Stream one BLS file to disk; return sha256 of its bytes."""
    headers = {"User-Agent": f"county-unemployment-project ({os.environ['BLS_CONTACT_EMAIL']})"}
    sha = hashlib.sha256()
    with requests.get(BASE + name, headers=headers, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                sha.update(chunk)
                f.write(chunk)
    return sha.hexdigest()


def connect():
    con = duckdb.connect("md:")
    con.execute("create database if not exists unemployment")
    con.execute("use unemployment")
    con.execute("create schema if not exists raw")
    con.execute("""create table if not exists raw.laus_vintages (series_id text, year text, period text, value text,
        footnote_codes text, snapshot_date date, snapshot_hash text, loaded_at timestamp)""")
    con.execute("""create table if not exists raw.laus_area (area_type_code text, area_code text, area_text text,
        snapshot_date date, snapshot_hash text, loaded_at timestamp)""")
    return con


def already_loaded(con, table, snapshot_hash):
    return con.execute(f"select count(*) from {table} where snapshot_hash = ?", [snapshot_hash]).fetchone()[0] > 0


def main(dev):
    con, today = connect(), datetime.date.today()
    with tempfile.TemporaryDirectory() as tmp:
        area_path, data_path = os.path.join(tmp, "la.area"), os.path.join(tmp, "la.data.64.County")
        area_hash = download("la.area", area_path)
        if already_loaded(con, "raw.laus_area", area_hash):
            print("la.area: hash already loaded, skipping")
        else:
            con.execute(f"""insert into raw.laus_area select trim(area_type_code), trim(area_code), trim(area_text),
                ?, ?, now() from read_csv('{area_path}', delim='\t', header=true, all_varchar=true,
                names=['area_type_code','area_code','area_text','display_level','selectable','sort_sequence'])""", [today, area_hash])
            print("la.area: appended", area_hash[:12])

        data_hash = download("la.data.64.County", data_path)
        # A --dev load holds only 3 states, so it gets its own hash and never blocks a later full load.
        snapshot_hash = data_hash + ("-dev" if dev else "")
        if already_loaded(con, "raw.laus_vintages", snapshot_hash):
            print("la.data.64.County: hash already loaded, skipping")
            return
        state_filter = f"and substr(trim(series_id), 6, 2) in {tuple(DEV_STATES)}" if dev else ""
        con.execute(f"""insert into raw.laus_vintages
            select trim(series_id), trim(year), trim(period), trim(value), trim(footnote_codes), ?, ?, now()
            from read_csv('{data_path}', delim='\t', header=true, all_varchar=true,
                names=['series_id','year','period','value','footnote_codes'])
            where trim(series_id) like 'LAUCN%' and right(trim(series_id), 2) in ('03','04','05','06')
              and trim(year) >= '2012' and trim(period) <> 'M13' {state_filter}""", [today, snapshot_hash])
        n = con.execute("select count(*) from raw.laus_vintages where snapshot_hash = ?", [snapshot_hash]).fetchone()[0]
        print(f"la.data.64.County: appended {n} rows, hash {snapshot_hash[:12]}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dev", action="store_true", help="only WV, VA, OH")
    main(p.parse_args().dev)
