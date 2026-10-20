import duckdb

from src.ingest.laus import filter_sql

SAMPLE = (
    "series_id                     \tyear\tperiod\t       value\tfootnote_codes\r\n"
    "LAUCN540010000000003          \t2011\tM01\t         6.5\t\r\n"
    "LAUCN540010000000003          \t2012\tM01\t         6.4\t\r\n"
    "LAUCN540010000000003          \t2012\tM13\t         6.0\t\r\n"
    "LAUCN540010000000007          \t2012\tM01\t        55.0\t\r\n"
    "LAUCN010010000000003          \t2012\tM01\t         5.0\t\r\n"
    "LAUCN540010000000004          \t2025\tM10\t           -\tG\r\n"
)


def test_filter_sql(tmp_path):
    p = tmp_path / "la.data"
    p.write_bytes(SAMPLE.encode())
    rows = duckdb.connect().execute(filter_sql(str(p), ["54"], 2012) + " ORDER BY 1,2,3").fetchall()
    assert rows == [
        ("LAUCN540010000000003", "2012", "M01", "6.4", None),
        ("LAUCN540010000000003", "2012", "M13", "6.0", None),  # M13 removed in staging, not raw
        ("LAUCN540010000000004", "2025", "M10", "-", "G"),
    ]
