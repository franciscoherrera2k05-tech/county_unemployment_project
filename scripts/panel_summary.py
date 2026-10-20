"""Print a descriptive summary of marts.county_month_panel (no modeling), as Markdown for docs/BUILD_LOG.md.

What it does: row/county counts, date range, share of counties and labor force excluded for geography problems,
positive-label rate overall and by year (default threshold), and missing values per feature.
In: the live MotherDuck tables. Out: Markdown text on stdout.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.helpers import connect, load_config  # noqa: E402

con = connect()
threshold = load_config()["target"]["threshold_pp"]
rows, counties, first, last = con.execute("""SELECT count(*), count(DISTINCT county_fips), min(month), max(month)
    FROM marts.county_month_panel""").fetchone()
print(f"- Rows: {rows:,}; counties: {counties:,}; months: {first} to {last}")

excluded, with_data, lf_excluded, lf_total = con.execute("""SELECT
    count(*) FILTER (WHERE status <> 'ok' AND months_with_data > 0), count(*) FILTER (WHERE months_with_data > 0),
    sum(avg_labor_force) FILTER (WHERE status <> 'ok'), sum(avg_labor_force) FROM ref.geography_gap_report""").fetchone()
no_data = con.execute("SELECT count(*) FROM ref.geography_gap_report WHERE months_with_data = 0").fetchone()[0]
print(f"- Excluded for geography: {excluded} of {with_data:,} counties with data ({100 * excluded / with_data:.3f}%), "
      f"{100 * lf_excluded / lf_total:.4f}% of summed average labor force; plus {no_data} reference counties with no data")

labeled, positive_rate = con.execute("SELECT count(y_label), avg(y_label) FROM marts.county_month_panel").fetchone()
print(f"- Positive-label rate (y_delta_3m >= {threshold} pp): {100 * positive_rate:.1f}% of {labeled:,} labeled rows")
print("\n| Year | Labeled rows | Positive rate |\n|---|---|---|")
for year, n, rate in con.execute("""SELECT year(month), count(y_label), avg(y_label) FROM marts.county_month_panel
        GROUP BY 1 ORDER BY 1""").fetchall():
    print(f"| {year} | {n:,} | {'' if rate is None else f'{100 * rate:.1f}%'} |")

print("\n| Column | Missing rows | Missing % |\n|---|---|---|")
for (column,) in con.execute("SELECT column_name FROM (DESCRIBE marts.county_month_panel)").fetchall():
    missing = con.execute(f'SELECT count(*) - count("{column}") FROM marts.county_month_panel').fetchone()[0]
    if missing:
        print(f"| {column} | {missing:,} | {100 * missing / rows:.2f}% |")
print("\n(Columns not listed have no missing values.)")
