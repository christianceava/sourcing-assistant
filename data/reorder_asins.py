"""
Reorder asins.txt so the highest-priority ASINs (most units sold) come first.
This way the keepa fetcher pulls our biggest winners first and the profile
is well-grounded even if we don't finish all 383.
"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent

rows = list(csv.DictReader(open(ROOT / 'joined.csv')))


def priority(r):
    units = float(r.get('units_sold') or 0)
    purchases = float(r.get('purchase_count') or 0)
    bought_units = float(r.get('total_units_bought') or 0)
    # Composite priority: units sold dominates, but reorder frequency and bought volume matter
    return units + purchases * 30 + bought_units * 0.3


rows.sort(key=priority, reverse=True)
with open(ROOT / 'asins.txt', 'w') as f:
    for r in rows:
        f.write(r['asin'] + '\n')

print(f"Reordered {len(rows)} ASINs by priority.")
print("Top 10:")
for r in rows[:10]:
    print(f"  {r['asin']}  units={r.get('units_sold','-')}  buys={r.get('purchase_count','-')}  vendor={r.get('top_vendor','-')[:30]}")
