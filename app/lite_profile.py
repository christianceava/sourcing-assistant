"""
Build a 'lite' winner profile from joined.csv alone (no Keepa pulls required).

Used so the web app can deploy instantly. Once Keepa pulls finish, the full
profile from build_profile.py overrides this.
"""
import csv, json, math
from pathlib import Path
from collections import Counter
from statistics import median


def _num(v):
    if v is None or v == '': return None
    try: return float(v)
    except (ValueError, TypeError): return None


def _band(values, label, low_pct=10, high_pct=90):
    values = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not values: return None
    values.sort()
    n = len(values)
    return {
        'label': label, 'n': n,
        'min': round(values[0], 2),
        'p10': round(values[max(0, int(n * low_pct / 100) - 1)], 2),
        'p25': round(values[max(0, int(n * 0.25) - 1)], 2),
        'median': round(values[n // 2], 2),
        'p75': round(values[min(n - 1, int(n * 0.75))], 2),
        'p90': round(values[min(n - 1, int(n * high_pct / 100))], 2),
        'max': round(values[-1], 2),
        'mean': round(sum(values) / n, 2),
    }


def assign_tier(row):
    units = _num(row.get('units_sold')) or 0
    purchases = _num(row.get('purchase_count')) or 0
    np_ = _num(row.get('net_profit')) or 0
    refunds = _num(row.get('pct_refunds')) or 0
    roi = _num(row.get('roi_pct')) or 0
    if refunds > 20 or np_ < -200: return 'LOSER'
    if units >= 200 and purchases >= 3 and np_ > 100: return 'CASH_COW'
    if purchases >= 3 and units > 30 and roi >= 15: return 'STEADY'
    if purchases <= 2: return 'ONE_AND_DONE'
    return 'STEADY' if np_ > 0 else 'ONE_AND_DONE'


def build_lite_profile(joined_csv_path):
    rows = list(csv.DictReader(open(joined_csv_path, encoding='utf-8')))
    for r in rows:
        r['_tier'] = assign_tier(r)

    winners = [r for r in rows if r['_tier'] in ('CASH_COW', 'STEADY')]
    losers = [r for r in rows if r['_tier'] in ('LOSER', 'ONE_AND_DONE')]

    def col(rs, k): return [_num(r.get(k)) for r in rs]

    profile = {
        'mode': 'lite',  # built from joined.csv only
        'generated_from_n_winners': len(winners),
        'generated_from_n_losers': len(losers),
        'tier_counts': dict(Counter(r['_tier'] for r in rows)),
        'bands': {
            'sell_price':         _band(col(winners, 'sell_price'), 'Sell price ($)'),
            'avg_cost_per_unit':  _band(col(winners, 'avg_cost_per_unit'), 'Cost per unit ($)'),
            'computed_roi_pct':   _band(col(winners, 'computed_roi_pct'), 'ROI %'),
            'margin_pct':         _band(col(winners, 'margin_pct'), 'Margin %'),
            'current_bsr':        _band(col(winners, 'bsr'), 'Current BSR'),
            'avg90_bsr':          _band(col(winners, 'bsr'), '90-day avg BSR'),
            'units_sold':         _band(col(winners, 'units_sold'), 'Units sold (12 mo)'),
            'purchase_count':     _band(col(winners, 'purchase_count'), 'Reorder count'),
            'pct_refunds':        _band(col(winners, 'pct_refunds'), 'Refund %'),
        },
        'preferred_brands_from_title': dict(Counter(
            (r.get('title', '').split()[0] if r.get('title') else '') for r in winners
        ).most_common(40)),
        'preferred_vendors': dict(Counter(
            (r.get('top_vendor') or '') for r in winners if r.get('top_vendor')
        ).most_common(40)),
        'avoid_vendors': dict(Counter(
            (r.get('top_vendor') or '') for r in losers if r.get('top_vendor')
        ).most_common(20)),
        'known_asins': sorted({r.get('asin') for r in rows if r.get('asin')}),
    }
    return profile


if __name__ == '__main__':
    here = Path(__file__).resolve().parent
    joined = here.parent / 'data' / 'joined.csv'
    out = here.parent / 'profile' / 'winner_profile_lite.json'
    p = build_lite_profile(joined)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(p, indent=2, default=str))
    print(f"Wrote {out}")
    for k, b in p['bands'].items():
        if b: print(f"  {k:25s} median={b['median']:>10}  p25-p75={b['p25']}-{b['p75']}  n={b['n']}")
    print("Top vendors:", list(p['preferred_vendors'].items())[:8])
