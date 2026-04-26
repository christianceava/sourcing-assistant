"""
Build a unified per-ASIN dataset by joining:
  1) Frontier sales dashboard (units sold, sales $, profit, margin, ROI, BSR, refunds, sessions)
  2) MAIN The One Sheet purchase log (vendor, cost per unit, units purchased, prep center, payment method, repeat-buy frequency)

Output: data/joined.csv (one row per ASIN, with all aggregated metrics)
        data/asins.txt    (one ASIN per line, ready for Keepa)
"""
import csv, json, re
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
OUT_DIR = SCRIPT_DIR
# Inputs: by default the two CSVs live alongside this script in `data/`.
# Override via environment variables SALES_CSV / BUY_CSV if needed.
import os
SALES_CSV = Path(os.environ.get('SALES_CSV') or next(
    (p for p in [SCRIPT_DIR / 'sales.csv',
                 *SCRIPT_DIR.glob('*Frontier*Dashboard*.csv'),
                 *ROOT.glob('*Frontier*Dashboard*.csv')] if p.exists()),
    SCRIPT_DIR / 'sales.csv'))
BUY_CSV = Path(os.environ.get('BUY_CSV') or next(
    (p for p in [SCRIPT_DIR / 'buy.csv',
                 *SCRIPT_DIR.glob('*Purchase*Sheet*.csv'),
                 *SCRIPT_DIR.glob('*One*Sheet*.csv'),
                 *ROOT.glob('*Purchase*Sheet*.csv'),
                 *ROOT.glob('*One*Sheet*.csv')] if p.exists()),
    SCRIPT_DIR / 'buy.csv'))


def num(s):
    if s is None: return None
    s = str(s).strip().replace('"', '').replace(' ', '').replace('\u00a0', '')
    if not s or s == '-': return None
    try: return float(s)
    except ValueError: return None


def money(s):
    if s is None: return None
    s = str(s).strip().replace('$', '').replace(',', '').replace(' ', '')
    if not s: return None
    try: return float(s)
    except ValueError: return None


def parse_sales():
    rows = {}
    with SALES_CSV.open(encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';', quotechar='"')
        headers = next(reader)
        headers = [h.replace('\ufeff', '').strip() for h in headers]
        for r in reader:
            if len(r) < 5: continue
            row = dict(zip(headers, r))
            asin = (row.get('ASIN') or '').strip()
            if not asin or len(asin) != 10: continue
            title = row.get('Product', '')
            cog_m = re.search(r'COG[:\s]+([\d.]+)', title)
            price_m = re.search(r'Price[:\s]+([\d.]+)', title)
            rec = {
                'asin': asin,
                'sku': row.get('SKU', '').strip(),
                'title': re.sub(r'COG[:\s]+[\d.]+\s*/\s*Price[:\s]+[\d.]+\s*$', '', title).strip(),
                'units_sold': num(row.get('Units')),
                'refunds': num(row.get('Refunds')),
                'sales_usd': num(row.get('Sales')),
                'pct_refunds': num(row.get('% Refunds')),
                'amazon_fees': num(row.get('Amazon fees')),
                'cog_total': num(row.get('Cost of Goods')),
                'gross_profit': num(row.get('Gross profit')),
                'net_profit': num(row.get('Net profit')),
                'estimated_payout': num(row.get('Estimated payout')),
                'margin_pct': num(row.get('Margin')),
                'roi_pct': num(row.get('ROI')),
                'bsr': num(row.get('BSR')),
                'sessions': num(row.get('Sessions')),
                'unit_session_pct': num(row.get('Unit Session Percentage')),
                'cog_per_unit': float(cog_m.group(1)) if cog_m else None,
                'sell_price': float(price_m.group(1)) if price_m else None,
            }
            if asin in rows:
                prev = rows[asin]
                for k in ('units_sold', 'refunds', 'sales_usd', 'amazon_fees', 'cog_total',
                         'gross_profit', 'net_profit', 'estimated_payout', 'sessions'):
                    if rec[k] is not None and prev.get(k) is not None:
                        prev[k] = (prev[k] or 0) + rec[k]
                    elif rec[k] is not None:
                        prev[k] = rec[k]
                if (rec['units_sold'] or 0) > (prev.get('_top_units') or 0):
                    prev['cog_per_unit'] = rec['cog_per_unit'] or prev.get('cog_per_unit')
                    prev['sell_price'] = rec['sell_price'] or prev.get('sell_price')
                    prev['title'] = rec['title']
                    prev['bsr'] = rec['bsr'] or prev.get('bsr')
                    prev['_top_units'] = rec['units_sold']
            else:
                rec['_top_units'] = rec.get('units_sold')
                rows[asin] = rec
    for r in rows.values():
        r.pop('_top_units', None)
    return rows


def parse_buy():
    by_asin = defaultdict(lambda: {
        'purchase_dates': [], 'vendors': defaultdict(int), 'costs_per_unit': [],
        'be_prices': [], 'units_purchased_total': 0, 'rows_total': 0, 'rows_canceled': 0,
        'prep_centers': defaultdict(int), 'payment_methods': defaultdict(int),
        'discount_codes': defaultdict(int), 'titles': set(),
    })
    with BUY_CSV.open(encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        for row in reader:
            if len(row) < 12: continue
            asin = row[2].strip().upper() if len(row) > 2 else ''
            if not asin or len(asin) != 10: continue
            d = by_asin[asin]
            d['rows_total'] += 1
            status = (row[17] if len(row) > 17 else '').strip().lower()
            if 'cancel' in status:
                d['rows_canceled'] += 1
                continue
            date = row[0].strip() if len(row) > 0 else ''
            if date: d['purchase_dates'].append(date)
            title = row[1].strip().replace('\n', ' ').replace('\r', ' ') if len(row) > 1 else ''
            if title and len(title) > 5: d['titles'].add(title[:200])
            vendor = (row[5] if len(row) > 5 else '').strip()
            if vendor: d['vendors'][vendor] += 1
            try:
                u = int(row[6]) if len(row) > 6 and row[6].strip() else 0
                d['units_purchased_total'] += u
            except ValueError: pass
            be = money(row[9] if len(row) > 9 else '')
            cost = money(row[10] if len(row) > 10 else '')
            if be is not None: d['be_prices'].append(be)
            if cost is not None: d['costs_per_unit'].append(cost)
            pay = (row[11] if len(row) > 11 else '').strip()
            if pay: d['payment_methods'][pay] += 1
            disc = (row[12] if len(row) > 12 else '').strip()
            if disc: d['discount_codes'][disc] += 1
            prep = (row[16] if len(row) > 16 else '').strip()
            if prep: d['prep_centers'][prep] += 1
    return dict(by_asin)


def avg(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 4) if xs else None


def join(sales, buys):
    all_asins = set(sales) | set(buys)
    joined = []
    for asin in sorted(all_asins):
        s = sales.get(asin, {})
        b = buys.get(asin, {})
        rec = {
            'asin': asin,
            'title': s.get('title') or (next(iter(b.get('titles', set())), '') if b else ''),
            'units_sold': s.get('units_sold'),
            'sales_usd': s.get('sales_usd'),
            'sell_price': s.get('sell_price'),
            'cog_per_unit_listed': s.get('cog_per_unit'),
            'gross_profit': s.get('gross_profit'),
            'net_profit': s.get('net_profit'),
            'margin_pct': s.get('margin_pct'),
            'roi_pct': s.get('roi_pct'),
            'pct_refunds': s.get('pct_refunds'),
            'bsr': s.get('bsr'),
            'sessions': s.get('sessions'),
            'unit_session_pct': s.get('unit_session_pct'),
            'purchase_count': len(b.get('purchase_dates', [])),
            'cancelled_purchases': b.get('rows_canceled', 0),
            'total_units_bought': b.get('units_purchased_total', 0),
            'avg_cost_per_unit': avg(b.get('costs_per_unit', [])),
            'min_cost_per_unit': min(b.get('costs_per_unit', []) or [None]) if b.get('costs_per_unit') else None,
            'max_cost_per_unit': max(b.get('costs_per_unit', []) or [None]) if b.get('costs_per_unit') else None,
            'avg_be_price': avg(b.get('be_prices', [])),
            'top_vendor': max(b.get('vendors', {}).items(), key=lambda x: x[1])[0] if b.get('vendors') else '',
            'all_vendors': '|'.join(f'{v}({c})' for v, c in sorted(b.get('vendors', {}).items(), key=lambda x: -x[1])),
            'top_payment': max(b.get('payment_methods', {}).items(), key=lambda x: x[1])[0] if b.get('payment_methods') else '',
            'discount_codes': '|'.join(b.get('discount_codes', {}).keys()),
            'prep_centers_used': '|'.join(b.get('prep_centers', {}).keys()),
            'first_purchase': sorted(b.get('purchase_dates', []))[0] if b.get('purchase_dates') else '',
            'last_purchase': sorted(b.get('purchase_dates', []))[-1] if b.get('purchase_dates') else '',
        }
        if rec['avg_cost_per_unit'] and rec['sell_price']:
            rec['computed_roi_pct'] = round(((rec['sell_price'] - rec['avg_cost_per_unit']) / rec['avg_cost_per_unit']) * 100, 2)
        else:
            rec['computed_roi_pct'] = None
        joined.append(rec)
    return joined


def main():
    print("Parsing sales sheet...")
    sales = parse_sales()
    print(f"  {len(sales)} unique ASINs in sales sheet")

    print("Parsing buy sheet...")
    buys = parse_buy()
    print(f"  {len(buys)} unique ASINs in buy sheet")

    print("Joining...")
    joined = join(sales, buys)

    overlap = sum(1 for r in joined if r['units_sold'] is not None and r['purchase_count'] > 0)
    sales_only = sum(1 for r in joined if r['units_sold'] is not None and r['purchase_count'] == 0)
    buy_only = sum(1 for r in joined if r['units_sold'] is None and r['purchase_count'] > 0)
    print(f"  Sales+Buy:  {overlap} ASINs (perfect match)")
    print(f"  Sales only: {sales_only} ASINs")
    print(f"  Buy only:   {buy_only} ASINs")
    print(f"  TOTAL:      {len(joined)} ASINs")

    out = OUT_DIR / 'joined.csv'
    fields = list(joined[0].keys())
    with out.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(joined)
    print(f"\nWrote {out}")

    asin_file = OUT_DIR / 'asins.txt'
    with asin_file.open('w') as f:
        for r in joined: f.write(r['asin'] + '\n')
    print(f"Wrote {asin_file} ({len(joined)} ASINs)")

    print("\n=== TIER BREAKDOWN (units sold) ===")
    units = sorted([r['units_sold'] for r in joined if r['units_sold']], reverse=True)
    if units:
        print(f"  Top 10 ASINs sold: {units[:10]}")
        print(f"  Median units: {units[len(units)//2]}")
        print(f"  Total ASINs with sales: {len(units)}")


if __name__ == '__main__':
    main()
