"""
Build the WINNER PROFILE — a statistical signature of Christian's proven OA winners.

Inputs:
  - data/joined.csv          (per-ASIN sales/buy aggregation)
  - keepa_cache/{ASIN}.json  (raw Keepa product data)

Outputs:
  - profile/winner_profile.json  (statistical bands + categorical filters used by the scoring engine)
  - profile/winner_table.csv     (per-ASIN feature table — internal, used to spot-check & retrain)

Tiers (assigned automatically from the data):
  CASH_COW    : top 20% by units_sold AND >= 3 reorders AND positive net_profit
  STEADY      : >= 3 reorders, units_sold > 30, ROI >= 15%
  ONE_AND_DONE: 1-2 purchases (didn't justify reorder)
  LOSER       : negative net profit OR >20% refund rate

The "active winner profile" used for scoring = CASH_COW + STEADY tiers.

Note: this is the assistant's internal knowledge — never surfaced to the VA. The VA only sees
candidate-ASIN scores out of 100 with reasoning.
"""
import csv, json, math
from pathlib import Path
from statistics import median, mean, pstdev
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parent.parent
JOINED = ROOT / "data" / "joined.csv"
CACHE = ROOT / "keepa_cache"
OUT = ROOT / "profile"
OUT.mkdir(exist_ok=True)


# ---------- Keepa decoding helpers ----------

KEEPA_PRICE_TYPES = [
    "AMAZON", "NEW", "USED", "SALES_RANK", "LISTPRICE", "COLLECTIBLE",
    "REFURBISHED", "NEW_FBM_SHIPPING", "LIGHTNING_DEAL", "WAREHOUSE", "NEW_FBA",
    "COUNT_NEW", "COUNT_USED", "COUNT_REFURBISHED", "COUNT_COLLECTIBLE",
    "EXTRA_INFO_UPDATES", "RATING", "COUNT_REVIEWS", "BUY_BOX_SHIPPING",
    "USED_NEW_SHIPPING", "USED_VERY_GOOD_SHIPPING", "USED_GOOD_SHIPPING",
    "USED_ACCEPTABLE_SHIPPING", "COLLECTIBLE_NEW_SHIPPING",
    "COLLECTIBLE_VERY_GOOD_SHIPPING", "COLLECTIBLE_GOOD_SHIPPING",
    "COLLECTIBLE_ACCEPTABLE_SHIPPING", "REFURBISHED_SHIPPING", "EBAY_NEW_SHIPPING",
    "EBAY_USED_SHIPPING", "TRADE_IN", "RENT", "BUY_BOX", "BUY_BOX_USED",
    "PRIME_EXCL"
]


def decode_csv_series(csv_arrays, idx):
    """Keepa csv arrays: [time1, val1, time2, val2, ...]. Returns list of (kpa_minute, val)."""
    if not csv_arrays or idx >= len(csv_arrays) or csv_arrays[idx] is None:
        return []
    arr = csv_arrays[idx]
    return [(arr[i], arr[i+1]) for i in range(0, len(arr), 2) if arr[i+1] != -1]


def kpa_to_unix(t):
    """Keepa time minutes since 2011-01-01 -> unix sec."""
    return (t + 21564000) * 60


def estimate_monthly_sales_from_bsr(bsr, root_category):
    """Rough rule-of-thumb sales estimator (Jungle Scout / Helium 10 style) for top categories."""
    if not bsr or bsr <= 0:
        return None
    # Per-category multipliers (very rough; tune from observed data later)
    cat_factor = {
        'Health & Household': 1.4,
        'Grocery & Gourmet Food': 1.3,
        'Beauty & Personal Care': 1.2,
        'Sports & Outdoors': 1.0,
        'Home & Kitchen': 0.9,
        'Toys & Games': 0.8,
        'Pet Supplies': 1.0,
        'Clothing, Shoes & Jewelry': 0.6,
        'Office Products': 0.7,
        'Industrial & Scientific': 0.5,
    }.get(root_category, 0.9)
    if bsr < 100:    base = 6000
    elif bsr < 500:  base = 3000
    elif bsr < 1000: base = 1500
    elif bsr < 5000: base = 700
    elif bsr < 10000:base = 350
    elif bsr < 50000:base = 120
    elif bsr < 100000:base = 60
    elif bsr < 250000:base = 25
    else:            base = 10
    return int(base * cat_factor)


def features_from_keepa(kdata):
    """Extract a flat feature dict from one Keepa product object."""
    if not kdata or kdata.get('_not_found'):
        return None
    f = {}
    f['asin'] = kdata.get('asin')
    f['brand'] = (kdata.get('brand') or '').strip()
    f['manufacturer'] = (kdata.get('manufacturer') or '').strip()
    f['title'] = (kdata.get('title') or '')[:120]
    f['hazmat'] = bool(kdata.get('hazardousMaterialType'))
    f['is_adult'] = bool(kdata.get('isAdultProduct'))
    f['number_of_items'] = kdata.get('numberOfItems') or 1
    # Package weight in grams -> ounces
    pw_g = kdata.get('packageWeight')
    f['package_weight_oz'] = round(pw_g / 28.3495, 2) if pw_g else None
    # Dimensions (mm) -> longest side in inches
    dims = [kdata.get('packageHeight'), kdata.get('packageLength'), kdata.get('packageWidth')]
    dims = [d for d in dims if d]
    f['longest_side_in'] = round(max(dims) / 25.4, 1) if dims else None
    f['oversize_flag'] = (f['longest_side_in'] or 0) > 18 or (f['package_weight_oz'] or 0) > 320

    cat_tree = kdata.get('categoryTree') or []
    f['root_category'] = cat_tree[0]['name'] if cat_tree else ''
    f['leaf_category'] = cat_tree[-1]['name'] if cat_tree else ''
    f['category_depth'] = len(cat_tree)

    # Stats block (180-day window)
    stats = kdata.get('stats') or {}
    cur = stats.get('current') or []
    avg30 = stats.get('avg30') or []
    avg90 = stats.get('avg90') or []
    avg180 = stats.get('avg180') or []

    def _at(arr, idx):
        if arr and idx < len(arr) and arr[idx] not in (-1, None): return arr[idx]
        return None

    bb_idx = 18  # BUY_BOX_SHIPPING aka actual buy box price including shipping
    new_idx = 1  # NEW
    rank_idx = 3 # SALES_RANK
    review_idx = 17

    f['current_buy_box'] = (_at(cur, bb_idx) or 0) / 100 if _at(cur, bb_idx) else None
    f['avg30_buy_box'] = (_at(avg30, bb_idx) or 0) / 100 if _at(avg30, bb_idx) else None
    f['avg90_buy_box'] = (_at(avg90, bb_idx) or 0) / 100 if _at(avg90, bb_idx) else None
    f['avg180_buy_box'] = (_at(avg180, bb_idx) or 0) / 100 if _at(avg180, bb_idx) else None
    f['current_new'] = (_at(cur, new_idx) or 0) / 100 if _at(cur, new_idx) else None

    f['current_bsr'] = _at(cur, rank_idx)
    f['avg30_bsr'] = _at(avg30, rank_idx)
    f['avg90_bsr'] = _at(avg90, rank_idx)
    f['avg180_bsr'] = _at(avg180, rank_idx)

    f['review_count'] = _at(cur, review_idx)
    f['rating'] = (_at(cur, 16) or 0) / 10 if _at(cur, 16) else None  # RATING idx 16

    f['est_monthly_sales'] = estimate_monthly_sales_from_bsr(f['avg90_bsr'] or f['current_bsr'], f['root_category'])

    # Buy box stats
    f['buy_box_is_amazon'] = stats.get('buyBoxIsAmazon')
    f['buy_box_is_fba'] = stats.get('buyBoxIsFBA')
    f['buy_box_seller_id'] = stats.get('buyBoxSellerId')

    # Price volatility (CV of buy-box history over recent 180-day average)
    csvs = kdata.get('csv') or []
    bb_series = decode_csv_series(csvs, bb_idx)
    if len(bb_series) >= 5:
        prices = [p[1]/100 for p in bb_series[-180:] if p[1] > 0]
        if len(prices) >= 5 and mean(prices) > 0:
            f['price_cv_pct'] = round((pstdev(prices) / mean(prices)) * 100, 1)
        else:
            f['price_cv_pct'] = None
    else:
        f['price_cv_pct'] = None

    # Offer count (buyBoxEligibleOfferCounts is [new,used,collectible,refurb])
    bbeoc = kdata.get('buyBoxEligibleOfferCounts') or []
    f['live_offer_count_new'] = bbeoc[0] if len(bbeoc) > 0 else None

    # FBA fees
    fees = kdata.get('fbaFees') or {}
    f['fba_pick_pack_fee'] = (fees.get('pickAndPackFee') or 0) / 100 if fees.get('pickAndPackFee') else None
    f['fba_storage_class'] = fees.get('storageFeesByCategory')

    return f


# ---------- Tier assignment ----------

def assign_tier(row):
    units = float(row.get('units_sold') or 0)
    purchases = float(row.get('purchase_count') or 0)
    np_ = float(row.get('net_profit') or 0)
    refunds = float(row.get('pct_refunds') or 0)
    roi = float(row.get('roi_pct') or 0)
    bought = float(row.get('total_units_bought') or 0)
    if refunds > 20 or np_ < -200:
        return 'LOSER'
    # Top 20% by units sold AND >=3 reorders AND positive profit
    if units >= 200 and purchases >= 3 and np_ > 100:
        return 'CASH_COW'
    if purchases >= 3 and units > 30 and roi >= 15:
        return 'STEADY'
    if purchases <= 2:
        return 'ONE_AND_DONE'
    return 'STEADY' if np_ > 0 else 'ONE_AND_DONE'


# ---------- Profile aggregation ----------

def stats_band(values, label, low_pct=10, high_pct=90):
    values = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not values:
        return None
    values.sort()
    n = len(values)
    return {
        'label': label,
        'n': n,
        'min': round(values[0], 2),
        'p10': round(values[max(0, int(n * low_pct / 100) - 1)], 2),
        'p25': round(values[max(0, int(n * 0.25) - 1)], 2),
        'median': round(values[n // 2], 2),
        'p75': round(values[min(n - 1, int(n * 0.75))], 2),
        'p90': round(values[min(n - 1, int(n * high_pct / 100))], 2),
        'max': round(values[-1], 2),
        'mean': round(sum(values) / n, 2),
    }


def main():
    rows = list(csv.DictReader(JOINED.open()))
    print(f"Loaded {len(rows)} joined rows.")

    # Load Keepa features for every cached ASIN
    keepa = {}
    for jp in CACHE.glob('*.json'):
        try:
            kd = json.loads(jp.read_text())
            f = features_from_keepa(kd)
            if f and f.get('asin'):
                keepa[f['asin']] = f
        except Exception as e:
            print(f"  warn: {jp.name}: {e}")
    print(f"Loaded Keepa data for {len(keepa)} ASINs.")

    # Merge + tier
    enriched = []
    for r in rows:
        asin = r['asin']
        kf = keepa.get(asin)
        if not kf: continue
        rec = {**r, **kf}
        rec['tier'] = assign_tier(r)
        enriched.append(rec)
    print(f"Enriched {len(enriched)} ASINs with Keepa features.")

    # Tier counts
    tier_counts = Counter(r['tier'] for r in enriched)
    print(f"Tiers: {dict(tier_counts)}")

    # Save the per-ASIN winner table (internal)
    if enriched:
        keys = list(enriched[0].keys())
        with (OUT / 'winner_table.csv').open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for r in enriched: w.writerow(r)

    # Build the actual profile from CASH_COW + STEADY tiers
    winners = [r for r in enriched if r['tier'] in ('CASH_COW', 'STEADY')]
    losers = [r for r in enriched if r['tier'] in ('LOSER', 'ONE_AND_DONE')]
    print(f"Winners: {len(winners)} | Losers/ones: {len(losers)}")

    def collect(field, source=None, transform=None):
        src = source or winners
        out = []
        for r in src:
            v = r.get(field)
            if transform: v = transform(v) if v is not None else None
            if v is not None: out.append(v)
        return out

    # --- numeric bands ---
    profile = {
        'generated_from_n_winners': len(winners),
        'generated_from_n_losers': len(losers),
        'tier_counts': dict(tier_counts),
        'bands': {
            'sell_price':         stats_band(collect('sell_price'), 'Sell price ($)'),
            'avg_cost_per_unit':  stats_band(collect('avg_cost_per_unit'), 'Cost per unit ($)'),
            'computed_roi_pct':   stats_band(collect('computed_roi_pct'), 'ROI %'),
            'margin_pct':         stats_band(collect('margin_pct'), 'Margin %'),
            'current_bsr':        stats_band(collect('current_bsr'), 'Current BSR'),
            'avg90_bsr':          stats_band(collect('avg90_bsr'), '90-day avg BSR'),
            'avg180_bsr':         stats_band(collect('avg180_bsr'), '180-day avg BSR'),
            'review_count':       stats_band(collect('review_count'), 'Review count'),
            'rating':             stats_band(collect('rating'), 'Rating'),
            'live_offer_count':   stats_band(collect('live_offer_count_new'), 'Live FBA offers'),
            'price_cv_pct':       stats_band(collect('price_cv_pct'), 'Price volatility (CV %)'),
            'package_weight_oz':  stats_band(collect('package_weight_oz'), 'Pkg weight oz'),
            'longest_side_in':    stats_band(collect('longest_side_in'), 'Longest side in'),
            'est_monthly_sales':  stats_band(collect('est_monthly_sales'), 'Est. monthly sales'),
            'units_sold':         stats_band(collect('units_sold', transform=float), 'Units sold (12 mo)'),
            'purchase_count':     stats_band(collect('purchase_count', transform=float), 'Reorder count'),
        },
        'preferred_categories': dict(Counter([r.get('root_category', '') for r in winners if r.get('root_category')]).most_common()),
        'preferred_brands': dict(Counter([r.get('brand', '') for r in winners if r.get('brand')]).most_common(40)),
        'preferred_vendors': dict(Counter([r.get('top_vendor', '') for r in winners if r.get('top_vendor')]).most_common(40)),
        'avoid_brands': sorted({r.get('brand') for r in losers if r.get('brand')} - {r.get('brand') for r in winners if r.get('brand')}),
        'avoid_categories': dict(Counter([r.get('root_category', '') for r in losers if r.get('root_category')]).most_common(10)),
        'flags': {
            'pct_winners_hazmat': round(sum(1 for r in winners if r.get('hazmat'))/max(len(winners),1)*100, 1),
            'pct_winners_oversize': round(sum(1 for r in winners if r.get('oversize_flag'))/max(len(winners),1)*100, 1),
        },
    }

    (OUT / 'winner_profile.json').write_text(json.dumps(profile, indent=2, default=str))
    print(f"\nWrote {OUT / 'winner_profile.json'}")
    # Print quick summary
    for k, b in profile['bands'].items():
        if b: print(f"  {k:25s} median={b['median']:>10}  p25-p75={b['p25']}-{b['p75']}  n={b['n']}")
    print("\nTop categories:", list(profile['preferred_categories'].items())[:5])
    print("Top brands:    ", list(profile['preferred_brands'].items())[:8])


if __name__ == '__main__':
    main()
