"""
Sourcing scoring engine.

Given a candidate ASIN and the WinnerProfile, calls Keepa and computes:
  - Match score 0-100 (weighted across criteria)
  - Per-criterion verdict + reasoning
  - Final recommendation: BUY / MAYBE / SKIP

Used by the Streamlit app (single ASIN + bulk).
"""
import json, gzip, urllib.request, urllib.error, time, math
from pathlib import Path
from statistics import mean, pstdev

KEEPA_BASE = "https://api.keepa.com/product"
TOKEN_URL = "https://api.keepa.com/token"


# ============== Keepa client ==============

class Keepa:
    def __init__(self, api_key, cache_dir=None):
        self.key = api_key
        self.cache_dir = Path(cache_dir) if cache_dir else None
        if self.cache_dir: self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get(self, url):
        req = urllib.request.Request(url, headers={'Accept-Encoding': 'gzip'})
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read()
            if r.headers.get('Content-Encoding') == 'gzip':
                data = gzip.decompress(data)
            return json.loads(data.decode('utf-8'))

    def tokens(self):
        return self._get(f"{TOKEN_URL}?key={self.key}")

    def product(self, asins, force_refresh=False):
        """Fetch one or more ASINs. Returns {asin: product_dict}.
           Uses cache when available unless force_refresh=True."""
        if isinstance(asins, str): asins = [asins]
        out = {}
        to_fetch = []
        for a in asins:
            cf = self.cache_dir / f"{a}.json" if self.cache_dir else None
            if cf and cf.exists() and not force_refresh:
                try:
                    out[a] = json.loads(cf.read_text())
                    continue
                except Exception: pass
            to_fetch.append(a)
        if to_fetch:
            url = (f"{KEEPA_BASE}?key={self.key}&domain=1&asin={','.join(to_fetch)}"
                   f"&stats=180&buybox=1&offers=20")
            data = self._get(url)
            for p in data.get('products') or []:
                a = (p.get('asin') or '').upper()
                out[a] = p
                if self.cache_dir:
                    (self.cache_dir / f"{a}.json").write_text(json.dumps(p, separators=(',',':')))
        return out


# ============== Feature extraction (mirrors profile/build_profile.py) ==============

def estimate_monthly_sales(bsr, root_cat):
    if not bsr or bsr <= 0: return None
    cat_factor = {
        'Health & Household': 1.4, 'Grocery & Gourmet Food': 1.3,
        'Beauty & Personal Care': 1.2, 'Sports & Outdoors': 1.0,
        'Home & Kitchen': 0.9, 'Toys & Games': 0.8,
        'Pet Supplies': 1.0, 'Clothing, Shoes & Jewelry': 0.6,
        'Office Products': 0.7, 'Industrial & Scientific': 0.5,
    }.get(root_cat, 0.9)
    if   bsr < 100:   base = 6000
    elif bsr < 500:   base = 3000
    elif bsr < 1000:  base = 1500
    elif bsr < 5000:  base = 700
    elif bsr < 10000: base = 350
    elif bsr < 50000: base = 120
    elif bsr < 100000:base = 60
    elif bsr < 250000:base = 25
    else:             base = 10
    return int(base * cat_factor)


def features(kdata):
    if not kdata: return None
    f = {'asin': kdata.get('asin')}
    f['title'] = (kdata.get('title') or '')[:120]
    f['brand'] = (kdata.get('brand') or '').strip()
    f['hazmat'] = bool(kdata.get('hazardousMaterialType'))
    f['is_adult'] = bool(kdata.get('isAdultProduct'))
    pw = kdata.get('packageWeight')
    f['package_weight_oz'] = round(pw / 28.3495, 2) if pw else None
    dims = [kdata.get('packageHeight'), kdata.get('packageLength'), kdata.get('packageWidth')]
    dims = [d for d in dims if d]
    f['longest_side_in'] = round(max(dims) / 25.4, 1) if dims else None
    f['oversize'] = (f['longest_side_in'] or 0) > 18 or (f['package_weight_oz'] or 0) > 320
    cat_tree = kdata.get('categoryTree') or []
    f['root_category'] = cat_tree[0]['name'] if cat_tree else ''
    f['leaf_category'] = cat_tree[-1]['name'] if cat_tree else ''
    s = kdata.get('stats') or {}
    cur, a30, a90, a180 = s.get('current') or [], s.get('avg30') or [], s.get('avg90') or [], s.get('avg180') or []
    def at(arr, i):
        if arr and i < len(arr) and arr[i] not in (-1, None): return arr[i]
        return None
    BB, NEW, RANK, REV, RATING = 18, 1, 3, 17, 16
    f['current_buy_box'] = (at(cur, BB) or 0) / 100 if at(cur, BB) else None
    f['avg90_buy_box'] = (at(a90, BB) or 0) / 100 if at(a90, BB) else None
    f['avg180_buy_box'] = (at(a180, BB) or 0) / 100 if at(a180, BB) else None
    f['current_new'] = (at(cur, NEW) or 0) / 100 if at(cur, NEW) else None
    f['current_bsr'] = at(cur, RANK)
    f['avg30_bsr'] = at(a30, RANK)
    f['avg90_bsr'] = at(a90, RANK)
    f['avg180_bsr'] = at(a180, RANK)
    f['review_count'] = at(cur, REV)
    f['rating'] = (at(cur, RATING) or 0) / 10 if at(cur, RATING) else None
    f['est_monthly_sales'] = estimate_monthly_sales(f['avg90_bsr'] or f['current_bsr'], f['root_category'])
    f['buy_box_is_amazon'] = s.get('buyBoxIsAmazon')
    f['buy_box_is_fba'] = s.get('buyBoxIsFBA')
    bbeoc = kdata.get('buyBoxEligibleOfferCounts') or []
    f['live_offer_count_new'] = bbeoc[0] if len(bbeoc) > 0 else None
    csvs = kdata.get('csv') or []
    if csvs and BB < len(csvs) and csvs[BB]:
        bb_pairs = [(csvs[BB][i], csvs[BB][i+1]) for i in range(0, len(csvs[BB]), 2) if csvs[BB][i+1] != -1]
        prices = [p[1]/100 for p in bb_pairs[-180:] if p[1] > 0]
        if len(prices) >= 5 and mean(prices) > 0:
            f['price_cv_pct'] = round((pstdev(prices) / mean(prices)) * 100, 1)
        else:
            f['price_cv_pct'] = None
    else:
        f['price_cv_pct'] = None
    return f


# ============== Scoring ==============

class Scorer:
    """Scores a candidate against the profile bands.

    Each criterion contributes a 0-100 sub-score. The final score is a weighted average.
    """
    def __init__(self, profile, strictness=0.5):
        self.p = profile
        self.strictness = max(0.0, min(1.0, strictness))
        # Strictness adjusts how much "outside the band" gets penalized.
        # 0.0 = lenient (loose net), 1.0 = strict (only near-clones).

    def _band_score(self, value, band, prefer_low=False):
        """Return 0-100 for how well `value` fits inside the band's p10-p90 range."""
        if value is None or band is None: return 50  # neutral if data missing
        p10, p25, p75, p90 = band['p10'], band['p25'], band['p75'], band['p90']
        median = band['median']
        # Inside p25-p75 = 100; inside p10-p90 = 80; near band = 60; well outside = penalize
        if prefer_low:
            # Lower is better (e.g., BSR, price volatility)
            if value <= median: return 100
            if value <= p75: return 90
            if value <= p90: return 70
            ratio = value / max(p90, 1)
            return max(0, int(70 - (ratio - 1) * 60 * (0.5 + self.strictness)))
        else:
            if p25 <= value <= p75: return 100
            if p10 <= value <= p90: return 85
            # outside p10-p90 — distance penalty
            if value < p10:
                gap = (p10 - value) / max(p10, 1)
                return max(0, int(85 - gap * 100 * (0.5 + self.strictness)))
            else:
                gap = (value - p90) / max(p90, 1)
                return max(0, int(85 - gap * 100 * (0.5 + self.strictness)))

    def score(self, kdata, cost=None, sell_override=None):
        """Score a single Keepa product. Optionally pass a known buy cost for ROI scoring.
           Returns (score, verdict, criteria, features)."""
        f = features(kdata)
        if not f:
            return 0, 'NO_DATA', [], {}
        bands = self.p.get('bands', {})
        criteria = []  # list of dicts: {name, value, band_label, score, weight, note}

        # 1. BSR — heavy weight (proxy for sales velocity)
        bsr = f.get('avg90_bsr') or f.get('current_bsr')
        s_bsr = self._band_score(bsr, bands.get('avg90_bsr'), prefer_low=True)
        criteria.append({
            'name': 'Sales rank (90-day)', 'value': bsr,
            'band': self._fmt_band(bands.get('avg90_bsr'), 'BSR (lower = faster)'),
            'score': s_bsr, 'weight': 25,
            'note': self._bsr_note(bsr, bands.get('avg90_bsr'))})

        # 2. Sell price band
        sell = sell_override if sell_override is not None else (f.get('current_buy_box') or f.get('current_new'))
        s_price = self._band_score(sell, bands.get('sell_price'))
        criteria.append({
            'name': 'Sell price', 'value': sell,
            'band': self._fmt_band(bands.get('sell_price'), 'Winner price band'),
            'score': s_price, 'weight': 12,
            'note': 'Buy box is in our profitable range.' if s_price > 80 else 'Outside our typical price range — verify margin.'})

        # 3. ROI estimate (if cost provided)
        roi_score = 50
        roi_value = None
        if cost and sell:
            roi_value = round((sell - cost) / cost * 100, 2) if cost > 0 else None
            roi_score = self._band_score(roi_value, bands.get('computed_roi_pct'))
        criteria.append({
            'name': 'ROI estimate', 'value': roi_value,
            'band': self._fmt_band(bands.get('computed_roi_pct'), 'Winner ROI band (%)'),
            'score': roi_score, 'weight': 18 if cost else 5,
            'note': 'Provide cost-per-unit for ROI scoring.' if not cost else
                    f'Estimated ROI {roi_value:.1f}%.'})

        # 4. Category match
        root = f.get('root_category', '')
        pref = self.p.get('preferred_categories', {})
        avoid = self.p.get('avoid_categories', {})
        s_cat = 100 if pref.get(root, 0) >= 5 else (75 if pref.get(root, 0) >= 1 else (40 if not root else 30))
        if root in avoid and root not in pref: s_cat = 25
        criteria.append({
            'name': 'Category match', 'value': root or '(unknown)',
            'band': f"Top winner cats: {', '.join(list(pref.keys())[:3])}",
            'score': s_cat, 'weight': 12,
            'note': 'Strong match to a category we win in.' if s_cat >= 90 else
                    ('Adjacent / less-tested category.' if s_cat >= 60 else 'Outside our winning categories.')})

        # 5. Live FBA offer count — too few or too many is bad
        offers = f.get('live_offer_count_new')
        if offers is None: s_off = 50
        elif offers <= 1: s_off = 30  # likely Amazon-only or risky
        elif offers <= 12: s_off = 95
        elif offers <= 20: s_off = 80
        elif offers <= 35: s_off = 55
        else:              s_off = 25
        criteria.append({
            'name': 'Live FBA offers', 'value': offers,
            'band': '~3-12 = sweet spot', 'score': s_off, 'weight': 8,
            'note': 'Healthy competition.' if s_off >= 80 else
                    ('Saturated — race to bottom risk.' if (offers or 0) > 20 else 'Watch for Amazon takeover.')})

        # 6. Price volatility
        cv = f.get('price_cv_pct')
        s_cv = self._band_score(cv, bands.get('price_cv_pct'), prefer_low=True) if cv is not None else 50
        criteria.append({
            'name': 'Price volatility', 'value': f"{cv}%" if cv is not None else None,
            'band': 'Lower = stable buy-box', 'score': s_cv, 'weight': 7,
            'note': 'Stable buy box.' if s_cv >= 80 else 'Erratic pricing — careful with margin.'})

        # 7. Reviews / social proof
        rev = f.get('review_count')
        rating = f.get('rating')
        if rev is None: s_rev = 50
        elif rev < 50:  s_rev = 35
        elif rev < 200: s_rev = 65
        elif rev < 1000:s_rev = 85
        else:            s_rev = 95
        if rating and rating < 3.8: s_rev = max(20, s_rev - 30)
        criteria.append({
            'name': 'Reviews & rating', 'value': f"{rev or '?'} ({rating or '?'}\u2605)",
            'band': '200-1000+ reviews, 4.0+ stars', 'score': s_rev, 'weight': 6,
            'note': 'Strong social proof.' if s_rev >= 85 else
                    ('Building reviews — risk of slow movement.' if s_rev < 50 else 'OK reputation.')})

        # 8. Risk flags
        risks = []
        if f.get('hazmat'): risks.append('hazmat')
        if f.get('oversize'): risks.append('oversize')
        if f.get('is_adult'): risks.append('adult')
        if f.get('buy_box_is_amazon'): risks.append('Amazon on buy box')
        s_risk = max(0, 100 - len(risks) * 30)
        criteria.append({
            'name': 'Risk flags', 'value': ', '.join(risks) if risks else 'none',
            'band': 'Avoid hazmat, oversize, Amazon-on-listing',
            'score': s_risk, 'weight': 12,
            'note': 'Clean.' if not risks else f"Flagged: {', '.join(risks)}"})

        # Weighted final score
        total_w = sum(c['weight'] for c in criteria)
        score = round(sum(c['score'] * c['weight'] for c in criteria) / total_w, 1)

        # Verdict
        if score >= 80: verdict = 'BUY'
        elif score >= 60: verdict = 'MAYBE'
        else: verdict = 'SKIP'

        return score, verdict, criteria, f

    def _fmt_band(self, b, label):
        if not b: return f"{label}: insufficient profile data"
        return f"{label}: p25-p75 = {b['p25']}\u2013{b['p75']} (median {b['median']})"

    def _bsr_note(self, bsr, band):
        if not band: return ''
        if not bsr: return 'BSR not available.'
        if bsr <= band['median']: return 'Selling faster than our median winner.'
        if bsr <= band['p90']: return 'Within our normal selling-speed range.'
        return f"Slow mover (BSR {bsr:,.0f} > our p90 {band['p90']:,.0f})."
