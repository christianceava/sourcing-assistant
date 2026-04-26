"""
Active lead finder.

Translates the winner profile into Keepa Product Finder filters, queries
Keepa for matching candidates, then scores each via the Scorer and returns
the top N.

Token cost (rough):
  - 1 product-finder query  : ~5 tokens
  - N candidate detail pulls: 3 tokens each
For 5 leads we pull ~10 candidates  ≈ 35 tokens (~7 min on free 5/min plan)
For 10 leads we pull ~20 candidates ≈ 65 tokens (~13 min)
For 20 leads we pull ~40 candidates ≈125 tokens (~25 min)

If the plan refill rate is higher (paid Keepa tier), it's much faster.
"""
import json, gzip, urllib.request, urllib.parse, time
from pathlib import Path


KEEPA_QUERY_URL = "https://api.keepa.com/query"


# Mapping: Keepa root-category-name -> rootCategory ID (US marketplace).
# Curated list of the categories Christian's winners live in.
ROOT_CAT_IDS_US = {
    'Health & Household':            3760901,
    'Grocery & Gourmet Food':        16310101,
    'Beauty & Personal Care':        3760911,
    'Sports & Outdoors':             3375251,
    'Home & Kitchen':                1055398,
    'Pet Supplies':                  2619533011,
    'Office Products':               1064954,
    'Tools & Home Improvement':      228013,
    'Industrial & Scientific':       16310091,
    'Toys & Games':                  165793011,
    'Patio, Lawn & Garden':          2972638011,
    'Clothing, Shoes & Jewelry':     7141123011,
    'Baby Products':                 165796011,
    'Automotive':                    15684181,
    'Electronics':                   172282,
    'Arts, Crafts & Sewing':         2617941011,
}


class LeadFinder:
    def __init__(self, keepa_client, profile, scorer, known_asins=None):
        self.keepa = keepa_client
        self.profile = profile
        self.scorer = scorer
        self.known = set(known_asins or [])

    # ---------- Filter construction ----------

    def _filters_from_profile(self, n_leads, category_id=None):
        """Build a Keepa Product Finder selection JSON from the winner profile."""
        bands = self.profile.get('bands', {}) or {}
        sel = {
            'page': 0,
            'perPage': max(20, n_leads * 2),
            # Sort by current sales rank ascending (best sellers first within filter)
            'sort': [['current_SALES', 'asc']],
        }

        # BSR: tighten to the winner band but expand a bit to get enough results
        bsr_band = bands.get('avg90_bsr') or bands.get('current_bsr')
        if bsr_band:
            sel['current_SALES_gte'] = max(50, int(bsr_band.get('p10') or 1000))
            sel['current_SALES_lte'] = int((bsr_band.get('p90') or 200000) * 1.5)

        # Sell price band — Keepa uses cents
        price_band = bands.get('sell_price')
        if price_band:
            sel['current_BUY_BOX_SHIPPING_gte'] = int(max(5, price_band.get('p10') or 10) * 100)
            sel['current_BUY_BOX_SHIPPING_lte'] = int((price_band.get('p90') or 100) * 100)

        # Reviews: avoid brand-new listings (need some social proof)
        sel['current_COUNT_REVIEWS_gte'] = 50

        # Rating: 4.0+ (Keepa rating is x10)
        sel['current_RATING_gte'] = 40

        # Buy box must be available (not Amazon-locked is a softer filter we apply later)
        sel['buyBoxIsAmazon'] = False  # exclude listings where Amazon dominates buy box

        # Restrict to a category (if specified)
        if category_id:
            sel['rootCategory'] = category_id

        return sel

    def _winning_categories(self):
        """Return list of (cat_name, cat_id) for our top winner categories."""
        prefs = self.profile.get('preferred_categories') or self.profile.get('preferred_brands_from_title') or {}
        # If the full profile is loaded we'll have category names; if lite, return None to skip
        if not self.profile.get('preferred_categories'):
            return [(None, None)]  # single pass, no category filter
        out = []
        for name, count in self.profile['preferred_categories'].items():
            cat_id = ROOT_CAT_IDS_US.get(name)
            if cat_id and count >= 2:  # only solid winning cats
                out.append((name, cat_id))
        if not out: out = [(None, None)]
        return out

    # ---------- Querying ----------

    def query_keepa_finder(self, selection):
        """Hit /query with the selection JSON. Returns asinList."""
        sel_str = urllib.parse.quote(json.dumps(selection))
        url = f"{KEEPA_QUERY_URL}?key={self.keepa.key}&domain=1&selection={sel_str}"
        req = urllib.request.Request(url, headers={'Accept-Encoding': 'gzip'})
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read()
            if r.headers.get('Content-Encoding') == 'gzip':
                data = gzip.decompress(data)
            return json.loads(data.decode('utf-8'))

    # ---------- Main: source N leads ----------

    def source(self, n_leads, on_progress=None):
        """Return up to n_leads scored candidates as a list of dicts.
           on_progress(stage, pct, msg) called for UI updates."""
        results = []
        seen = set()
        cats = self._winning_categories()

        # How many candidates to consider total
        target_candidates = max(n_leads * 3, 15)
        per_cat = max(8, target_candidates // max(len(cats), 1))

        if on_progress: on_progress('search', 0, f'Querying Keepa across {len(cats)} winning category(ies)...')

        all_asins = []
        for i, (cat_name, cat_id) in enumerate(cats):
            sel = self._filters_from_profile(per_cat, category_id=cat_id)
            sel['perPage'] = per_cat
            try:
                data = self.query_keepa_finder(sel)
            except Exception as e:
                if on_progress: on_progress('search', (i+1)/len(cats), f"Search error in {cat_name}: {e}")
                continue
            asins = data.get('asinList') or []
            new_asins = [a for a in asins if a not in self.known and a not in seen]
            seen.update(new_asins)
            all_asins.extend(new_asins)
            if on_progress:
                on_progress('search', (i+1)/len(cats),
                            f"Got {len(asins)} from {cat_name or 'all cats'} ({len(new_asins)} new)")
            if len(all_asins) >= target_candidates: break

        # Trim to target
        candidates = all_asins[:target_candidates]
        if not candidates:
            return []

        if on_progress: on_progress('score', 0, f'Pulling Keepa details for {len(candidates)} candidates...')

        # Pull product details (batched)
        try:
            products = self.keepa.product(candidates)
        except Exception as e:
            if on_progress: on_progress('score', 0, f"Keepa product error: {e}")
            return []

        # Score
        for i, asin in enumerate(candidates):
            kd = products.get(asin)
            if not kd: continue
            score, verdict, criteria, f = self.scorer.score(kd)
            results.append({
                'asin': asin, 'score': score, 'verdict': verdict,
                'title': f.get('title', ''), 'brand': f.get('brand', ''),
                'category': f.get('root_category', ''),
                'sell_price': f.get('current_buy_box') or f.get('current_new'),
                'bsr_90d': f.get('avg90_bsr') or f.get('current_bsr'),
                'reviews': f.get('review_count'), 'rating': f.get('rating'),
                'live_offers': f.get('live_offer_count_new'),
                'est_monthly_sales': f.get('est_monthly_sales'),
                'hazmat': bool(f.get('hazmat')),
                'oversize': bool(f.get('oversize')),
                'amazon_on_listing': bool(f.get('buy_box_is_amazon')),
                'criteria': criteria,
                'amazon_url': f"https://www.amazon.com/dp/{asin}",
                'keepa_url':  f"https://keepa.com/#!product/1-{asin}",
                'selleramp_url': f"https://sas.selleramp.com/sas/lookup?searchterm={asin}",
            })
            if on_progress:
                on_progress('score', (i+1)/len(candidates), f"Scored {asin}: {score}")

        # Top N by score
        results.sort(key=lambda r: -r['score'])
        return results[:n_leads]
