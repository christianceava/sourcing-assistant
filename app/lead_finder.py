"""
Active lead finder.

Translates the winner profile into Keepa Product Finder filters, queries
Keepa for matching candidates, then scores each via the Scorer and returns
the top N.

Keepa Product Finder filter reference (the gotchas):
  - Use `categories_include` (a list of category IDs), NOT `rootCategory`.
  - Numeric range filters use `current_<TYPE>_gte`/`_lte` where TYPE is one of
    AMAZON, NEW, NEW_FBA, BUY_BOX_SHIPPING, SALES (sales rank), RATING,
    COUNT_REVIEWS, etc. Prices are in cents (×100). Rating is ×10.
  - `sort` is a list of [fieldName, "asc"|"desc"] tuples.
  - Don't send made-up filter keys — Keepa returns 400 if it doesn't recognize one.
"""
import json, gzip, urllib.request, urllib.parse, urllib.error, time
from pathlib import Path


KEEPA_QUERY_URL = "https://api.keepa.com/query"


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


class KeepaRateLimitError(Exception):
    def __init__(self, msg, refill_in=None, tokens_left=None):
        super().__init__(msg)
        self.refill_in = refill_in
        self.tokens_left = tokens_left


class KeepaBadRequestError(Exception):
    def __init__(self, msg, body=None, selection=None):
        super().__init__(msg)
        self.body = body
        self.selection = selection


class LeadFinder:
    def __init__(self, keepa_client, profile, scorer, known_asins=None):
        self.keepa = keepa_client
        self.profile = profile
        self.scorer = scorer
        self.known = set(known_asins or [])

    # ---------- Filter construction ----------

    def _build_selection(self, n_leads, category_id=None, relaxed=False):
        """Build a Keepa Product Finder selection JSON.
           If `relaxed=True`, drop the price/review filters and only keep BSR + category."""
        bands = self.profile.get('bands', {}) or {}
        sel = {
            'page': 0,
            # Keepa requires perPage >= 50; we'll over-fetch and trim post-score.
            'perPage': max(50, n_leads * 3),
            'sort': [['current_SALES', 'asc']],
        }

        bsr_band = bands.get('avg90_bsr') or bands.get('current_bsr')
        if bsr_band:
            sel['current_SALES_gte'] = max(50, int(bsr_band.get('p10') or 1000))
            sel['current_SALES_lte'] = int((bsr_band.get('p90') or 200000) * (2.0 if relaxed else 1.5))

        if not relaxed:
            price_band = bands.get('sell_price')
            if price_band:
                sel['current_BUY_BOX_SHIPPING_gte'] = int(max(5, price_band.get('p10') or 10) * 100)
                sel['current_BUY_BOX_SHIPPING_lte'] = int((price_band.get('p90') or 100) * 100)
            sel['current_COUNT_REVIEWS_gte'] = 50
            sel['current_RATING_gte'] = 40

        if category_id:
            sel['categories_include'] = [category_id]
        return sel

    def _winning_categories(self):
        if not self.profile.get('preferred_categories'):
            return [(None, None)]
        out = []
        for name, count in self.profile['preferred_categories'].items():
            cat_id = ROOT_CAT_IDS_US.get(name)
            if cat_id and count >= 2:
                out.append((name, cat_id))
        return out or [(None, None)]

    # ---------- Querying ----------

    def _do_query(self, selection):
        """One raw Keepa query. Returns parsed JSON or raises."""
        sel_str = urllib.parse.quote(json.dumps(selection))
        url = f"{KEEPA_QUERY_URL}?key={self.keepa.key}&domain=1&selection={sel_str}"
        req = urllib.request.Request(url, headers={'Accept-Encoding': 'gzip'})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                data = r.read()
                if r.headers.get('Content-Encoding') == 'gzip':
                    data = gzip.decompress(data)
                return json.loads(data.decode('utf-8'))
        except urllib.error.HTTPError as e:
            body = b''
            try:
                body = e.read()
                if e.headers.get('Content-Encoding') == 'gzip':
                    body = gzip.decompress(body)
                body = body.decode('utf-8', errors='replace')
            except Exception:
                pass
            if e.code == 429:
                try:
                    tok = self.keepa.tokens()
                    refill = tok.get('refillIn', 60000) / 1000.0
                    left = tok.get('tokensLeft', 0)
                except Exception:
                    refill, left = 60, 0
                raise KeepaRateLimitError(
                    f"Keepa 429. Tokens left: {left}. Refill in {int(refill)}s.",
                    refill_in=refill, tokens_left=left)
            if e.code == 400:
                raise KeepaBadRequestError(
                    f"Keepa rejected the query (HTTP 400). Body: {body[:300]}",
                    body=body, selection=selection)
            raise

    def query_keepa_finder(self, selection):
        """Try the strict selection; on 400, try a relaxed fallback.
           Returns asinList (possibly empty) or raises."""
        try:
            return self._do_query(selection)
        except KeepaBadRequestError as e:
            # Try once with a minimal filter set as a fallback
            relaxed = dict(selection)
            # Strip filters we know can be touchy
            for k in list(relaxed.keys()):
                if k.startswith('current_BUY_BOX_SHIPPING'): relaxed.pop(k, None)
                if k.startswith('current_COUNT_REVIEWS'):    relaxed.pop(k, None)
                if k.startswith('current_RATING'):           relaxed.pop(k, None)
            return self._do_query(relaxed)

    # ---------- Main: source N leads ----------

    def source(self, n_leads, on_progress=None):
        results = []
        seen = set()
        cats = self._winning_categories()
        target_candidates = max(n_leads * 3, 15)
        per_cat = max(8, target_candidates // max(len(cats), 1))

        if on_progress: on_progress('search', 0, f'Querying Keepa across {len(cats)} bucket(s)...')

        all_asins = []
        last_error = None
        for i, (cat_name, cat_id) in enumerate(cats):
            sel = self._build_selection(per_cat, category_id=cat_id)
            # honor Keepa's minimum
            sel['perPage'] = max(50, per_cat)
            try:
                data = self.query_keepa_finder(sel)
            except KeepaRateLimitError:
                raise
            except KeepaBadRequestError as e:
                last_error = e
                if on_progress: on_progress('search', (i+1)/len(cats),
                                            f"Keepa rejected query (cat={cat_name}): {str(e)[:200]}")
                continue
            except Exception as e:
                last_error = e
                if on_progress: on_progress('search', (i+1)/len(cats), f"Search error: {e}")
                continue
            asins = data.get('asinList') or []
            new_asins = [a for a in asins if a not in self.known and a not in seen]
            seen.update(new_asins)
            all_asins.extend(new_asins)
            if on_progress:
                on_progress('search', (i+1)/len(cats),
                            f"Got {len(asins)} from {cat_name or 'all cats'} ({len(new_asins)} new)")
            if len(all_asins) >= target_candidates: break

        candidates = all_asins[:target_candidates]
        if not candidates:
            # Re-raise any latent error so the UI can show it
            if last_error: raise last_error
            return []

        if on_progress: on_progress('score', 0, f'Pulling Keepa details for {len(candidates)} candidates...')
        try:
            products = self.keepa.product(candidates)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                raise KeepaRateLimitError("Rate-limited fetching product details. Try again in ~1 min.")
            raise

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

        results.sort(key=lambda r: -r['score'])
        return results[:n_leads]
