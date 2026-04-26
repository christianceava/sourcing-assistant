"""
Pull Keepa product data for every ASIN in asins.txt.
- Batches 20 ASINs per request to amortize HTTP overhead.
- Caches each product as keepa_cache/{ASIN}.json.
- Skips ASINs already cached. Resumable.
- Throttles: if tokensLeft < 30, sleeps until refill catches up.
- Prints a progress line per batch.
"""
import json, os, sys, time, gzip, urllib.request, urllib.error
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
KEY = os.environ.get('KEEPA_KEY') or "4ar1u0a82tjo3pscefaae54qghvab4jnoc0ci3gfj2u53vuoh9593i0102uajpm1"
CACHE = ROOT / "keepa_cache"
ASINS = ROOT / "data" / "asins.txt"
CACHE.mkdir(exist_ok=True)

BATCH = 20  # ASINs per Keepa call
MIN_TOKENS_BUFFER = 30  # don't drop below this
SAFETY_TOKENS_PER_ASIN = 4  # estimated upper bound


def get_url(url):
    req = urllib.request.Request(url, headers={'Accept-Encoding': 'gzip'})
    with urllib.request.urlopen(req, timeout=60) as r:
        data = r.read()
        if r.headers.get('Content-Encoding') == 'gzip':
            data = gzip.decompress(data)
        return json.loads(data.decode('utf-8'))


def needed():
    if not ASINS.exists(): return []
    have = {p.stem for p in CACHE.glob('*.json')}
    return [a for a in (l.strip() for l in ASINS.read_text().splitlines()) if a and a not in have]


def fetch_batch(asin_list):
    asin_str = ",".join(asin_list)
    url = (f"https://api.keepa.com/product?key={KEY}&domain=1&asin={asin_str}"
           f"&stats=180&buybox=1&offers=20")
    return get_url(url)


def main():
    pending = needed()
    have = len(list(CACHE.glob('*.json')))
    total = have + len(pending)
    print(f"Cached: {have} / Pending: {len(pending)} / Total: {total}")
    if not pending:
        print("Nothing to fetch. Cache complete.")
        return

    consumed_total = 0
    fetched_total = 0
    failed = []
    start = time.time()

    for i in range(0, len(pending), BATCH):
        chunk = pending[i:i+BATCH]
        # Token check
        try:
            tok = get_url(f"https://api.keepa.com/token?key={KEY}")
        except Exception as e:
            print(f"  token check failed: {e}; sleeping 30s")
            time.sleep(30); continue
        left = tok.get('tokensLeft', 0)
        rate = tok.get('refillRate', 5)
        need = len(chunk) * SAFETY_TOKENS_PER_ASIN
        if left < need + MIN_TOKENS_BUFFER:
            wait_secs = max(60, ((need + MIN_TOKENS_BUFFER - left) / rate) * 60)
            wait_secs = min(wait_secs, 600)  # cap each wait at 10 min
            print(f"  [{i+1}/{len(pending)}] tokens={left}, need~{need}, sleeping {int(wait_secs)}s")
            time.sleep(wait_secs)

        # Fetch
        try:
            data = fetch_batch(chunk)
        except urllib.error.HTTPError as e:
            print(f"  HTTP error {e.code}: {e.reason}; sleeping 30s")
            time.sleep(30); continue
        except Exception as e:
            print(f"  fetch error: {e}; sleeping 30s")
            time.sleep(30); continue

        consumed = data.get('tokensConsumed', 0)
        consumed_total += consumed
        products = data.get('products') or []
        for p in products:
            asin = (p.get('asin') or '').upper()
            if not asin: continue
            (CACHE / f"{asin}.json").write_text(json.dumps(p, separators=(',',':')))
            fetched_total += 1
        # Track ASINs in batch that didn't return a product
        returned = {(p.get('asin') or '').upper() for p in products}
        missing = [a for a in chunk if a not in returned]
        for a in missing:
            (CACHE / f"{a}.json").write_text(json.dumps({"asin": a, "_not_found": True}))
            failed.append(a)
            fetched_total += 1

        elapsed = time.time() - start
        rate_per_min = (fetched_total / max(elapsed, 1)) * 60
        eta_min = (len(pending) - i - len(chunk)) / max(rate_per_min, 1)
        print(f"  [{i+len(chunk)}/{len(pending)}] +{len(products)} got, {len(missing)} missing, "
              f"tokens-1min={data.get('tokensLeft')}, consumed={consumed}, "
              f"rate={rate_per_min:.1f}/min, eta={eta_min:.1f}min")

    print(f"\nDone. Fetched={fetched_total} | Consumed_total={consumed_total} | Failed={len(failed)}")
    if failed: print("Failed:", failed[:20], "..." if len(failed)>20 else "")


if __name__ == '__main__':
    main()
