# Deploy to Streamlit Cloud (online for the VAs)

This makes the sourcing assistant available at `https://your-app.streamlit.app` so any VA can use it from their browser.

## Step 1 — Push to GitHub

You can do this in a fresh empty repo, or add it to your existing VA Tools Portal repo as a separate folder.

**Option A — fresh repo (easiest, ~2 min):**

```powershell
cd "C:\Users\avall\OneDrive\Desktop\AMAZON FBA\VA & Tools\VA Tools\sourcing_assistant"

git init
git branch -M main
git add .
git commit -m "Initial commit — sourcing assistant"

# Create the repo on github.com (call it `sourcing-assistant` and keep it Private)
git remote add origin https://github.com/<YOUR-USERNAME>/sourcing-assistant.git
git push -u origin main
```

Make sure these files made it in:
- `app/sourcing_app.py` ← main file
- `app/sourcing_engine.py`
- `app/lead_finder.py`
- `app/lite_profile.py`
- `data/joined.csv` ← the trained data (380+ ASINs)
- `requirements.txt`
- `.streamlit/config.toml`
- `.gitignore`

**Skipped (good — these stay private):**
- `.streamlit/secrets.toml` (you'll paste the Keepa key into Streamlit Cloud directly)
- The two source CSVs in `data/` (`MAIN The One Sheet*.csv`, `Frontier*.csv`) — only `joined.csv` needs to ship.

## Step 2 — Connect to Streamlit Cloud

1. Go to https://streamlit.io/cloud and sign in with the GitHub account you just pushed to.
2. Click **"Create app"** → **"From existing repo"**.
3. Repo: `<YOUR-USERNAME>/sourcing-assistant`
4. Branch: `main`
5. **Main file path:** `app/sourcing_app.py`
6. App URL: pick something like `frontier-sourcing` (becomes `frontier-sourcing.streamlit.app`).
7. Don't deploy yet — first add the secret.

## Step 3 — Add the Keepa key as a secret

Click **"Advanced settings"** → **Secrets** → paste:

```toml
keepa_key = "4ar1u0a82tjo3pscefaae54qghvab4jnoc0ci3gfj2u53vuoh9593i0102uajpm1"
```

Then click **Deploy**.

First boot takes ~1 minute. The app will:
1. Load `data/joined.csv`
2. Compute the lite winner profile in memory (BSR/price/ROI bands, brand/vendor patterns)
3. Show the homepage with the **5 / 10 / 20 leads** buttons

## Step 4 — Send the URL to the VAs

That's it. The VA hits `https://frontier-sourcing.streamlit.app`, picks 5/10/20, gets fresh leads matching the winner signature.

## How they'll use it

1. Click one of **⚡ 5 leads / 🔥 10 leads / 💪 20 leads**.
2. Wait for the progress bar to finish (Keepa rate-limits the search; ~6-25 minutes depending on quantity and your Keepa plan).
3. Each lead shows:
   - BUY / MAYBE / SKIP verdict + 0-100 score
   - Title, brand, category, BSR, price, reviews, FBA offer count, est. monthly sales
   - Risk flags (hazmat, oversize, Amazon on listing)
   - Direct links to **Amazon**, **Keepa chart**, and **SellerAmp** for the VA to verify
   - "Why this scored where it did" expander with per-criterion reasoning
4. Save promising leads with the 💾 button. They persist in the app for review later.
5. Download the full results as CSV.

## Tuning

Sidebar → **Match strictness** slider:
- 0 = wide net (more leads, looser BSR/price match — useful for exploration)
- 1 = only near-clones of past winners (high precision, fewer "interesting" leads)
- Default 0.55 is a balanced starting point.

## Keepa token usage

Each "Source N leads" click costs roughly:
- 5 leads: ~35 Keepa tokens
- 10 leads: ~65 tokens
- 20 leads: ~125 tokens

Your refill rate determines how often the VAs can click. To check live: sidebar → "Check Keepa tokens".

## Upgrading from lite profile to full profile

Right now the deployed app uses a **lite profile** (built from `joined.csv` alone) — fast to deploy but doesn't have category/brand/risk-flag data from Keepa.

To upgrade to the **full profile** (recommended after the app is running):

1. On your PC, run `SETUP.bat` (in the `sourcing_assistant` folder). This pulls Keepa for all 383 ASINs and builds `profile/winner_profile.json` (the full version).
2. `git add profile/winner_profile.json && git commit -m "full profile" && git push`
3. Streamlit Cloud auto-redeploys. App now uses the full profile.

Full profile improvements:
- Category-aware filtering (search drills into your winning cats)
- Brand-pattern matching
- Per-criterion bands trained on real Keepa stats (offer count, price volatility, review velocity)
- Avoidance lists (categories/brands where your past leads underperformed)
