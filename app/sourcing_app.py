"""
Sourcing Assistant — ONLINE web app.

Single-button UX:  pick 5 / 10 / 20 → Source → ranked list of leads matching
the winner signature. Every lead is a NEW ASIN we don't already sell.

Deploy on Streamlit Cloud:
  1. Push this folder to GitHub
  2. Connect on streamlit.io/cloud
  3. Set main file: app/sourcing_app.py
  4. Add secret: keepa_key = "..."
"""
import json, time, csv, io, os
from pathlib import Path

import streamlit as st
import pandas as pd

from sourcing_engine import Keepa, Scorer
from lead_finder import LeadFinder
from lite_profile import build_lite_profile
from auth import login_gate

# ---------- Config ----------
APP_DIR = Path(__file__).resolve().parent
ROOT = APP_DIR.parent
PROFILE_FULL = ROOT / 'profile' / 'winner_profile.json'
PROFILE_LITE = ROOT / 'profile' / 'winner_profile_lite.json'
JOINED_CSV = ROOT / 'data' / 'joined.csv'
KEEPA_CACHE = ROOT / 'keepa_cache'
LEADS_PATH = ROOT / 'profile' / 'saved_leads.json'

KEEPA_KEY = (st.secrets.get('keepa_key', None) if hasattr(st, 'secrets') else None) \
    or os.environ.get('KEEPA_KEY') \
    or "4ar1u0a82tjo3pscefaae54qghvab4jnoc0ci3gfj2u53vuoh9593i0102uajpm1"

st.set_page_config(page_title="Sourcing Assistant", page_icon="🎯", layout="wide",
                   initial_sidebar_state="collapsed")

# ---------- Auth gate ----------
login_gate()

# ---------- Styling ----------
st.markdown("""
<style>
.block-container { padding-top: 2rem; padding-bottom: 4rem; max-width: 1280px; }
.big-cta {
  background: linear-gradient(135deg, #0f1115 0%, #1a1f2a 100%);
  border-radius: 18px; padding: 36px 28px; margin: 24px 0;
  border: 1px solid #2a2f3a;
}
.lead-card {
  background: #181c23; border: 1px solid #262b34;
  border-radius: 12px; padding: 18px 20px; margin-bottom: 12px;
}
.lead-title { font-size: 1.05rem; font-weight: 600; color: #fff; line-height: 1.4; }
.lead-meta { color: #9ba3b1; font-size: 0.85rem; margin-top: 4px; }
.kpi { display:inline-block; background: #11141a; border:1px solid #262b34;
       border-radius: 6px; padding: 4px 10px; margin-right: 6px; font-size: 0.82rem; color:#cbd1dc; }
.verdict-buy   { background:#0d8a3e; color:#fff; padding:4px 12px; border-radius:6px; font-weight:600; font-size:.9rem;}
.verdict-maybe { background:#b8860b; color:#fff; padding:4px 12px; border-radius:6px; font-weight:600; font-size:.9rem;}
.verdict-skip  { background:#9c2222; color:#fff; padding:4px 12px; border-radius:6px; font-weight:600; font-size:.9rem;}
.score-badge   { background:#2563eb; color:#fff; padding:4px 12px; border-radius:6px; font-weight:700; }
.criterion-row { padding:8px 12px; border-radius:6px; margin-bottom:4px; background:#11141a; font-size:.85rem; }
.warn-flag { color: #f59e0b; font-size: 0.85rem; }
hr { border-color: #262b34; }
</style>
""", unsafe_allow_html=True)


# ---------- Cached resources ----------

@st.cache_resource
def get_keepa():
    return Keepa(KEEPA_KEY, cache_dir=str(KEEPA_CACHE))


@st.cache_resource
def get_profile():
    """Load full profile if available, else build/load lite profile from joined.csv."""
    if PROFILE_FULL.exists():
        return json.loads(PROFILE_FULL.read_text())
    if PROFILE_LITE.exists():
        return json.loads(PROFILE_LITE.read_text())
    if JOINED_CSV.exists():
        p = build_lite_profile(JOINED_CSV)
        PROFILE_LITE.parent.mkdir(parents=True, exist_ok=True)
        PROFILE_LITE.write_text(json.dumps(p, indent=2, default=str))
        return p
    return None


def get_scorer(strictness):
    profile = get_profile()
    if not profile: return None
    return Scorer(profile, strictness=strictness)


def load_leads():
    if LEADS_PATH.exists():
        try: return json.loads(LEADS_PATH.read_text())
        except Exception: return []
    return []


def save_leads(leads):
    LEADS_PATH.parent.mkdir(parents=True, exist_ok=True)
    LEADS_PATH.write_text(json.dumps(leads, indent=2, default=str))


def verdict_html(v):
    cls = {'BUY':'verdict-buy','MAYBE':'verdict-maybe','SKIP':'verdict-skip'}.get(v,'verdict-skip')
    return f'<span class="{cls}">{v}</span>'


# ---------- Header ----------

st.markdown("# 🎯 Sourcing Assistant")
st.caption("Finds NEW Amazon ASINs that match the signature of products we've already won with.")

profile = get_profile()
keepa = get_keepa()

if not profile:
    st.error("No winner profile found. Place `joined.csv` in `data/` and reload.")
    st.stop()

# ---------- Compact profile summary ----------
ws = profile.get('generated_from_n_winners', 0)
mode = profile.get('mode', 'full')
top_vendors = list(profile.get('preferred_vendors', {}).items())[:5]
bsr_band = profile.get('bands', {}).get('avg90_bsr') or profile.get('bands', {}).get('current_bsr')
price_band = profile.get('bands', {}).get('sell_price')

c1, c2, c3, c4 = st.columns(4)
c1.metric("Profile mode", mode.upper())
c2.metric("Trained on # winners", ws)
if bsr_band: c3.metric("Winner BSR (median)", f"{int(bsr_band['median']):,}")
if price_band: c4.metric("Winner price (median)", f"${price_band['median']:.2f}")

# ---------- Big CTA ----------

st.markdown('<div class="big-cta">', unsafe_allow_html=True)
st.markdown("### 🚀 Source new leads")
st.markdown("Click below — I'll find Amazon products matching your winner signature, score each, and return the strongest ones.")

cca, ccb, ccc, ccd = st.columns([1,1,1,3])
n_leads = None
if cca.button("⚡ 5 leads", type="primary", use_container_width=True): n_leads = 5
if ccb.button("🔥 10 leads", type="primary", use_container_width=True): n_leads = 10
if ccc.button("💪 20 leads", type="primary", use_container_width=True): n_leads = 20

with st.sidebar:
    st.markdown("### Settings")
    strictness = st.slider("Match strictness", 0.0, 1.0, 0.55, 0.05,
                           help="0 = wider net (more results, less precise). 1 = only near-clones of past winners.")
    if st.button("Check Keepa tokens"):
        try:
            t = keepa.tokens()
            st.write(f"**{t.get('tokensLeft')}** tokens, refilling **{t.get('refillRate')}/min**")
        except Exception as e:
            st.error(str(e))
    st.divider()
    st.markdown("### Top winning vendors")
    for v, c in top_vendors:
        st.markdown(f"- {v}  &nbsp; <span class='kpi'>{c}</span>", unsafe_allow_html=True)
    st.divider()
    st.markdown("### About this profile")
    st.caption("The assistant has internalized your 365-day sold history — what BSR, price, and vendor patterns predict winners. It's used silently to filter Keepa for matching candidates.")

st.markdown('</div>', unsafe_allow_html=True)

# ---------- Run sourcing ----------

if n_leads:
    scorer = get_scorer(strictness)
    known = set(profile.get('known_asins') or [])
    finder = LeadFinder(keepa, profile, scorer, known_asins=known)

    progress = st.progress(0.0, text=f"Sourcing {n_leads} leads...")
    status = st.empty()

    def on_progress(stage, pct, msg):
        # Stage weights: search 30%, score 70%
        if stage == 'search':
            progress.progress(min(0.3 * pct, 0.3), text=f"🔍 Searching Keepa: {msg}")
        else:
            progress.progress(0.3 + 0.7 * pct, text=f"🧮 Scoring: {msg}")
        status.caption(msg)

    t0 = time.time()
    try:
        leads = finder.source(n_leads, on_progress=on_progress)
    except Exception as e:
        st.error(f"Sourcing failed: {e}")
        st.stop()
    progress.progress(1.0, text="Done.")
    elapsed = time.time() - t0

    if not leads:
        st.warning("No matching candidates returned. Try a lower strictness, or check token balance.")
        st.stop()

    st.success(f"Found {len(leads)} leads in {elapsed:.1f}s")

    # Summary tally
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Returned", len(leads))
    s2.metric("✅ BUY", sum(1 for l in leads if l['verdict'] == 'BUY'))
    s3.metric("⚠️ MAYBE", sum(1 for l in leads if l['verdict'] == 'MAYBE'))
    s4.metric("Avg score", f"{sum(l['score'] for l in leads)/len(leads):.1f}")

    st.divider()
    st.markdown(f"### Top {len(leads)} leads")

    # Render lead cards
    for lead in leads:
        with st.container():
            st.markdown('<div class="lead-card">', unsafe_allow_html=True)
            cA, cB = st.columns([5, 1.5])
            with cA:
                st.markdown(f"<div class='lead-title'>{lead['title'][:140]}</div>", unsafe_allow_html=True)
                meta = []
                if lead.get('brand'): meta.append(lead['brand'])
                if lead.get('category'): meta.append(lead['category'])
                meta.append(lead['asin'])
                st.markdown(f"<div class='lead-meta'>{' · '.join(meta)}</div>", unsafe_allow_html=True)
                # KPI row
                kpis = []
                if lead.get('sell_price'): kpis.append(f"${lead['sell_price']:.2f}")
                if lead.get('bsr_90d'): kpis.append(f"BSR {int(lead['bsr_90d']):,}")
                if lead.get('reviews'): kpis.append(f"{lead['reviews']} reviews")
                if lead.get('rating'): kpis.append(f"{lead['rating']:.1f}⭐")
                if lead.get('live_offers') is not None: kpis.append(f"{lead['live_offers']} FBA")
                if lead.get('est_monthly_sales'): kpis.append(f"~{lead['est_monthly_sales']:,}/mo")
                st.markdown(' '.join(f"<span class='kpi'>{k}</span>" for k in kpis), unsafe_allow_html=True)
                # Risk flags
                flags = []
                if lead.get('hazmat'): flags.append('hazmat')
                if lead.get('oversize'): flags.append('oversize')
                if lead.get('amazon_on_listing'): flags.append('Amazon on listing')
                if flags:
                    st.markdown(f"<span class='warn-flag'>⚠️ {', '.join(flags)}</span>", unsafe_allow_html=True)
            with cB:
                st.markdown(verdict_html(lead['verdict']) +
                            f"&nbsp;&nbsp;<span class='score-badge'>{lead['score']}</span>",
                            unsafe_allow_html=True)
                st.markdown(f"[Amazon]({lead['amazon_url']}) · "
                            f"[Keepa]({lead['keepa_url']}) · "
                            f"[SAS]({lead['selleramp_url']})")

            with st.expander("Why this scored where it did"):
                for c in lead['criteria']:
                    color = '#0d8a3e' if c['score'] >= 80 else ('#b8860b' if c['score'] >= 50 else '#9c2222')
                    st.markdown(
                        f"<div class='criterion-row'>"
                        f"<b>{c['name']}</b> "
                        f"<span style='color:{color};font-weight:600;float:right'>{c['score']}/100</span><br>"
                        f"<span style='color:#9ba3b1'>Value: {c['value']}</span> &nbsp; "
                        f"<span style='color:#7a8290;font-size:.8rem'>{c['band']}</span><br>"
                        f"<span style='color:#cbd1dc'>{c['note']}</span>"
                        f"</div>", unsafe_allow_html=True)
                if st.button(f"💾 Save {lead['asin']}", key=f"save_{lead['asin']}"):
                    saved = load_leads()
                    saved.append({**{k: lead[k] for k in
                                     ('asin','score','verdict','title','brand','category',
                                      'sell_price','bsr_90d','reviews','rating',
                                      'amazon_url','keepa_url') if k in lead},
                                  'saved_at': time.strftime('%Y-%m-%d %H:%M')})
                    save_leads(saved); st.success("Saved.")
            st.markdown('</div>', unsafe_allow_html=True)

    # Download CSV
    df = pd.DataFrame([{k: v for k, v in l.items() if k != 'criteria'} for l in leads])
    st.download_button(
        "⬇ Download leads as CSV",
        df.to_csv(index=False).encode('utf-8'),
        file_name=f"sourcing_leads_{int(time.time())}.csv",
        mime='text/csv', use_container_width=True)


# ---------- Saved leads ----------
saved = load_leads()
if saved and not n_leads:
    st.divider()
    st.markdown(f"### 💾 Saved leads ({len(saved)})")
    df = pd.DataFrame(saved)
    show = df[['asin','title','score','verdict','sell_price','bsr_90d','saved_at']] if 'asin' in df.columns else df
    st.dataframe(show, use_container_width=True, hide_index=True)
    cc1, cc2 = st.columns(2)
    if cc1.button("🗑 Clear all saved"):
        save_leads([]); st.rerun()
    cc2.download_button("⬇ Download saved CSV", df.to_csv(index=False).encode('utf-8'),
                       file_name="saved_leads.csv", mime='text/csv', use_container_width=True)
