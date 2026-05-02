"""
NEPSE Smart Money Scanner — Production-Grade Single File App
Auto-fetches floorsheet from NEPSE API with CSV fallback
"""

import io
import time
import random
import warnings
import requests
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, timedelta
from typing import Optional, Dict, List, Tuple

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NEPSE Smart Money Scanner",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# THEME & CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

:root {
    --bg:        #06080f;
    --bg2:       #0b0f1a;
    --bg3:       #101520;
    --border:    #1a2235;
    --border2:   #243050;
    --text:      #d0daf0;
    --text2:     #6a80a8;
    --text3:     #3a506a;
    --accent:    #2563ff;
    --accent2:   #1a3db0;
    --green:     #00e676;
    --green2:    #00b85c;
    --red:       #ff3d5a;
    --red2:      #c02040;
    --yellow:    #ffd740;
    --yellow2:   #c8a800;
    --purple:    #8b5cf6;
    --mono:      'IBM Plex Mono', monospace;
    --sans:      'IBM Plex Sans', sans-serif;
}

html, body, [class*="css"] {
    font-family: var(--sans);
    background: var(--bg);
    color: var(--text);
}
.stApp { background: var(--bg); }
.main .block-container { padding: 1.2rem 2rem; max-width: 1700px; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: var(--bg2) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--text) !important; }
[data-testid="stSidebar"] .stSelectbox > div > div,
[data-testid="stSidebar"] .stDateInput > div > div > input,
[data-testid="stSidebar"] .stTextInput > div > div > input {
    background: var(--bg3) !important;
    border: 1px solid var(--border2) !important;
    color: var(--text) !important;
}

/* Metrics */
[data-testid="metric-container"] {
    background: var(--bg3) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    padding: 14px !important;
}
[data-testid="metric-container"] label { color: var(--text2) !important; font-family: var(--mono) !important; font-size: 0.7rem !important; letter-spacing: 1px !important; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { color: var(--text) !important; font-family: var(--mono) !important; font-size: 1.4rem !important; font-weight: 600 !important; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { background: transparent; border-bottom: 1px solid var(--border); gap: 0; }
.stTabs [data-baseweb="tab"] { background: transparent !important; color: var(--text2) !important; border: none !important; padding: 8px 20px !important; font-family: var(--mono) !important; font-size: 0.75rem !important; letter-spacing: 0.5px; text-transform: uppercase; border-bottom: 2px solid transparent !important; }
.stTabs [aria-selected="true"] { color: var(--accent) !important; border-bottom: 2px solid var(--accent) !important; background: rgba(37,99,255,0.06) !important; }

/* Buttons */
.stButton > button { background: var(--bg3) !important; color: var(--accent) !important; border: 1px solid var(--border2) !important; border-radius: 4px !important; font-family: var(--mono) !important; font-size: 0.78rem !important; padding: 8px 20px !important; letter-spacing: 0.5px; transition: all 0.15s !important; }
.stButton > button:hover { background: rgba(37,99,255,0.12) !important; border-color: var(--accent) !important; }

/* Dataframe */
.stDataFrame { border: 1px solid var(--border) !important; border-radius: 6px !important; }
iframe[title="st_aggrid"] { border-radius: 6px; }

/* Divider */
hr { border-color: var(--border) !important; margin: 1rem 0 !important; }

/* Spinner */
.stSpinner > div { border-top-color: var(--accent) !important; }

/* File uploader */
[data-testid="stFileUploader"] { border: 1px dashed var(--border2) !important; border-radius: 6px !important; background: var(--bg3) !important; }

/* Progress bar */
.stProgress > div > div > div { background-color: var(--accent) !important; }

/* Select + multiselect */
div[data-baseweb="select"] > div { background: var(--bg3) !important; border-color: var(--border2) !important; }
div[data-baseweb="popover"] { background: var(--bg2) !important; border-color: var(--border2) !important; }

/* Scrollbar */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
API_URL   = "https://newweb.nepalstock.com/api/nots/nepse-data/floorsheet"
API_TOKEN = "https://newweb.nepalstock.com/api/nots/authenticate"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

NEPSE_SYMBOLS = [
    "NABIL","NICA","SANIMA","GBIME","EBL","KBL","SBI","PRVU","SRBL","NMB",
    "MBL","HBL","ADBL","PCBL","MEGA","KUMARI","HIDCL","UPPER","AKPL","NHPC",
    "NLG","NLICL","NLIC","LICN","PRIN","SLICL","NIFRA","SHINE","UMHL","CHCL",
]

# Score weights
W_ACCUM    = 30
W_SEQ      = 25
W_CLUSTER  = 15
W_DOMINANCE= 20
W_IMPACT   = 10

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def get_headers() -> dict:
    return {
        "User-Agent"     : random.choice(USER_AGENTS),
        "Accept"         : "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type"   : "application/json",
        "Origin"         : "https://newweb.nepalstock.com",
        "Referer"        : "https://newweb.nepalstock.com/floor-sheet",
        "Connection"     : "keep-alive",
    }

def safe_float(v, default=0.0):
    try:
        if isinstance(v, str):
            v = v.replace(",", "").strip()
        return float(v)
    except Exception:
        return default

def fmt_num(n: float) -> str:
    if abs(n) >= 1e7:  return f"{n/1e7:.2f}Cr"
    if abs(n) >= 1e5:  return f"{n/1e5:.2f}L"
    if abs(n) >= 1e3:  return f"{n/1e3:.1f}K"
    return f"{n:.2f}"

def signal_badge(signal: str) -> str:
    cfg = {
        "BUY"  : ("✅ BUY",   "#00e676", "#002a14"),
        "WATCH": ("👀 WATCH", "#ffd740", "#2a2000"),
        "AVOID": ("❌ AVOID", "#ff3d5a", "#2a0010"),
    }
    label, color, bg = cfg.get(signal, ("— N/A", "#6a80a8", "#101520"))
    return (f'<span style="font-family:\'IBM Plex Mono\',monospace; font-size:0.8rem;'
            f' font-weight:700; color:{color}; background:{bg};'
            f' padding:3px 10px; border-radius:4px; border:1px solid {color}44;">'
            f'{label}</span>')

def score_bar(score: float) -> str:
    pct   = min(score, 100)
    color = "#00e676" if score >= 75 else "#ffd740" if score >= 50 else "#ff3d5a"
    return (f'<div style="background:#101520; border-radius:3px; height:6px; width:100%; margin-top:4px;">'
            f'<div style="background:{color}; width:{pct}%; height:100%; border-radius:3px;'
            f' transition:width 0.3s;"></div></div>')

# ─────────────────────────────────────────────────────────────────────────────
# API FETCHER
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def fetch_floorsheet_api(
    trade_date: str,
    symbol: Optional[str] = None,
    max_pages: int = 10,
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Fetch floorsheet from NEPSE API with retry + pagination.
    Returns (DataFrame | None, status_message)
    """
    session = requests.Session()
    all_rows: List[dict] = []
    errors: List[str] = []

    # Try to get token first (some endpoints need it)
    token = None
    try:
        r = session.post(
            API_TOKEN,
            json={"username": "", "password": ""},
            headers=get_headers(),
            timeout=8,
        )
        if r.status_code == 200:
            token = r.json().get("token") or r.json().get("jwt")
    except Exception:
        pass  # Token optional

    page = 0
    while page < max_pages:
        payload: dict = {
            "id"          : 0,
            "startDate"   : trade_date,
            "endDate"     : trade_date,
            "stockSymbol" : symbol or "",
            "page"        : page,
            "size"        : 500,
            "sort"        : "contractId,desc",
        }

        headers = get_headers()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        success = False
        for attempt in range(3):
            try:
                resp = session.post(
                    API_URL,
                    json=payload,
                    headers=headers,
                    timeout=15,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    content = (
                        data.get("floorsheets", {}).get("content")
                        or data.get("content")
                        or (data if isinstance(data, list) else [])
                    )
                    if not content:
                        # Empty page — done
                        return _build_df(all_rows), f"Fetched {len(all_rows)} records (page {page})"

                    all_rows.extend(content)

                    # Pagination check
                    total_pages = (
                        data.get("floorsheets", {}).get("totalPages")
                        or data.get("totalPages")
                        or 1
                    )
                    if page + 1 >= total_pages:
                        return _build_df(all_rows), f"✅ Fetched {len(all_rows)} records from {total_pages} page(s)"

                    success = True
                    break

                elif resp.status_code == 401:
                    errors.append(f"Auth error (401) — page {page}")
                    break
                elif resp.status_code == 404:
                    errors.append("Endpoint not found (404)")
                    return None, "API endpoint not found (404). Use manual upload."
                else:
                    errors.append(f"HTTP {resp.status_code} — attempt {attempt+1}")
                    time.sleep(0.5 * (attempt + 1))

            except requests.exceptions.Timeout:
                errors.append(f"Timeout on page {page} attempt {attempt+1}")
                time.sleep(1)
            except requests.exceptions.ConnectionError:
                errors.append("Connection error — network issue")
                time.sleep(2)
            except Exception as e:
                errors.append(f"Unexpected: {e}")
                break

        if not success:
            break
        page += 1
        time.sleep(0.3)  # polite delay

    if all_rows:
        return _build_df(all_rows), f"Partial fetch: {len(all_rows)} records. Errors: {errors[-1] if errors else 'none'}"

    err_str = "; ".join(errors[-3:]) if errors else "Unknown error"
    return None, f"API failed: {err_str}"


def _build_df(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # Normalize known column names from NEPSE API
    col_map = {
        "stockSymbol"    : "symbol",
        "symbol"         : "symbol",
        "contractRate"   : "price",
        "rate"           : "price",
        "contractQuantity": "quantity",
        "quantity"       : "quantity",
        "contractAmount" : "amount",
        "amount"         : "amount",
        "buyerMemberId"  : "buyer_broker",
        "buyerBroker"    : "buyer_broker",
        "sellerMemberId" : "seller_broker",
        "sellerBroker"   : "seller_broker",
        "contractId"     : "contract_id",
        "businessDate"   : "date",
        "tradeTime"      : "time",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    for col in ["price", "quantity", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ""), errors="coerce"
            )
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    return df

# ─────────────────────────────────────────────────────────────────────────────
# CSV PARSER
# ─────────────────────────────────────────────────────────────────────────────
def parse_uploaded_csv(file) -> Tuple[Optional[pd.DataFrame], str]:
    try:
        df = pd.read_csv(file)
    except Exception:
        try:
            file.seek(0)
            df = pd.read_excel(file)
        except Exception as e:
            return None, f"Cannot parse file: {e}"

    if df.empty:
        return None, "File is empty"
    return df, f"Loaded {len(df)} rows, {len(df.columns)} columns"

def apply_column_mapping(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Apply user-defined column mapping."""
    renamed = {}
    for target, source in mapping.items():
        if source and source != "— skip —" and source in df.columns:
            renamed[source] = target
    if renamed:
        df = df.rename(columns=renamed)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# DATA VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_COLS = {"symbol", "price", "quantity", "buyer_broker", "seller_broker"}

def validate_df(df: pd.DataFrame) -> Tuple[bool, str, pd.DataFrame]:
    """Ensure required columns exist and types are correct."""
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        return False, f"Missing columns: {missing}", df

    for col in ["price", "quantity"]:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", ""), errors="coerce"
        )
    df = df.dropna(subset=["price", "quantity"])
    df = df[df["price"] > 0]
    df = df[df["quantity"] > 0]

    if "amount" not in df.columns:
        df["amount"] = df["price"] * df["quantity"]

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    for col in ["buyer_broker", "seller_broker"]:
        df[col] = df[col].astype(str).str.strip()

    return True, f"Valid: {len(df)} clean rows across {df['symbol'].nunique()} symbols", df

# ─────────────────────────────────────────────────────────────────────────────
# SMART MONEY ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def _score_accumulation(sym_df: pd.DataFrame) -> Tuple[float, str]:
    """
    Broker Accumulation: detect brokers consistently buying more than selling.
    Max: 30 pts
    """
    buy_vol  = sym_df.groupby("buyer_broker")["quantity"].sum()
    sell_vol = sym_df.groupby("seller_broker")["quantity"].sum()
    all_brokers = set(buy_vol.index) | set(sell_vol.index)
    net = {b: buy_vol.get(b, 0) - sell_vol.get(b, 0) for b in all_brokers}
    net_series = pd.Series(net)
    total_vol = sym_df["quantity"].sum()
    if total_vol == 0:
        return 0, "No volume"

    # Strong accumulators: brokers with net_buy > 10% of total
    strong = net_series[net_series > total_vol * 0.10]
    accum_ratio = net_series[net_series > 0].sum() / (total_vol + 1)
    score = min(accum_ratio * 60, W_ACCUM)

    top_buyer = net_series.idxmax() if len(net_series) > 0 else "—"
    reason = (f"Broker {top_buyer} net-buying {fmt_num(net_series.max())} shares "
              f"({len(strong)} dominant buyer(s))")
    return round(score, 2), reason


def _score_sequential(sym_df: pd.DataFrame) -> Tuple[float, str]:
    """
    Sequential Trade Detection: same broker buying repeatedly in short succession.
    Proxy for metaorder / institutional accumulation pattern.
    Max: 25 pts
    """
    if len(sym_df) < 5:
        return 0, "Too few trades"

    # Count repeat buyer trades
    buyer_counts = sym_df["buyer_broker"].value_counts()
    total_trades = len(sym_df)
    top_broker   = buyer_counts.index[0]
    top_count    = buyer_counts.iloc[0]
    concentration = top_count / total_trades

    # If one broker is in > 20% of buy-side trades → sequential pattern
    score = min(concentration * W_SEQ * 2, W_SEQ)

    # Also check: increasing trade sizes (size walk-up = distribution ramp)
    if "amount" in sym_df.columns:
        top_trades = sym_df[sym_df["buyer_broker"] == top_broker]["quantity"].values
        if len(top_trades) >= 3:
            trend = np.polyfit(range(len(top_trades)), top_trades, 1)[0]
            if trend > 0:
                score = min(score * 1.2, W_SEQ)

    reason = (f"Broker {top_broker} appears in {top_count}/{total_trades} trades "
              f"({concentration*100:.1f}% concentration)")
    return round(score, 2), reason


def _score_clustering(sym_df: pd.DataFrame) -> Tuple[float, str]:
    """
    Trade Size Clustering: institutional trades are large and cluster around specific sizes.
    Max: 15 pts
    """
    q = sym_df["quantity"]
    if len(q) < 5:
        return 0, "Too few trades"

    p90 = q.quantile(0.90)
    p50 = q.quantile(0.50)
    large_trades = q[q >= p90]
    large_vol_pct = large_trades.sum() / q.sum() * 100

    # Coefficient of variation among large trades (low CV = clustering)
    cv = large_trades.std() / (large_trades.mean() + 1e-6) if len(large_trades) > 1 else 1.0

    # Score: more large trades + low CV = institutional
    size_score = min(large_vol_pct / 100 * W_CLUSTER * 2, W_CLUSTER)
    cluster_bonus = max(0, (1 - cv) * W_CLUSTER * 0.3)
    score = min(size_score + cluster_bonus, W_CLUSTER)

    reason = (f"Large trades (≥p90={fmt_num(p90)}) = {large_vol_pct:.1f}% of volume; "
              f"median lot: {fmt_num(p50)}")
    return round(score, 2), reason


def _score_dominance(sym_df: pd.DataFrame) -> Tuple[float, str]:
    """
    Broker Dominance: HHI-style measure of buy-side concentration.
    Max: 20 pts
    """
    total_vol = sym_df["quantity"].sum()
    if total_vol == 0:
        return 0, "No volume"

    buy_shares = sym_df.groupby("buyer_broker")["quantity"].sum() / total_vol * 100
    hhi = float((buy_shares ** 2).sum())  # 0–10000

    # HHI > 2500 = highly concentrated (monopoly-like)
    score = min(hhi / 10000 * W_DOMINANCE * 2.5, W_DOMINANCE)

    top = buy_shares.nlargest(1)
    reason = (f"Top buyer Broker {top.index[0]}: {top.iloc[0]:.1f}% of buy volume; "
              f"HHI={hhi:.0f}")
    return round(score, 2), reason


def _score_price_impact(sym_df: pd.DataFrame) -> Tuple[float, str]:
    """
    Price Impact: large trades moving price more than average = institutional.
    Max: 10 pts
    """
    if len(sym_df) < 5 or "price" not in sym_df.columns:
        return 0, "Insufficient data"

    prices = sym_df["price"].values
    qtys   = sym_df["quantity"].values

    if prices.std() == 0:
        return 5, "Price stable (no impact)"

    # Weighted correlation: large trades → larger price moves?
    if len(prices) > 2:
        price_changes = np.abs(np.diff(prices))
        qty_mid = qtys[:-1]  # align
        if qty_mid.std() > 0:
            corr = np.corrcoef(qty_mid, price_changes)[0, 1]
            corr = 0 if np.isnan(corr) else corr
        else:
            corr = 0
    else:
        corr = 0

    # Amihud proxy
    returns_abs = np.abs(np.diff(prices) / (prices[:-1] + 1e-6))
    amihud = float(np.mean(returns_abs / (qtys[:-1] + 1e-6))) * 1e6
    amihud_score = min(amihud * 2, W_IMPACT * 0.7)
    impact_score = min((abs(corr) * W_IMPACT * 0.5) + amihud_score, W_IMPACT)

    reason = (f"Price-volume correlation: {corr:.2f}; "
              f"Amihud illiquidity: {amihud:.4f}")
    return round(impact_score, 2), reason


def analyze_symbol(sym_df: pd.DataFrame, symbol: str) -> dict:
    """Run full smart money analysis for one symbol."""
    s_accum,   r_accum   = _score_accumulation(sym_df)
    s_seq,     r_seq     = _score_sequential(sym_df)
    s_cluster, r_cluster = _score_clustering(sym_df)
    s_dom,     r_dom     = _score_dominance(sym_df)
    s_impact,  r_impact  = _score_price_impact(sym_df)

    total = s_accum + s_seq + s_cluster + s_dom + s_impact
    total = round(min(total, 100), 1)

    if total >= 75:
        signal, signal_key = "✅ BUY",   "BUY"
    elif total >= 50:
        signal, signal_key = "👀 WATCH", "WATCH"
    else:
        signal, signal_key = "❌ AVOID", "AVOID"

    # Dominant broker
    buy_vol   = sym_df.groupby("buyer_broker")["quantity"].sum()
    sell_vol  = sym_df.groupby("seller_broker")["quantity"].sum()
    dom_buyer = buy_vol.idxmax() if len(buy_vol) > 0 else "—"
    dom_seller= sell_vol.idxmax() if len(sell_vol) > 0 else "—"

    net_map = {}
    all_b = set(buy_vol.index) | set(sell_vol.index)
    for b in all_b:
        net_map[b] = buy_vol.get(b, 0) - sell_vol.get(b, 0)

    return {
        "symbol"       : symbol,
        "total_score"  : total,
        "signal"       : signal,
        "signal_key"   : signal_key,
        "scores"       : {
            "accumulation" : s_accum,
            "sequential"   : s_seq,
            "clustering"   : s_cluster,
            "dominance"    : s_dom,
            "price_impact" : s_impact,
        },
        "reasons"      : {
            "accumulation" : r_accum,
            "sequential"   : r_seq,
            "clustering"   : r_cluster,
            "dominance"    : r_dom,
            "price_impact" : r_impact,
        },
        "stats"        : {
            "trades"       : len(sym_df),
            "total_volume" : int(sym_df["quantity"].sum()),
            "total_turnover": float(sym_df["amount"].sum()),
            "avg_price"    : float(sym_df["price"].mean()),
            "price_range"  : f"{sym_df['price'].min():.2f} – {sym_df['price'].max():.2f}",
            "unique_buyers": sym_df["buyer_broker"].nunique(),
            "unique_sellers": sym_df["seller_broker"].nunique(),
            "dom_buyer"    : dom_buyer,
            "dom_seller"   : dom_seller,
        },
        "net_flow"     : net_map,
        "broker_buy"   : buy_vol.to_dict(),
        "broker_sell"  : sell_vol.to_dict(),
    }


@st.cache_data(ttl=180, show_spinner=False)
def run_analysis(df_json: str) -> List[dict]:
    """Run analysis on all symbols. Accepts JSON string for cache compatibility."""
    df = pd.read_json(io.StringIO(df_json), orient="split")
    results = []
    for sym, grp in df.groupby("symbol"):
        if len(grp) >= 3:
            results.append(analyze_symbol(grp.copy(), str(sym)))
    results.sort(key=lambda x: -x["total_score"])
    return results

# ─────────────────────────────────────────────────────────────────────────────
# CHARTS
# ─────────────────────────────────────────────────────────────────────────────
BG   = "#06080f"
BG2  = "#0b0f1a"
BG3  = "#101520"
GRD  = "#1a2235"
TXT  = "#d0daf0"
TXT2 = "#6a80a8"
GREEN  = "#00e676"
RED    = "#ff3d5a"
YELLOW = "#ffd740"
BLUE   = "#2563ff"
PURPLE = "#8b5cf6"

LAYOUT = dict(
    paper_bgcolor=BG, plot_bgcolor=BG3,
    font=dict(color=TXT, family="'IBM Plex Mono', monospace", size=11),
    xaxis=dict(gridcolor=GRD, zeroline=False, tickfont=dict(color=TXT2)),
    yaxis=dict(gridcolor=GRD, zeroline=False, tickfont=dict(color=TXT2)),
    margin=dict(l=50, r=20, t=45, b=40),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=TXT)),
    hovermode="closest",
)

def chart_score_breakdown(result: dict) -> go.Figure:
    scores  = result["scores"]
    max_map = {"accumulation": W_ACCUM, "sequential": W_SEQ,
               "clustering": W_CLUSTER, "dominance": W_DOMINANCE, "price_impact": W_IMPACT}
    cats    = list(scores.keys())
    vals    = [scores[c] for c in cats]
    maxvals = [max_map[c] for c in cats]
    pcts    = [v / m * 100 for v, m in zip(vals, maxvals)]
    colors  = [GREEN if p >= 70 else YELLOW if p >= 40 else RED for p in pcts]

    fig = go.Figure(go.Bar(
        x=[c.replace("_", " ").title() for c in cats],
        y=vals,
        marker_color=colors,
        text=[f"{v:.1f}/{m}" for v, m in zip(vals, maxvals)],
        textposition="outside",
        textfont=dict(size=10, color=TXT),
        customdata=maxvals,
        hovertemplate="<b>%{x}</b><br>Score: %{y:.1f}/%{customdata}<extra></extra>",
    ))
    fig.update_layout(**LAYOUT, title=dict(
        text=f"{result['symbol']} — Score Breakdown", x=0.01,
        font=dict(color=TXT, size=13)
    ), height=300, yaxis=dict(range=[0, max(maxvals) * 1.3], **LAYOUT["yaxis"]))
    return fig


def chart_broker_flow(result: dict, top_n: int = 12) -> go.Figure:
    net = pd.Series(result["net_flow"]).sort_values()
    if len(net) > top_n:
        # Show extreme positive and negative
        pos = net.nlargest(top_n // 2)
        neg = net.nsmallest(top_n // 2)
        net = pd.concat([neg, pos]).sort_values()

    colors = [GREEN if v > 0 else RED for v in net.values]
    fig = go.Figure(go.Bar(
        x=net.values, y=[f"Broker {b}" for b in net.index],
        orientation="h",
        marker_color=colors,
        text=[fmt_num(abs(v)) for v in net.values],
        textposition="outside",
        textfont=dict(size=9, color=TXT),
    ))
    fig.add_vline(x=0, line=dict(color=TXT2, width=1))
    fig.update_layout(**LAYOUT, title=dict(
        text=f"{result['symbol']} — Broker Net Flow", x=0.01,
        font=dict(color=TXT, size=13)
    ), height=max(250, len(net) * 25 + 80))
    return fig


def chart_price_volume(sym_df: pd.DataFrame, symbol: str) -> go.Figure:
    from plotly.subplots import make_subplots
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.65, 0.35], vertical_spacing=0.04)
    idx = range(len(sym_df))
    # Price line
    fig.add_trace(go.Scatter(
        x=list(idx), y=sym_df["price"],
        mode="lines", name="Price",
        line=dict(color=BLUE, width=1.8),
    ), row=1, col=1)

    # Volume bars
    vol_colors = []
    for i in range(len(sym_df)):
        if i == 0:
            vol_colors.append(GREEN)
        else:
            vol_colors.append(GREEN if sym_df["price"].iloc[i] >= sym_df["price"].iloc[i-1] else RED)
    fig.add_trace(go.Bar(
        x=list(idx), y=sym_df["quantity"],
        marker_color=vol_colors, name="Quantity", opacity=0.75,
    ), row=2, col=1)

    layout_copy = LAYOUT.copy()
    layout_copy["xaxis2"] = dict(gridcolor=GRD, zeroline=False, tickfont=dict(color=TXT2))
    layout_copy["yaxis2"] = dict(gridcolor=GRD, zeroline=False, tickfont=dict(color=TXT2))
    fig.update_layout(**layout_copy,
        title=dict(text=f"{symbol} — Price & Volume", x=0.01,
                   font=dict(color=TXT, size=13)),
        height=340, showlegend=False)
    fig.update_xaxes(rangeslider_visible=False)
    return fig


def chart_signals_summary(results: List[dict]) -> go.Figure:
    df = pd.DataFrame([{
        "symbol": r["symbol"],
        "score" : r["total_score"],
        "signal": r["signal_key"],
    } for r in results]).sort_values("score", ascending=True)

    color_map = {"BUY": GREEN, "WATCH": YELLOW, "AVOID": RED}
    colors = [color_map.get(s, TXT2) for s in df["signal"]]

    fig = go.Figure(go.Bar(
        x=df["score"],
        y=df["symbol"],
        orientation="h",
        marker_color=colors,
        text=df["score"].apply(lambda v: f"{v:.0f}"),
        textposition="outside",
        textfont=dict(size=9, color=TXT),
    ))
    fig.add_vline(x=75, line=dict(color=GREEN, width=1, dash="dot"), opacity=0.5,
                  annotation_text="BUY", annotation_font=dict(color=GREEN, size=9))
    fig.add_vline(x=50, line=dict(color=YELLOW, width=1, dash="dot"), opacity=0.5,
                  annotation_text="WATCH", annotation_font=dict(color=YELLOW, size=9))
    fig.update_layout(**LAYOUT,
        title=dict(text="All Symbols — Smart Money Score", x=0.01,
                   font=dict(color=TXT, size=13)),
        height=max(300, len(df) * 26 + 80),
        xaxis=dict(range=[0, 115], **LAYOUT["xaxis"]),
    )
    return fig


def chart_radar(result: dict) -> go.Figure:
    scores  = result["scores"]
    max_map = {"accumulation": W_ACCUM, "sequential": W_SEQ,
               "clustering": W_CLUSTER, "dominance": W_DOMINANCE, "price_impact": W_IMPACT}
    cats  = list(scores.keys())
    pcts  = [scores[c] / max_map[c] * 100 for c in cats]
    cats_plot = [c.replace("_", " ").title() for c in cats] + [cats[0].replace("_"," ").title()]
    vals_plot = pcts + [pcts[0]]
    color = GREEN if result["total_score"] >= 75 else YELLOW if result["total_score"] >= 50 else RED

    fig = go.Figure(go.Scatterpolar(
        r=vals_plot, theta=cats_plot,
        fill="toself", fillcolor=color + "22",
        line=dict(color=color, width=2),
        name=result["symbol"],
    ))
    fig.update_layout(
        polar=dict(
            bgcolor=BG3,
            radialaxis=dict(visible=True, range=[0, 100], gridcolor=GRD,
                           tickfont=dict(color=TXT2, size=8), tickvals=[25,50,75,100]),
            angularaxis=dict(tickfont=dict(color=TXT, size=10), gridcolor=GRD),
        ),
        paper_bgcolor=BG,
        font=dict(color=TXT, family="'IBM Plex Mono', monospace"),
        height=320, margin=dict(l=50, r=50, t=40, b=30),
        title=dict(text=f"{result['symbol']} — Signal Radar", x=0.01,
                   font=dict(color=TXT, size=13)),
        showlegend=False,
    )
    return fig

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────
for key, val in [("df", None), ("results", None), ("data_source", ""), ("data_status", "")]:
    if key not in st.session_state:
        st.session_state[key] = val

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="padding: 20px 0 12px 0; border-bottom: 1px solid #1a2235; margin-bottom: 20px;">
    <div style="display:flex; align-items:center; gap:16px;">
        <div style="width:40px; height:40px; background: linear-gradient(135deg, #2563ff, #8b5cf6);
                    border-radius: 8px; display:flex; align-items:center; justify-content:center;
                    font-size:1.2rem;">🔬</div>
        <div>
            <div style="font-family:'IBM Plex Mono',monospace; font-size:1.3rem; font-weight:700;
                        color:#d0daf0; letter-spacing:1px;">NEPSE SMART MONEY SCANNER</div>
            <div style="font-size:0.72rem; color:#6a80a8; font-family:'IBM Plex Mono',monospace;
                        letter-spacing:0.5px; margin-top:2px;">
                FLOORSHEET ANALYSIS · BROKER INTELLIGENCE · INSTITUTIONAL DETECTION
            </div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — CONTROLS
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="font-family:'IBM Plex Mono',monospace; font-size:0.65rem;
                color:#3a506a; text-transform:uppercase; letter-spacing:1px;
                padding-bottom:8px; border-bottom:1px solid #1a2235; margin-bottom:14px;">
        ⚙ CONFIGURATION
    </div>
    """, unsafe_allow_html=True)

    # Mode
    st.markdown("**Data Mode**")
    mode = st.radio(
        "Mode", ["🌐 Auto Fetch (API)", "📁 Manual Upload (CSV)"],
        label_visibility="collapsed"
    )

    st.divider()

    if "Auto Fetch" in mode:
        st.markdown("**📅 Trade Date**")
        today = date.today()
        # Default to last weekday
        default_date = today - timedelta(days=1 if today.weekday() != 0 else 3)
        trade_date = st.date_input(
            "Date", value=default_date, max_value=today,
            label_visibility="collapsed"
        )

        st.markdown("**📈 Symbol Filter** *(optional)*")
        symbol_filter = st.selectbox(
            "Symbol", ["All Symbols"] + sorted(NEPSE_SYMBOLS),
            label_visibility="collapsed"
        )
        sym_param = None if symbol_filter == "All Symbols" else symbol_filter

        st.markdown("**📄 Max Pages**")
        max_pages = st.slider("Pages", 1, 20, 5, label_visibility="collapsed",
                              help="More pages = more data but slower")

        if st.button("🔄 Fetch Floorsheet", use_container_width=True):
            with st.spinner("Connecting to NEPSE API..."):
                fetch_floorsheet_api.clear()
                df_raw, status = fetch_floorsheet_api(
                    str(trade_date), sym_param, max_pages
                )
            if df_raw is not None and not df_raw.empty:
                ok, msg, df_clean = validate_df(df_raw)
                if ok:
                    st.session_state["df"]          = df_clean
                    st.session_state["results"]     = None
                    st.session_state["data_source"] = f"API · {trade_date}"
                    st.session_state["data_status"] = status
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"Validation failed: {msg}")
                    st.session_state["df"] = None
            else:
                st.error(f"API failed: {status}")
                st.warning("👆 Switch to Manual Upload mode and upload a CSV file.")
                st.session_state["df"] = None
                st.session_state["data_status"] = status

    else:
        # MANUAL UPLOAD
        st.markdown("**📂 Upload Floorsheet**")
        uploaded = st.file_uploader(
            "CSV or Excel", type=["csv", "xlsx", "xls"],
            label_visibility="collapsed"
        )

        if uploaded:
            df_raw, status = parse_uploaded_csv(uploaded)
            if df_raw is not None:
                st.success(f"✅ {status}")
                st.markdown("**🗂 Column Mapping**")
                st.caption("Map your CSV columns to required fields:")
                cols_options = ["— skip —"] + list(df_raw.columns)
                mapping = {}
                field_labels = {
                    "symbol"        : "Symbol / Script",
                    "price"         : "Price / Rate",
                    "quantity"      : "Quantity / Volume",
                    "buyer_broker"  : "Buyer Broker",
                    "seller_broker" : "Seller Broker",
                    "amount"        : "Amount / Turnover (opt)",
                    "time"          : "Time (opt)",
                }
                # Auto-detect defaults
                auto_detect = {
                    "symbol"       : next((c for c in df_raw.columns if any(k in c.lower() for k in ["symbol","script","stock"])), None),
                    "price"        : next((c for c in df_raw.columns if any(k in c.lower() for k in ["price","rate","ltp"])), None),
                    "quantity"     : next((c for c in df_raw.columns if any(k in c.lower() for k in ["qty","quantity","vol","shares"])), None),
                    "buyer_broker" : next((c for c in df_raw.columns if any(k in c.lower() for k in ["buyer","buy_broker","buymember"])), None),
                    "seller_broker": next((c for c in df_raw.columns if any(k in c.lower() for k in ["seller","sell_broker","sellmember"])), None),
                    "amount"       : next((c for c in df_raw.columns if any(k in c.lower() for k in ["amount","turnover","value"])), None),
                    "time"         : next((c for c in df_raw.columns if any(k in c.lower() for k in ["time","hour"])), None),
                }

                for field, label in field_labels.items():
                    default_idx = (cols_options.index(auto_detect[field])
                                   if auto_detect[field] in cols_options else 0)
                    mapping[field] = st.selectbox(
                        label, cols_options, index=default_idx, key=f"map_{field}"
                    )

                if st.button("✅ Apply Mapping & Load", use_container_width=True):
                    df_mapped = apply_column_mapping(df_raw.copy(), mapping)
                    ok, msg, df_clean = validate_df(df_mapped)
                    if ok:
                        st.session_state["df"]          = df_clean
                        st.session_state["results"]     = None
                        st.session_state["data_source"] = f"Upload · {uploaded.name}"
                        st.session_state["data_status"] = msg
                        st.success(f"✅ {msg}")
                    else:
                        st.error(f"❌ {msg}")
            else:
                st.error(status)

    st.divider()

    # Analysis trigger
    if st.session_state["df"] is not None:
        if st.button("🧠 Run Smart Money Analysis", use_container_width=True,
                     type="primary"):
            with st.spinner("Analyzing institutional activity..."):
                df_json = st.session_state["df"].to_json(orient="split")
                run_analysis.clear()
                st.session_state["results"] = run_analysis(df_json)
            st.success(f"✅ Analyzed {len(st.session_state['results'])} symbols")

    st.divider()

    # Data status
    if st.session_state["data_source"]:
        st.markdown(
            f'<div style="font-family:\'IBM Plex Mono\',monospace; font-size:0.65rem;'
            f' color:#3a506a;">Source: {st.session_state["data_source"]}</div>',
            unsafe_allow_html=True
        )
    if st.session_state["data_status"]:
        st.markdown(
            f'<div style="font-family:\'IBM Plex Mono\',monospace; font-size:0.62rem;'
            f' color:#3a506a; margin-top:4px;">{st.session_state["data_status"]}</div>',
            unsafe_allow_html=True
        )

# ─────────────────────────────────────────────────────────────────────────────
# MAIN CONTENT
# ─────────────────────────────────────────────────────────────────────────────
df       = st.session_state["df"]
results  = st.session_state["results"]

# ── Empty state ──
if df is None:
    st.markdown("""
    <div style="text-align:center; padding: 80px 40px; border: 1px dashed #1a2235;
                border-radius: 8px; background: #0b0f1a; margin-top: 20px;">
        <div style="font-size: 3rem; margin-bottom: 16px;">🔬</div>
        <div style="font-family:'IBM Plex Mono',monospace; font-size:1rem;
                    color:#d0daf0; margin-bottom: 8px;">No Data Loaded</div>
        <div style="color:#6a80a8; font-size:0.85rem; max-width: 480px; margin:0 auto;">
            Select <b>Auto Fetch</b> to pull live floorsheet from NEPSE API,<br>
            or switch to <b>Manual Upload</b> to import a CSV file.
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── Data loaded, show tabs ──
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Dashboard",
    "🎯 Signals",
    "🏦 Broker Activity",
    "🔍 Symbol Deep-Dive",
])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — DASHBOARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab1:
    # Summary metrics
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    total_records   = len(df)
    total_symbols   = df["symbol"].nunique()
    total_volume    = int(df["quantity"].sum())
    total_turnover  = float(df["amount"].sum())
    unique_buyers   = df["buyer_broker"].nunique()
    unique_sellers  = df["seller_broker"].nunique()

    c1.metric("Total Trades",   f"{total_records:,}")
    c2.metric("Symbols",         total_symbols)
    c3.metric("Total Volume",    fmt_num(total_volume))
    c4.metric("Turnover",        fmt_num(total_turnover))
    c5.metric("Buyer Brokers",   unique_buyers)
    c6.metric("Seller Brokers",  unique_sellers)

    st.divider()

    if results:
        col_chart, col_stats = st.columns([3, 2])

        with col_chart:
            st.plotly_chart(chart_signals_summary(results), use_container_width=True)

        with col_stats:
            buys    = [r for r in results if r["signal_key"] == "BUY"]
            watches = [r for r in results if r["signal_key"] == "WATCH"]
            avoids  = [r for r in results if r["signal_key"] == "AVOID"]

            st.markdown("""
            <div style="font-family:'IBM Plex Mono',monospace; font-size:0.7rem;
                        color:#6a80a8; text-transform:uppercase; letter-spacing:1px;
                        margin-bottom:12px;">Signal Distribution</div>
            """, unsafe_allow_html=True)

            for label, items, color in [
                ("✅ BUY",   buys,    "#00e676"),
                ("👀 WATCH", watches, "#ffd740"),
                ("❌ AVOID", avoids,  "#ff3d5a"),
            ]:
                pct = len(items) / len(results) * 100 if results else 0
                st.markdown(
                    f'<div style="background:#101520; border:1px solid #1a2235; border-radius:6px;'
                    f' padding:10px 14px; margin-bottom:8px;">'
                    f'<div style="display:flex; justify-content:space-between; align-items:center;">'
                    f'<div style="font-family:\'IBM Plex Mono\',monospace; font-size:0.85rem; color:{color}; font-weight:700;">{label}</div>'
                    f'<div style="font-family:\'IBM Plex Mono\',monospace; font-size:1.1rem; color:#d0daf0; font-weight:600;">{len(items)}</div>'
                    f'</div>'
                    f'<div style="background:#1a2235; border-radius:3px; height:5px; margin-top:6px;">'
                    f'<div style="background:{color}; width:{pct:.0f}%; height:100%; border-radius:3px;"></div>'
                    f'</div></div>',
                    unsafe_allow_html=True
                )

            # Top BUY picks
            if buys:
                st.markdown("""
                <div style="font-family:'IBM Plex Mono',monospace; font-size:0.7rem;
                            color:#6a80a8; text-transform:uppercase; letter-spacing:1px;
                            margin: 16px 0 8px 0;">🔥 Top BUY Picks</div>
                """, unsafe_allow_html=True)
                for r in buys[:4]:
                    sc = r["total_score"]
                    st.markdown(
                        f'<div style="display:flex; justify-content:space-between; align-items:center;'
                        f' padding:7px 12px; background:#0b0f1a; border-radius:5px;'
                        f' border-left:3px solid #00e676; margin-bottom:5px;">'
                        f'<div style="font-family:\'IBM Plex Mono\',monospace; font-weight:600;'
                        f' color:#d0daf0;">{r["symbol"]}</div>'
                        f'<div style="font-family:\'IBM Plex Mono\',monospace; font-size:0.85rem;'
                        f' color:#00e676; font-weight:700;">{sc:.0f}</div>'
                        f'</div>',
                        unsafe_allow_html=True
                    )
    else:
        # Pre-analysis: show symbol volume distribution
        st.info("👈 Click **Run Smart Money Analysis** in the sidebar to generate signals.")

        if not df.empty:
            sym_vol = df.groupby("symbol")["amount"].sum().sort_values(ascending=False).head(20)
            fig_bar = go.Figure(go.Bar(
                x=sym_vol.index, y=sym_vol.values,
                marker_color=BLUE, opacity=0.8,
                text=[fmt_num(v) for v in sym_vol.values],
                textposition="outside", textfont=dict(size=9, color=TXT),
            ))
            fig_bar.update_layout(**LAYOUT,
                title=dict(text="Turnover by Symbol", x=0.01,
                           font=dict(color=TXT, size=13)),
                height=340,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    # Raw data preview
    with st.expander("📋 Raw Floorsheet Data (sample 200 rows)"):
        disp = [c for c in ["symbol","price","quantity","amount","buyer_broker","seller_broker","time"]
                if c in df.columns]
        st.dataframe(df[disp].tail(200), use_container_width=True, hide_index=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — SIGNALS TABLE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab2:
    if not results:
        st.info("Run analysis first (sidebar).")
    else:
        # Filter controls
        fc1, fc2, fc3 = st.columns([2, 2, 3])
        with fc1:
            sig_filter = st.multiselect(
                "Filter by Signal", ["BUY", "WATCH", "AVOID"],
                default=["BUY", "WATCH", "AVOID"],
            )
        with fc2:
            min_score = st.slider("Min Score", 0, 100, 0)
        with fc3:
            search = st.text_input("Search symbol", "").upper().strip()

        filtered = [
            r for r in results
            if r["signal_key"] in sig_filter
            and r["total_score"] >= min_score
            and (not search or search in r["symbol"])
        ]

        st.markdown(f"**{len(filtered)} signals** matching filters")
        st.divider()

        # Signal cards
        for r in filtered:
            sig_color = {"BUY": "#00e676", "WATCH": "#ffd740", "AVOID": "#ff3d5a"}.get(r["signal_key"], TXT2)
            sc = r["total_score"]
            stats = r["stats"]
            scores = r["scores"]
            reasons = r["reasons"]

            with st.expander(
                f"{'▲' if r['signal_key']=='BUY' else '▼' if r['signal_key']=='AVOID' else '◆'} "
                f"{r['symbol']}   |   Score: {sc:.0f}/100   |   {r['signal']}   |   "
                f"Trades: {stats['trades']}   |   Vol: {fmt_num(stats['total_volume'])}",
                expanded=(r["signal_key"] == "BUY"),
            ):
                row1, row2 = st.columns([3, 2])

                with row1:
                    # Score component display
                    score_items = [
                        ("Accumulation",  scores["accumulation"],  W_ACCUM,     reasons["accumulation"]),
                        ("Sequential",    scores["sequential"],    W_SEQ,       reasons["sequential"]),
                        ("Clustering",    scores["clustering"],    W_CLUSTER,   reasons["clustering"]),
                        ("Dominance",     scores["dominance"],     W_DOMINANCE, reasons["dominance"]),
                        ("Price Impact",  scores["price_impact"],  W_IMPACT,    reasons["price_impact"]),
                    ]
                    table_html = """
                    <table style="width:100%; border-collapse:collapse;
                                  font-family:'IBM Plex Mono',monospace; font-size:0.78rem;">
                        <thead>
                            <tr style="border-bottom:1px solid #1a2235;">
                                <th style="text-align:left; color:#6a80a8; padding:6px 8px; font-weight:500;
                                           font-size:0.68rem; text-transform:uppercase; letter-spacing:0.5px;">Component</th>
                                <th style="text-align:center; color:#6a80a8; padding:6px 8px; font-weight:500;
                                           font-size:0.68rem; text-transform:uppercase;">Score</th>
                                <th style="text-align:left; color:#6a80a8; padding:6px 8px; font-weight:500;
                                           font-size:0.68rem; text-transform:uppercase;">Finding</th>
                            </tr>
                        </thead><tbody>
                    """
                    for label, score, max_s, reason in score_items:
                        pct = score / max_s * 100
                        c   = "#00e676" if pct >= 70 else "#ffd740" if pct >= 40 else "#ff3d5a"
                        table_html += f"""
                        <tr style="border-bottom:1px solid #0f1520;">
                            <td style="padding:7px 8px; color:#d0daf0;">{label}</td>
                            <td style="padding:7px 8px; text-align:center;">
                                <span style="color:{c}; font-weight:700;">{score:.1f}</span>
                                <span style="color:#3a506a;">/{max_s}</span>
                            </td>
                            <td style="padding:7px 8px; color:#6a80a8; font-size:0.72rem;">{reason}</td>
                        </tr>
                        """
                    sc_color = "#00e676" if sc >= 75 else "#ffd740" if sc >= 50 else "#ff3d5a"
                    table_html += f"""
                        <tr style="border-top:1px solid #1a2235; background:#0b0f1a;">
                            <td style="padding:8px 8px; color:#d0daf0; font-weight:700;">TOTAL</td>
                            <td style="padding:8px 8px; text-align:center;">
                                <span style="color:{sc_color}; font-weight:700; font-size:1rem;">{sc:.1f}</span>
                                <span style="color:#3a506a;">/100</span>
                            </td>
                            <td style="padding:8px 8px;">{signal_badge(r['signal_key'])}</td>
                        </tr>
                    </tbody></table>
                    """
                    st.markdown(table_html, unsafe_allow_html=True)

                with row2:
                    # Stats
                    stat_items = [
                        ("Trades",    f"{stats['trades']:,}"),
                        ("Volume",    fmt_num(stats['total_volume'])),
                        ("Turnover",  fmt_num(stats['total_turnover'])),
                        ("Avg Price", f"{stats['avg_price']:.2f}"),
                        ("Range",     stats['price_range']),
                        ("Buyers",    stats['unique_buyers']),
                        ("Sellers",   stats['unique_sellers']),
                        ("Dom Buyer", f"Broker {stats['dom_buyer']}"),
                        ("Dom Seller",f"Broker {stats['dom_seller']}"),
                    ]
                    for k, v in stat_items:
                        st.markdown(
                            f'<div style="display:flex; justify-content:space-between;'
                            f' padding:4px 0; border-bottom:1px solid #1a2235;">'
                            f'<span style="font-family:\'IBM Plex Mono\',monospace;'
                            f' font-size:0.72rem; color:#6a80a8;">{k}</span>'
                            f'<span style="font-family:\'IBM Plex Mono\',monospace;'
                            f' font-size:0.78rem; color:#d0daf0; font-weight:500;">{v}</span>'
                            f'</div>',
                            unsafe_allow_html=True
                        )

        # Compact export table
        st.divider()
        st.markdown("**📥 Export Table**")
        export_rows = []
        for r in filtered:
            lvl = r["scores"]
            export_rows.append({
                "Symbol"      : r["symbol"],
                "Score"       : r["total_score"],
                "Signal"      : r["signal"],
                "Accumulation": lvl["accumulation"],
                "Sequential"  : lvl["sequential"],
                "Clustering"  : lvl["clustering"],
                "Dominance"   : lvl["dominance"],
                "PriceImpact" : lvl["price_impact"],
                "Trades"      : r["stats"]["trades"],
                "Volume"      : r["stats"]["total_volume"],
                "DomBuyer"    : f"Broker {r['stats']['dom_buyer']}",
            })
        export_df = pd.DataFrame(export_rows)
        st.dataframe(export_df, use_container_width=True, hide_index=True, height=300)
        csv_bytes = export_df.to_csv(index=False).encode()
        st.download_button("⬇ Download CSV", csv_bytes,
                           "nepse_signals.csv", "text/csv",
                           use_container_width=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — BROKER ACTIVITY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab3:
    st.markdown("### 🏦 Market-Wide Broker Activity")

    # Global broker net flow across all symbols
    global_buy  = df.groupby("buyer_broker")["quantity"].sum()
    global_sell = df.groupby("seller_broker")["quantity"].sum()
    all_brokers = set(global_buy.index) | set(global_sell.index)
    global_net  = pd.Series(
        {b: global_buy.get(b, 0) - global_sell.get(b, 0) for b in all_brokers}
    ).sort_values()

    b1, b2, b3 = st.columns(3)
    b1.metric("Active Brokers",    len(all_brokers))
    b2.metric("Largest Net Buyer", f"Broker {global_net.idxmax()}" if len(global_net) > 0 else "—",
              delta=fmt_num(global_net.max()) if len(global_net) > 0 else "")
    b3.metric("Largest Net Seller",f"Broker {global_net.idxmin()}" if len(global_net) > 0 else "—",
              delta=f"-{fmt_num(abs(global_net.min()))}" if len(global_net) > 0 else "",
              delta_color="inverse")

    st.divider()

    col_flow, col_table = st.columns([3, 2])

    with col_flow:
        # Global net flow chart
        top_n_net = pd.concat([global_net.nsmallest(10), global_net.nlargest(10)]).sort_values()
        colors = [GREEN if v > 0 else RED for v in top_n_net.values]
        fig_gnet = go.Figure(go.Bar(
            x=top_n_net.values,
            y=[f"Broker {b}" for b in top_n_net.index],
            orientation="h",
            marker_color=colors,
            text=[fmt_num(abs(v)) for v in top_n_net.values],
            textposition="outside",
            textfont=dict(size=9, color=TXT),
        ))
        fig_gnet.add_vline(x=0, line=dict(color=TXT2, width=1))
        fig_gnet.update_layout(**LAYOUT,
            title=dict(text="Top 20 Brokers — Global Net Flow", x=0.01,
                       font=dict(color=TXT, size=13)),
            height=440,
        )
        st.plotly_chart(fig_gnet, use_container_width=True)

    with col_table:
        # Broker activity table
        broker_table = pd.DataFrame({
            "Broker"      : [f"Broker {b}" for b in all_brokers],
            "Buy Vol"     : [int(global_buy.get(b, 0)) for b in all_brokers],
            "Sell Vol"    : [int(global_sell.get(b, 0)) for b in all_brokers],
            "Net Flow"    : [int(global_buy.get(b, 0) - global_sell.get(b, 0)) for b in all_brokers],
        }).sort_values("Net Flow", ascending=False)
        broker_table["Bias"] = broker_table["Net Flow"].apply(
            lambda x: "NET BUY" if x > 0 else "NET SELL"
        )
        st.markdown("**Broker Summary Table**")
        st.dataframe(broker_table, use_container_width=True, hide_index=True, height=380)

    # Broker cross-symbol participation
    st.divider()
    st.markdown("**🕸 Broker Participation Across Symbols**")
    broker_sym = df.groupby("buyer_broker")["symbol"].nunique().sort_values(ascending=False).head(15)
    fig_part = go.Figure(go.Bar(
        x=[f"Broker {b}" for b in broker_sym.index], y=broker_sym.values,
        marker_color=PURPLE, opacity=0.85,
        text=broker_sym.values, textposition="outside",
        textfont=dict(size=9, color=TXT),
    ))
    fig_part.update_layout(**LAYOUT,
        title=dict(text="Top Brokers by Number of Symbols Traded", x=0.01,
                   font=dict(color=TXT, size=13)),
        height=290,
    )
    st.plotly_chart(fig_part, use_container_width=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — SYMBOL DEEP DIVE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab4:
    symbols_available = sorted(df["symbol"].unique().tolist())
    sel_sym = st.selectbox("Select Symbol", symbols_available)

    if sel_sym:
        sym_df = df[df["symbol"] == sel_sym].copy()
        result = next((r for r in (results or []) if r["symbol"] == sel_sym), None)

        if result is None:
            st.info("Run analysis first to see signal details for this symbol.")
            # Still show price/volume chart
            if len(sym_df) > 0:
                st.plotly_chart(chart_price_volume(sym_df, sel_sym), use_container_width=True)
        else:
            # Signal header
            sig_color = {"BUY": "#00e676", "WATCH": "#ffd740", "AVOID": "#ff3d5a"}.get(
                result["signal_key"], TXT2
            )
            sc = result["total_score"]
            st.markdown(
                f'<div style="background:#0b0f1a; border:1px solid {sig_color}33; border-radius:8px;'
                f' padding:16px 20px; margin-bottom:20px; display:flex;'
                f' justify-content:space-between; align-items:center;">'
                f'<div>'
                f'<div style="font-family:\'IBM Plex Mono\',monospace; font-size:1.5rem;'
                f' font-weight:700; color:#d0daf0;">{sel_sym}</div>'
                f'<div style="font-size:0.78rem; color:#6a80a8; margin-top:4px;">'
                f'Trades: {result["stats"]["trades"]} · Vol: {fmt_num(result["stats"]["total_volume"])} · '
                f'Turnover: {fmt_num(result["stats"]["total_turnover"])}</div>'
                f'</div>'
                f'<div style="text-align:right;">'
                f'<div style="font-family:\'IBM Plex Mono\',monospace; font-size:2.2rem;'
                f' font-weight:700; color:{sig_color};">{sc:.0f}</div>'
                f'<div style="font-size:0.72rem; color:#6a80a8;">/ 100</div>'
                f'<div style="margin-top:6px;">{signal_badge(result["signal_key"])}</div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True
            )

            col_r, col_b = st.columns(2)
            with col_r:
                st.plotly_chart(chart_radar(result), use_container_width=True)
            with col_b:
                st.plotly_chart(chart_score_breakdown(result), use_container_width=True)

            st.plotly_chart(chart_price_volume(sym_df, sel_sym), use_container_width=True)
            st.plotly_chart(chart_broker_flow(result), use_container_width=True)

            # Trade log
            with st.expander(f"📋 All {len(sym_df)} trades for {sel_sym}"):
                disp = [c for c in ["price","quantity","amount","buyer_broker","seller_broker","time"]
                        if c in sym_df.columns]
                st.dataframe(sym_df[disp], use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center; padding:24px 0 10px 0; border-top:1px solid #1a2235; margin-top:30px;">
    <div style="font-family:'IBM Plex Mono',monospace; font-size:0.62rem; color:#3a506a;">
        NEPSE Smart Money Scanner · Floorsheet Intelligence Engine · v2.0<br>
        ⚠ For research and educational purposes only. Not financial advice.
    </div>
</div>
""", unsafe_allow_html=True)
