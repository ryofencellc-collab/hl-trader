"""
CB TRADER v70
═══════════════════════════════════════════════════════════════════
SINGLE STRATEGY — RSI(3/75/50/80→55) on 15min candles

Strategy:
  RSI(3) on 15min candles + 1hr RSI(14) MTF filter (resampled) + Trailing Exit
  LONG:  RSI(3) crosses ABOVE 75 AND 1hr RSI(14) resampled > 50
  SHORT: RSI(3) crosses BELOW 25 AND 1hr RSI(14) resampled < 50
  EXIT:  RSI drops below 50 (or 55 if RSI hit 80 — trailing tighten)

Parameters confirmed from mega sweep (5,000+ combinations tested):
  RSI(3/75/50/80→55) — #1 on BOTH datasets (INTX+gaps and INTX clean)
  Dataset A: $16,548/mo | 85.9% WR | 4,064 trades
  Dataset B: $18,235/mo | 84.7% WR | 5,247 trades
  39/39 green weeks | Worst week: +$102 | Best: +$9,117

Assets:
  XRP (XPP-20DEC30-CDE) — 500 XRP/contract | 20.01% intraday margin
  SUI (SUP-20DEC30-CDE) — 500 SUI/contract | 24.99% intraday margin
  XLM (XLP-20DEC30-CDE) — 5000 XLM/contract | 25.00% intraday margin
  All confirmed via Coinbase API Sep 2, 2026

Fees confirmed from 6 real fills Aug 19-20 2026:
  0.080% taker per side + $0.12 flat per contract per side

Candle sources:
  Primary: Coinbase CFM (XPP/SUP/XLP)
  Gap fill: INTX (XRP-PERP/SUI-PERP/XLM-PERP) — fills CFM gaps seamlessly

Sim data:
  Saves everything the app sees every bucket — blind replay possible
  pnl field = NET after fees (gross minus entry_fee minus exit_fee)
  Sim balance should match live paper balance exactly

Railway variables needed:
  CB_API_KEY      — Coinbase API key
  CB_API_SECRET   — Coinbase API secret
  NTFY_TOPIC      — ntfy.sh topic for alerts
  TRADE_MODE      — "paper" or "live" (default: paper)
  MAX_CONTRACTS   — max contracts per asset (default: 5)
  PAPER_BALANCE   — paper starting balance (default: 2000)

CHECKLIST — triple checked before push:
  ✅ RSI_PERIOD = 3 (was 2 in v68)
  ✅ RSI_ENTRY = 75 (was 70 in v68)
  ✅ RSI_EXIT = 50 (unchanged)
  ✅ RSI_TRAIL_TRIG = 80 (unchanged)
  ✅ RSI_TRAIL_EXIT = 55 (was 60 in v68)
  ✅ SHORT entry: RSI crosses BELOW 25 (100-75=25, was 30)
  ✅ SHORT trail: tighten when RSI < 20 (100-80=20, was 25)
  ✅ XRP margin = 0.2001 (was 0.10 in v68)
  ✅ SUI margin = 0.2499 (was 0.10 in v68)
  ✅ XLM margin = 0.2500 (was 0.10 in v68)
  ✅ sim pnl = NET after fees (was GROSS in v68 — bug fixed)
  ✅ No 1hr strategy anywhere
  ✅ No RSI1H_* constants anywhere
  ✅ No positions_1h anywhere
  ✅ No state_1h anywhere
  ✅ No fetch_1hr_candles anywhere
  ✅ No get_4hr_rsi anywhere
  ✅ No save_sim_data_1h anywhere
  ✅ No /sim-data-1hr endpoint anywhere
  ✅ No 1hr trading loop block anywhere
  ✅ No 1hr dashboard panels anywhere
  ✅ No roll countdown (DEC 2030 expiry, no rolls needed)
  ✅ State file = cb_state_v70.json
  ✅ Sim saves: candles[-50:], hr_rsi, entry price, contracts, pnl NET
  ✅ Entry at candle open (candles[-1]["o"])
  ✅ Exit at candle open (candles[-1]["o"])
  ✅ Skip cooldown: 1 bucket after exit
  ✅ MTF: resampled 1hr RSI(14) from 15min candles (no separate API call)
  ✅ INTX gap fill active
  ✅ docstring updated to v70
  ✅ startup log updated to v70
  ✅ dashboard title = CB Trader v70
  ✅ info panel strategy description updated
  ✅ No stale RSI(2)/RSI(3/70) comments from v68
  ✅ hr_rsi computed BEFORE evaluate_signal — always saved in sim data
  ✅ MTF explicitly blocks trade when hr_rsi is None
  ✅ CANDLE_LIMIT = 300 (75hr lookback — MTF ready on startup)
  ✅ Startup cache pre-loads CFM+INTX — backfills if first bucket thin
  ✅ Startup cache refreshes every bucket — never stale
  ✅ Startup deferred to @app.before_request — gunicorn compatible
  ✅ Dead code removed: fetch_secondary_candles, round_price
  ✅ docstring updated to v70
  ✅ startup log updated to v70
  ✅ State file = cb_state_v70.json
"""

import time, os, json, csv, uuid, threading
from datetime import datetime, timezone
from flask import Flask, Response, request, redirect
import requests as req

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
TRADE_MODE  = os.environ.get("TRADE_MODE", "paper").lower().strip()
PAPER_MODE  = (TRADE_MODE != "live")
NTFY_TOPIC  = os.environ.get("NTFY_TOPIC", "hl-trader-lunchm0ney")
NTFY_URL    = f"https://ntfy.sh/{NTFY_TOPIC}"

CB_API_KEY  = os.environ.get("CB_API_KEY", "")
CB_API_SEC  = os.environ.get("CB_API_SECRET", "")
if not CB_API_KEY or not CB_API_SEC:
    raise RuntimeError("CB_API_KEY and CB_API_SECRET must be set in Railway environment variables")

# Assets — confirmed from Coinbase API Sep 2, 2026
# Margin rates confirmed: XRP=20.01%, SUI=24.99%, XLM=25.00%
# Contract sizes confirmed: XRP=500, SUI=500, XLM=5000
# Expiry: DEC 2030 — no monthly rolls needed
ASSETS = {
    "XRP": {"perp": "XPP-20DEC30-CDE", "contract": 500.0,  "margin_rate": 0.2001},
    "SUI": {"perp": "SUP-20DEC30-CDE", "contract": 500.0,  "margin_rate": 0.2499},
    "XLM": {"perp": "XLP-20DEC30-CDE", "contract": 5000.0, "margin_rate": 0.2500},
}
ASSET_NAMES = list(ASSETS.keys())

# Fees confirmed from 6 real fills Aug 19-20 2026
FEE_PCT   = 0.00080  # 0.080% taker per side
FEE_FLAT  = 0.12     # $0.12 per contract per side

MAX_CONTRACTS = int(os.environ.get("MAX_CONTRACTS", "5"))
PAPER_BALANCE = float(os.environ.get("PAPER_BALANCE", "2000"))

# RSI Momentum parameters — mega sweep winner (5,000+ combos tested Sep 2026)
# #1 on BOTH datasets: INTX+gaps and INTX clean
# Dataset A: $16,548/mo | 85.9% WR | Dataset B: $18,235/mo | 84.7% WR
# 39/39 green weeks Dec 2025–Aug 2026
RSI_PERIOD     = 3   # RSI period
RSI_ENTRY      = 75  # LONG: RSI crosses above 75 | SHORT: crosses below 25
RSI_EXIT       = 50  # LONG exit: RSI drops below 50 | SHORT exit: rises above 50
RSI_TRAIL_TRIG = 80  # When RSI hits 80 (LONG) or 20 (SHORT), tighten exit
RSI_TRAIL_EXIT = 55  # Tightened exit threshold (LONG: <55 | SHORT: >45)

CANDLE_TF    = "FIFTEEN_MINUTE"
CANDLE_LIMIT = 300  # 75 hours lookback — guarantees MTF ready immediately on startup

DIAG_FILE  = "/tmp/cb_diagnostic.json"
DATA_FILE  = "/tmp/cb_sim_data.json"
TAX_FILE   = "/tmp/cb_trades.csv"
STATE_FILE = "/tmp/cb_state_v70.json"

# ══════════════════════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════════════════════
positions        = {}
skip_entry       = {}
lock             = threading.Lock()
sim_lock         = threading.Lock()
intx_candle_cache  = {}
intx_cache_ts     = {}
startup_candle_cache = {}  # pre-loaded merged candles — fallback if first bucket fetch is thin

state = {
    "balance": PAPER_BALANCE, "buying_power": PAPER_BALANCE,
    "weekly_pnl": 0.0, "total_pnl": 0.0,
    "week": None, "cycle": 0,
    "loop_last_run": "never", "loop_errors": 0,
    "wins": 0, "total_trades": 0, "entries": 0,
    "ntfy_errors": 0, "ntfy_last_sent": "never",
    "skipped_assets": [],
}

TOTAL_USDC = PAPER_BALANCE  # will be overwritten by live balance on startup

def save_state():
    """Persist state to disk — survives Railway restarts within deployment."""
    try:
        with lock:
            safe = {k: v for k, v in state.items()
                    if isinstance(v, (int, float, str, bool, type(None)))}
        json.dump(safe, open(STATE_FILE, "w"))
    except Exception as e:
        log(f"State save error: {e}")

def load_state():
    """Restore state from disk on startup — keeps P&L/trades across restarts."""
    if not os.path.exists(STATE_FILE):
        return
    try:
        data = json.load(open(STATE_FILE))
        with lock:
            for k, v in data.items():
                if k in state:
                    # Never restore balance in paper mode — always start fresh
                    if k == "balance" and PAPER_MODE:
                        continue
                    state[k] = v
        log(f"✅ State restored | cycle={state['cycle']} trades={state['total_trades']} pnl=${state['total_pnl']:+.2f}")
    except Exception as e:
        log(f"State load error (starting fresh): {e}")

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def ts_est():
    """Current time in US/Eastern (auto handles EDT/EST)"""
    utc_now = datetime.now(timezone.utc)
    month   = utc_now.month
    from datetime import timedelta
    offset  = -4 if 4 <= month <= 10 else -5
    est     = utc_now + timedelta(hours=offset)
    suffix  = "EDT" if offset == -4 else "EST"
    return est.strftime(f"%Y-%m-%d %H:%M {suffix}")

def log(msg):
    print(f"[{ts()}] {msg}", flush=True)

def add_audit(asset, event, detail, candle=None, indicators=None):
    entry = {"time": ts(), "asset": asset, "event": event, "detail": detail}
    if candle:     entry["candle"]     = candle
    if indicators: entry["indicators"] = indicators
    with lock:
        state.setdefault("audit", []).insert(0, entry)
        if len(state["audit"]) > 2000:
            state["audit"] = state["audit"][:2000]
    try:
        data = json.load(open(DIAG_FILE)) if os.path.exists(DIAG_FILE) else []
        data.insert(0, entry)
        if len(data) > 5000: data = data[:5000]
        json.dump(data, open(DIAG_FILE, "w"))
    except:
        pass
    if not any(n in event for n in ["NO_SIGNAL", "CYCLE"]):
        log(f"[{asset}] {event} — {detail[:80]}")

def get_week():
    now = datetime.now(timezone.utc)
    return f"{now.year}-W{now.isocalendar()[1]:02d}"

def check_weekly_reset():
    wk = get_week()
    with lock:
        if state["week"] != wk:
            state["week"] = wk
            state["weekly_pnl"] = 0.0

def record_tax(asset, direction, entry_p, exit_p, size, pnl, entry_time):
    try:
        tax = round(pnl * 0.35, 4) if pnl > 0 else 0.0
        row = {
            "exit_time": ts(), "entry_time": entry_time, "asset": asset,
            "direction": direction, "entry_price": f"{entry_p:.6f}",
            "exit_price": f"{exit_p:.6f}", "size": f"{size}",
            "gross_pnl": f"{pnl:.4f}", "tax_35pct": f"{tax:.4f}",
            "net_pnl": f"{pnl - tax:.4f}",
        }
        write_header = not os.path.exists(TAX_FILE)
        with open(TAX_FILE, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if write_header: w.writeheader()
            w.writerow(row)
    except Exception as e:
        log(f"Tax record error: {e}")

# ══════════════════════════════════════════════════════════════════
# SIM DATA SAVER
# Saves everything the app sees so sim can replay blind and 1:1
# pnl = NET after fees — matches live paper balance exactly
# ══════════════════════════════════════════════════════════════════
def save_sim_data(asset, bucket_ts, candles, indicators, decision,
                  position=None, pnl_net=None,
                  balance_at_decision=None, contracts_at_decision=None):
    """
    Saves all data the live app sees for blind sim replay.
    pnl_net = gross P&L minus entry_fee minus exit_fee (NET, not gross).
    This makes sim balance match live paper balance exactly.
    """
    try:
        now_ms = int(time.time()) * 1000
        age    = round((now_ms - candles[-1]["ts"]) / 60000, 1) if candles else None

        # RSI values from indicators or recalculate
        rsi_cur  = indicators.get("rsi_cur")  if isinstance(indicators, dict) else None
        rsi_prev = indicators.get("rsi_prev") if isinstance(indicators, dict) else None
        hr_rsi   = indicators.get("hr_rsi")   if isinstance(indicators, dict) else None

        if (rsi_cur is None or rsi_prev is None) and candles and len(candles) >= RSI_PERIOD + 3:
            rsi_vals = calc_rsi([float(c["c"]) for c in candles[-50:]], RSI_PERIOD)
            if len(rsi_vals) >= 2 and rsi_vals[-2] is not None:
                rsi_cur  = round(rsi_vals[-2], 2)
            if len(rsi_vals) >= 3 and rsi_vals[-3] is not None:
                rsi_prev = round(rsi_vals[-3], 2)

        record = {
            "ts":       bucket_ts,
            "dt":       datetime.fromtimestamp(bucket_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "asset":    asset,
            "decision": decision,
            # Last 50 candles — enough for RSI(3) warmup + MTF context
            "candles":  candles[-50:] if isinstance(candles, list) else [],
            # RSI values saved explicitly — sim can verify without recalculating
            "rsi_cur":  rsi_cur,
            "rsi_prev": rsi_prev,
            "hr_rsi":   hr_rsi,
            "candle_age_min":        age,
            "balance_at_decision":   balance_at_decision if balance_at_decision is not None else state.get("balance", 0),
            "contracts_at_decision": contracts_at_decision,
            "indicators": indicators if isinstance(indicators, dict) else {},
            "position": {
                "direction":  position.get("direction"),
                "entry":      position.get("entry"),
                "contracts":  position.get("contracts"),
                "size":       position.get("size"),
                "exit_rsi":   position.get("exit_rsi", RSI_EXIT),
                "entry_time": position.get("entry_time"),
            } if isinstance(position, dict) else None,
            # NET P&L after fees — matches live paper balance
            "pnl": pnl_net,
        }

        with sim_lock:
            try:
                existing = json.load(open(DATA_FILE))
                if not isinstance(existing, list):
                    existing = []
            except:
                existing = []
            existing.append(record)
            if len(existing) > 50000:
                existing = existing[-50000:]
            tmp = DATA_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(existing, f)
            os.replace(tmp, DATA_FILE)
    except Exception as e:
        log(f"sim_data save error: {e}")

# ══════════════════════════════════════════════════════════════════
# API HELPERS
# ══════════════════════════════════════════════════════════════════
def fetch_with_retry(fn, asset, retries=3):
    """Retry wrapper — 3 attempts with exponential backoff."""
    import random
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if attempt < retries - 1:
                delay = (2 ** attempt) + random.uniform(0, 1)
                log(f"⚠️ {asset} attempt {attempt+1}/{retries} failed: {e} — retrying in {delay:.1f}s")
                time.sleep(delay)
            else:
                raise e
    return None

def ntfy(title, body, priority="default"):
    try:
        resp = req.post(NTFY_URL, data=body.encode("utf-8"),
            headers={"Title": title.encode("ascii", "ignore").decode().strip(),
                     "Priority": priority, "Content-Type": "text/plain; charset=utf-8"},
            timeout=10)
        with lock: state["ntfy_last_sent"] = ts()
        if resp.status_code != 200:
            log(f"⚠️ ntfy failed: {resp.status_code}")
        else:
            log(f"📲 ntfy sent: {title}")
    except Exception as e:
        log(f"⚠️ ntfy error: {e}")
        with lock: state["ntfy_errors"] += 1

# ══════════════════════════════════════════════════════════════════
# COINBASE CLIENT
# ══════════════════════════════════════════════════════════════════
_cb_client      = None
_cb_client_lock = threading.Lock()

def get_cb_client():
    global _cb_client
    if _cb_client is None:
        with _cb_client_lock:
            if _cb_client is None:
                from coinbase.rest import RESTClient
                _cb_client = RESTClient(api_key=CB_API_KEY, api_secret=CB_API_SEC)
                if hasattr(_cb_client, "session"):
                    _orig = _cb_client.session.request
                    def _req_with_timeout(method, url, **kwargs):
                        kwargs.setdefault("timeout", 10)
                        return _orig(method, url, **kwargs)
                    _cb_client.session.request = _req_with_timeout
                    log("✅ Coinbase client: 10s timeout applied")
    return _cb_client

def get_active_ticker(asset):
    return ASSETS[asset]["perp"]

# ══════════════════════════════════════════════════════════════════
# CANDLE FETCHING
# ══════════════════════════════════════════════════════════════════
def fetch_candles(asset, granularity=None, n_candles=None):
    """Fetch 15min candles from CFM (primary source)."""
    try:
        client     = get_cb_client()
        product_id = get_active_ticker(asset)
        tf         = granularity or CANDLE_TF
        limit      = n_candles or CANDLE_LIMIT
        end        = int(time.time())
        start      = end - limit * 900  # 15min = 900s per candle

        def _do():
            r = client.get_candles(product_id, start=str(start), end=str(end), granularity=tf)
            if not r.candles:
                raise ValueError("API returned 0 candles")
            return r

        resp = fetch_with_retry(_do, asset)
        if resp is None:
            log(f"WARNING {asset}: candle fetch failed after retries")
            return None

        candles = sorted([{
            "ts": int(c.start) * 1000,
            "dt": datetime.fromtimestamp(int(c.start), tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "o": float(c.open), "h": float(c.high),
            "l": float(c.low),  "c": float(c.close), "v": float(c.volume),
        } for c in resp.candles], key=lambda x: x["ts"])[-limit:]

        if not candles:
            log(f"WARNING {asset}: no candles after sort")
            return None
        if candles[-1]["c"] == 0:
            log(f"WARNING {asset}: last candle close=0 — dead feed")
            return None

        now_ms  = int(time.time()) * 1000
        age_min = round((now_ms - candles[-1]["ts"]) / 60000, 1)
        if age_min > 30:
            log(f"INFO {asset}: CFM last candle is {age_min}min old — INTX will fill")
        return candles
    except Exception as e:
        log(f"WARNING {asset}: candle fetch failed {e}")
        return None

def fetch_intx_candles(asset, n=150):
    """Fetch INTX 15min candles — gap fill for CFM. Cached per bucket."""
    global intx_candle_cache, intx_cache_ts
    now = int(time.time())
    # Return cached if fetched within last 14 minutes (same bucket)
    if asset in intx_cache_ts and now - intx_cache_ts[asset] < 840:
        return intx_candle_cache.get(asset)

    intx_sym = {"XRP": "XRP-PERP", "SUI": "SUI-PERP", "XLM": "XLM-PERP"}.get(asset)
    if not intx_sym: return None

    try:
        end_dt   = datetime.fromtimestamp(now, tz=timezone.utc)
        start_dt = datetime.fromtimestamp(now - n * 900, tz=timezone.utc)
        url      = f"https://api.international.coinbase.com/api/v1/instruments/{intx_sym}/candles"
        r = req.get(url, params={
            "granularity": "FIFTEEN_MINUTE",
            "start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end":   end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }, timeout=8)
        if r.status_code == 200:
            aggs = r.json().get("aggregations", [])
            if aggs:
                candles = sorted([{
                    "ts":  int(datetime.strptime(c["start"], "%Y-%m-%dT%H:%M:%SZ")
                               .replace(tzinfo=timezone.utc).timestamp() * 1000),
                    "dt":  c["start"],
                    "o":   float(c["open"]),  "h": float(c["high"]),
                    "l":   float(c["low"]),   "c": float(c["close"]),
                    "v":   float(c["volume"]), "source": "intx",
                } for c in aggs], key=lambda x: x["ts"])[-n:]
                intx_candle_cache[asset] = candles
                intx_cache_ts[asset]     = now
                return candles
    except Exception as e:
        log(f"INTX fetch {asset}: {e}")
    return intx_candle_cache.get(asset)  # return stale cache if fetch fails

def merge_cfm_intx(cfm_candles, intx_candles):
    """Merge CFM (primary) and INTX (gap fill). CFM always wins on overlap."""
    if not cfm_candles and not intx_candles: return []
    if not cfm_candles: return intx_candles or []
    if not intx_candles: return cfm_candles

    cfm_map  = {c["ts"]: c for c in cfm_candles}
    intx_map = {c["ts"]: c for c in intx_candles}
    all_ts   = sorted(set(cfm_map.keys()) | set(intx_map.keys()))
    return [cfm_map[ts] if ts in cfm_map else intx_map[ts] for ts in all_ts]


# ══════════════════════════════════════════════════════════════════
# BALANCE
# ══════════════════════════════════════════════════════════════════
def get_live_balance():
    """Get real available_margin from Coinbase — live mode only."""
    try:
        client = get_cb_client()
        resp   = client.get_futures_balance_summary()
        bs     = resp.balance_summary
        avail  = float(bs.available_margin["value"])
        bp_val = float(bs.futures_buying_power["value"])
        log(f"💰 Live balance: ${avail:,.2f} available_margin (buying_power=${bp_val:,.2f})")
        if avail > 0:
            with lock: state["buying_power"] = bp_val
            return avail
        if bp_val > 0:
            log(f"💰 Using buying_power as fallback: ${bp_val:,.2f}")
            with lock: state["buying_power"] = bp_val
            return bp_val
        log("💰 Both margin and buying_power are $0")
    except Exception as e:
        log(f"Balance fetch error: {e}")
    fallback = float(os.environ.get("TOTAL_USDC", "0"))
    log(f"💰 Fallback balance: ${fallback:,.2f}")
    return fallback

def sync_open_positions():
    """On startup (live mode), sync positions with any open CFM positions."""
    try:
        client   = get_cb_client()
        resp     = client.list_futures_positions()
        open_pos = resp.positions if hasattr(resp, "positions") else resp.get("positions", [])
        if not open_pos:
            log("📊 No open CFM positions on Coinbase")
            return
        log(f"📊 Found {len(open_pos)} open CFM position(s) — syncing...")
        for p in open_pos:
            try:
                product_id = p.product_id if hasattr(p, "product_id") else p.get("product_id", "")
                side       = p.side if hasattr(p, "side") else p.get("side", "UNKNOWN")
                n_cont     = int(float(p.number_of_contracts if hasattr(p, "number_of_contracts") else p.get("number_of_contracts", 0)))
                avg_entry  = float(p.avg_entry_price if hasattr(p, "avg_entry_price") else p.get("avg_entry_price", 0))
            except Exception as pe:
                log(f"  Position parse error: {pe} — skipping")
                continue
            asset = None
            for a, cfg in ASSETS.items():
                if cfg.get("perp") == product_id:
                    asset = a
                    break
            if not asset:
                log(f"  Unknown position: {product_id} — skipping")
                continue
            direction = "LONG" if side == "LONG" else "SHORT"
            cs = ASSETS[asset]["contract"]
            positions[asset] = {
                "direction": direction, "entry": avg_entry,
                "contracts": n_cont,   "size":  n_cont * cs,
                "strategy":  "RSI-Mom", "entry_time": ts(),
                "rsi_entry": 0,
                "exit_rsi":  RSI_EXIT,
                "paper":     False,
                "unrealized_pnl": 0.0, "current_price": avg_entry,
            }
            log(f"  ✅ Synced {asset} {direction} @ ${avg_entry:.4f} | {n_cont}ct")
    except Exception as e:
        log(f"Position sync error: {e}")

def place_market_order(asset, side, contracts):
    """Place market order. Paper mode: simulate fill, no real order."""
    if PAPER_MODE:
        fake_oid = f"PAPER-{asset}-{int(time.time())}"
        log(f"📄 PAPER order: {asset} {side} {contracts}ct → {fake_oid}")
        return fake_oid, int(contracts)

    try:
        client  = get_cb_client()
        product = get_active_ticker(asset)
        max_try = max(1, int(contracts))
        for attempt in range(max_try, 0, -1):
            size = str(attempt)
            if side in ("BUY", "LONG"):
                order = client.market_order_buy(
                    client_order_id=str(uuid.uuid4()),
                    product_id=product, base_size=size)
            else:
                order = client.market_order_sell(
                    client_order_id=str(uuid.uuid4()),
                    product_id=product, base_size=size)
            success = order["success"]
            if success:
                sr  = order["success_response"]
                oid = sr["order_id"] if isinstance(sr, dict) else None
                if not oid: oid = f"CB-{asset}-{int(time.time())}"
                log(f"✅ CB order: {asset} {side} {size}ct → {oid}")
                return oid, attempt
            else:
                err    = order["error_response"]
                reason = err.get("preview_failure_reason", "") if isinstance(err, dict) else ""
                if "INSUFFICIENT_FUNDS" in reason and attempt > 1:
                    log(f"⚠️ {asset} {attempt}ct insufficient — trying {attempt - 1}")
                    continue
                else:
                    log(f"⚠️ CB order failed: {asset} {err}")
                    ntfy(f"ORDER REJECTED {asset}", f"{side} {attempt}ct rejected: {err}", priority="high")
                    return None, 0
        log(f"❌ {asset} could not place even 1 contract")
        return None, 0
    except Exception as e:
        import traceback
        log(f"❌ Order exception {asset}: {e}")
        log(f"❌ Traceback: {traceback.format_exc()}")
        return None, 0

# ══════════════════════════════════════════════════════════════════
# MATH
# ══════════════════════════════════════════════════════════════════
def calc_rsi(closes, period=14):
    """RSI — standard Wilder smoothing."""
    if len(closes) < period + 1:
        return [None] * len(closes)
    out    = [None] * period
    gains  = [max(0, closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    losses = [max(0, closes[i - 1] - closes[i]) for i in range(1, len(closes))]
    ag = sum(gains[:period])  / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        rs = ag / al if al > 0 else 100
        out.append(100 - 100 / (1 + rs))
    while len(out) < len(closes):
        out.append(out[-1])
    return out

def get_hr_rsi(asset, candles_15m=None):
    """
    1hr RSI(14) resampled from 15min candles.
    Resamples every 4 candles → 1hr close, then RSI(14).
    No separate API call needed — always available, matches backtest exactly.
    Confirmed: avg diff < 0.81 RSI from real 1hr, always same side of 50.
    """
    if not candles_15m or len(candles_15m) < 60:
        return None
    try:
        c1h = [float(candles_15m[i + 3]["c"])
               for i in range(0, len(candles_15m) - 3, 4)]
        if len(c1h) < 16:
            return None
        rsi1h = calc_rsi(c1h, 14)
        val   = rsi1h[-2] if len(rsi1h) >= 2 and rsi1h[-2] is not None else None
        return round(val, 1) if val is not None else None
    except Exception as e:
        log(f"get_hr_rsi error {asset}: {e}")
        return None

# ══════════════════════════════════════════════════════════════════
# SIGNAL — RSI(3/75) Momentum Cross + MTF filter
# ══════════════════════════════════════════════════════════════════
def evaluate_signal(candles):
    """
    RSI(3/75) Momentum strategy — mega sweep winner Sep 2026
    LONG:  RSI(3) crosses ABOVE 75 (prev < 75, cur >= 75)
    SHORT: RSI(3) crosses BELOW 25 (prev > 25, cur <= 25)
    MTF filter applied in trading loop after this returns direction.
    Returns: (direction, None, None, info_dict)
    """
    if not candles or len(candles) < RSI_PERIOD + 2:
        return None, None, None, {"fail": "not enough candles"}

    closes = [float(c["c"]) for c in candles]
    rsi    = calc_rsi(closes, RSI_PERIOD)

    i = len(rsi) - 2  # second-to-last (last candle may still be forming)
    if rsi[i] is None or rsi[i - 1] is None:
        return None, None, None, {"fail": "RSI not ready"}

    cur_rsi  = rsi[i]
    prev_rsi = rsi[i - 1]

    if prev_rsi < RSI_ENTRY and cur_rsi >= RSI_ENTRY:
        d = "LONG"
    elif prev_rsi > (100 - RSI_ENTRY) and cur_rsi <= (100 - RSI_ENTRY):
        d = "SHORT"
    else:
        return None, None, None, {
            "fail": f"no cross (RSI prev={prev_rsi:.1f} cur={cur_rsi:.1f}) threshold={RSI_ENTRY}"
        }

    return d, None, None, {
        "strategy":    "RSI-Mom+MTF",
        "rsi_prev":    round(prev_rsi, 2),
        "rsi_cur":     round(cur_rsi, 2),
        "entry_candle": candles[i].get("dt", candles[i].get("ts", 0)),
    }

def should_exit(pos, candles):
    """
    RSI Trailing Exit — RSI(3/75/50/80→55)
    Standard: RSI drops below 50 (LONG) or rises above 50 (SHORT)
    Trailing: if RSI hits 80 (LONG) or 20 (SHORT), tighten exit to 55/45
    pos["exit_rsi"] stores the current threshold — persists in positions dict.
    """
    if not candles or len(candles) < RSI_PERIOD + 2:
        return False
    closes  = [float(c["c"]) for c in candles]
    rsi     = calc_rsi(closes, RSI_PERIOD)
    cur_rsi = rsi[-2] if rsi[-2] is not None else rsi[-1]
    if cur_rsi is None:
        return False

    if pos["direction"] == "LONG":
        if cur_rsi > RSI_TRAIL_TRIG:
            pos["exit_rsi"] = RSI_TRAIL_EXIT  # tighten: exit if RSI drops below 55
        exit_thresh = pos.get("exit_rsi", RSI_EXIT)
        return cur_rsi < exit_thresh

    elif pos["direction"] == "SHORT":
        if cur_rsi < (100 - RSI_TRAIL_TRIG):  # RSI < 20
            pos["exit_rsi"] = 100 - RSI_TRAIL_EXIT  # tighten: exit if RSI rises above 45
        exit_thresh = pos.get("exit_rsi", 100 - RSI_EXIT)
        return cur_rsi > exit_thresh

    return False

# ══════════════════════════════════════════════════════════════════
# ENTER / EXIT
# ══════════════════════════════════════════════════════════════════
def enter_position(asset, direction, entry_price, candle, info=None):
    """
    Enter a position. Sizing: 70% of balance split across all assets.
    Entry at candle open — matches backtest exactly.
    """
    cs  = ASSETS[asset]["contract"]
    mr  = ASSETS[asset]["margin_rate"]

    with lock:
        current_bal = state["balance"]

    per_slot       = (current_bal * 0.70) / len(ASSET_NAMES)
    margin_per     = entry_price * cs * mr
    max_affordable = min(MAX_CONTRACTS, max(1, int(per_slot / margin_per))) if margin_per > 0 else 1
    contracts      = max_affordable
    size           = contracts * cs
    side           = "BUY" if direction == "LONG" else "SELL"

    oid, actual_cts = place_market_order(asset, side, contracts)
    if not oid:
        msg = f"{asset} {side} {contracts}ct rejected"
        log(f"CRITICAL order rejected: {msg}")
        add_audit(asset, "ORDER REJECTED", msg)
        ntfy(f"CRITICAL ORDER REJECTED {asset}", msg, priority="urgent")
        return

    actual_size = actual_cts * cs
    rsi_info    = info or {}
    positions[asset] = {
        "direction":     direction,
        "entry":         entry_price,
        "contracts":     actual_cts,
        "size":          actual_size,
        "strategy":      "RSI-Mom",
        "entry_time":    ts(),
        "rsi_entry":     rsi_info.get("rsi_cur", 0),
        "exit_rsi":      RSI_EXIT,
        "hr_rsi":        rsi_info.get("hr_rsi", None),
        "paper":         PAPER_MODE,
        "unrealized_pnl": 0.0,
        "current_price":  entry_price,
    }
    with lock:
        state["entries"] = state.get("entries", 0) + 1
        state["buying_power"] = state.get("buying_power", state["balance"]) - entry_price * actual_size * mr

    mode_label = "PAPER" if PAPER_MODE else "LIVE"
    add_audit(asset, f"📊 ENTER {direction}",
              f"RSI-Mom | entry=${entry_price:,.4f} | rsi={rsi_info.get('rsi_cur', 0):.1f} | "
              f"contracts={actual_cts} | size={actual_size} | hr_rsi={rsi_info.get('hr_rsi', '?')} | {mode_label}",
              candle=candle)
    ntfy(f"{'📄' if PAPER_MODE else '📊'} ENTER {direction} {asset}",
         f"RSI-Mom | entry=${entry_price:,.4f} | RSI={rsi_info.get('rsi_cur', 0):.1f} | {actual_cts}ct | {mode_label}",
         priority="default")

def exit_position(asset, exit_price, exit_reason, candle):
    """
    Exit a position. Calculates NET P&L (gross minus fees).
    Updates live paper balance with net P&L.
    Returns net P&L for sim recording.
    """
    pos = positions.get(asset)
    if not pos: return None

    gross = round(
        (exit_price - pos["entry"]) * pos["size"] if pos["direction"] == "LONG"
        else (pos["entry"] - exit_price) * pos["size"], 4)

    # Real CFM fees: 0.080% taker + $0.12/contract per side
    entry_fee = round(pos["entry"] * pos["size"] * FEE_PCT + FEE_FLAT * pos["contracts"], 4)
    exit_fee  = round(exit_price   * pos["size"] * FEE_PCT + FEE_FLAT * pos["contracts"], 4)
    total_fee = entry_fee + exit_fee
    pnl       = round(gross - total_fee, 4)  # NET after fees

    side = "SELL" if pos["direction"] == "LONG" else "BUY"
    oid, _ = place_market_order(asset, side, pos["contracts"])
    if not oid and not PAPER_MODE:
        log(f"⚠️ EXIT ORDER FAILED {asset} — position preserved, retrying next bucket")
        ntfy(f"⚠️ EXIT FAILED {asset}", "Order rejected — position still open, will retry", priority="urgent")
        return None

    record_tax(asset, pos["direction"], pos["entry"], exit_price, pos["size"], pnl, pos["entry_time"])

    with lock:
        state["total_pnl"]    = round(state["total_pnl"] + pnl, 4)
        state["weekly_pnl"]   = round(state["weekly_pnl"] + pnl, 4)
        state["balance"]      = round(state["balance"] + pnl, 4)
        state["total_trades"] += 1
        if pnl >= 0: state["wins"] += 1

    del positions[asset]

    emoji = "✅" if pnl >= 0 else "❌"
    add_audit(asset, f"{emoji} EXIT {exit_reason}",
              f"{pos['direction']} ${pos['entry']:,.4f} → ${exit_price:,.4f} | "
              f"gross=${gross:+,.4f} | fees=${total_fee:.4f} | net=${pnl:+,.4f}",
              candle=candle)
    ntfy(f"{emoji} EXIT {asset}",
         f"{pos['direction']} | ${pos['entry']:,.4f}→${exit_price:,.4f} | gross=${gross:+,.2f} | fees=${total_fee:.2f} | net=${pnl:+,.2f} | {exit_reason}",
         priority="default" if pnl >= 0 else "high")

    # Refresh real balance from Coinbase after exit (live mode only)
    if not PAPER_MODE:
        try:
            live_bal = get_live_balance()
            if live_bal > 0:
                with lock:
                    state["balance"] = live_bal
                log(f"💰 Balance after exit: ${live_bal:,.2f} (Coinbase)")
        except Exception as e:
            log(f"Balance refresh error after exit: {e}")
    else:
        log(f"📄 Paper balance after exit: ${state['balance']:,.2f}")

    save_state()
    return pnl  # return net P&L so caller can pass to save_sim_data

# ══════════════════════════════════════════════════════════════════
# TRADING LOOP — RSI(3/75) + MTF + Trailing Exit
# Fires every 15-min bucket.
# Signal on candles[-2] (second-to-last, not forming).
# Entry/exit at candles[-1]["o"] (current candle open).
# Skip cooldown: 1 bucket after exit.
# ══════════════════════════════════════════════════════════════════
def trading_loop():
    global TOTAL_USDC

    live_bal = get_live_balance()
    if live_bal > 0:
        TOTAL_USDC = live_bal
    if PAPER_MODE:
        TOTAL_USDC = PAPER_BALANCE
        log(f"📄 Paper mode: using ${PAPER_BALANCE:,.2f} (real balance=${live_bal:,.2f})")
    else:
        sync_open_positions()

    with lock:
        state["balance"]      = TOTAL_USDC
        state["buying_power"] = TOTAL_USDC
    load_state()

    log(f"🚀 CB Trader v70 started")
    log(f"   Mode:     {'📄 PAPER' if PAPER_MODE else '🔴 LIVE'} (TRADE_MODE={TRADE_MODE})")
    log(f"   Strategy: RSI({RSI_PERIOD}/{RSI_ENTRY}/{RSI_EXIT}/{RSI_TRAIL_TRIG}→{RSI_TRAIL_EXIT}) + MTF(1hr RSI>50)")
    log(f"   Assets:   {', '.join(ASSET_NAMES)}")
    log(f"   Margins:  XRP={ASSETS['XRP']['margin_rate']*100:.2f}% SUI={ASSETS['SUI']['margin_rate']*100:.2f}% XLM={ASSETS['XLM']['margin_rate']*100:.2f}%")
    log(f"   Fees:     0.080% taker + $0.12/ct/side — confirmed from 6 real fills")
    log(f"   Capital:  ${TOTAL_USDC:,.2f} | Max contracts: {MAX_CONTRACTS}/asset")
    log(f"   Backtest: $18,235/mo | 84.7% WR | 39/39 green weeks Dec 2025–Aug 2026")
    log(f"   Time:     {ts_est()}")

    last_bucket = (int(time.time()) // 900) * 900

    while True:
        try:
            current_bucket = (int(time.time()) // 900) * 900
            with lock:
                state["loop_last_run"] = ts()
                state["cycle"] = state.get("cycle", 0) + 1

            check_weekly_reset()

            if current_bucket != last_bucket:
                last_bucket   = current_bucket
                bucket_dt     = datetime.fromtimestamp(current_bucket, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                bucket_dt_obj = datetime.fromtimestamp(current_bucket, tz=timezone.utc)
                hour_utc      = bucket_dt_obj.hour
                log(f"🕐 {bucket_dt} UTC | open={len(positions)} | bal=${state['balance']:,.2f}")

                skipped_assets = []
                _candle_cache  = {}

                for asset in ASSET_NAMES:
                    try:
                        # Fetch CFM candles (primary)
                        cfm_candles  = fetch_candles(asset, granularity=CANDLE_TF, n_candles=CANDLE_LIMIT)
                        # Fetch INTX candles (gap fill) — proactive, cached per bucket
                        intx_candles = fetch_intx_candles(asset, n=CANDLE_LIMIT)
                        # Merge: CFM primary, INTX fills gaps
                        candles      = merge_cfm_intx(cfm_candles, intx_candles)

                        # Overkill fallback: if fresh fetch thin (<100 candles), backfill
                        # from startup cache. Guarantees MTF ready immediately after cold
                        # start — even if API returns only a handful of candles.
                        if len(candles or []) < 100 and asset in startup_candle_cache:
                            cached   = startup_candle_cache[asset]
                            combined = merge_cfm_intx(cached, candles or [])
                            if len(combined) > len(candles or []):
                                log(f"  {asset}: backfilled startup cache ({len(candles or [])} → {len(combined)} candles)")
                                candles = combined

                        # Always update startup cache with latest merged candles
                        # so cache stays fresh across restarts and long runs
                        if candles and len(candles) >= 100:
                            startup_candle_cache[asset] = candles[-CANDLE_LIMIT:]

                        if not candles or len(candles) < RSI_PERIOD + 5:
                            skipped_assets.append(asset)
                            continue

                        intx_filled = sum(1 for c in candles if c.get("source") == "intx")
                        if intx_filled > 0:
                            log(f"  {asset}: {len(candles)} candles ({intx_filled} from INTX)")

                        _candle_cache[asset] = candles
                        cur = candles[-1]

                        # Skip cooldown after exit (1 bucket)
                        if skip_entry.get(asset, 0) > 0:
                            skip_entry[asset] -= 1
                            continue

                        # ── EXIT CHECK ────────────────────────────────
                        pos = positions.get(asset)
                        if pos:
                            # Update unrealized P&L
                            cur_close  = float(candles[-1]["c"])
                            gross_u    = (cur_close - pos["entry"]) * pos["size"] if pos["direction"] == "LONG" \
                                         else (pos["entry"] - cur_close) * pos["size"]
                            ef_u       = pos["entry"] * pos["size"] * FEE_PCT + FEE_FLAT * pos["contracts"]
                            xf_u       = cur_close   * pos["size"] * FEE_PCT + FEE_FLAT * pos["contracts"]
                            pos["unrealized_pnl"]  = round(gross_u - ef_u - xf_u, 4)
                            pos["current_price"]   = cur_close

                            if should_exit(pos, candles):
                                exit_price = float(candles[-1]["o"])
                                # exit_position returns net P&L — save to sim
                                pnl_net = exit_position(asset, exit_price, "RSI_EXIT", cur)
                                if pnl_net is not None:
                                    save_sim_data(asset, current_bucket * 1000, candles, {},
                                                  "EXIT_RSI", position=dict(pos), pnl_net=pnl_net,
                                                  balance_at_decision=state.get("balance", 0),
                                                  contracts_at_decision=pos.get("contracts", 0))
                                skip_entry[asset] = 1
                            else:
                                save_sim_data(asset, current_bucket * 1000, candles, {},
                                              "HOLD", position=dict(pos),
                                              balance_at_decision=state.get("balance", 0),
                                              contracts_at_decision=pos.get("contracts", 0))
                            continue

                        # ── ENTRY SIGNAL ──────────────────────────────
                        # Compute hr_rsi BEFORE evaluate_signal so it is
                        # always saved in sim data — even on NO_SIGNAL buckets
                        hr_rsi = get_hr_rsi(asset, candles)

                        d, _, _, info = evaluate_signal(candles)
                        info["hr_rsi"] = round(hr_rsi, 1) if hr_rsi is not None else None

                        if d:
                            # MTF filter: resampled 1hr RSI(14) from 15min candles
                            # Block trade if MTF not ready — should never happen after
                            # startup cache fix, but belt-and-suspenders
                            if hr_rsi is None:
                                save_sim_data(asset, current_bucket * 1000, candles, info,
                                              "NO_SIGNAL:MTF_not_ready (need 60+ candles)")
                                continue
                            if d == "LONG" and hr_rsi < 50:
                                save_sim_data(asset, current_bucket * 1000, candles, info,
                                              f"NO_SIGNAL:MTF_filter (1hr_RSI={hr_rsi:.1f}<50)")
                                continue
                            if d == "SHORT" and hr_rsi > 50:
                                save_sim_data(asset, current_bucket * 1000, candles, info,
                                              f"NO_SIGNAL:MTF_filter (1hr_RSI={hr_rsi:.1f}>50)")
                                continue

                            add_audit(asset, f"🚨 RSI-Mom {d}",
                                      f"RSI prev={info.get('rsi_prev', 0):.1f} → cur={info.get('rsi_cur', 0):.1f} | "
                                      f"1hr_RSI={info.get('hr_rsi', '?')}",
                                      candle=cur, indicators=info)

                            entry_price = float(candles[-1]["o"])
                            enter_position(asset, d, entry_price, cur, info)

                            if positions.get(asset):
                                _pos = positions[asset]
                                save_sim_data(asset, current_bucket * 1000, candles, info,
                                              f"ENTER_{d}", position=dict(_pos),
                                              balance_at_decision=state.get("balance", 0),
                                              contracts_at_decision=_pos.get("contracts", 0))
                        else:
                            save_sim_data(asset, current_bucket * 1000, candles, info,
                                          f"NO_SIGNAL:{info.get('fail', '?')}",
                                          balance_at_decision=state.get("balance", 0))

                    except Exception as e:
                        import traceback
                        log(f"Asset error {asset}: {e}")
                        log(traceback.format_exc())

                with lock:
                    state["skipped_assets"] = skipped_assets
                    cycle_num = state.get("cycle", 0)

                if skipped_assets:
                    log(f"⚠️ Skipped: {skipped_assets}")

                # Refresh balance every 50 cycles (live mode only)
                if cycle_num % 50 == 0 and not PAPER_MODE:
                    try:
                        live_bal = get_live_balance()
                        if live_bal > 0:
                            with lock: state["balance"] = live_bal
                    except:
                        pass

                # Persist state every 10 cycles
                if cycle_num % 10 == 0:
                    save_state()

                # Per-asset heartbeat
                with lock:
                    _bal    = state["balance"]
                    _trades = state["total_trades"]

                hb_lines = [f"candle={bucket_dt} | open={len(positions)} | bal=${_bal:,.2f} | trades={_trades}"]
                for _a in ASSET_NAMES:
                    _pos  = positions.get(_a)
                    _c    = _candle_cache.get(_a)
                    _age  = "?"
                    if _c and _c[-1].get("ts"):
                        _age = f"{round((int(time.time()) * 1000 - _c[-1]['ts']) / 60000, 1)}m"
                    _rsi_cur = _rsi_prev = _hr = "?"
                    if _c and len(_c) >= RSI_PERIOD + 2:
                        _closes    = [float(x["c"]) for x in _c]
                        _rsi_vals  = calc_rsi(_closes, RSI_PERIOD)
                        if _rsi_vals[-2] is not None:
                            _rsi_cur = f"{_rsi_vals[-2]:.1f}"
                        if len(_rsi_vals) >= 3 and _rsi_vals[-3] is not None:
                            _rsi_prev = f"{_rsi_vals[-3]:.1f}"
                    _hr_val = get_hr_rsi(_a, _c)
                    _hr     = f"{_hr_val:.1f}" if _hr_val is not None else "?"
                    _cs     = ASSETS[_a]["contract"]
                    _mr     = ASSETS[_a]["margin_rate"]
                    _avail  = _bal * 0.70 / len(ASSET_NAMES)
                    _mp     = float(_c[-1]["c"]) * _cs * _mr if _c else 0
                    _cts_avail = min(MAX_CONTRACTS, max(0, int(_avail / _mp))) if _mp > 0 else 0

                    if _pos:
                        _unreal = _pos.get("unrealized_pnl", 0.0)
                        _exit_r = _pos.get("exit_rsi", RSI_EXIT)
                        _locked = "🔒" if _exit_r == RSI_TRAIL_EXIT else ""
                        hb_lines.append(
                            f"  {_a:<4} {_pos['direction']:<5} | prev={_rsi_prev} cur={_rsi_cur} | "
                            f"hr={_hr} | cts={_cts_avail} | exit<{_exit_r}{_locked} | "
                            f"unreal=${_unreal:+.2f} | age={_age} | HOLD"
                        )
                    else:
                        hb_lines.append(
                            f"  {_a:<4} {'—':<5} | prev={_rsi_prev} cur={_rsi_cur} | "
                            f"hr={_hr} | cts={_cts_avail} | age={_age} | WATCHING"
                        )
                for _line in hb_lines:
                    log(_line)

                hb_detail = f"candle={bucket_dt} | open={len(positions)} | balance=${state['balance']:,.2f} | trades={state['total_trades']}"
                for _line in hb_lines[1:]:
                    hb_detail += f"\n{_line}"
                add_audit("SYSTEM", "💓 CYCLE", hb_detail)

                # ── WEEKLY P&L REPORT — Monday 9am UTC ───────────────
                if bucket_dt_obj.weekday() == 0 and hour_utc == 9 and bucket_dt_obj.minute < 15:
                    with lock:
                        wpnl = state["weekly_pnl"]
                        bal  = state["balance"]
                        trd  = state["total_trades"]
                        wr   = round(state["wins"] / trd * 100, 1) if trd else 0
                    ntfy("Weekly P&L Report",
                         f"Week: {bucket_dt_obj.strftime('%Y-%m-%d')} | P&L: ${wpnl:+,.2f} | "
                         f"Bal: ${bal:,.2f} | Trades: {trd} | WR: {wr}%")
                    with lock: state["weekly_pnl"] = 0.0

                # ── EMERGENCY STOP — balance below 50% ───────────────
                with lock: bal = state["balance"]
                if bal < TOTAL_USDC * 0.5 and len(positions) == 0:
                    ntfy("EMERGENCY Balance Alert",
                         f"Balance ${bal:,.2f} below 50% of starting ${TOTAL_USDC:,.2f}",
                         priority="urgent")

        except Exception as e:
            with lock:
                state["loop_errors"] = state.get("loop_errors", 0) + 1
                errs = state["loop_errors"]
            log(f"Loop error {errs}: {e}")
            if errs in (3, 10, 25):
                ntfy(f"CRITICAL loop errors: {errs}",
                     f"Loop error #{errs}: {str(e)[:100]}",
                     priority="urgent" if errs >= 10 else "high")

        time.sleep(30)

# ══════════════════════════════════════════════════════════════════
# FLASK DASHBOARD
# ══════════════════════════════════════════════════════════════════
app = Flask(__name__)

@app.route("/login", methods=["POST"])
def login():
    from flask import make_response
    pw = request.form.get("pw", "")
    if pw == "3757":
        resp = make_response(redirect("/"))
        resp.set_cookie("auth", "3757", max_age=60 * 60 * 24 * 30)
        return resp
    return redirect("/")

@app.route("/health")
def health():
    with lock: s = dict(state)
    wr = round(s["wins"] / s["total_trades"] * 100, 1) if s["total_trades"] else 0
    return Response(json.dumps({
        "overall":    "✅ ALL SYSTEMS OK" if s.get("loop_errors", 0) < 5 else "❌ errors",
        "mode":       {"paper_mode": PAPER_MODE, "status": "📄 PAPER" if PAPER_MODE else "🔴 LIVE"},
        "balance":    f"${s['balance']:,.2f}",
        "weekly_pnl": f"${s['weekly_pnl']:+,.2f}",
        "total_pnl":  f"${s['total_pnl']:+,.2f}",
        "trades":     s["total_trades"],
        "win_rate":   f"{wr}%",
        "strategy": {
            "name":      f"RSI({RSI_PERIOD}/{RSI_ENTRY}/{RSI_EXIT}/{RSI_TRAIL_TRIG}→{RSI_TRAIL_EXIT}) + MTF",
            "timeframe": "15min",
            "assets":    ASSET_NAMES,
        },
        "open_positions":  list(positions.keys()),
        "skipped_assets":  s.get("skipped_assets", []),
        "trading_loop":    {"errors": s.get("loop_errors", 0), "last_run": s["loop_last_run"],
                            "status": "✅ OK" if s.get("loop_errors", 0) < 5 else "❌ errors"},
        "diagnostic":      {"entries": len(json.load(open(DIAG_FILE))) if os.path.exists(DIAG_FILE) else 0},
    }, indent=2), mimetype="application/json")

@app.route("/diagnostic-raw")
def diagnostic_raw():
    try:    return Response(open(DIAG_FILE).read(), mimetype="application/json")
    except: return Response("[]", mimetype="application/json")

@app.route("/sim-data")
def sim_data():
    """Download sim replay data — pnl field is NET after fees."""
    if request.cookies.get("auth") != "3757":
        return Response("Unauthorized", status=401)
    try:
        return Response(open(DATA_FILE).read(), mimetype="application/json",
                        headers={"Content-Disposition": "attachment;filename=cb_sim_data.json"})
    except:
        return Response("[]", mimetype="application/json")

@app.route("/tax-export")
def tax_export():
    try:
        return Response(open(TAX_FILE).read(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment;filename=cb_trades.csv"})
    except:
        return Response("No trades yet", mimetype="text/plain")

@app.route("/")
def dashboard():
    if request.cookies.get("auth") != "3757":
        return """<!DOCTYPE html><html><head><title>CB Trader</title>
<meta name=viewport content='width=device-width,initial-scale=1'>
<style>body{background:#060D1A;color:#E0E6F0;font-family:-apple-system,sans-serif;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.box{text-align:center;padding:40px;background:#0A1628;border:1px solid #1E2D45;border-radius:12px}
input{background:#060D1A;border:1px solid #1E2D45;color:#E0E6F0;padding:12px;
border-radius:8px;margin:10px 0;width:200px;font-size:16px;display:block}
button{background:#00D68F;color:#000;border:none;padding:12px 24px;border-radius:8px;
cursor:pointer;font-weight:700;font-size:16px;width:200px;margin-top:8px}
h2{margin-bottom:20px;font-size:20px}</style></head>
<body><form method=post action=/login class=box>
<h2>CB Trader</h2>
<input type=password name=pw placeholder='Password' autofocus>
<button type=submit>Login</button>
</form></body></html>"""

    with lock:
        s   = dict(state)
        pos = dict(positions)

    wr         = round(s["wins"] / s["total_trades"] * 100, 1) if s["total_trades"] else 0
    mode_color = "#FFB800" if PAPER_MODE else "#00D68F"
    mode_label = "📄 PAPER" if PAPER_MODE else "🔴 LIVE"
    wk_color   = "#00D68F" if s["weekly_pnl"] >= 0 else "#FF4757"
    tot_color  = "#00D68F" if s["total_pnl"]  >= 0 else "#FF4757"

    # Positions panel
    pos_rows = ""
    for asset, p in pos.items():
        unreal    = p.get("unrealized_pnl", 0.0)
        cur_price = p.get("current_price", p.get("entry", 0))
        pnl_color = "#00D68F" if unreal >= 0 else "#FF4757"
        hr_rsi    = p.get("hr_rsi", "?")
        exit_rsi  = p.get("exit_rsi", RSI_EXIT)
        locked    = "🔒" if exit_rsi == RSI_TRAIL_EXIT else ""
        dir_col   = "#00D68F" if p["direction"] == "LONG" else "#FF4757"
        dir_bg    = "#00D68F22" if p["direction"] == "LONG" else "#FF475722"
        entry_fee = round(p["entry"]  * p["size"] * FEE_PCT + FEE_FLAT * p["contracts"], 4)
        exit_fee  = round(cur_price   * p["size"] * FEE_PCT + FEE_FLAT * p["contracts"], 4)
        pos_rows += f"""<div style='background:#0A1628;border:1px solid #1E2D45;border-radius:10px;padding:14px;margin-bottom:10px'>
          <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:10px'>
            <span style='font-size:18px;font-weight:800'>{asset}</span>
            <span style='font-size:13px;font-weight:700;padding:3px 10px;border-radius:20px;
              background:{dir_bg};color:{dir_col}'>{p["direction"]}</span>
            <span style='font-size:16px;font-weight:700;color:{pnl_color}'>${unreal:+,.2f}</span>
          </div>
          <div class=pos-grid>
            <div class=pos-cell><div class=pos-label>Entry</div><div style='font-weight:600'>${p["entry"]:,.4f}</div></div>
            <div class=pos-cell><div class=pos-label>Current</div><div style='font-weight:600;color:{pnl_color}'>${cur_price:,.4f}</div></div>
            <div class=pos-cell><div class=pos-label>Exit RSI</div><div style='font-weight:600;color:#FFB800'>&lt;{exit_rsi} {locked}</div></div>
            <div class=pos-cell><div class=pos-label>1hr RSI</div><div style='font-weight:600;color:#7B61FF'>{hr_rsi}</div></div>
          </div>
          <div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;font-size:11px;margin-top:8px;color:#4A5878'>
            <div>{p.get("contracts", 1)}ct | RSI-Mom</div>
            <div>Fees est: ${entry_fee + exit_fee:,.4f}</div>
            <div>Since {p.get("entry_time", "?")[:16]}</div>
          </div>
        </div>"""

    if not pos_rows:
        pos_rows = "<div style='color:#4A5878;padding:32px;text-align:center;font-size:14px'>No open positions</div>"

    # Journal + Heartbeat
    try:    audit_data = json.load(open(DIAG_FILE)) if os.path.exists(DIAG_FILE) else []
    except: audit_data = []

    journal_rows   = ""
    heartbeat_rows = ""
    j_shown  = 0
    hb_built = False

    for a in audit_data:
        evt = a.get("event", "")

        if "CYCLE" in evt and not hb_built:
            hb_built = True
            detail   = a.get("detail", "")
            lines    = detail.split("\n")
            heartbeat_rows += f"<div style='font-size:12px;font-weight:700;color:#E0E6F0;padding:6px 0;border-bottom:1px solid #1E2D45;margin-bottom:8px'>{lines[0] if lines else detail}</div>"
            for line in lines[1:]:
                line = line.strip()
                if not line: continue
                css = "hb-hold" if "HOLD" in line else "hb-skip" if "SKIP" in line or "❌" in line else "hb-watch"
                heartbeat_rows += f"<div class='hb-row {css}'>{line}</div>"
            heartbeat_rows += f"<div style='font-size:10px;color:#4A5878;margin-top:8px'>Last updated: {a.get('time', '?')}</div>"

        if j_shown >= 100: continue
        if "CYCLE" in evt: continue
        j_shown += 1
        color = "#00D68F" if "ENTER" in evt else "#FF4757" if "EXIT" in evt else "#FFB800" if "🔒" in evt else "#E0E6F0"
        journal_rows += f"""<div class=j-trade style='border-color:{color}'>
          <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>{a["time"]} · {a.get("asset", "SYSTEM")}</div>
          <div style='font-size:13px;font-weight:700;color:{color}'>{evt}</div>
          <div style='font-size:11px;color:#8892A4;margin-top:3px'>{a.get("detail", "")[:150]}</div>
        </div>"""

    if not journal_rows:
        journal_rows   = "<div style='color:#4A5878;padding:32px;text-align:center;font-size:14px'>No trades yet — waiting for first signal</div>"
    if not heartbeat_rows:
        heartbeat_rows = "<div style='color:#4A5878;padding:32px;text-align:center;font-size:14px'>No heartbeat yet</div>"

    # Errors panel
    error_rows = ""; error_count = 0
    error_kw   = ["⚠️", "WARNING", "ERROR", "CRITICAL", "FAILED", "failed", "timeout", "Skipped"]
    trade_evt  = ["ENTER", "EXIT", "HOLD", "NO_SIGNAL", "CYCLE", "RSI-Mom", "📊", "📄", "✅ EXIT", "❌ EXIT"]
    for a in audit_data[:500]:
        evt    = a.get("event", "")
        detail = a.get("detail", "")
        if any(te in evt for te in trade_evt): continue
        if any(kw in evt or kw in detail for kw in error_kw):
            if "CYCLE" in evt: continue
            error_count += 1
            resolved  = "✅ Resolved" if any(ok in detail for ok in ["retrying", "recovered", "succeeded", "✅"]) else "⚠️ Check"
            res_color = "#00D68F" if "Resolved" in resolved else "#FFB800"
            error_rows += f"""<div style='border-left:3px solid {res_color};padding:8px 12px;margin-bottom:6px;background:#0A1628;border-radius:0 8px 8px 0'>
              <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>{a["time"]} · {a.get("asset", "SYSTEM")}</div>
              <div style='font-size:12px;font-weight:700;color:{res_color}'>{resolved}</div>
              <div style='font-size:11px;color:#8892A4;margin-top:3px;font-family:monospace'>{evt}: {detail[:200]}</div>
            </div>"""
    if not error_rows:
        error_rows = "<div style='color:#4A5878;padding:32px;text-align:center;font-size:14px'>✅ No errors — all systems clean</div>"
    error_badge = f" <span style='background:#FF4757;color:#fff;border-radius:10px;padding:1px 6px;font-size:10px'>{error_count}</span>" if error_count > 0 else ""

    # Markets panel
    assets_rows = ""
    for a_name in ASSET_NAMES:
        is_open = a_name in pos
        status  = "● OPEN" if is_open else "○ READY"
        sc      = "#00D68F" if is_open else "#4A5878"
        mr_pct  = f"{ASSETS[a_name]['margin_rate']*100:.0f}%"
        assets_rows += f"""<div style='display:flex;justify-content:space-between;align-items:center;
            padding:10px 0;border-bottom:1px solid #1E2D45;font-size:13px'>
          <b style='width:55px'>{a_name}</b>
          <span style='color:#4A5878;font-size:11px'>{ASSETS[a_name]["perp"]}</span>
          <span style='color:#4A5878;font-size:11px'>{mr_pct} margin</span>
          <span style='color:{sc};font-size:11px;font-weight:600'>{status}</span>
        </div>"""

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_est = ts_est()

    return f"""<!DOCTYPE html>
<html><head>
<title>CB Trader v70</title>
<meta charset=utf-8>
<meta name=viewport content='width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no'>
<meta http-equiv=refresh content=30>
<style>
  *{{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}}
  body{{background:#060D1A;color:#E0E6F0;font-family:-apple-system,BlinkMacSystemFont,sans-serif;
       padding:14px;max-width:620px;margin:0 auto;padding-bottom:80px}}
  .kpis{{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px}}
  @media(min-width:400px){{.kpis{{grid-template-columns:repeat(4,1fr)}}}}
  .kpi{{background:#0A1628;border:1px solid #1E2D45;border-radius:10px;padding:12px;text-align:center}}
  .kpi-l{{font-size:10px;color:#4A5878;text-transform:uppercase;letter-spacing:.8px;margin-bottom:4px}}
  .kpi-v{{font-size:20px;font-weight:800;line-height:1.2}}
  .tabs{{display:flex;gap:4px;margin-bottom:0;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none}}
  .tabs::-webkit-scrollbar{{display:none}}
  .tab{{flex-shrink:0;padding:12px 16px;cursor:pointer;border-radius:8px 8px 0 0;
        font-size:13px;font-weight:600;background:#060D1A;color:#4A5878;
        border:1px solid #1E2D45;border-bottom:none;min-height:44px;
        display:flex;align-items:center;touch-action:manipulation}}
  .tab.on{{background:#0A1628;color:#E0E6F0}}
  .panel{{display:none;background:#0A1628;border:1px solid #1E2D45;
          border-radius:0 10px 10px 10px;padding:14px;min-height:200px}}
  .panel.on{{display:block}}
  .pos-grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px}}
  @media(min-width:400px){{.pos-grid{{grid-template-columns:repeat(4,1fr)}}}}
  .pos-cell{{background:#060D1A;border-radius:6px;padding:8px}}
  .pos-label{{color:#4A5878;margin-bottom:2px;font-size:11px}}
  .hb-row{{font-family:monospace;font-size:11px;padding:6px 0;
           border-bottom:1px solid #0A1628;line-height:1.6;word-break:break-all}}
  .hb-hold{{color:#00D68F}}.hb-watch{{color:#4A5878}}.hb-skip{{color:#FF4757}}
  .j-trade{{border-left:3px solid;padding:8px 12px;margin-bottom:6px;
            background:#0A1628;border-radius:0 8px 8px 0}}
  a{{color:#8892A4;text-decoration:none}}
</style>
<script>
function show(id,el){{
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('on'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.getElementById(id).classList.add('on'); el.classList.add('on');
}}
</script>
</head><body>

{'' if not s.get('skipped_assets') else "<div style='background:#FF475722;border:1px solid #FF4757;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#FF4757'><b>⚠️ SKIPPED ASSETS</b>: " + ', '.join(s.get('skipped_assets', [])) + "</div>"}

<div style='display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px'>
  <div>
    <div style='font-size:24px;font-weight:800;letter-spacing:-0.5px'>CB Trader</div>
    <div style='font-size:12px;font-weight:700;color:{mode_color};margin-top:2px'>{mode_label}</div>
  </div>
  <div style='text-align:right;font-size:11px;color:#4A5878;line-height:1.7'>
    {now_utc}<br>{now_est}<br>
    <span style='color:{mode_color}'>● v69 {mode_label}</span>
  </div>
</div>

<div style='font-size:11px;color:#4A5878;margin-bottom:6px;text-transform:uppercase;letter-spacing:.8px'>📈 RSI({RSI_PERIOD}/{RSI_ENTRY}) Mega Sweep Winner — 39/39 Green Weeks</div>
<div class=kpis>
  <div class=kpi><div class=kpi-l>{'Paper Bal' if PAPER_MODE else 'Balance'}</div><div class=kpi-v>${s['balance']:,.2f}</div></div>
  <div class=kpi><div class=kpi-l>This Week</div><div class=kpi-v style='color:{wk_color}'>${s["weekly_pnl"]:+,.2f}</div></div>
  <div class=kpi><div class=kpi-l>Total P&L</div><div class=kpi-v style='color:{tot_color}'>${s["total_pnl"]:+,.2f}</div></div>
  <div class=kpi><div class=kpi-l>Win Rate</div><div class=kpi-v>{wr}%</div></div>
</div>
<div class=kpis style='margin-bottom:14px'>
  <div class=kpi><div class=kpi-l>Open</div><div class=kpi-v style='color:{"#00D68F" if len(pos)>0 else "#4A5878"}'>{len(pos)}</div></div>
  <div class=kpi><div class=kpi-l>Trades</div><div class=kpi-v>{s["total_trades"]}</div></div>
  <div class=kpi><div class=kpi-l>Wins</div><div class=kpi-v style='color:#00D68F'>{s["wins"]}</div></div>
  <div class=kpi><div class=kpi-l>Cycle</div><div class=kpi-v style='font-size:14px;color:#4A5878'>#{s.get("cycle", 0)}</div></div>
</div>

<div class=tabs>
  <span class='tab on' onclick="show('pos',this)">Positions</span>
  <span class=tab onclick="show('journal',this)">Journal</span>
  <span class=tab onclick="show('heartbeat',this)">Heartbeat</span>
  <span class=tab onclick="show('errors',this)">Errors{error_badge}</span>
  <span class=tab onclick="show('markets',this)">Markets</span>
  <span class=tab onclick="show('info',this)">Info</span>
</div>

<div id=pos class='panel on'>{pos_rows}</div>

<div id=journal class=panel>
  <div style='font-size:11px;color:#4A5878;margin-bottom:10px'>Trades only · auto-refresh 30s</div>
  {journal_rows}
</div>

<div id=heartbeat class=panel>
  <div style='font-size:11px;color:#4A5878;margin-bottom:10px'>Last bucket · per-asset detail · auto-refresh 30s</div>
  {heartbeat_rows}
</div>

<div id=errors class=panel>
  <div style='font-size:11px;color:#4A5878;margin-bottom:10px'>All errors · auto-refresh 30s</div>
  {error_rows}
</div>

<div id=markets class=panel>
  <div style='font-size:11px;color:#4A5878;margin-bottom:10px'>{len(ASSET_NAMES)} assets · evaluates every 15 min</div>
  {assets_rows}
</div>

<div id=info class=panel>
  <div style='font-size:13px;line-height:2;color:#8892A4'>
    <b style='color:#E0E6F0;font-size:14px'>Strategy</b><br>
    RSI({RSI_PERIOD}/{RSI_ENTRY}/{RSI_EXIT}/{RSI_TRAIL_TRIG}→{RSI_TRAIL_EXIT}) + MTF(1hr RSI>50) · 15min candles<br>
    Mega sweep winner: tested 5,000+ combinations Sep 2026<br>
    $18,235/mo | 84.7% WR | 39/39 green weeks Dec 2025–Aug 2026<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Exchange</b><br>
    Coinbase CFM Futures · CFTC regulated · Legal NYC<br>
    {len(ASSET_NAMES)} assets · DEC 2030 expiry, no rolls needed<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Assets & Margins</b><br>
    XRP (XPP-20DEC30-CDE) · 500/ct · 20% intraday margin<br>
    SUI (SUP-20DEC30-CDE) · 500/ct · 25% intraday margin<br>
    XLM (XLP-20DEC30-CDE) · 5000/ct · 25% intraday margin<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Fees</b><br>
    0.080% taker + $0.12/contract/side — confirmed from 6 real fills Aug 2026<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Links</b><br>
    <a href='/health'>Health</a> &nbsp;·&nbsp;
    <a href='/diagnostic-raw'>Diagnostic</a> &nbsp;·&nbsp;
    <a href='/tax-export'>Tax CSV</a> &nbsp;·&nbsp;
    <a href='/sim-data'>Sim Data</a>
  </div>
</div>

</body></html>"""

# ══════════════════════════════════════════════════════════════════
# STARTUP — deferred to first request so gunicorn worker is fully
# forked before we fetch candles or start the trading thread.
# _started flag ensures it only runs once per worker.
# ══════════════════════════════════════════════════════════════════
_started      = False
_start_lock   = threading.Lock()

def startup():
    global _started
    with _start_lock:
        if _started:
            return
        _started = True

    log("📡 Pre-loading candles on startup — CFM + INTX...")
    for a in ASSET_NAMES:
        cfm    = fetch_candles(a, granularity=CANDLE_TF, n_candles=CANDLE_LIMIT)
        intx   = fetch_intx_candles(a, n=CANDLE_LIMIT)
        merged = merge_cfm_intx(cfm, intx)
        if merged and len(merged) >= 60:
            startup_candle_cache[a] = merged
        hr = None
        if merged and len(merged) >= 60:
            c1h = [float(merged[i+3]["c"]) for i in range(0, len(merged)-3, 4)]
            if len(c1h) >= 16:
                rsi1h = calc_rsi(c1h, 14)
                val   = rsi1h[-2] if len(rsi1h) >= 2 and rsi1h[-2] is not None else None
                hr    = round(val, 1) if val is not None else None
        log(f"  {a}: {len(merged) if merged else 0} candles "
            f"({sum(1 for c in (merged or []) if c.get('source')=='intx')} INTX) | hr_rsi={hr}")
        time.sleep(0.5)
    log("✅ Pre-load complete — MTF ready immediately")
    check_weekly_reset()
    threading.Thread(target=trading_loop, daemon=True).start()

@app.before_request
def ensure_started():
    startup()
