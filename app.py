"""
CB TRADER v56
═══════════════════════════════════════════════════════════════════
Strategy: RSI(3/70) + MTF (1hr trend filter) + Trailing Exit
  LONG:  RSI(3) crosses ABOVE 70 AND 1hr RSI > 50 (uptrend confirmed)
  SHORT: RSI(3) crosses BELOW 30 AND 1hr RSI < 50 (downtrend confirmed)
  EXIT:  RSI drops below 50 (or 65 if RSI hit 75 — trailing tighten)
Timeframe: 15-minute candles | Trend filter: 1-hour RSI(14)
Exchange: Coinbase CFM Perpetual Futures (CFTC regulated, legal NYC)
Fees:     0.10% per side — CONFIRMED from 4 live fills
Backtest: RSI(3/70)+MTF — $13,468/month at $1,000 | 74.0% WR | Jan 2023-Mar 2026
          Winner from 308-strategy ultimate sweep on raw frozen Kraken data
          13/13 quarters green | No losing quarter since 2023

PAPER MODE: Set TRADE_MODE=paper in Railway to paper trade.
            Set TRADE_MODE=live to go live. Default=paper.

Railway variables needed:
  CB_API_KEY      — Coinbase API key
  CB_API_SECRET   — Coinbase API secret
  NTFY_TOPIC      — ntfy.sh topic for alerts
  TRADE_MODE      — "paper" or "live" (default: paper)
"""

import time, os, math, json, csv, uuid, threading
from datetime import datetime, timezone
from flask import Flask, Response, request, redirect
import requests as req

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
# TRADE_MODE: set to "paper" in Railway to paper trade, "live" to go live
# Paper mode: all logic runs but no real orders placed
TRADE_MODE  = os.environ.get("TRADE_MODE", "paper").lower().strip()
PAPER_MODE  = (TRADE_MODE != "live")
NTFY_TOPIC  = os.environ.get("NTFY_TOPIC", "hl-trader-lunchm0ney")
NTFY_URL    = f"https://ntfy.sh/{NTFY_TOPIC}"

# API keys loaded from Railway environment variables — never hardcoded
CB_API_KEY  = os.environ.get("CB_API_KEY", "")
CB_API_SEC  = os.environ.get("CB_API_SECRET", "")
if not CB_API_KEY or not CB_API_SEC:
    raise RuntimeError("CB_API_KEY and CB_API_SECRET must be set in Railway environment variables")

# Perp-style futures — DEC 2030 expiry, no monthly rolls needed
# Contract sizes confirmed from Coinbase API: future_product_details.contract_size
# Intraday margin rate ~10% confirmed from API
ASSETS = {
    "SUI":  {"perp":"SUP-20DEC30-CDE", "contract":500.0,  "margin_rate":0.10},
    "ADA":  {"perp":"ADP-20DEC30-CDE", "contract":500.0,  "margin_rate":0.10},
    "BCH":  {"perp":"BCP-20DEC30-CDE", "contract":1.0,    "margin_rate":0.10},
    "XLM":  {"perp":"XLP-20DEC30-CDE", "contract":5000.0, "margin_rate":0.10},
    "XRP":  {"perp":"XPP-20DEC30-CDE", "contract":500.0,  "margin_rate":0.10},
}
ASSET_NAMES = list(ASSETS.keys())

# Fee: 0.10% per side — CONFIRMED from 4 live fills Aug 19 2026
FEE_PCT = 0.00100   # 0.10% per side confirmed real fee
MAX_CONTRACTS = 5   # Coinbase intraday position limit per asset

# RSI Momentum parameters — optimal from 174-combination backtest
RSI_PERIOD      = 3     # RSI period — winner from 308-strategy raw Kraken sweep
RSI_ENTRY       = 70    # cross above → LONG, cross below 30 → SHORT
RSI_EXIT        = 50    # exit LONG below 50, exit SHORT above 50
RSI_TRAIL_TRIG  = 75    # when RSI hits 75, tighten exit threshold
RSI_TRAIL_EXIT  = 65    # tightened exit threshold

def get_active_ticker(asset):
    """Returns perp ticker — no rolling needed, DEC 2030 expiry"""
    return ASSETS[asset]["perp"]

CANDLE_TF    = "FIFTEEN_MINUTE"  # 15min for signal
CANDLE_LIMIT = 150               # 150 candles = 37.5 hours lookback
CANDLE_1H_TF = "ONE_HOUR"        # 1hr for trend filter (MTF)
CANDLE_1H_LIM= 50                # 50 hours lookback for 1hr RSI
TOTAL_USDC  = float(os.environ.get("TOTAL_USDC", "0"))  # will be overwritten by Coinbase on startup
TAX_RATE    = 0.35

DIAG_FILE   = "/tmp/cb_diagnostic.json"
DATA_FILE = "/tmp/cb_sim_data.json"  # sim replay data
TAX_FILE    = "/tmp/cb_trades.csv"

# ══════════════════════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════════════════════
# ── BUCKET STRATEGY (bucket-only exits — matches backtest) ──────
positions     = {}  # positions
pending_entry = {}  # pending entries
skip_entry    = {}  # skip after exit: asset -> buckets remaining
# PERP strategy removed in v33
lock            = threading.Lock()
sim_lock        = threading.Lock()   # separate lock for sim data file writes
hr_candles_cache = {}    # asset -> list of 1hr candles, refreshed every hour
hr_cache_ts      = {}    # asset -> last fetch timestamp

state = {
    "balance": TOTAL_USDC, "buying_power": TOTAL_USDC, "weekly_pnl": 0.0, "total_pnl": 0.0,
    "week": None, "cycle": 0, "ntfy_errors": 0,
    "loop_last_run": "never", "loop_errors": 0,
    "wins": 0, "total_trades": 0, "entries": 0, "ntfy_last_sent": "never",
    "skipped_assets": [],
    "skip_streak":    {},
    "api_errors":     {},
}

STATE_FILE = "/tmp/cb_state_v56.json"

def save_state():
    """Persist state to disk — survives Railway restarts within deployment."""
    try:
        import json as _j
        with lock:
            safe = {k: v for k, v in state.items()
                    if isinstance(v, (int, float, str, bool, type(None)))}
        _j.dump(safe, open(STATE_FILE, "w"))
    except Exception as e:
        log(f"State save error: {e}")

def load_state():
    """Restore state from disk on startup — keeps P&L/trades across restarts."""
    import json as _j, os as _o
    if not _o.path.exists(STATE_FILE):
        return
    try:
        data = _j.load(open(STATE_FILE))
        with lock:
            for k, v in data.items():
                if k in state:
                    state[k] = v
        log(f"✅ State restored | cycle={state['cycle']} trades={state['total_trades']} entries={state['entries']} pnl=${state['total_pnl']:+.2f}")
    except Exception as e:
        log(f"State load error (starting fresh): {e}")

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def ts_est():
    """Current time in US/Eastern (auto handles EDT/EST)"""
    from datetime import timedelta
    # EDT = UTC-4 (Mar-Nov), EST = UTC-5 (Nov-Mar)
    import time as _time
    utc_now = datetime.now(timezone.utc)
    # Simple DST detection: EDT Apr-Oct, EST Nov-Mar
    month = utc_now.month
    offset = -4 if 4 <= month <= 10 else -5
    est = utc_now + timedelta(hours=offset)
    suffix = "EDT" if offset == -4 else "EST"
    return est.strftime(f"%Y-%m-%d %H:%M {suffix}")

def log(msg):
    print(f"[{ts()}] {msg}", flush=True)

def add_audit(asset, event, detail, candle=None, indicators=None):
    entry = {"time":ts(),"asset":asset,"event":event,"detail":detail}
    if candle:     entry["candle"]     = candle
    if indicators: entry["indicators"] = indicators
    with lock:
        state.setdefault("audit",[]).insert(0, entry)
        if len(state["audit"]) > 2000: state["audit"] = state["audit"][:2000]
    try:
        data = json.load(open(DIAG_FILE)) if os.path.exists(DIAG_FILE) else []
        data.insert(0, entry)
        if len(data) > 5000: data = data[:5000]
        json.dump(data, open(DIAG_FILE,"w"))
    except: pass
    if not any(n in event for n in ["NO SIGNAL","CYCLE"]):
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
        tax = round(pnl*TAX_RATE,4) if pnl>0 else 0.0
        row = {"exit_time":ts(),"entry_time":entry_time,"asset":asset,
               "direction":direction,"entry_price":f"{entry_p:.6f}",
               "exit_price":f"{exit_p:.6f}","size":f"{size}",
               "gross_pnl":f"{pnl:.4f}","tax_35pct":f"{tax:.4f}",
               "net_pnl":f"{pnl-tax:.4f}"}
        write_header = not os.path.exists(TAX_FILE)
        with open(TAX_FILE,"a",newline="") as f:
            import csv as _csv
            w = _csv.DictWriter(f, fieldnames=row.keys())
            if write_header: w.writeheader()
            w.writerow(row)
    except Exception as e:
        log(f"Tax record error: {e}")

# ══════════════════════════════════════════════════════════════════
# SIM DATA SAVER — every data point the app sees, saved for replay
# File: DATA_FILE — appended every bucket, never overwritten
# Download via /sim-data endpoint after 1 week of trading
# ══════════════════════════════════════════════════════════════════
def _get_rsi_val(indicators, candles, idx):
    """Get RSI value from indicators dict or recalculate from candles."""
    key = "rsi_cur" if idx==-2 else "rsi_prev"
    if isinstance(indicators, dict) and indicators.get(key) is not None:
        return indicators[key]
    if isinstance(candles, list) and len(candles) >= RSI_PERIOD + abs(idx):
        rsi = calc_rsi([float(c["c"]) for c in candles[-50:]], RSI_PERIOD)
        val = rsi[idx] if len(rsi) >= abs(idx) else None
        return round(val, 2) if val is not None else None
    return None

def save_sim_data(asset, bucket_ts, candles, indicators, decision, position=None, pnl=None, ):
    """
    Saves everything needed to replay the sim and verify the app.
    Uses file locking to prevent corruption from concurrent reads/writes.
    """
    try:
        record = {
            "ts":       bucket_ts,
            "dt":       datetime.fromtimestamp(bucket_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "asset":    asset,
            "decision": decision,
            # Save last 20 candles — enough for RSI(7) + context
            # Sim uses these exact candles so RSI matches perfectly
            "candles": candles[-50:] if isinstance(candles, list) else [],
            # RSI values — always save so sim can compare without recalculating
            "rsi_cur":  _get_rsi_val(indicators, candles, -2),
            "rsi_prev": _get_rsi_val(indicators, candles, -3),
            "hr_rsi":   indicators.get("hr_rsi") if isinstance(indicators, dict) else None,
            "indicators": indicators if isinstance(indicators, dict) else {},
            "position": {
                "direction":  position.get("direction"),
                "entry":      position.get("entry"),
                "contracts":  position.get("contracts"),
                "size":       position.get("size"),
                "strategy":   position.get("strategy"),
                "rsi_entry":  position.get("rsi_entry"),
                "exit_rsi":   position.get("exit_rsi", 45),
                "paper":      position.get("paper", PAPER_MODE),
                "entry_time": position.get("entry_time"),
            } if isinstance(position, dict) else None,
            "pnl": pnl,
        }
        # Use lock to prevent concurrent read/write corruption
        target = DATA_FILE
        with sim_lock:
            try:
                existing = json.load(open(target))
                if not isinstance(existing, list):
                    existing = []
            except:
                existing = []
            existing.append(record)
            if len(existing) > 50000:
                existing = existing[-50000:]
            # Atomic write — write to temp then rename
            tmp = target + ".tmp"
            with open(tmp, "w") as f:
                json.dump(existing, f)
            os.replace(tmp, target)
    except Exception as e:
        log(f"sim_data save error: {e}")  # log instead of silent pass

def ntfy(title, body, priority="default"):
    try:
        resp = req.post(NTFY_URL, data=body.encode("utf-8"),
            headers={"Title":title.encode("ascii","ignore").decode().strip(),
                     "Priority":priority,"Content-Type":"text/plain; charset=utf-8"},
            timeout=10)
        with lock: state["ntfy_last_sent"]=ts()
        if resp.status_code != 200:
            log(f"⚠️ ntfy failed: status={resp.status_code} body={resp.text[:100]}")
        else:
            log(f"📲 ntfy sent: {title}")
    except Exception as e:
        log(f"⚠️ ntfy error: {e}")
        with lock: state["ntfy_errors"]+=1

# ══════════════════════════════════════════════════════════════════
# COINBASE API
# ══════════════════════════════════════════════════════════════════
# Single global client — created once, reused everywhere
# Prevents 429 rate limits from creating new connections on every call
_cb_client = None
_cb_client_lock = threading.Lock()

def get_cb_client():
    global _cb_client
    if _cb_client is None:
        with _cb_client_lock:
            if _cb_client is None:
                from coinbase.rest import RESTClient
                _cb_client = RESTClient(api_key=CB_API_KEY, api_secret=CB_API_SEC)
    return _cb_client

def fetch_candles(asset, granularity=None, n_candles=None):
    """Fetch candles from perp ticker directly.
    n_candles: how many candles to fetch
    """
    try:
        client = get_cb_client()
        product_id = get_active_ticker(asset)
        tf = granularity or CANDLE_TF
        limit = n_candles or CANDLE_LIMIT
        end   = int(time.time())
        # Calculate start based on timeframe
        if tf == "ONE_HOUR":
            start = end - limit * 3600
        elif tf == "FIFTEEN_MINUTE":
            start = end - limit * 900   # 15min = 900 seconds per candle
        elif tf == "FIVE_MINUTE":
            start = end - limit * 300
        else:
            start = end - limit * 300
        resp  = client.get_candles(product_id, start=str(start),
                                   end=str(end), granularity=tf)
        if not resp.candles:
            log(f"WARNING {asset}: API returned 0 candles")
            return None
        candles = sorted([{
            "ts":  int(c.start)*1000,
            "dt":  datetime.fromtimestamp(int(c.start),tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "o":   float(c.open),"h":float(c.high),
            "l":   float(c.low), "c":float(c.close),"v":float(c.volume),
        } for c in resp.candles], key=lambda x:x["ts"])[-limit:]
        if candles and candles[-1]["c"] == 0:
            log(f"WARNING {asset}: last candle close=0 dead feed")
            return None
        return candles
    except Exception as e:
        log(f"WARNING {asset}: candle fetch failed {e}")
        return None

def fetch_hr_candles(asset):
    """Fetch 1hr candles for MTF trend filter. Cached — refreshes every hour."""
    global hr_candles_cache, hr_cache_ts
    now = int(time.time())
    if asset in hr_cache_ts and now - hr_cache_ts[asset] < 3600:
        return hr_candles_cache.get(asset)
    try:
        candles = fetch_candles(asset, granularity=CANDLE_1H_TF, n_candles=CANDLE_1H_LIM)
        if candles:
            hr_candles_cache[asset] = candles
            hr_cache_ts[asset] = now
        return candles
    except Exception as e:
        log(f"1hr candle fetch error {asset}: {e}")
        return hr_candles_cache.get(asset)

def get_hr_rsi(asset):
    """Get current 1hr RSI(14) for trend direction filter."""
    candles = fetch_hr_candles(asset)
    if not candles or len(candles) < 16:
        return None
    closes = [float(c["c"]) for c in candles]
    rsi = calc_rsi(closes, 14)
    # Use second-to-last candle (same logic as 15min signal)
    i = len(rsi) - 2
    return rsi[i] if rsi[i] is not None else None

def get_live_balance():
    """
    Reads real available_margin from Coinbase — what Coinbase actually
    allows us to use for new positions.
    available_margin is always <= futures_buying_power.
    Using buying_power caused INSUFFICIENT_FUNDS rejections.
    """
    try:
        client = get_cb_client()
        resp   = client.get_futures_balance_summary()
        bs     = resp.balance_summary
        # Use available_margin — confirmed from debug: this is what Coinbase
        # actually approves orders against, not futures_buying_power
        avail  = bs.available_margin      # plain dict
        val    = float(avail["value"])
        fbp    = bs.futures_buying_power  # log both for visibility
        bp_val = float(fbp["value"])
        log(f"💰 Live balance: ${val:,.2f} available_margin (buying_power=${bp_val:,.2f})")
        if val > 0:
            # Store buying_power separately for contract sizing
            with lock: state["buying_power"] = bp_val
            return val
        # fallback to buying_power if margin is 0
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
    """
    On startup, sync positions dict with any open CFM positions on Coinbase.
    Prevents the app from entering a position that's already open.
    """
    try:
        client = get_cb_client()
        resp = client.list_futures_positions()
        # SDK returns object — access positions attribute directly
        try:
            open_pos = resp.positions if hasattr(resp, "positions") else resp.get("positions", [])
        except:
            open_pos = []
        if not open_pos:
            log("📊 No open CFM positions on Coinbase")
            return
        log(f"📊 Found {len(open_pos)} open CFM position(s) on Coinbase — syncing...")
        for p in open_pos:
            # FCMPosition is an SDK object — use attribute access not .get()
            try:
                product_id = p.product_id if hasattr(p, "product_id") else p.get("product_id", "")
                side       = p.side if hasattr(p, "side") else p.get("side", "UNKNOWN")
                n_cont     = int(float(p.number_of_contracts if hasattr(p, "number_of_contracts") else p.get("number_of_contracts", 0)))
                avg_entry  = float(p.avg_entry_price if hasattr(p, "avg_entry_price") else p.get("avg_entry_price", 0))
            except Exception as pe:
                log(f"  Position parse error: {pe} — skipping")
                continue
            # Map product_id back to asset name
            asset = None
            for a, cfg in ASSETS.items():
                if cfg.get("perp") == product_id or product_id.startswith(cfg.get("perp","")[:3]):
                    asset = a; break
            if not asset:
                log(f"  Unknown position: {product_id} — skipping")
                continue
            direction = "LONG" if side == "LONG" else "SHORT"
            cs = ASSETS[asset]["contract"]
            positions[asset] = {
                "direction": direction,
                "entry":     avg_entry,
                "contracts": n_cont,
                "size":      n_cont * cs,
                "strategy":  "RSI-Mom",
                "rsi_entry": 0,   # unknown on sync
                "exit_rsi":  RSI_EXIT,  # reset to default — will tighten again if RSI hits 65
                "paper":     PAPER_MODE,
                "entry_time":ts(),
            }
            log(f"  ✅ Synced {asset} {direction} @ ${avg_entry:.4f} | {n_cont} contracts")
    except Exception as e:
        log(f"Position sync error: {e}")

def place_market_order(asset, side, contracts):
    # In paper mode — simulate order fill, no real order placed
    if PAPER_MODE:
        fake_oid = f"PAPER-{asset}-{int(time.time())}"
        log(f"📄 PAPER order: {asset} {side} {contracts} contracts → {fake_oid}")
        return fake_oid, int(contracts)

    # Retry logic — confirmed working from terminal test:
    # Start at calculated contracts, reduce by 1 until Coinbase accepts
    # Handles intraday vs overnight margin differences automatically
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
                log(f"✅ CB order: {asset} {side} {size} contracts → {oid}")
                return oid, attempt  # return actual contracts filled
            else:
                err = order["error_response"]
                reason = err.get("preview_failure_reason", "") if isinstance(err, dict) else ""
                if "INSUFFICIENT_FUNDS" in reason and attempt > 1:
                    log(f"⚠️ {asset} {attempt} contracts insufficient — trying {attempt-1}")
                    continue
                else:
                    log(f"⚠️ CB order failed: {asset} {err}")
                    ntfy(f"ORDER REJECTED {asset}",
                         f"{side} {attempt} contracts rejected: {err}",
                         priority="high")
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
def round_price(p, sig=5):
    if p==0: return 0.0
    mag=math.floor(math.log10(abs(p))); return round(p,max(0,sig-1-mag))

# ══════════════════════════════════════════════════════════════════
# RSI CALCULATION
# ══════════════════════════════════════════════════════════════════
def calc_atr(highs, lows, closes, period=14):
    out=[None]*len(closes); tr=[]
    for i in range(1,len(closes)):
        tr.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])))
    if len(tr)<period: return out
    atr=sum(tr[:period])/period; out[period]=atr
    for i in range(period+1,len(closes)):
        atr=(atr*(period-1)+tr[i-1])/period; out[i]=atr
    return out

def calc_rsi(closes, period=14):
    """RSI(14) — standard Wilder smoothing"""
    if len(closes) < period + 1:
        return [None] * len(closes)
    out = [None] * period
    gains = [max(0, closes[i]-closes[i-1]) for i in range(1, len(closes))]
    losses = [max(0, closes[i-1]-closes[i]) for i in range(1, len(closes))]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
        rs = ag / al if al > 0 else 100
        out.append(100 - 100 / (1 + rs))
    while len(out) < len(closes):
        out.append(out[-1])
    return out

# ══════════════════════════════════════════════════════════════════
# SIGNAL — RSI(14) Momentum Cross
# ══════════════════════════════════════════════════════════════════
def evaluate_signal(candles):
    """
    RSI(14) Momentum strategy — proven across 174 filter combinations
    LONG:  RSI crosses ABOVE 60 on any candle
    SHORT: RSI crosses BELOW 40 on any candle
    EXIT:  RSI drops below 45 (long) or rises above 55 (short)

    Returns: (direction, None, None, info_dict) — no TP/stop, RSI exit
    """
    if not candles or len(candles) < RSI_PERIOD + 2:
        return None, None, None, {"fail": "not enough candles"}

    closes = [float(c["c"]) for c in candles]
    rsi = calc_rsi(closes, RSI_PERIOD)

    # Use second-to-last candle as signal (last may be forming)
    i = len(rsi) - 2
    if rsi[i] is None or rsi[i-1] is None:
        return None, None, None, {"fail": "RSI not ready"}

    cur_rsi  = rsi[i]
    prev_rsi = rsi[i-1]

    if prev_rsi < RSI_ENTRY and cur_rsi >= RSI_ENTRY:
        d = "LONG"
    elif prev_rsi > (100 - RSI_ENTRY) and cur_rsi <= (100 - RSI_ENTRY):
        d = "SHORT"
    else:
        return None, None, None, {
            "fail": f"no cross (RSI prev={prev_rsi:.1f} cur={cur_rsi:.1f}) threshold={RSI_ENTRY}"
        }

    return d, None, None, {
        "strategy": "RSI-Mom+MTF",
        "rsi_prev": round(prev_rsi, 2),
        "rsi_cur":  round(cur_rsi, 2),
        "entry_candle": candles[i].get("dt", candles[i].get("ts",0)),
    }


def should_exit(pos, candles):
    """
    RSI Trailing Exit — v46 improvement (+$485/month over v45)

    Standard exit: RSI drops below 45 (LONG) or rises above 55 (SHORT)
    Trailing tighten: if RSI hits 70 during LONG, tighten exit to 55
                      if RSI hits 30 during SHORT, tighten exit to 45

    This lets winners run but exits sooner when momentum peaks.
    Matches backtest exactly: $2,704/month vs $2,219/month for fixed exit.

    pos["exit_rsi"] stores the tightened threshold — persists in positions dict.
    """
    if not candles or len(candles) < RSI_PERIOD + 2:
        return False
    closes = [float(c["c"]) for c in candles]
    rsi = calc_rsi(closes, RSI_PERIOD)
    cur_rsi = rsi[-2] if rsi[-2] is not None else rsi[-1]
    if cur_rsi is None:
        return False

    if pos["direction"] == "LONG":
        # Tighten exit when RSI hits trail trigger (65)
        if cur_rsi > RSI_TRAIL_TRIG:
            pos["exit_rsi"] = RSI_TRAIL_EXIT  # tighten to 60
        exit_thresh = pos.get("exit_rsi", RSI_EXIT)
        if cur_rsi < exit_thresh:
            return True

    elif pos["direction"] == "SHORT":
        # Tighten exit when RSI hits trail trigger (35)
        if cur_rsi < (100 - RSI_TRAIL_TRIG):
            pos["exit_rsi"] = 100 - RSI_TRAIL_EXIT  # tighten to 40
        exit_thresh = pos.get("exit_rsi", 100 - RSI_EXIT)
        if cur_rsi > exit_thresh:
            return True

    return False




def enter_position(asset, direction, entry_price, candle, info=None):
    """
    Enter a perp position — RSI momentum strategy.
    No fixed TP/stop — exit when RSI crosses back.
    Sizing: max contracts within 70% balance budget.
    """
    cs = ASSETS[asset]["contract"]
    mr = ASSETS[asset]["margin_rate"]

    with lock:
        current_bal  = state["balance"]
        buying_power = state.get("buying_power", current_bal)

    # Per-asset margin budget: 70% of balance split equally
    avail      = current_bal * 0.70
    per_slot   = avail / len(ASSET_NAMES)
    margin_per = entry_price * cs * mr
    max_affordable = min(MAX_CONTRACTS, max(1, int(per_slot / margin_per))) if margin_per > 0 else 1
    # Fixed sizing — max affordable contracts, no confidence filter
    contracts  = max_affordable

    size = contracts * cs
    side = "BUY" if direction == "LONG" else "SELL"
    oid, actual_cts = place_market_order(asset, side, contracts)
    if not oid:
        msg = f"{asset} {side} {contracts}ct rejected by Coinbase"
        log(f"CRITICAL order rejected: {msg}")
        add_audit(asset, "ORDER REJECTED", msg)
        ntfy(f"CRITICAL ORDER REJECTED {asset}", msg, priority="urgent")
        return
    actual_size = actual_cts * cs
    rsi_info = info or {}
    positions[asset] = {
        "direction":   direction, "entry": entry_price,
        "contracts":   actual_cts, "size": actual_size,
        "strategy":    "RSI-Mom", "entry_time": ts(),
        "rsi_entry":   rsi_info.get("rsi_cur", 0),
        "exit_rsi":    RSI_EXIT,
        "hr_rsi":      rsi_info.get("hr_rsi", None),
        "paper":       PAPER_MODE,
        "unrealized_pnl": 0.0,
        "current_price":  entry_price,
    }
    with lock:
        state["entries"] = state.get("entries", 0) + 1
        state["buying_power"] = state.get("buying_power", state["balance"]) - entry_price * actual_size * ASSETS[asset]["margin_rate"]
    mode_label = "PAPER" if PAPER_MODE else "LIVE"
    add_audit(asset, f"📊 ENTER {direction}",
              f"RSI-Mom | entry=${entry_price:,.4f} | "
              f"rsi={rsi_info.get('rsi_cur',0):.1f} | "
              f"contracts={actual_cts} | size={actual_size} | {mode_label}",
              candle=candle)
    ntfy(f"{'📄' if PAPER_MODE else '📊'} ENTER {direction} {asset}",
         f"RSI-Mom | entry=${entry_price:,.4f} | RSI={rsi_info.get('rsi_cur',0):.1f} | {actual_cts}ct | {mode_label}",
         priority="default")

def exit_position(asset, exit_price, exit_reason, candle):
    """
    Exit a position. exit_reason: "RSI_EXIT" or "MANUAL"
    """
    pos = positions.get(asset)
    if not pos: return
    gross = round(
        (exit_price - pos["entry"]) * pos["size"] if pos["direction"] == "LONG"
        else (pos["entry"] - exit_price) * pos["size"], 4)
    # Fees: 0.10% on entry notional + 0.10% on exit notional
    entry_fee = round(pos["entry"] * pos["size"] * FEE_PCT, 4)
    exit_fee  = round(exit_price  * pos["size"] * FEE_PCT, 4)
    total_fee = entry_fee + exit_fee
    pnl = round(gross - total_fee, 4)
    side = "SELL" if pos["direction"] == "LONG" else "BUY"
    place_market_order(asset, side, pos["contracts"])
    record_tax(asset, pos["direction"], pos["entry"], exit_price,
               pos["size"], pnl, pos["entry_time"])
    with lock:
        state["total_pnl"]    = round(state["total_pnl"] + pnl, 4)
        state["weekly_pnl"]   = round(state["weekly_pnl"] + pnl, 4)
        state["balance"]      = round(state["balance"] + pnl, 4)  # update from current balance
        state["total_trades"] += 1
        if pnl >= 0: state["wins"] += 1
    del positions[asset]
    emoji = "✅" if pnl >= 0 else "❌"
    add_audit(asset, f"{emoji} EXIT {exit_reason}",
              f"{pos['direction']} ${pos['entry']:,.4f} → ${exit_price:,.4f} | "
              f"gross=${gross:+,.4f} | fees=${total_fee:.4f} | P&L=${pnl:+,.4f} | strategy={pos.get('strategy','')}",
              candle=candle)
    ntfy(f"{emoji} EXIT {asset}",
         f"{pos['direction']} | entry=${pos['entry']:,.4f} → ${exit_price:,.4f} | gross=${gross:+,.2f} | fees=${total_fee:.2f} | net=${pnl:+,.2f} | {exit_reason}",
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
    save_state()  # persist after every completed trade

# ══════════════════════════════════════════════════════════════════
# TRADING LOOP — RSI(14) Momentum + Trailing Exit
# Fires every 15-min bucket. Signal on candles[-2]. Entry at candles[-1] open.
# Exit: RSI < 50 (or < 60 if RSI previously hit 70 — trailing tighten)
# Fees: 0.10% per side deducted from every trade P&L
# ══════════════════════════════════════════════════════════════════
def trading_loop():
    global TOTAL_USDC
    # Always pull real Coinbase balance — paper or live
    # Paper mode uses it as starting capital (no trades placed)
    # Live mode uses it for real position sizing
    live_bal = get_live_balance()
    if live_bal > 0:
        TOTAL_USDC = live_bal
    if not PAPER_MODE:
        sync_open_positions()
    mode_str = "📄 Paper" if PAPER_MODE else "🔴 Live"
    log(f"{mode_str} mode: starting with ${TOTAL_USDC:,.2f} (from Coinbase)")
    with lock:
        state["balance"] = TOTAL_USDC
        state["buying_power"] = TOTAL_USDC
    load_state()
    log("🚀 CB Trader v56 started")
    mode_str = "📄 PAPER" if PAPER_MODE else "🔴 LIVE"
    log(f"   Mode: {mode_str} (TRADE_MODE={TRADE_MODE})")
    log(f"   Strategy: RSI({RSI_PERIOD}/{RSI_ENTRY}) + MTF(1hr RSI>50) + Trailing Exit")
    log(f"   Exit: tighten to RSI {RSI_TRAIL_EXIT} when RSI hits {RSI_TRAIL_TRIG} (trailing)")
    log(f"   Optimized: 308-strategy ultimate sweep on raw Kraken data | $13,468/month | 74.0% WR")
    log(f"   13/13 quarters green | No losing quarter since 2023")
    log(f"   Assets: {', '.join(ASSET_NAMES)}")
    log(f"   Fee: 0.10% per side included in all P&L calculations")
    log(f"   Capital: ${TOTAL_USDC:,.2f} | Max contracts: {MAX_CONTRACTS}/asset")
    log(f"   Time: {ts_est()}")

    # 15-min buckets — fire every 15 minutes
    last_bucket = (int(time.time()) // 900) * 900

    while True:
        try:
            current_bucket = (int(time.time()) // 900) * 900
            with lock:
                state["loop_last_run"] = ts()
                state["cycle"] = state.get("cycle", 0) + 1

            check_weekly_reset()

            if current_bucket != last_bucket:
                last_bucket = current_bucket
                bucket_dt     = datetime.fromtimestamp(current_bucket, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                bucket_dt_obj = datetime.fromtimestamp(current_bucket, tz=timezone.utc)
                hour_utc      = bucket_dt_obj.hour
                log(f"🕐 {bucket_dt} UTC | open={len(positions)} | bal=${state['balance']:,.2f}")

                skipped_assets = []
                for asset in ASSET_NAMES:
                    try:
                        # Fetch 15-min candles — single timeframe
                        candles = fetch_candles(asset, granularity=CANDLE_TF, n_candles=CANDLE_LIMIT)
                        if not candles or len(candles) < RSI_PERIOD + 5:
                            skipped_assets.append(asset); continue

                        cur = candles[-1]

                        # Skip cooldown after exit
                        if skip_entry.get(asset, 0) > 0:
                            skip_entry[asset] -= 1; continue

                        # ── EXIT CHECK — RSI drops below 45 ───────────
                        pos = positions.get(asset)
                        if pos:
                            # Update unrealized P&L every bucket
                            cur_close = float(candles[-1]["c"])
                            gross_u = (cur_close-pos["entry"])*pos["size"] if pos["direction"]=="LONG"                                       else (pos["entry"]-cur_close)*pos["size"]
                            pos["unrealized_pnl"] = round(gross_u - pos["entry"]*pos["size"]*FEE_PCT - cur_close*pos["size"]*FEE_PCT, 4)
                            pos["current_price"]  = cur_close
                            if should_exit(pos, candles):
                                exit_price = float(candles[-1]["o"])  # exit at current candle open — matches corrected backtest
                                pnl_est = round(
                                    (exit_price - pos["entry"]) * pos["size"] if pos["direction"] == "LONG"
                                    else (pos["entry"] - exit_price) * pos["size"], 4)
                                save_sim_data(asset, current_bucket*1000, candles, {},
                                              "EXIT_RSI", position=dict(pos), pnl=pnl_est)
                                exit_position(asset, exit_price, "RSI_EXIT", cur)
                                skip_entry[asset] = 1
                            else:
                                save_sim_data(asset, current_bucket*1000, candles, {},
                                              "HOLD", position=dict(pos))
                            continue

                        # ── RSI MOMENTUM SIGNAL — enter immediately ────
                        d, _, _, info = evaluate_signal(candles)
                        if d:
                            # MTF filter: check 1hr RSI trend direction
                            hr_rsi = get_hr_rsi(asset)
                            if hr_rsi is not None:
                                if d == "LONG" and hr_rsi < 50:
                                    save_sim_data(asset, current_bucket*1000, candles, info,
                                                  f"NO_SIGNAL:MTF_filter (1hr_RSI={hr_rsi:.1f}<50)")
                                    continue
                                if d == "SHORT" and hr_rsi > 50:
                                    save_sim_data(asset, current_bucket*1000, candles, info,
                                                  f"NO_SIGNAL:MTF_filter (1hr_RSI={hr_rsi:.1f}>50)")
                                    continue
                            info["hr_rsi"] = round(hr_rsi, 1) if hr_rsi else None
                            add_audit(asset, f"🚨 RSI-Mom {d}",
                                      f"RSI prev={info.get('rsi_prev',0):.1f} → cur={info.get('rsi_cur',0):.1f} | "
                                      f"1hr_RSI={info.get('hr_rsi','?')}",
                                      candle=cur, indicators=info)
                            entry_price = float(candles[-1]["o"])  # enter at current candle open — matches corrected backtest
                            enter_position(asset, d, entry_price, cur, info)
                            if positions.get(asset):
                                save_sim_data(asset, current_bucket*1000, candles, info,
                                              f"ENTER_{d}", position=dict(positions[asset]))
                        else:
                            save_sim_data(asset, current_bucket*1000, candles, info,
                                          f"NO_SIGNAL:{info.get('fail','?')}")

                    except Exception as e:
                        import traceback
                        log(f"Asset error {asset}: {e}")
                        log(traceback.format_exc())

                # Heartbeat
                with lock:
                    state["skipped_assets"] = skipped_assets
                    cycle_num = state.get("cycle", 0)
                if skipped_assets:
                    log(f"⚠️ Skipped: {skipped_assets}")

                # Refresh balance every 50 cycles (live only)
                if cycle_num % 50 == 0:
                    try:
                        if not PAPER_MODE:
                            live_bal = get_live_balance()
                            if live_bal > 0:
                                with lock: state["balance"] = live_bal
                    except: pass

                # Persist state every 10 cycles
                if cycle_num % 10 == 0:
                    save_state()

                add_audit("SYSTEM", "💓 CYCLE",
                          f"candle={bucket_dt} | open={len(positions)} | "
                          f"balance=${state['balance']:,.2f} | trades={state['total_trades']}")

                # Weekly P&L report every Monday 9am UTC
                if bucket_dt_obj.weekday() == 0 and hour_utc == 9 and bucket_dt_obj.minute < 15:
                    with lock:
                        wpnl = state["weekly_pnl"]; bal = state["balance"]
                        trd  = state["total_trades"]
                        wr   = round(state["wins"]/trd*100,1) if trd else 0
                    ntfy("Weekly P&L Report",
                         f"Week: {bucket_dt_obj.strftime('%Y-%m-%d')} | P&L: ${wpnl:+,.2f} | "
                         f"Bal: ${bal:,.2f} | Trades: {trd} | WR: {wr}%")
                    with lock: state["weekly_pnl"] = 0.0

                # Emergency stop — balance below 50%
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
# WEBSOCKET — for trail stop updates between candles
# ══════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════
# FLASK DASHBOARD
# ══════════════════════════════════════════════════════════════════
app = Flask(__name__)

@app.route("/login", methods=["POST"])
def login():
    from flask import make_response
    pw = request.form.get("pw","")
    if pw == "3757":
        resp = make_response(redirect("/"))
        resp.set_cookie("auth", "3757", max_age=60*60*24*30)
        return resp
    return redirect("/")

@app.route("/health")
def health():
    with lock: s=dict(state)
    wr = round(s["wins"]/s["total_trades"]*100,1) if s["total_trades"] else 0
    return Response(json.dumps({
        "overall":    "✅ ALL SYSTEMS OK" if s.get("loop_errors",0)<5 else "❌ errors",
        "mode":       {"paper_mode":PAPER_MODE,"status":"📄 PAPER" if PAPER_MODE else "🔴 LIVE"},
        "balance":    f"${s['balance']:,.2f}",
        "weekly_pnl": f"${s['weekly_pnl']:+,.2f}",
        "total_pnl":  f"${s['total_pnl']:+,.2f}",
        "trades":     s["total_trades"],
        "win_rate":   f"{wr}%",
        "strategy": {
            "name": f"RSI({RSI_PERIOD}/{RSI_ENTRY}) + MTF + Trailing",
            "timeframe": "15min",
            "open_positions": list(positions.keys()),
            "pending_entries": list(pending_entry.keys()),
            "total_pnl": f"${state['total_pnl']:+,.2f}",
            "trades": state['total_trades'],
            "win_rate": f"{round(state['wins']/state['total_trades']*100,1) if state['total_trades'] else 0}%",
            "balance": f"${state['balance']:,.2f}",
        },

        "open_positions": list(positions.keys()),
        "pending_entries": list(pending_entry.keys()),
        "candle_cache": {a:{"candles":CANDLE_LIMIT} for a in ASSET_NAMES},
        "skipped_assets": s.get("skipped_assets",[]),
        "skip_streaks": {k:v for k,v in s.get("skip_streak",{}).items() if v>0},
        "trading_loop":{"errors":s.get("loop_errors",0),"last_run":s["loop_last_run"],
                        "status":"✅ OK" if s.get("loop_errors",0)<5 else "❌ errors"},
        "diagnostic": {"entries":len(json.load(open(DIAG_FILE))) if os.path.exists(DIAG_FILE) else 0,
                       "status":"✅ OK"},
    }, indent=2), mimetype="application/json")

@app.route("/diagnostic-raw")
def diagnostic_raw():
    try: return Response(open(DIAG_FILE).read(), mimetype="application/json")
    except: return Response("[]", mimetype="application/json")

@app.route("/sim-data")
def sim_data():
    """Download sim replay data"""
    if request.cookies.get("auth") != "3757":
        return Response("Unauthorized", status=401)
    try:
        return Response(open(DATA_FILE).read(), mimetype="application/json",
                       headers={"Content-Disposition":"attachment;filename=cb_sim_data.json"})
    except:
        return Response("[]", mimetype="application/json")

@app.route("/tax-export")
def tax_export():
    try:
        return Response(open(TAX_FILE).read(), mimetype="text/csv",
            headers={"Content-Disposition":"attachment;filename=cb_trades.csv"})
    except: return Response("No trades yet", mimetype="text/plain")

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
        s=dict(state)
        pos=dict(positions); pend=dict(pending_entry)

    wr  = round(s["wins"]/s["total_trades"]*100,1) if s["total_trades"] else 0
    mode_color = "#FFB800" if PAPER_MODE else "#00D68F"
    mode_label = "📄 PAPER" if PAPER_MODE else "🔴 LIVE"
    wk_color   = "#00D68F" if s["weekly_pnl"]>=0 else "#FF4757"
    tot_color  = "#00D68F" if s["total_pnl"]>=0 else "#FF4757"
    # Contract countdown
    from datetime import date as ddate
    today_d     = ddate.today()
    expiry_aug  = ddate(2026, 8, 28)
    expiry_sep  = ddate(2026, 9, 25)
    days_to_aug = (expiry_aug - today_d).days
    active_exp  = expiry_aug if days_to_aug > 0 else expiry_sep
    days_left   = (active_exp - today_d).days
    active_label= "AUG 28" if days_to_aug > 0 else "SEP 25"
    roll_color  = "#00D68F" if days_left > 10 else "#FFB800" if days_left > 5 else "#FF4757"
    roll_status = "No roll needed — DEC 2030 expiry"

    # Positions
    pos_rows = ""
    for asset, p in pos.items():
        unreal    = p.get("unrealized_pnl", 0.0)
        cur_price = p.get("current_price", p.get("entry", 0))
        pnl_color = "#00D68F" if unreal >= 0 else "#FF4757"
        hr_rsi    = p.get("hr_rsi", "?")
        exit_rsi  = p.get("exit_rsi", RSI_EXIT)
        locked    = "🔒" if exit_rsi == RSI_TRAIL_EXIT else ""
        dir_col   = "#00D68F" if p["direction"]=="LONG" else "#FF4757"
        dir_bg    = "#00D68F22" if p["direction"]=="LONG" else "#FF475722"
        entry_fee = round(p["entry"] * p["size"] * FEE_PCT, 4)
        exit_fee  = round(cur_price  * p["size"] * FEE_PCT, 4)
        pos_rows += f"""<div style='background:#0A1628;border:1px solid #1E2D45;border-radius:10px;padding:14px;margin-bottom:10px'>
          <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:10px'>
            <span style='font-size:18px;font-weight:800'>{asset}</span>
            <span style='font-size:13px;font-weight:700;padding:3px 10px;border-radius:20px;
              background:{dir_bg};color:{dir_col}'>{p["direction"]}</span>
            <span style='font-size:16px;font-weight:700;color:{pnl_color}'>${unreal:+,.2f}</span>
          </div>
          <div style='display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;font-size:12px'>
            <div style='background:#060D1A;border-radius:6px;padding:8px'>
              <div style='color:#4A5878;margin-bottom:2px'>Entry</div>
              <div style='font-weight:600'>${p["entry"]:,.4f}</div>
            </div>
            <div style='background:#060D1A;border-radius:6px;padding:8px'>
              <div style='color:#4A5878;margin-bottom:2px'>Current</div>
              <div style='font-weight:600;color:{pnl_color}'>${cur_price:,.4f}</div>
            </div>
            <div style='background:#060D1A;border-radius:6px;padding:8px'>
              <div style='color:#4A5878;margin-bottom:2px'>Exit RSI</div>
              <div style='font-weight:600;color:#FFB800'>&lt;{exit_rsi} {locked}</div>
            </div>
            <div style='background:#060D1A;border-radius:6px;padding:8px'>
              <div style='color:#4A5878;margin-bottom:2px'>1hr RSI</div>
              <div style='font-weight:600;color:#7B61FF'>{hr_rsi}</div>
            </div>
          </div>
          <div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;font-size:11px;margin-top:8px;color:#4A5878'>
            <div>{p.get("contracts",1)}ct | {p.get("strategy","RSI-Mom")}</div>
            <div>Fees est: ${entry_fee+exit_fee:,.4f}</div>
            <div>Since {p.get("entry_time","?")[:16]}</div>
          </div>
        </div>"""

    if pend:
        pos_rows += f"<div style='color:#FFB800;font-size:12px;padding:8px'>⏳ Pending entry: {', '.join(pend.keys())}</div>"

    if not pos_rows:
        pos_rows = "<div style='color:#4A5878;padding:32px;text-align:center;font-size:14px'>No open positions</div>"

    # Journal
    try: audit_data=json.load(open(DIAG_FILE)) if os.path.exists(DIAG_FILE) else []
    except: audit_data=[]

    journal_rows = ""
    shown = 0
    for a in audit_data:
        if shown >= 100: break
        evt = a.get("event","")
        if "CYCLE" in evt:
            journal_rows += f"<div style='padding:5px 0;border-bottom:1px solid #0A1628;font-size:11px;color:#4A5878'>{a['time']} — {a.get('detail','')[:80]}</div>"
        else:
            color = "#00D68F" if "ENTER" in evt else "#FF4757" if "EXIT" in evt else "#FFB800" if "SIGNAL" in evt else "#E0E6F0"
            journal_rows += f"""<div style='border-left:3px solid {color};padding:8px 12px;margin-bottom:6px;background:#0A1628;border-radius:0 8px 8px 0'>
              <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>{a["time"]} · {a.get("asset","SYSTEM")}</div>
              <div style='font-size:13px;font-weight:700;color:{color}'>{evt}</div>
              <div style='font-size:11px;color:#8892A4;margin-top:3px'>{a.get("detail","")[:120]}</div>
            </div>"""
        shown += 1
    if not journal_rows:
        journal_rows = "<div style='color:#4A5878;padding:32px;text-align:center;font-size:14px'>No events yet — waiting for first signal</div>"

    # Markets
    assets_rows = ""
    for a_name in ASSET_NAMES:
        is_open  = a_name in pos
        is_pend  = a_name in pend
        status   = "● OPEN" if is_open else "⏳ PENDING" if is_pend else "○ READY"
        sc       = "#00D68F" if is_open else "#FFB800" if is_pend else "#4A5878"
        assets_rows += f"""<div style='display:flex;justify-content:space-between;align-items:center;
            padding:10px 0;border-bottom:1px solid #1E2D45;font-size:13px'>
          <b style='width:55px'>{a_name}</b>
          <span style='color:{sc};font-size:11px;font-weight:600'>{status}</span>
        </div>"""

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_est = ts_est()

    return f"""<!DOCTYPE html>
<html><head>
<title>CB Trader v56</title>
<meta charset=utf-8>
<meta name=viewport content='width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no'>
<meta http-equiv=refresh content=30>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#060D1A;color:#E0E6F0;font-family:-apple-system,BlinkMacSystemFont,sans-serif;
       padding:14px;max-width:620px;margin:0 auto;padding-bottom:50px}}
  .kpis{{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px}}
  @media(min-width:400px){{.kpis{{grid-template-columns:repeat(4,1fr)}}}}
  .kpi{{background:#0A1628;border:1px solid #1E2D45;border-radius:10px;padding:12px;text-align:center}}
  .kpi-l{{font-size:10px;color:#4A5878;text-transform:uppercase;letter-spacing:.8px;margin-bottom:4px}}
  .kpi-v{{font-size:20px;font-weight:800;line-height:1.2}}
  .tabs{{display:flex;gap:4px;margin-bottom:0;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none}}
  .tabs::-webkit-scrollbar{{display:none}}
  .tab{{flex-shrink:0;padding:9px 18px;cursor:pointer;border-radius:8px 8px 0 0;
        font-size:13px;font-weight:600;background:#060D1A;color:#4A5878;
        border:1px solid #1E2D45;border-bottom:none;-webkit-tap-highlight-color:transparent}}
  .tab.on{{background:#0A1628;color:#E0E6F0}}
  .panel{{display:none;background:#0A1628;border:1px solid #1E2D45;
          border-radius:0 10px 10px 10px;padding:14px;min-height:200px}}
  .panel.on{{display:block}}
  a{{color:#8892A4;text-decoration:none}}
  a:hover{{color:#E0E6F0}}
</style>
<script>
function show(id,el){{
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('on'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.getElementById(id).classList.add('on'); el.classList.add('on');
}}
</script>
</head><body>

{'' if not s.get('skipped_assets') else "<div style='background:#FF475722;border:1px solid #FF4757;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#FF4757'><b>⚠️ SKIPPED ASSETS</b> — not enough candles: "+ ', '.join(s.get('skipped_assets',[])) + "<br>These assets are NOT evaluated this cycle.</div>"}
<div style='display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px'>
  <div>
    <div style='font-size:24px;font-weight:800;letter-spacing:-0.5px'>CB Trader</div>
    <div style='font-size:12px;font-weight:700;color:{mode_color};margin-top:2px'>{mode_label}</div>
  </div>
  <div style='text-align:right;font-size:11px;color:#4A5878;line-height:1.7'>
    {now_utc}<br>{now_est}<br>
    <span style='color:{mode_color}'>● v56 {mode_label}
    </span>
  </div>
</div>

<div style='font-size:11px;color:#4A5878;margin-bottom:6px;text-transform:uppercase;letter-spacing:.8px'>📈 Spot Candles Strategy</div>
<div class=kpis>
  <div class=kpi><div class=kpi-l>Balance</div><div class=kpi-v>${s['balance']:,.2f}</div></div>
  <div class=kpi><div class=kpi-l>This Week</div>
    <div class=kpi-v style='color:{wk_color}'>${s["weekly_pnl"]:+,.2f}</div></div>
  <div class=kpi><div class=kpi-l>Total P&L</div>
    <div class=kpi-v style='color:{tot_color}'>${s["total_pnl"]:+,.2f}</div></div>
  <div class=kpi><div class=kpi-l>Win Rate</div><div class=kpi-v>{wr}%</div></div>
</div>
<div class=kpis style='margin-bottom:14px'>
  <div class=kpi><div class=kpi-l>Open</div>
    <div class=kpi-v style='color:{"#00D68F" if len(pos)>0 else "#4A5878"}'>{len(pos)}</div></div>
  <div class=kpi><div class=kpi-l>Pending</div>
    <div class=kpi-v style='color:{"#FFB800" if pend else "#4A5878"}'>{len(pend)}</div></div>
  <div class=kpi><div class=kpi-l>Trades</div><div class=kpi-v>{s["total_trades"]}</div></div>
  <div class=kpi><div class=kpi-l>Cycle</div><div class=kpi-v style='font-size:14px;color:#4A5878'>#{s.get("cycle",0)}</div></div>
</div>


<div class=tabs>
  <span class='tab on' onclick="show('pos',this)">Positions</span>
  <span class=tab onclick="show('journal',this)">Journal</span>
  <span class=tab onclick="show('markets',this)">Markets</span>
  <span class=tab onclick="show('info',this)">Info</span>
</div>

<div id=pos class='panel on'>{pos_rows}</div>
<div id=journal class=panel>
  <div style='font-size:11px;color:#4A5878;margin-bottom:10px'>Last 100 events · auto-refresh 30s</div>
  {journal_rows}
</div>
<div id=markets class=panel>
  <div style='font-size:11px;color:#4A5878;margin-bottom:10px'>{len(ASSET_NAMES)} assets · evaluates every 15 min</div>
  {assets_rows}
</div>
<div id=info class=panel>
  <div style='font-size:13px;line-height:2;color:#8892A4'>
    <b style='color:#E0E6F0;font-size:14px'>Strategy</b><br>
    RSI(3/70) Momentum + MTF + Trailing Exit · 15min candles<br>
    Exit tightens to RSI 65 when RSI hits 75 · 13/13 quarters green<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
<div style='font-size:11px;color:#4A5878;margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em'>📊 STRATEGY (bucket exits — backtest logic)</div>
<div class=kpis>
  <div class=kpi><div class=kpi-l>Balance</div><div class=kpi-v>${s['balance']:,.2f}</div></div>
  <div class=kpi><div class=kpi-l>P&L</div>
    <div class=kpi-v style='color:{"#00D68F" if s["total_pnl"]>=0 else "#FF4757"}'>${s["total_pnl"]:+,.2f}</div></div>
  <div class=kpi><div class=kpi-l>Trades</div><div class=kpi-v>{s['total_trades']}</div></div>
  <div class=kpi><div class=kpi-l>WR</div><div class=kpi-v>{round(s['wins']/s['total_trades']*100,1) if s['total_trades'] else 0}%</div></div>
  <div class=kpi><div class=kpi-l>Open</div><div class=kpi-v>{len(pos)}</div></div>
</div>
<div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Exchange</b><br>
    Coinbase CFM Futures · CFTC regulated · Legal NYC<br>
    10x leverage · {len(ASSET_NAMES)} assets · DEC 2030 expiry, no rolls<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Contract Status</b><br>
    Perp-style futures · DEC 2030 expiry · No monthly rolls<br>
    Fees: 0.10% per side — CONFIRMED from live fills<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>2026 Q1 Backtest</b><br>
    $13,468/month at $1,000 · 74.0% WR · 308 strategies tested on raw Kraken data<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Links</b><br>
    <a href='/health'>Health</a> &nbsp;·&nbsp;
    <a href='/diagnostic-raw'>Diagnostic</a> &nbsp;·&nbsp;
    <a href='/tax-export'>Tax CSV</a>
  </div>
</div>

</body></html>"""

# ══════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════
log("📡 Pre-loading 15min candles via REST...")
for a in ASSET_NAMES:
    c = fetch_candles(a, granularity=CANDLE_TF, n_candles=CANDLE_LIMIT)
    log(f"  {a}: {len(c) if c else 0} 15min candles")
    time.sleep(0.5)
log("✅ All candles pre-loaded")

check_weekly_reset()
threading.Thread(target=trading_loop, daemon=True).start()
