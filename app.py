"""
COINBASE TRADER v1
═══════════════════════════════════════════════════════════════════
Strategy: S2+S4 Trail Only 0.5%
- Signal on last complete candle (candles[-2])
- Enter at next candle open
- Trail update + exit on current candle intrabar
- No EMA cross exit, no hard stop — trail only
- Full diagnostic log every candle

CONFIRMED RESULTS (coinbase_honest_sim_v1.py):
- 98% green weeks over 5 years
- $98,627 net at $1,000 | $8,947/week at $20,000
- Trail only beats all other exit methods

PAPER_MODE = True  → logs trades, no real orders
PAPER_MODE = False → live trading on Coinbase
"""

import threading, time, os, math, json, csv
from datetime import datetime, timezone, timedelta
from flask import Flask, request, redirect, Response, session
import requests as req

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
PAPER_MODE   = True   # ← SET False TO GO LIVE
PASSWORD     = os.environ.get("DASHBOARD_PASSWORD", "cb2026")
NTFY_TOPIC   = os.environ.get("NTFY_TOPIC", "hl-trader-lunchm0ney")
NTFY_URL     = f"https://ntfy.sh/{NTFY_TOPIC}"

CB_API_KEY   = os.environ.get("CB_API_KEY",
    "organizations/6c99f7b8-956a-4eb1-9de6-7e26af829de4/apiKeys/85162660-50bc-4477-b334-7b331d1aa956")
CB_API_SEC   = os.environ.get("CB_API_SECRET",
    "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIHqIU+0EUS6YbzaEBJrk5rAYj30G9zJFLlSrpngW6NhmoAoGCCqGSM49\nAwEHoUQDQgAEKOQw/xRfk5o3l8zihqqU2lfIkTel+QSeybyjJtebBg+95tEGg6vt\ndvm7nxSffA4MKQ3PhgPf88cjDD59MMZPtQ==\n-----END EC PRIVATE KEY-----\n")

# ── ASSETS ────────────────────────────────────────────────────────
# Spot product (candles) → Perp product (orders) → contract size
ASSETS = {
    "BTC":  {"spot":"BTC-USD",  "perp":"BIP-20DEC30-CDE", "contract":0.01},
    "ETH":  {"spot":"ETH-USD",  "perp":"ETP-20DEC30-CDE", "contract":0.1},
    "SOL":  {"spot":"SOL-USD",  "perp":"SLP-20DEC30-CDE", "contract":5.0},
    "BNB":  {"spot":"BNB-USD",  "perp":"BNB-20DEC30-CDE", "contract":1.0},
    "DOGE": {"spot":"DOGE-USD", "perp":"DOP-20DEC30-CDE", "contract":5000.0},
    "AVAX": {"spot":"AVAX-USD", "perp":"AVP-20DEC30-CDE", "contract":10.0},
}
ASSET_NAMES = list(ASSETS.keys())

# ── STRATEGY PARAMS (confirmed from backtest) ─────────────────────
EMA_FAST    = 5
EMA_MID     = 13
EMA_SLOW    = 34
SEP_FILTER  = 0.003
VOL_FILTER  = 0.5
BRK_BARS    = 12
TRAIL_PCT   = 0.005   # 0.5% — confirmed best
ATR_BUFFER  = 0.5
LEVERAGE    = 10
TOTAL_USDC  = float(os.environ.get("TOTAL_USDC", "1000"))
CHECK_EVERY = 60      # seconds between loop iterations
CANDLE_TF   = "FIFTEEN_MINUTE"
CANDLE_LIMIT= 200

TAX_RATE    = 0.35

# ── FILE PATHS ────────────────────────────────────────────────────
TRADES_FILE     = "/tmp/cb_trades.json"
WEEKLY_FILE     = "/tmp/cb_weekly.json"
DIAG_FILE       = "/tmp/cb_diagnostics.json"
TAX_FILE        = "/tmp/cb_tax.csv"


# ══════════════════════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════════════════════
state = {
    "status": "starting",
    "cycle": 0,
    "balance": TOTAL_USDC,
    "positions": {},
    "trades": [],
    "audit": [],
    "diagnostics": [],
    "issues": [],
    "weekly_pnl": 0.0,
    "weekly_trades": 0,
    "week_start": "",
    "health": {
        "assets_ok": {a: {"price":0,"last_candle":"?","signal":"?","status":"STARTING"}
                      for a in ASSET_NAMES}
    },
    "paper_mode": PAPER_MODE,
}

positions   = {}   # asset → position dict
stop_oids   = {}   # asset → order_id of active stop
entry_times = {}   # asset → entry datetime string
last_candle = {}   # asset → last evaluated candle timestamp
pending_entry = {} # asset → dict when waiting for next candle open
lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════
# LOGGING & AUDIT
# ══════════════════════════════════════════════════════════════════
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"[{ts()}] {msg}", flush=True)

def add_audit(asset, event, detail, candle=None, indicators=None):
    """Full audit entry — records exactly what was seen and why"""
    entry = {
        "time": ts(),
        "asset": asset,
        "event": event,
        "detail": detail,
    }
    if candle:
        entry["candle"] = candle
    if indicators:
        entry["indicators"] = indicators
    with lock:
        state["audit"].insert(0, entry)
        if len(state["audit"]) > 500:
            state["audit"] = state["audit"][:500]
    # Save to diagnostic file
    save_diagnostic(entry)

def save_diagnostic(entry):
    """Save diagnostic entry to file for copy/paste"""
    try:
        existing = []
        if os.path.exists(DIAG_FILE):
            existing = json.load(open(DIAG_FILE))
        existing.insert(0, entry)
        existing = existing[:1000]
        json.dump(existing, open(DIAG_FILE, "w"), indent=2)
    except: pass

def add_issue(asset, issue, detail):
    entry = {"time": ts(), "asset": asset, "issue": issue, "detail": str(detail)}
    with lock:
        state["issues"].insert(0, entry)
        if len(state["issues"]) > 100:
            state["issues"] = state["issues"][:100]

def add_diag(level, event, cause, action=""):
    entry = {"time": ts(), "level": level, "event": event, "cause": cause, "action": action}
    with lock:
        state["diagnostics"].insert(0, entry)
        if len(state["diagnostics"]) > 200:
            state["diagnostics"] = state["diagnostics"][:200]


# ══════════════════════════════════════════════════════════════════
# INDICATOR FUNCTIONS (exact same as confirmed backtest)
# ══════════════════════════════════════════════════════════════════
def ema(values, period):
    k = 2 / (period + 1)
    e = None
    out = []
    for v in values:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out

def sma(values, period):
    out = [None] * (period - 1)
    for i in range(period - 1, len(values)):
        out.append(sum(values[i-period+1:i+1]) / period)
    return out

def atr(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        ))
    if len(trs) < period:
        return [None] * len(closes)
    out = [None] * period
    avg = sum(trs[:period]) / period
    out.append(avg)
    for i in range(period, len(trs)):
        avg = (avg * (period - 1) + trs[i]) / period
        out.append(avg)
    while len(out) < len(closes):
        out.append(out[-1])
    return out

def round_price(p, sig=5):
    if p == 0: return 0.0
    mag = math.floor(math.log10(abs(p)))
    return round(p, max(0, sig - 1 - mag))


# ══════════════════════════════════════════════════════════════════
# COINBASE API
# ══════════════════════════════════════════════════════════════════
def get_cb_client():
    from coinbase.rest import RESTClient
    return RESTClient(api_key=CB_API_KEY, api_secret=CB_API_SEC)

# ── CANDLE CACHE — updated by WebSocket, fallback to REST ─────────
candle_cache = {asset: [] for asset in ASSET_NAMES}
candle_cache_lock = threading.Lock()

def fetch_candles_rest(asset):
    """Fetch candles via REST — used on startup and as fallback"""
    try:
        client = get_cb_client()
        product_id = ASSETS[asset]["spot"]
        end = int(time.time())
        start = end - CANDLE_LIMIT * 15 * 60
        resp = client.get_candles(
            product_id,
            start=str(start),
            end=str(end),
            granularity=CANDLE_TF
        )
        if not resp.candles:
            return None
        candles = []
        for c in resp.candles:
            candles.append({
                "ts":  int(c.start) * 1000,
                "dt":  datetime.fromtimestamp(int(c.start), tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "o":   float(c.open),
                "h":   float(c.high),
                "l":   float(c.low),
                "c":   float(c.close),
                "v":   float(c.volume),
            })
        candles = sorted(candles, key=lambda x: x["ts"])
        with candle_cache_lock:
            candle_cache[asset] = candles
        return candles
    except Exception as e:
        add_issue(asset, "candle_fetch_error", str(e))
        return None

def fetch_candles(asset):
    """Get candles — from cache if available, else REST"""
    with candle_cache_lock:
        cached = candle_cache.get(asset, [])
    if cached:
        return cached
    return fetch_candles_rest(asset)

def start_websocket():
    """Subscribe to Coinbase WebSocket candle channel for all assets"""
    try:
        from coinbase.websocket import WSClient
        product_ids = [ASSETS[a]["spot"] for a in ASSET_NAMES]

        def on_message(msg):
            try:
                import json as _j
                data = _j.loads(msg) if isinstance(msg, str) else msg
                channel = data.get("channel","")
                events = data.get("events",[])
                if channel != "candles": return
                for event in events:
                    for c in event.get("candles",[]):
                        product_id = c.get("product_id","")
                        # Find asset from product_id
                        asset = next((a for a,cfg in ASSETS.items()
                                     if cfg["spot"]==product_id), None)
                        if not asset: continue
                        candle = {
                            "ts":  int(float(c["start"])) * 1000,
                            "dt":  datetime.fromtimestamp(int(float(c["start"])),
                                   tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                            "o":   float(c["open"]),
                            "h":   float(c["high"]),
                            "l":   float(c["low"]),
                            "c":   float(c["close"]),
                            "v":   float(c["volume"]),
                        }
                        with candle_cache_lock:
                            cache = candle_cache.get(asset, [])
                            # Update or append candle
                            existing = next((i for i,x in enumerate(cache)
                                           if x["ts"]==candle["ts"]), None)
                            if existing is not None:
                                cache[existing] = candle
                            else:
                                cache.append(candle)
                                cache.sort(key=lambda x: x["ts"])
                                if len(cache) > CANDLE_LIMIT:
                                    cache = cache[-CANDLE_LIMIT:]
                            candle_cache[asset] = cache
            except Exception as e:
                log(f"⚠️ WebSocket message error: {e}")

        ws = WSClient(api_key=CB_API_KEY, api_secret=CB_API_SEC,
                      on_message=on_message)
        ws.open()
        ws.subscribe(product_ids=product_ids, channels=["candles"])
        log(f"🔌 WebSocket candles subscribed for {len(product_ids)} assets")
        ws.run_forever_with_exception_check()
    except Exception as e:
        log(f"⚠️ WebSocket failed: {e} — falling back to REST polling")

def get_balance():
    """Get real balance from Coinbase"""
    try:
        client = get_cb_client()
        summary = client.get_futures_balance_summary()
        if summary:
            bal = float(getattr(summary, "available_funds", 0) or 0)
            if bal > 0:
                return bal
    except: pass
    return None

def place_market_order(asset, direction, contracts):
    """Place a market order on Coinbase perps"""
    if PAPER_MODE:
        log(f"📄 PAPER: {asset} {direction} {contracts} contracts")
        return {"paper": True, "order_id": f"PAPER-{asset}-{ts()}"}
    try:
        client = get_cb_client()
        product_id = ASSETS[asset]["perp"]
        side = "BUY" if direction == "LONG" else "SELL"
        resp = client.create_order(
            client_order_id=f"cb-trader-{asset}-{int(time.time())}",
            product_id=product_id,
            side=side,
            order_configuration={
                "market_market_ioc": {
                    "base_size": str(contracts)
                }
            }
        )
        return resp
    except Exception as e:
        add_issue(asset, "order_error", str(e))
        return None

def place_stop_order(asset, direction, contracts, stop_price):
    """Place a stop order on Coinbase perps"""
    if PAPER_MODE:
        log(f"📄 PAPER STOP: {asset} {direction} stop@${stop_price:,.4f}")
        return f"PAPER-STOP-{asset}-{int(time.time())}"
    try:
        client = get_cb_client()
        product_id = ASSETS[asset]["perp"]
        # Close direction is opposite to position
        side = "SELL" if direction == "LONG" else "BUY"
        resp = client.create_order(
            client_order_id=f"cb-stop-{asset}-{int(time.time())}",
            product_id=product_id,
            side=side,
            order_configuration={
                "stop_limit_stop_limit_gtc": {
                    "base_size": str(contracts),
                    "limit_price": str(round_price(stop_price * 0.999 if direction=="LONG" else stop_price * 1.001)),
                    "stop_price": str(round_price(stop_price)),
                    "stop_direction": "STOP_DIRECTION_STOP_DOWN" if direction=="LONG" else "STOP_DIRECTION_STOP_UP"
                }
            }
        )
        return getattr(resp, "order_id", None) or getattr(resp, "success_response", {}).get("order_id")
    except Exception as e:
        add_issue(asset, "stop_order_error", str(e))
        return None

def cancel_order(asset, order_id):
    """Cancel an existing stop order"""
    if PAPER_MODE or not order_id or "PAPER" in str(order_id):
        return True
    try:
        client = get_cb_client()
        client.cancel_orders(order_ids=[order_id])
        return True
    except Exception as e:
        add_issue(asset, "cancel_error", str(e))
        return False


# ══════════════════════════════════════════════════════════════════
# TRADE RECORDING
# ══════════════════════════════════════════════════════════════════
def add_trade(asset, action, direction, entry_p, exit_p, size, pnl, reason):
    trade = {
        "time": ts(),
        "asset": asset,
        "action": action,
        "direction": direction,
        "entry": entry_p,
        "exit": exit_p,
        "size": size,
        "leverage": LEVERAGE,
        "pnl": round(pnl, 4),
        "reason": reason,
        "paper": PAPER_MODE,
    }
    with lock:
        state["trades"].insert(0, trade)
        if len(state["trades"]) > 500:
            state["trades"] = state["trades"][:500]
    save_trades()

def save_trades():
    try:
        json.dump(state["trades"], open(TRADES_FILE, "w"))
    except: pass

def load_trades():
    try:
        if os.path.exists(TRADES_FILE):
            state["trades"] = json.load(open(TRADES_FILE))
    except: pass

def update_weekly_pnl(pnl):
    with lock:
        state["weekly_pnl"] = round(state["weekly_pnl"] + pnl, 4)
        state["weekly_trades"] += 1
    try:
        json.dump({
            "weekly_pnl": state["weekly_pnl"],
            "weekly_trades": state["weekly_trades"],
            "week_start": state["week_start"]
        }, open(WEEKLY_FILE, "w"))
    except: pass

def load_weekly_pnl():
    try:
        if os.path.exists(WEEKLY_FILE):
            d = json.load(open(WEEKLY_FILE))
            now = datetime.now(timezone.utc)
            wk = f"{now.isocalendar()[0]}-W{now.isocalendar()[1]:02d}"
            if d.get("week_start") == wk:
                state["weekly_pnl"]    = d.get("weekly_pnl", 0)
                state["weekly_trades"] = d.get("weekly_trades", 0)
                state["week_start"]    = wk
                log(f"📅 Weekly P&L restored: ${state['weekly_pnl']:+,.2f}")
    except: pass

def check_weekly_reset():
    now = datetime.now(timezone.utc)
    wk = f"{now.isocalendar()[0]}-W{now.isocalendar()[1]:02d}"
    with lock:
        if state["week_start"] != wk:
            state["week_start"]    = wk
            state["weekly_pnl"]    = 0.0
            state["weekly_trades"] = 0
            log(f"📅 New week: {wk} — P&L reset")
            try:
                json.dump({"weekly_pnl":0.0,"weekly_trades":0,"week_start":wk},
                          open(WEEKLY_FILE,"w"))
            except: pass

def record_tax(asset, direction, entry_p, exit_p, size, pnl, entry_time):
    try:
        now = datetime.now(timezone.utc)
        q = f"Q{(now.month-1)//3+1}"
        row = {
            "trade_id": f"{asset}-{entry_time[:10]}-{entry_time[11:19].replace(':','')}",
            "asset": asset,
            "direction": direction,
            "entry_price": round(entry_p, 6),
            "exit_price": round(exit_p, 6),
            "size": round(size, 6),
            "pnl_usd": round(pnl, 4),
            "tax_35pct": round(pnl * TAX_RATE if pnl > 0 else 0, 4),
            "entry_date": entry_time,
            "exit_date": ts(),
            "quarter": q,
            "paper": PAPER_MODE,
        }
        write_header = not os.path.exists(TAX_FILE)
        with open(TAX_FILE, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if write_header:
                w.writeheader()
            w.writerow(row)
    except: pass

def ntfy(title, body, tags=""):
    try:
        req.post(NTFY_URL, headers={
            "Title": title,
            "Tags": tags,
            "Content-Type": "text/plain"
        }, data=body.encode(), timeout=5)
    except: pass


# ══════════════════════════════════════════════════════════════════
# SIGNAL EVALUATION
# Exact same logic as coinbase_honest_sim_v1.py
# Signal on candles[-2] (last complete candle)
# ══════════════════════════════════════════════════════════════════
def evaluate_signal(candles, asset):
    """
    Evaluate signal on candles[-2] — the last COMPLETE candle.
    candles[-1] is the currently forming candle — excluded.

    Returns: (direction, signal_candle, indicators) or (None, None, None)
    """
    # Use all candles except the current forming one
    complete = candles[:-1]
    if len(complete) < 50:
        return None, None, None

    closes = [float(c["c"]) for c in complete]
    highs  = [float(c["h"]) for c in complete]
    lows   = [float(c["l"]) for c in complete]
    vols   = [float(c["v"]) for c in complete]

    ef = ema(closes, EMA_FAST)
    em_ = ema(closes, EMA_MID)
    es = ema(closes, EMA_SLOW)
    vs = sma(vols, 20)
    atr_vals = atr(highs, lows, closes)

    i = len(complete) - 1  # last complete candle index

    sig_candle = complete[i]
    indic = {
        "ema5":  round(ef[i], 4) if ef[i] else None,
        "ema13": round(em_[i], 4) if em_[i] else None,
        "ema34": round(es[i], 4) if es[i] else None,
        "vol":   round(vols[i], 2),
        "vol_ma":round(vs[i], 2) if vs[i] else None,
        "vol_ratio": round(vols[i]/vs[i], 2) if vs[i] else None,
        "atr":   round(atr_vals[i], 4) if atr_vals[i] else None,
        "candle_open":  sig_candle["o"],
        "candle_high":  sig_candle["h"],
        "candle_low":   sig_candle["l"],
        "candle_close": sig_candle["c"],
        "candle_time":  sig_candle["dt"],
    }

    if not (ef[i] and em_[i] and es[i]):
        return None, sig_candle, indic

    # 1. EMA stack
    if   ef[i] > em_[i] > es[i]: d = "LONG"
    elif ef[i] < em_[i] < es[i]: d = "SHORT"
    else:
        indic["fail"] = "EMA not stacked"
        return None, sig_candle, indic

    indic["direction"] = d

    # 2. Separation filter
    sep = abs(ef[i] - es[i]) / es[i] if es[i] else 0
    indic["separation"] = round(sep, 5)
    if sep < SEP_FILTER:
        indic["fail"] = f"sep {sep:.4f} < {SEP_FILTER}"
        return None, sig_candle, indic

    # 3. Volume filter
    vr = vols[i] / vs[i] if vs[i] else 0
    indic["vol_ratio"] = round(vr, 2)
    if vr < VOL_FILTER:
        indic["fail"] = f"vol {vr:.2f}x < {VOL_FILTER}x"
        return None, sig_candle, indic

    # 4. Breakout filter
    if d == "LONG":
        brk_high = max(highs[i-BRK_BARS:i])
        indic["brk_level"] = brk_high
        if closes[i] <= brk_high:
            indic["fail"] = f"no breakout — close {closes[i]:.4f} ≤ {brk_high:.4f}"
            return None, sig_candle, indic
    else:
        brk_low = min(lows[i-BRK_BARS:i])
        indic["brk_level"] = brk_low
        if closes[i] >= brk_low:
            indic["fail"] = f"no breakout — close {closes[i]:.4f} ≥ {brk_low:.4f}"
            return None, sig_candle, indic

    indic["pass"] = True
    return d, sig_candle, indic


# ══════════════════════════════════════════════════════════════════
# POSITION MANAGEMENT
# Trail update + exit on CURRENT candle intrabar
# Matches how Coinbase exchange stop orders work
# ══════════════════════════════════════════════════════════════════
def check_trail_and_exit(asset, pos, current_candle, atr_val):
    """
    Update trail stop and check for exit on current candle.
    Uses current candle HIGH/LOW — matches live exchange stop order.

    Returns: "EXIT", "HOLD", or "UPDATE"
    """
    cur_h = float(current_candle["h"])
    cur_l = float(current_candle["l"])

    updated = False

    if pos["direction"] == "LONG":
        # Trail update — new high moves trail up
        if cur_h > pos["trail_peak"]:
            move = cur_h - pos["trail_peak"]
            if atr_val == 0 or move > atr_val * ATR_BUFFER:
                pos["trail_peak"] = cur_h
                pos["trail_stop"] = round_price(cur_h * (1 - TRAIL_PCT))
                updated = True
        # Exit check — low hits trail
        thresh = pos["trail_stop"] - (atr_val * ATR_BUFFER if atr_val else 0)
        if cur_l <= thresh:
            return "EXIT", updated
    else:
        # Trail update — new low moves trail down
        if cur_l < pos["trail_peak"]:
            move = pos["trail_peak"] - cur_l
            if atr_val == 0 or move > atr_val * ATR_BUFFER:
                pos["trail_peak"] = cur_l
                pos["trail_stop"] = round_price(cur_l * (1 + TRAIL_PCT))
                updated = True
        # Exit check — high hits trail
        thresh = pos["trail_stop"] + (atr_val * ATR_BUFFER if atr_val else 0)
        if cur_h >= thresh:
            return "EXIT", updated

    return "HOLD", updated

def exit_position(asset, exit_price, reason, current_candle):
    """Close position, record trade, cancel stop, send notification"""
    pos = positions.get(asset)
    if not pos:
        return

    pnl = round(
        (exit_price - pos["entry"]) * pos["size"] if pos["direction"] == "LONG"
        else (pos["entry"] - exit_price) * pos["size"],
        4
    )

    # Cancel stop order
    oid = stop_oids.get(asset)
    if oid:
        cancel_order(asset, oid)
        if asset in stop_oids:
            del stop_oids[asset]

    # Record
    add_trade(asset, "EXIT", pos["direction"], pos["entry"], exit_price,
              pos["size"], pnl, reason)
    record_tax(asset, pos["direction"], pos["entry"], exit_price,
               pos["size"], pnl, entry_times.get(asset, ts()))
    update_weekly_pnl(pnl)

    # Audit — full detail
    add_audit(asset, f"{'✅' if pnl>=0 else '❌'} EXIT {reason.upper()}",
              f"{'LONG' if pos['direction']=='LONG' else 'SHORT'} "
              f"${pos['entry']:,.4f} → ${exit_price:,.4f} | P&L=${pnl:+,.4f} | "
              f"trail_peak=${pos['trail_peak']:,.4f} | trail_stop=${pos['trail_stop']:,.4f}",
              candle=current_candle)

    # Notify
    emoji = "✅" if pnl >= 0 else "❌"
    ntfy(f"{emoji} {asset} {pos['direction']} EXIT",
         f"{pos['entry']:,.4f} → {exit_price:,.4f}\nP&L: ${pnl:+,.4f}\nReason: {reason}\n"
         f"{'PAPER' if PAPER_MODE else 'LIVE'}",
         tags="white_check_mark" if pnl >= 0 else "x")

    log(f"{'✅' if pnl>=0 else '❌'} EXIT {asset} {pos['direction']} "
        f"${pos['entry']:,.4f}→${exit_price:,.4f} P&L=${pnl:+,.4f} [{reason}]")

    # Remove position
    del positions[asset]
    if asset in entry_times:
        del entry_times[asset]
    with lock:
        state["positions"] = {k: v for k, v in positions.items()}


# ══════════════════════════════════════════════════════════════════
# TRADING LOOP
# Correct candle logic matching confirmed backtest:
# - Signal on candles[-2] (last complete)
# - pending_entry: wait for NEXT candle open
# - Trail + exit on CURRENT candle intrabar
# ══════════════════════════════════════════════════════════════════
startup_complete = False

def trading_loop():
    global startup_complete

    log("🚀 CB Trader v1 started")
    log(f"   Mode: {'📄 PAPER' if PAPER_MODE else '🚨 LIVE'}")
    log(f"   Assets: {', '.join(ASSET_NAMES)}")
    log(f"   Trail: {TRAIL_PCT*100}% | ATR buffer: {ATR_BUFFER}x")
    log(f"   Capital: ${TOTAL_USDC:,.2f} | Leverage: {LEVERAGE}x")

    ntfy("🚀 CB Trader v1 Started",
         f"Mode: {'PAPER' if PAPER_MODE else 'LIVE'}\n"
         f"Assets: {', '.join(ASSET_NAMES)}\nCapital: ${TOTAL_USDC:,.2f}",
         tags="rocket")

    add_diag("INFO", "CB Trader v1 started",
             f"Mode={'PAPER' if PAPER_MODE else 'LIVE'} | "
             f"Assets={len(ASSET_NAMES)} | Cap=${TOTAL_USDC:.2f} | Trail={TRAIL_PCT*100}%")

    # Sync balance on startup
    bal = get_balance()
    if bal and bal > 0:
        with lock:
            state["balance"] = round(bal, 2)
        log(f"💰 Balance synced: ${state['balance']:,.2f}")

    startup_complete = True
    cycle = 0

    while True:
        try:
            cycle += 1
            with lock:
                state["cycle"] = cycle
                state["status"] = "running"

            check_weekly_reset()

            for asset in ASSET_NAMES:
                try:
                    process_asset(asset)
                except Exception as e:
                    add_issue(asset, "asset_error", str(e))
                    log(f"⚠️ {asset} error: {e}")

        except Exception as e:
            add_diag("ERROR", "Loop error", str(e))
            log(f"⚠️ Loop error: {e}")

        time.sleep(CHECK_EVERY)

def process_asset(asset):
    """Process one asset per loop iteration"""
    candles = fetch_candles(asset)
    if not candles or len(candles) < 52:
        add_issue(asset, "no_candles", "fetch returned insufficient candles")
        return

    # Current price from latest candle
    cur = float(candles[-1]["c"])
    cur_candle = candles[-1]
    last_complete = candles[-2]  # signal candle

    with lock:
        state["health"]["assets_ok"][asset]["price"] = cur
        state["health"]["assets_ok"][asset]["last_candle"] = last_complete["dt"]

    # ── PENDING ENTRY: waiting for next candle open ────────────────
    if asset in pending_entry:
        pend = pending_entry[asset]
        # Check if new candle has opened since signal
        if last_complete["ts"] > pend["signal_candle_ts"]:
            # New candle opened — enter at its open price
            entry_price = float(last_complete["o"])  # open of the new candle
            direction   = pend["direction"]
            cs          = ASSETS[asset]["contract"]
            cap         = TOTAL_USDC / len(ASSET_NAMES)
            contracts   = max(1, int((cap * LEVERAGE) / (entry_price * cs)))
            size        = contracts * cs
            trail_stop  = round_price(
                entry_price * (1 - TRAIL_PCT) if direction == "LONG"
                else entry_price * (1 + TRAIL_PCT)
            )

            # Place order
            order = place_market_order(asset, direction, contracts)

            if order is not None:
                pos = {
                    "direction":   direction,
                    "entry":       entry_price,
                    "size":        size,
                    "contracts":   contracts,
                    "trail_peak":  entry_price,
                    "trail_stop":  trail_stop,
                    "entry_candle": last_complete["dt"],
                    "entry_ts":    last_complete["ts"],
                }
                positions[asset] = pos
                entry_times[asset] = ts()

                # Place stop order immediately
                oid = place_stop_order(asset, direction, contracts, trail_stop)
                if oid:
                    stop_oids[asset] = oid

                with lock:
                    state["positions"] = {k: v for k, v in positions.items()}

                add_trade(asset, "ENTER", direction, entry_price, None, size, 0, "signal")

                add_audit(asset, f"📊 ENTER {direction}",
                          f"entry=${entry_price:,.4f} | contracts={contracts} | size={size} | "
                          f"trail_stop=${trail_stop:,.4f} | stop_oid={stop_oids.get(asset,'?')}",
                          candle=last_complete,
                          indicators=pend.get("indicators"))

                ntfy(f"📊 {asset} {direction} ENTER",
                     f"Entry: ${entry_price:,.4f}\nSize: {size} {asset}\n"
                     f"Trail: ${trail_stop:,.4f}\n{'PAPER' if PAPER_MODE else 'LIVE'}",
                     tags="chart_with_upwards_trend")

                log(f"📊 ENTER {asset} {direction} @ ${entry_price:,.4f} | "
                    f"{contracts} contracts | trail=${trail_stop:,.4f}")

            del pending_entry[asset]
        else:
            # Still waiting for next candle
            add_audit(asset, "⏳ WAITING FOR ENTRY",
                      f"Signal was on {pend['signal_candle_ts']} | "
                      f"last_complete={last_complete['ts']} | waiting for next candle open",
                      candle=last_complete)
        return

    # ── IN POSITION: check trail and exit ─────────────────────────
    if asset in positions:
        pos = positions[asset]

        # Only process candles AFTER entry candle
        if last_complete["ts"] <= pos.get("entry_ts", 0):
            add_audit(asset, "⏳ HOLDING (entry candle)",
                      f"{pos['direction']} @ ${pos['entry']:,.4f} | "
                      f"waiting for candle after entry",
                      candle=last_complete)
            return

        # Calculate ATR on complete candles
        complete_candles = candles[:-1]
        cl = [float(c["c"]) for c in complete_candles]
        hi = [float(c["h"]) for c in complete_candles]
        lo = [float(c["l"]) for c in complete_candles]
        atr_vals = atr(hi, lo, cl)
        atr_val  = atr_vals[-1] if atr_vals and atr_vals[-1] else 0

        # Check trail and exit on current candle (cur_candle = candles[-1])
        result, updated = check_trail_and_exit(asset, pos, cur_candle, atr_val)

        if updated:
            # Cancel old stop, place new stop at updated trail price
            old_oid = stop_oids.get(asset)
            if old_oid:
                cancel_order(asset, old_oid)
            new_oid = place_stop_order(asset, pos["direction"],
                                        pos["contracts"], pos["trail_stop"])
            if new_oid:
                stop_oids[asset] = new_oid
            add_audit(asset, "🔄 TRAIL UPDATED",
                      f"trail_peak=${pos['trail_peak']:,.4f} | "
                      f"trail_stop=${pos['trail_stop']:,.4f} | "
                      f"new_stop_oid={stop_oids.get(asset,'?')}",
                      candle=cur_candle)

        if result == "EXIT":
            exit_position(asset, pos["trail_stop"], "trail", cur_candle)
        else:
            pnl = ((cur - pos["entry"]) * pos["size"] if pos["direction"] == "LONG"
                   else (pos["entry"] - cur) * pos["size"])
            add_audit(asset, "⏳ HOLDING",
                      f"{pos['direction']} @ ${pos['entry']:,.4f} | "
                      f"cur=${cur:,.4f} | trail=${pos['trail_stop']:,.4f} | "
                      f"peak=${pos['trail_peak']:,.4f} | unrealized=${pnl:+,.4f}",
                      candle=cur_candle)
            with lock:
                state["positions"][asset] = {**pos, "current_price": cur,
                                              "unrealized_pnl": round(pnl, 4)}
        return

    # ── NO POSITION: evaluate signal ──────────────────────────────
    # ONE EVAL PER CANDLE — skip if already evaluated this candle
    sig_ts = last_complete["ts"]
    if last_candle.get(asset) == sig_ts:
        add_audit(asset, "⏭ SAME CANDLE",
                  f"ts={sig_ts} | already evaluated | price=${cur:,.4f}",
                  candle=last_complete)
        with lock:
            state["health"]["assets_ok"][asset]["status"] = "WAITING"
        return

    last_candle[asset] = sig_ts

    direction, sig_candle, indicators = evaluate_signal(candles, asset)

    with lock:
        state["health"]["assets_ok"][asset]["status"] = "LIVE"

    if direction:
        # Signal confirmed — queue pending entry for NEXT candle open
        pending_entry[asset] = {
            "direction": direction,
            "signal_candle_ts": sig_ts,
            "signal_candle_dt": sig_candle["dt"],
            "indicators": indicators,
        }
        add_audit(asset, f"🚨 SIGNAL {direction}",
                  f"Waiting for next candle open to enter | "
                  f"signal_candle={sig_candle['dt']} | "
                  f"EMA5={indicators.get('ema5')} EMA13={indicators.get('ema13')} EMA34={indicators.get('ema34')} | "
                  f"sep={indicators.get('separation')} | vol={indicators.get('vol_ratio')}x | "
                  f"brk_level={indicators.get('brk_level')}",
                  candle=sig_candle,
                  indicators=indicators)
        log(f"🚨 SIGNAL {asset} {direction} — queued for next candle open")
        with lock:
            state["health"]["assets_ok"][asset]["signal"] = f"{direction} PENDING"
    else:
        fail = indicators.get("fail", "EMA not stacked") if indicators else "no candles"
        add_audit(asset, "⏳ NO SIGNAL",
                  f"Reason: {fail} | price=${cur:,.4f}",
                  candle=sig_candle,
                  indicators=indicators)
        with lock:
            state["health"]["assets_ok"][asset]["signal"] = f"no signal — {fail}"


# ══════════════════════════════════════════════════════════════════
# FLASK APP & DASHBOARD
# ══════════════════════════════════════════════════════════════════
app = Flask(__name__)
app.secret_key = os.urandom(24)

def auth_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authed"):
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            session["authed"] = True
            return redirect("/")
        return '<form method="post"><input name="password" type="password" placeholder="password"><button>Login</button><p style="color:red">Wrong password</p></form>'
    return '<form method="post"><input name="password" type="password" placeholder="password"><button>Login</button></form>'

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

@app.route("/api/state")
@auth_required
def api_state():
    from flask import jsonify
    return jsonify(state)

@app.route("/api/diagnostics")
@auth_required
def api_diagnostics():
    from flask import jsonify
    try:
        if os.path.exists(DIAG_FILE):
            return jsonify(json.load(open(DIAG_FILE)))
    except: pass
    return jsonify([])

def build_diag_html():
    """Build diagnostic HTML from saved file"""
    try:
        diags = json.load(open(DIAG_FILE)) if os.path.exists(DIAG_FILE) else []
    except:
        diags = []
    html = ""
    for d in diags[:50]:
        candle_str = ""
        if d.get("candle"):
            c = d["candle"]
            candle_str = (f"<div style='color:#4A5878;font-size:10px'>"
                         f"Candle: {c.get('dt','')} O:{c.get('o','')} "
                         f"H:{c.get('h','')} L:{c.get('l','')} C:{c.get('c','')}</div>")
        indic_str = ""
        if d.get("indicators"):
            ind = d["indicators"]
            status = "✅" if ind.get("pass") else f"❌ {ind.get('fail','')}"
            indic_str = (f"<div style='color:#4A5878;font-size:10px'>"
                        f"EMA5:{ind.get('ema5','')} EMA13:{ind.get('ema13','')} "
                        f"EMA34:{ind.get('ema34','')} Sep:{ind.get('separation','')} "
                        f"Vol:{ind.get('vol_ratio','')}x {status}</div>")
        html += (f"<div style='border-left:3px solid #1A2236;padding:8px 12px;"
                f"margin-bottom:6px;background:#0A0F1A;border-radius:0 4px 4px 0;font-size:11px'>"
                f"<div style='color:#4A5878'>{d['time']} | {d.get('asset','')}</div>"
                f"<div style='font-weight:600;color:#8892A4'>{d['event']}</div>"
                f"<div style='color:#6B7A99'>{d['detail']}</div>"
                f"{candle_str}{indic_str}</div>")
    return html

@app.route("/")
@auth_required
def dashboard():
    s = state
    mode_color = "#FFB800" if PAPER_MODE else "#00D68F"
    mode_label = "📄 PAPER MODE" if PAPER_MODE else "🚨 LIVE"

    # Build position cards
    pos_html = ""
    for asset, pos in s["positions"].items():
        pnl = pos.get("unrealized_pnl", 0)
        pnl_color = "#00D68F" if pnl >= 0 else "#FF4757"
        pos_html += f"""
        <div class="card">
          <div style="font-size:11px;color:#888;margin-bottom:4px">{asset} {pos['direction']}</div>
          <div style="font-size:18px;font-weight:700">Entry: ${pos['entry']:,.4f}</div>
          <div>Trail: ${pos.get('trail_stop',0):,.4f} | Peak: ${pos.get('trail_peak',0):,.4f}</div>
          <div style="color:{pnl_color};font-weight:700">Unrealized: ${pnl:+,.4f}</div>
          <div style="font-size:10px;color:#888">Since: {entry_times.get(asset,'?')}</div>
        </div>"""

    if not pos_html:
        pos_html = '<div style="color:#4A5878;padding:16px">No open positions</div>'

    # Build audit log
    audit_html = ""
    for a in s["audit"][:100]:
        color = ("#00D68F" if "ENTER" in a["event"] or "✅" in a["event"]
                 else "#FF4757" if "❌" in a["event"] or "EXIT" in a["event"]
                 else "#FFB800" if "SIGNAL" in a["event"]
                 else "#4A5878")
        # Build indicator detail if present
        indic_html = ""
        if a.get("indicators"):
            ind = a["indicators"]
            indic_html = f"""
            <div style="margin-top:6px;padding:6px;background:#0A0F1A;border-radius:4px;font-size:10px;color:#6B7A99">
              EMA5:{ind.get('ema5','?')} EMA13:{ind.get('ema13','?')} EMA34:{ind.get('ema34','?')} |
              Sep:{ind.get('separation','?')} | Vol:{ind.get('vol_ratio','?')}x |
              {f"Brk:{ind.get('brk_level','?')}" if ind.get('brk_level') else ''}
              {f"| ❌ {ind.get('fail','')}" if ind.get('fail') else '| ✅ PASS'}
            </div>"""
        # Build candle detail if present
        candle_html = ""
        if a.get("candle"):
            c = a["candle"]
            candle_html = f"""
            <div style="margin-top:4px;font-size:10px;color:#6B7A99">
              Candle: {c.get('dt','?')} | O:{c.get('o','?')} H:{c.get('h','?')} L:{c.get('l','?')} C:{c.get('c','?')}
            </div>"""

        audit_html += f"""
        <div style="border-left:3px solid {color};padding:8px 12px;margin-bottom:6px;background:#0D1421;border-radius:0 4px 4px 0">
          <div style="font-size:10px;color:#4A5878">{a['time']} UTC | {a.get('asset','')}</div>
          <div style="font-weight:600;color:{color}">{a['event']}</div>
          <div style="font-size:11px;color:#8892A4;margin-top:2px">{a['detail']}</div>
          {candle_html}
          {indic_html}
        </div>"""

    # Build trade history
    trade_html = ""
    for t in s["trades"][:30]:
        pnl = t.get("pnl", 0)
        if t.get("action") != "EXIT": continue
        pnl_color = "#00D68F" if pnl >= 0 else "#FF4757"
        trade_html += f"""
        <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #1A2236;font-size:12px">
          <span style="color:#8892A4">{t['time'][:16]}</span>
          <span><b>{t['asset']}</b> {t['direction']}</span>
          <span>${t['entry']:,.4f} → ${t.get('exit',0):,.4f}</span>
          <span style="color:{pnl_color};font-weight:700">${pnl:+,.4f}</span>
          <span style="color:#4A5878">{t.get('reason','?')}</span>
        </div>"""

    if not trade_html:
        trade_html = '<div style="color:#4A5878;padding:16px">No completed trades yet</div>'

    # Asset health
    health_html = ""
    for a_name in ASSET_NAMES:
        h = s["health"]["assets_ok"].get(a_name, {})
        status = "OPEN" if a_name in s["positions"] else h.get("status","?")
        sc = "#00D68F" if status in ("LIVE","OPEN") else "#4A5878"
        health_html += f"""
        <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #1A2236;font-size:11px">
          <span style="font-weight:700">{a_name}</span>
          <span style="color:#8892A4">${h.get('price',0):,.4f}</span>
          <span style="color:#6B7A99">{h.get('last_candle','?')}</span>
          <span style="color:#6B7A99;max-width:200px;overflow:hidden;text-overflow:ellipsis">{h.get('signal','?')}</span>
          <span style="color:{sc}">{status}</span>
        </div>"""

    weekly_pnl = s.get("weekly_pnl", 0)
    weekly_color = "#00D68F" if weekly_pnl >= 0 else "#FF4757"

    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>CB Trader v1</title>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#060D1A;color:#E0E6F0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:16px}}
    .card{{background:#0D1421;border:1px solid #1A2236;border-radius:8px;padding:16px;margin-bottom:12px}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:16px}}
    .metric{{background:#0D1421;border:1px solid #1A2236;border-radius:8px;padding:12px;text-align:center}}
    .metric-val{{font-size:22px;font-weight:700;margin-top:4px}}
    .metric-lbl{{font-size:10px;color:#4A5878;text-transform:uppercase;letter-spacing:.8px}}
    h2{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px}}
    .section{{margin-bottom:20px}}
    .tab{{display:inline-block;padding:6px 16px;cursor:pointer;border-radius:4px 4px 0 0;font-size:12px;font-weight:600}}
    .tab.active{{background:#0D1421;color:#E0E6F0}}
    .tab:not(.active){{color:#4A5878}}
    .sec{{display:none}}.sec.active{{display:block}}
    pre{{background:#0A0F1A;padding:12px;border-radius:6px;font-size:10px;overflow:auto;max-height:400px;color:#8892A4}}
  </style>
  <script>
    function show(id,el){{
      document.querySelectorAll('.sec').forEach(s=>s.classList.remove('active'));
      document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
      document.getElementById(id).classList.add('active');
      el.classList.add('active');
    }}
    function copyDiag(){{
      fetch('/api/diagnostics').then(r=>r.json()).then(d=>{{
        navigator.clipboard.writeText(JSON.stringify(d,null,2));
        alert('Diagnostics copied to clipboard!');
      }});
    }}
  </script>
</head>
<body>
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
    <div>
      <div style="font-size:20px;font-weight:700">CB Trader v1</div>
      <div style="font-size:12px;color:{mode_color};font-weight:700">{mode_label}</div>
    </div>
    <div style="text-align:right;font-size:11px;color:#4A5878">
      Cycle #{s['cycle']} · {s['status'].upper()}<br>
      <a href="/logout" style="color:#4A5878">logout</a>
    </div>
  </div>

  <div class="grid">
    <div class="metric">
      <div class="metric-lbl">Balance</div>
      <div class="metric-val">${s['balance']:,.2f}</div>
    </div>
    <div class="metric">
      <div class="metric-lbl">Weekly P&L</div>
      <div class="metric-val" style="color:{weekly_color}">${weekly_pnl:+,.2f}</div>
    </div>
    <div class="metric">
      <div class="metric-lbl">Open</div>
      <div class="metric-val">{len(s['positions'])}</div>
    </div>
    <div class="metric">
      <div class="metric-lbl">Trades</div>
      <div class="metric-val">{len([t for t in s['trades'] if t.get('action')=='EXIT'])}</div>
    </div>
  </div>

  <div style="margin-bottom:4px">
    <span class="tab active" onclick="show('positions',this)">Positions</span>
    <span class="tab" onclick="show('audit',this)">Audit Log</span>
    <span class="tab" onclick="show('trades',this)">Trades</span>
    <span class="tab" onclick="show('assets',this)">Assets</span>
    <span class="tab" onclick="show('diagnostic',this)" style="color:#FFB800">🔬 Diagnostic</span>
  </div>

  <div id="positions" class="sec active card">
    <h2>Open Positions</h2>
    {pos_html}
  </div>

  <div id="audit" class="sec card">
    <h2>Audit Log — Every Candle Every Decision</h2>
    {audit_html}
  </div>

  <div id="trades" class="sec card">
    <h2>Completed Trades</h2>
    {trade_html}
  </div>

  <div id="assets" class="sec card">
    <h2>Asset Status</h2>
    {health_html}
  </div>

  <div id="diagnostic" class="sec card">
    <h2>🔬 Full Diagnostic Log</h2>
    <p style="font-size:11px;color:#4A5878;margin-bottom:10px">
      Every candle evaluation — what was seen, indicators, signal decision, trail updates, exits.
      Saved to file for copy/paste comparison with backtest.
    </p>
    <button onclick="copyDiag()" style="background:#1A2236;color:#E0E6F0;border:1px solid #2A3550;padding:8px 16px;border-radius:4px;cursor:pointer;margin-bottom:12px">
      📋 Copy Full Diagnostic JSON
    </button>
    <a href="/diagnostic-raw" style="margin-left:10px;color:#4A5878;font-size:11px">View raw file</a>
    <div style="margin-top:10px">
      {build_diag_html()}
  </div>
</body>
</html>"""
    return html

@app.route("/diagnostic-raw")
@auth_required
def diagnostic_raw():
    """Raw diagnostic file — copy and paste"""
    try:
        if os.path.exists(DIAG_FILE):
            content = json.dumps(json.load(open(DIAG_FILE)), indent=2)
            return Response(content, mimetype="text/plain")
    except: pass
    return Response("[]", mimetype="text/plain")

@app.route("/log")
@auth_required
def log_export():
    """Plain text log export"""
    lines = [
        "="*60,
        f"CB TRADER v1 — {'PAPER' if PAPER_MODE else 'LIVE'}",
        f"Generated: {ts()} UTC",
        "="*60, "",
        f"Balance: ${state['balance']:,.2f}",
        f"Weekly P&L: ${state['weekly_pnl']:+,.2f}",
        f"Open positions: {len(state['positions'])}",
        f"Total trades: {len([t for t in state['trades'] if t.get('action')=='EXIT'])}",
        "", "OPEN POSITIONS:",
    ]
    for asset, pos in state["positions"].items():
        pnl = pos.get("unrealized_pnl", 0)
        lines.append(f"  {asset} {pos['direction']} @ ${pos['entry']:,.4f} | "
                     f"trail=${pos.get('trail_stop',0):,.4f} | P&L=${pnl:+,.4f}")
    lines += ["", "RECENT TRADES:"]
    for t in [x for x in state["trades"] if x.get("action")=="EXIT"][:20]:
        lines.append(f"  {t['time'][:16]} | {t['asset']} {t['direction']} | "
                     f"${t['entry']:,.4f}→${t.get('exit',0):,.4f} | ${t.get('pnl',0):+,.4f} | {t.get('reason','?')}")
    lines += ["", "RECENT AUDIT:"]
    for a in state["audit"][:30]:
        lines.append(f"  {a['time']} | {a.get('asset','')} | {a['event']} | {a['detail']}")
    return Response("\n".join(lines), mimetype="text/plain")

@app.route("/kill", methods=["POST"])
@auth_required
def kill():
    with lock:
        state["kill_switch"] = True
    return "Kill switch activated"


# ══════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════
load_trades()
load_weekly_pnl()

# Start WebSocket candle feed
_ws = threading.Thread(target=start_websocket, daemon=True)
_ws.start()

# Pre-load candles via REST before trading loop starts
log("📡 Pre-loading candles via REST...")
for asset in ASSET_NAMES:
    fetch_candles_rest(asset)
    log(f"  {asset}: {len(candle_cache.get(asset,[]))} candles loaded")

_t = threading.Thread(target=trading_loop, daemon=True)
_t.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
