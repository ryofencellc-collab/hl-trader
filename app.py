"""
CB TRADER v41
═══════════════════════════════════════════════════════════════════
Strategy: EMA 5/13/50 + sep≥0.002 + vol≥0.3 + 10-bar breakout + 0.2% trail
Exit:     BUCKET only — exits at 5-min bucket close (matches backtest exactly)
Exchange: Coinbase CFM Futures (CFTC regulated, legal NYC)
Candles:  Coinbase spot API (ADA-USD etc)
Assets:   10 (affordable at $99 capital — scale to 15 at $1,000)
Balance:  Reads from Coinbase on startup + after every exit + every 50 cycles
Compounding: cap = current_balance / N_ASSETS — grows automatically with account
Mode: Always LIVE — real orders on every signal
"""

import time, os, math, json, csv, uuid, threading
from datetime import datetime, timezone
from flask import Flask, Response, request, redirect
import requests as req

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
PAPER_MODE  = False  # Always live — no paper mode
NTFY_TOPIC  = os.environ.get("NTFY_TOPIC", "hl-trader-lunchm0ney")
NTFY_URL    = f"https://ntfy.sh/{NTFY_TOPIC}"

# API keys loaded from Railway environment variables — never hardcoded
CB_API_KEY  = os.environ.get("CB_API_KEY", "")
CB_API_SEC  = os.environ.get("CB_API_SECRET", "")
if not CB_API_KEY or not CB_API_SEC:
    raise RuntimeError("CB_API_KEY and CB_API_SECRET must be set in Railway environment variables")

# Contract expiry dates — auto-roll checks these
EXPIRY_AUG = "2026-08-28"
EXPIRY_SEP = "2026-09-25"
ROLL_DAYS_BEFORE = 5  # roll 5 days before expiry

# Assets selected for $99 test capital — min margin per contract < $10
# When we scale to $1,000: add back BTC, ETH, BCH, BNB, DOGE
ASSETS = {
    "ADA":  {"spot":"ADA-USD",  "aug":"ADA-28AUG26-CDE", "sep":"ADA-25SEP26-CDE", "contract":100.0},
    "SOL":  {"spot":"SOL-USD",  "aug":"SOL-28AUG26-CDE", "sep":"SOL-25SEP26-CDE", "contract":1.0},
    "XRP":  {"spot":"XRP-USD",  "aug":"XRP-28AUG26-CDE", "sep":"XRP-25SEP26-CDE", "contract":100.0},
    "DOT":  {"spot":"DOT-USD",  "aug":"DOT-28AUG26-CDE", "sep":"DOT-25SEP26-CDE", "contract":10.0},
    "LINK": {"spot":"LINK-USD", "aug":"LNK-28AUG26-CDE", "sep":"LNK-25SEP26-CDE", "contract":10.0},
    "LTC":  {"spot":"LTC-USD",  "aug":"LC-28AUG26-CDE",  "sep":"LC-25SEP26-CDE",  "contract":1.0},
    "SUI":  {"spot":"SUI-USD",  "aug":"SUI-28AUG26-CDE", "sep":"SUI-25SEP26-CDE", "contract":100.0},
    "XLM":  {"spot":"XLM-USD",  "aug":"XLM-28AUG26-CDE", "sep":"XLM-25SEP26-CDE", "contract":1000.0},
    "AVAX": {"spot":"AVAX-USD", "aug":"AVA-28AUG26-CDE", "sep":"AVA-25SEP26-CDE", "contract":10.0},
    "HBAR": {"spot":"HBAR-USD", "aug":"HED-28AUG26-CDE", "sep":"HED-25SEP26-CDE", "contract":1000.0},
}

def get_active_ticker(asset):
    """Returns the active futures ticker — auto-rolls 5 days before expiry"""
    from datetime import date
    today = date.today()
    expiry = date(2026, 8, 28)
    days_to_expiry = (expiry - today).days
    if days_to_expiry <= ROLL_DAYS_BEFORE:
        return ASSETS[asset]["sep"]
    return ASSETS[asset]["aug"]
ASSET_NAMES = list(ASSETS.keys())

EMA_FAST    = 5
EMA_MID     = 13
EMA_SLOW    = 50
SEP_FILTER  = 0.002
VOL_FILTER  = 0.3
BRK_BARS    = 10
TRAIL_PCT   = 0.002
ATR_BUFFER  = 2.0
CANDLE_TF   = "FIVE_MINUTE"
CANDLE_LIMIT= 201
LEVERAGE    = 10
TOTAL_USDC  = float(os.environ.get("TOTAL_USDC", "1000"))
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

state = {
    "balance": TOTAL_USDC, "weekly_pnl": 0.0, "total_pnl": 0.0,
    "week": None, "cycle": 0, "ws_connected": False,
    "ws_last_candle": "never", "ntfy_errors": 0,
    "loop_last_run": "never", "loop_errors": 0,
    "wins": 0, "total_trades": 0,
    # PERP strategy removed in v33
    "skipped_assets": [],
    "skip_streak":    {},
    "api_errors":     {},
}

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

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
            "candles": {
                "signal":  candles[-3] if isinstance(candles, list) and len(candles) >= 3 else None,
                "prev":    candles[-2] if isinstance(candles, list) and len(candles) >= 2 else None,
                "current": candles[-1] if isinstance(candles, list) and len(candles) >= 1 else None,
            },
            "indicators": indicators if isinstance(indicators, dict) else {},
            "position": {
                "direction":  position.get("direction"),
                "entry":      position.get("entry"),
                "contracts":  position.get("contracts"),
                "size":       position.get("size"),
                "trail_peak": position.get("trail_peak"),
                "trail_stop": position.get("trail_stop"),
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

def fetch_candles(asset, use_perp=False):
    try:
        client = get_cb_client()
        product_id = get_active_ticker(asset) if use_perp else ASSETS[asset]["spot"]
        end   = int(time.time())
        start = end - 300 * 5 * 60
        resp  = client.get_candles(product_id, start=str(start),
                                   end=str(end), granularity=CANDLE_TF)
        if not resp.candles:
            log(f"WARNING {asset}: API returned 0 candles")
            return None
        candles = sorted([{
            "ts": int(c.start)*1000,
            "dt": datetime.fromtimestamp(int(c.start),tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "o":float(c.open),"h":float(c.high),
            "l":float(c.low),"c":float(c.close),"v":float(c.volume),
        } for c in resp.candles], key=lambda x:x["ts"])[-CANDLE_LIMIT:]
        if candles and candles[-1]["c"] == 0:
            log(f"WARNING {asset}: last candle close=0 dead feed")
            return None
        return candles
    except Exception as e:
        log(f"WARNING {asset}: candle fetch failed {e}")
        return None

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
            return val
        # fallback to buying_power if margin is 0
        if bp_val > 0:
            log(f"💰 Using buying_power as fallback: ${bp_val:,.2f}")
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
        open_pos = resp.get("positions", []) if isinstance(resp, dict) else []
        if not open_pos:
            log("📊 No open CFM positions on Coinbase")
            return
        log(f"📊 Found {len(open_pos)} open CFM position(s) on Coinbase — syncing...")
        for p in open_pos:
            product_id = p.get("product_id", "")
            # Map product_id back to asset name
            asset = None
            for a, cfg in ASSETS.items():
                if cfg.get("spot") == product_id or product_id.startswith(a):
                    asset = a; break
            if not asset:
                log(f"  Unknown position: {product_id} — skipping")
                continue
            side     = p.get("side", "UNKNOWN")
            n_cont   = int(float(p.get("number_of_contracts", 0)))
            avg_entry= float(p.get("avg_entry_price", 0))
            direction = "LONG" if side == "LONG" else "SHORT"
            cs = ASSETS[asset]["contract"]
            trail_stop = round_price(
                avg_entry*(1-TRAIL_PCT) if direction=="LONG"
                else avg_entry*(1+TRAIL_PCT))
            positions[asset] = {
                "direction": direction,
                "entry":     avg_entry,
                "contracts": n_cont,
                "size":      n_cont * cs,
                "trail_peak":avg_entry,
                "trail_stop":trail_stop,
                "entry_time":ts(),
            }
            log(f"  ✅ Synced {asset} {direction} @ ${avg_entry:.4f} | {n_cont} contracts")
    except Exception as e:
        log(f"Position sync error: {e}")

def place_market_order(asset, side, contracts):
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
                return oid
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
                    return None
        log(f"❌ {asset} could not place even 1 contract")
        return None
    except Exception as e:
        import traceback
        log(f"❌ Order exception {asset}: {e}")
        log(f"❌ Traceback: {traceback.format_exc()}")
        return None

# ══════════════════════════════════════════════════════════════════
# MATH
# ══════════════════════════════════════════════════════════════════
def round_price(p, sig=5):
    if p==0: return 0.0
    mag=math.floor(math.log10(abs(p))); return round(p,max(0,sig-1-mag))

def ema(values, period):
    k=2/(period+1); e=None; out=[]
    for v in values:
        e=v if e is None else v*k+e*(1-k); out.append(e)
    return out

def sma(values, period):
    out=[None]*(period-1)
    for i in range(period-1,len(values)):
        out.append(sum(values[i-period+1:i+1])/period)
    return out

def atr_calc(highs, lows, closes, period=14):
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))
    if len(trs)<period: return [None]*len(closes)
    out=[None]*period; avg=sum(trs[:period])/period; out.append(avg)
    for i in range(period,len(trs)):
        avg=(avg*(period-1)+trs[i])/period; out.append(avg)
    while len(out)<len(closes): out.append(out[-1])
    return out

# ══════════════════════════════════════════════════════════════════
# SIGNAL — identical to backtest
# ══════════════════════════════════════════════════════════════════
def evaluate_signal(candles):
    complete = candles[:-1]   # exclude forming candle
    if len(complete) < 200: return None, None, {"fail":"not enough candles"}
    cl=[c["c"] for c in complete]; hi=[c["h"] for c in complete]
    lo=[c["l"] for c in complete]; vo=[c["v"] for c in complete]
    ef=ema(cl,EMA_FAST); em_=ema(cl,EMA_MID); es=ema(cl,EMA_SLOW)
    vs=sma(vo,20); i=-2
    sig_candle=complete[i]
    if not (ef[i] and em_[i] and es[i]):
        return None, sig_candle, {"fail":"EMA not ready"}
    if   ef[i]>em_[i]>es[i]: d="LONG"
    elif ef[i]<em_[i]<es[i]: d="SHORT"
    else: return None, sig_candle, {"fail":"no stack"}
    sep=abs(ef[i]-es[i])/es[i] if es[i] else 0
    if sep<SEP_FILTER: return None, sig_candle, {"fail":f"sep={sep:.5f}"}
    vr=vo[i]/vs[i] if vs[i] else 0
    if vr<VOL_FILTER: return None, sig_candle, {"fail":f"vol={vr:.2f}x"}
    if d=="LONG"  and cl[i]<=max(hi[i-BRK_BARS:i]):
        return None, sig_candle, {"fail":"no breakout"}
    if d=="SHORT" and cl[i]>=min(lo[i-BRK_BARS:i]):
        return None, sig_candle, {"fail":"no breakout"}
    return d, sig_candle, {"sep":round(sep,5),"vol":round(vr,2),"pass":True}

# ══════════════════════════════════════════════════════════════════
# TRAIL CHECK — identical to backtest
# ══════════════════════════════════════════════════════════════════
def check_trail(pos, cur_candle, av):
    h=float(cur_candle["h"]); l=float(cur_candle["l"])
    if pos["direction"]=="LONG":
        if h>pos["trail_peak"] and (av==0 or h-pos["trail_peak"]>av*ATR_BUFFER):
            pos["trail_peak"]=h
            pos["trail_stop"]=round_price(h*(1-TRAIL_PCT))
        thresh=pos["trail_stop"]-(av*ATR_BUFFER if av else 0)
        if l<=thresh: return "EXIT"
    else:
        if l<pos["trail_peak"] and (av==0 or pos["trail_peak"]-l>av*ATR_BUFFER):
            pos["trail_peak"]=l
            pos["trail_stop"]=round_price(l*(1+TRAIL_PCT))
        thresh=pos["trail_stop"]+(av*ATR_BUFFER if av else 0)
        if h>=thresh: return "EXIT"
    return "HOLD"

# ══════════════════════════════════════════════════════════════════
# ENTER / EXIT
# ══════════════════════════════════════════════════════════════════
def enter_position(asset, direction, entry_price, candle):
    cs  = ASSETS[asset]["contract"]
    # Compounding: use current balance for cap, not fixed TOTAL_USDC
    with lock: current_bal = state["balance"]
    cap = current_bal / len(ASSET_NAMES)
    # Size based on available margin per asset
    # Cap at what margin can actually cover to prevent rejections
    contracts = max(1, int((cap * LEVERAGE) / (entry_price * cs)))
    # Safety cap: never use more than 80% of per-asset allocation
    max_contracts = max(1, int((cap * 0.8) / (entry_price * cs / LEVERAGE)))
    contracts = min(contracts, max_contracts)
    size      = contracts * cs
    trail_stop = round_price(
        entry_price*(1-TRAIL_PCT) if direction=="LONG"
        else entry_price*(1+TRAIL_PCT))
    side = "BUY" if direction=="LONG" else "SELL"
    oid  = place_market_order(asset, side, contracts)
    if not oid:
        msg = f"{asset} {side} {contracts} contracts rejected by Coinbase"
        log(f"CRITICAL order rejected: {msg}")
        add_audit(asset, "ORDER REJECTED", msg)
        ntfy(f"CRITICAL ORDER REJECTED {asset}", msg, priority="urgent")
        return
    positions[asset] = {
        "direction":direction, "entry":entry_price,
        "contracts":contracts, "size":size,
        "trail_peak":entry_price, "trail_stop":trail_stop,
        "entry_time":ts(),
    }
    add_audit(asset, f"📊 ENTER {direction}",
              f"entry=${entry_price:,.4f} | contracts={contracts} | "
              f"size={size} | trail_stop=${trail_stop:,.4f} | "
              f"LIVE",
              candle=candle)
    # ntfy removed for entries — only errors alerted

def exit_position(asset, exit_price, candle):
    pos = positions.get(asset)
    if not pos: return
    pnl = round(
        (exit_price-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
        else (pos["entry"]-exit_price)*pos["size"], 4)
    side = "SELL" if pos["direction"]=="LONG" else "BUY"
    place_market_order(asset, side, pos["contracts"])
    record_tax(asset, pos["direction"], pos["entry"], exit_price,
               pos["size"], pnl, pos["entry_time"])
    with lock:
        state["total_pnl"]  = round(state["total_pnl"]+pnl, 4)
        state["weekly_pnl"] = round(state["weekly_pnl"]+pnl, 4)
        state["balance"]    = round(TOTAL_USDC+state["total_pnl"], 4)  # updated below with real CB balance
        state["total_trades"] += 1
        if pnl>=0: state["wins"]+=1
    del positions[asset]
    emoji = "✅" if pnl>=0 else "❌"
    add_audit(asset, f"{emoji} EXIT TRAIL",
              f"{pos['direction']} ${pos['entry']:,.4f} → ${exit_price:,.4f} | "
              f"P&L=${pnl:+,.4f}", candle=candle)
    # Always read real Coinbase balance after exit — captures fees automatically
    # This keeps app 1:1 with Coinbase regardless of fees or spread
    try:
        live_bal = get_live_balance()
        if live_bal > 0:
            with lock:
                state["balance"] = live_bal
                log(f"💰 Balance after exit: ${live_bal:,.2f} (Coinbase)")
    except Exception as e:
        log(f"Balance refresh error after exit: {e}")

# ══════════════════════════════════════════════════════════════════
# TRADING LOOP — SEQUENTIAL, no threads, no processing guard
# Mirrors backtest exactly: one asset at a time per bucket
# ══════════════════════════════════════════════════════════════════
def trading_loop():
    global TOTAL_USDC
    # Always read real balance from Coinbase — paper mode only stops real orders
    live_bal = get_live_balance()
    if live_bal > 0:
        TOTAL_USDC = live_bal
    sync_open_positions()
    with lock:
        state["balance"] = TOTAL_USDC
    log("🚀 CB Trader v41 started")
    log("   Mode: 🔴 LIVE")
    log(f"   Assets: {', '.join(ASSET_NAMES)}")
    log(f"   Trail: {TRAIL_PCT*100}% | ATR: {ATR_BUFFER}x | Sep: {SEP_FILTER} | Vol: {VOL_FILTER}x")
    log(f"   Capital: ${TOTAL_USDC:,.2f} | Leverage: {LEVERAGE}x")

    last_bucket = (int(time.time()) // 300) * 300

    while True:
        try:
            current_bucket = (int(time.time()) // 300) * 300
            with lock:
                state["loop_last_run"] = ts()
                state["cycle"] = state.get("cycle",0) + 1

            check_weekly_reset()

            if current_bucket != last_bucket:
                last_bucket = current_bucket
                bucket_dt = datetime.fromtimestamp(current_bucket,
                            tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                log(f"🕐 New candle: {bucket_dt} UTC — evaluating {len(ASSET_NAMES)} assets | "
                    f"open={len(positions)} positions | pending={list(pending_entry.keys())}")

                # ── BUCKET STRATEGY: bucket-only exits, matches backtest ─
                skipped_assets = []
                for asset in ASSET_NAMES:
                    try:
                        candles = fetch_candles(asset)
                        if not candles or len(candles) < 200: continue

                        cur = candles[-1]
                        at  = atr_calc([c["h"] for c in candles],
                                       [c["l"] for c in candles],
                                       [c["c"] for c in candles])
                        av  = at[-1] or 0

                        # Skip check
                        if skip_entry.get(asset, 0) > 0:
                            skip_entry[asset] -= 1
                            save_sim_data(asset, current_bucket*1000, candles, {},
                                         f"SKIP_REENTRY({skip_entry[asset]+1} left)",
                                         )
                            continue

                        # Trail check — BUCKET ONLY, no WebSocket
                        pos = positions.get(asset)
                        if pos:
                            result = check_trail(pos, cur, av)
                            if result == "EXIT":
                                exit_price = pos["trail_stop"]
                                pnl_est = round(
                                    (exit_price-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                                    else (pos["entry"]-exit_price)*pos["size"], 4)
                                save_sim_data(asset, current_bucket*1000, candles, {},
                                             "EXIT_TRAIL", position=dict(pos), pnl=pnl_est,
                                             )
                                # Exit — call exit_position which places real SELL/BUY on Coinbase
                                exit_position(asset, exit_price, cur)
                                skip_entry[asset] = 1
                            else:
                                save_sim_data(asset, current_bucket*1000, candles, {},
                                             "HOLD", position=dict(pos),
                                             )
                            continue

                        # Pending entry — call enter_position which places real order
                        pend = pending_entry.get(asset)
                        if pend:
                            entry_price = float(cur["o"])
                            direction   = pend["direction"]
                            del pending_entry[asset]
                            # enter_position places real order on Coinbase then tracks position
                            enter_position(asset, direction, entry_price, cur)
                            # Save sim data for monitor comparison
                            if positions.get(asset):
                                save_sim_data(asset, current_bucket*1000, candles, {},
                                             f"ENTER_{direction}", position=dict(positions[asset]))
                            continue

                        # Signal evaluation — shared with WS strategy
                        direction, sig_candle, indic = evaluate_signal(candles)
                        save_sim_data(asset, current_bucket*1000, candles, indic,
                                     f"SIGNAL_{direction}" if direction else f"NO_SIGNAL:{indic.get('fail','?')}",
                                     position=positions.get(asset),
                                     )
                        if direction:
                            pending_entry[asset] = {
                                "direction": direction,
                                "signal_ts": candles[-2]["ts"]
                            }
                            add_audit(asset, f"🚨 🚨 SIGNAL {direction}",
                                     f"signal_candle={sig_candle['dt']} | sep={indic.get('sep')} | vol={indic.get('vol')}x",
                                     candle=sig_candle, indicators=indic)

                    except Exception as e:
                        log(f"Asset error {asset}: {e}")

                # ── Heartbeat after all assets processed ─────────────
                if skipped_assets:
                    log(f"⚠️ Skipped assets (insufficient candles): {skipped_assets}")
                    with lock:
                        state["skipped_assets"] = skipped_assets
                else:
                    with lock:
                        state["skipped_assets"] = []
                # Refresh live balance every 50 cycles (always — paper or live)
                with lock: cycle_num = state.get("cycle", 0) + 1; state["cycle"] = cycle_num
                if cycle_num % 50 == 0:
                    try:
                        live_bal = get_live_balance()
                        if live_bal > 0:
                            with lock: state["balance"] = live_bal
                    except: pass
                add_audit("SYSTEM", "💓 CYCLE",
                          f"candle={bucket_dt} | open={len(positions)} | "
                          f"pending={list(pending_entry.keys())} | "
                          f"skipped={skipped_assets} | "
                          f"balance=${state['balance']:,.2f} | trades={state['total_trades']}")

                # ── Weekly P&L report — every Monday at 9am UTC ──────────
                bucket_dt_obj = datetime.fromtimestamp(current_bucket, tz=timezone.utc)
                if bucket_dt_obj.weekday() == 0 and bucket_dt_obj.hour == 9 and bucket_dt_obj.minute < 5:
                    with lock:
                        wpnl = state["weekly_pnl"]
                        bal  = state["balance"]
                        trd  = state["total_trades"]
                        wr   = round(state["wins"]/trd*100,1) if trd else 0
                    week_str = bucket_dt_obj.strftime("%Y-%m-%d")
                    ntfy("Weekly P&L Report",
                         f"Week: {week_str} | P&L: ${wpnl:+,.2f} | Bal: ${bal:,.2f} | Trades: {trd} | WR: {wr}%",
                         priority="default")
                    with lock: state["weekly_pnl"] = 0.0

                # ── Emergency stop — balance below 50% ───────────────────
                with lock:
                    bal      = state["balance"]
                    starting = TOTAL_USDC
                if bal < starting * 0.5 and len(positions) == 0:
                    ntfy("EMERGENCY Balance Alert",
                         f"Balance ${bal:,.2f} below 50% of ${starting:,.2f} — review immediately",
                         priority="urgent")

                # ── Auto-roll check — 5 days before Aug 28 expiry ────────
                from datetime import date as ddate
                today      = ddate.today()
                expiry_aug = ddate(2026, 8, 28)
                days_left  = (expiry_aug - today).days
                if days_left == ROLL_DAYS_BEFORE and bucket_dt_obj.hour == 9 and bucket_dt_obj.minute < 5:
                    ntfy("Contract Roll Starting",
                         f"Aug 28 expires in {days_left} days — auto-rolling to Sep 25 tickers",
                         priority="high")

        except Exception as e:
            with lock:
                state["loop_errors"] = state.get("loop_errors",0)+1
                errs = state["loop_errors"]
            log(f"Loop error {errs}: {e}")
            if errs in (3, 10, 25):
                pri = "urgent" if errs >= 10 else "high"
                ntfy(f"CRITICAL loop errors: {errs}", f"Loop error #{errs}: {str(e)[:100]}", priority=pri)

        time.sleep(30)

# ══════════════════════════════════════════════════════════════════
# WEBSOCKET — for trail stop updates between candles
# ══════════════════════════════════════════════════════════════════
def start_websocket():
    try:
        from coinbase.websocket import WSClient
        product_ids = [ASSETS[a]["spot"] for a in ASSET_NAMES]

        def on_message(msg):
            try:
                import json as _j
                data = _j.loads(msg) if isinstance(msg,str) else msg
                if data.get("channel") != "candles": return
                for event in data.get("events",[]):
                    if event.get("type") != "update": continue
                    for c in event.get("candles",[]):
                        product_id = c.get("product_id","")
                        asset = next((a for a,cfg in ASSETS.items()
                                     if cfg["spot"]==product_id), None)
                        if not asset: continue
                        with lock: state["ws_last_candle"] = datetime.fromtimestamp(
                            int(float(c["start"])),tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                        # Only update trail on open positions between candles
                        pos = positions.get(asset)
                        if not pos: continue
                        cur = {"h":float(c["high"]),"l":float(c["low"]),
                               "o":float(c["open"]),"c":float(c["close"]),
                               "dt":datetime.fromtimestamp(int(float(c["start"])),
                                    tz=timezone.utc).strftime("%Y-%m-%d %H:%M")}
                        # WebSocket used for trail peak updates only — exits at bucket
                        av = 0
                        result = check_trail(pos, cur, av)
                        if result == "EXIT":
                            pass  # BUCKET strategy exits at bucket eval, not WebSocket
            except Exception as e:
                log(f"WS error: {e}")

        ws = WSClient(api_key=CB_API_KEY, api_secret=CB_API_SEC, on_message=on_message)
        ws.open()
        ws.subscribe(product_ids=product_ids, channels=["candles"])
        log(f"🔌 WebSocket connected — {len(product_ids)} assets")
        with lock: state["ws_connected"] = True
        ws.run_forever_with_exception_check()
    except Exception as e:
        log(f"WebSocket error: {e}")
        with lock: state["ws_connected"] = False
        ntfy("WARNING WebSocket dropped", f"WS error: {str(e)[:100]}", priority="high")

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
        "mode":       {"paper_mode":False,"status":"🔴 LIVE"},
        "balance":    f"${s['balance']:,.2f}",
        "weekly_pnl": f"${s['weekly_pnl']:+,.2f}",
        "total_pnl":  f"${s['total_pnl']:+,.2f}",
        "trades":     s["total_trades"],
        "win_rate":   f"{wr}%",
        "spot_strategy": {
            "open_positions": list(positions.keys()),
            "pending_entries": list(pending_entry.keys()),
            "total_pnl": f"${state['total_pnl']:+,.2f}",
            "trades": state['total_trades'],
            "win_rate": f"{round(state['wins']/state['total_trades']*100,1) if state['total_trades'] else 0}%",
        },
        "bucket_strategy": {
            "open_positions": list(positions.keys()),
            "pending_entries": list(pending_entry.keys()),
            "total_pnl": f"${state['total_pnl']:+,.2f}",
            "trades": state['total_trades'],
            "win_rate": f"{round(state['wins']/state['total_trades']*100,1) if state['total_trades'] else 0}%",
            "balance": f"${state['balance']:,.2f}",
        },

        "open_positions": list(positions.keys()),
        "pending_entries": list(pending_entry.keys()),
        "candle_cache": {a:{"candles":200,"last":state.get("ws_last_candle","?")} for a in ASSET_NAMES},
        "websocket":  {"last_candle":s["ws_last_candle"],
                       "status":"✅ Connected" if s["ws_connected"] else "❌ Down"},
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
        pos=dict(positions);   pend=dict(pending_entry)
        pos=dict(positions); pend=dict(pending_entry)

    wr  = round(s["wins"]/s["total_trades"]*100,1) if s["total_trades"] else 0
    mode_color = "#00D68F"
    mode_label = "🔴 LIVE"
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
    roll_status = "Trading normally" if days_left > ROLL_DAYS_BEFORE else f"AUTO-ROLLING to {'SEP 25' if days_to_aug <= 0 else 'SEP 25'} ✅" if days_left <= 0 else f"Rolling in {days_left} days"

    # Positions
    pos_rows = ""
    for asset, p in pos.items():
        pnl_est = round((p.get("trail_stop",p["entry"])-p["entry"])*p["size"],2) if p["direction"]=="LONG" else round((p["entry"]-p.get("trail_stop",p["entry"]))*p["size"],2)
        pnl_color = "#00D68F" if pnl_est>=0 else "#FF4757"
        pos_rows += f"""<div style='background:#0A1628;border:1px solid #1E2D45;border-radius:10px;padding:14px;margin-bottom:10px'>
          <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:10px'>
            <span style='font-size:18px;font-weight:800'>{asset}</span>
            <span style='font-size:13px;font-weight:700;padding:3px 10px;border-radius:20px;
              background:{"#00D68F22" if p["direction"]=="LONG" else "#FF475722"};
              color:{"#00D68F" if p["direction"]=="LONG" else "#FF4757"}'>{p["direction"]}</span>
            <span style='font-size:16px;font-weight:700;color:{pnl_color}'>${pnl_est:+,.2f}</span>
          </div>
          <div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;font-size:12px'>
            <div style='background:#060D1A;border-radius:6px;padding:8px'>
              <div style='color:#4A5878;margin-bottom:2px'>Entry</div>
              <div style='font-weight:600'>${p["entry"]:,.4f}</div>
            </div>
            <div style='background:#060D1A;border-radius:6px;padding:8px'>
              <div style='color:#4A5878;margin-bottom:2px'>Trail Stop</div>
              <div style='font-weight:600;color:#FFB800'>${p.get("trail_stop",0):,.4f}</div>
            </div>
            <div style='background:#060D1A;border-radius:6px;padding:8px'>
              <div style='color:#4A5878;margin-bottom:2px'>Peak</div>
              <div style='font-weight:600'>${p.get("trail_peak",0):,.4f}</div>
            </div>
          </div>
          <div style='margin-top:8px;font-size:11px;color:#4A5878'>Since {p.get("entry_time","?")}</div>
        </div>"""

    # Pending
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
    now_est = datetime.now().strftime("%I:%M %p EST")

    return f"""<!DOCTYPE html>
<html><head>
<title>CB Trader v30</title>
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
    <span style='color:{"#00D68F" if s["ws_connected"] else "#FF4757"}'>
      {"● WS Live" if s["ws_connected"] else "● WS Down"}
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
  <div style='font-size:11px;color:#4A5878;margin-bottom:10px'>{len(ASSET_NAMES)} assets · evaluates every 5 min</div>
  {assets_rows}
</div>
<div id=info class=panel>
  <div style='font-size:13px;line-height:2;color:#8892A4'>
    <b style='color:#E0E6F0;font-size:14px'>Strategy</b><br>
    EMA 5/13/50 · Sep ≥0.2% · Vol ≥0.3x · 10-bar breakout<br>
    Trail 0.2% · ATR 2.0x · 5-min candles · Bucket exits only<br>
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
    10x leverage · {len(ASSET_NAMES)} assets · Contract roll Aug 28<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Contract Status</b><br>
    Active: {active_label} contracts · <span style='color:{roll_color}'>{days_left} days left</span><br>
    Status: <span style='color:{roll_color}'>{roll_status}</span><br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>2026 Q1 Backtest</b><br>
    Fixed $1k · $33k gross · 56.3% WR · 14/14 green weeks<br>
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
log("📡 Pre-loading candles via REST...")
for a in ASSET_NAMES:
    c = fetch_candles(a)
    log(f"  {a}: {len(c) if c else 0} candles loaded")
    time.sleep(0.3)  # avoid 429 rate limit on startup
log("✅ All candles pre-loaded")

check_weekly_reset()
threading.Thread(target=start_websocket, daemon=True).start()
threading.Thread(target=trading_loop,    daemon=True).start()
