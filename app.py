"""
CB TRADER v45
═══════════════════════════════════════════════════════════════════
Strategy: RSI(14) Momentum Cross
  LONG:  RSI(14) crosses ABOVE 60 → enter long
  EXIT:  RSI(14) drops BELOW 45 → exit (both long and short)
  SHORT: RSI(14) crosses BELOW 40 → enter short
Timeframe: 15-minute candles (optimal from 174-combination backtest)
Exchange: Coinbase CFM Perpetual Futures (CFTC regulated, legal NYC)
  BTC(BIP) ETH(ETP) SOL(SLP) LINK(LNP) HBAR(HEP) SUI(SUP) XLM(XLP) BNB — DEC 2030
Fees:     0.10% per side — CONFIRMED from 4 live fills Aug 19 2026
          No fixed minimum — percentage only
Assets:   8 perp contracts — all liquid, verified via API
Backtest: $4,100-6,163/month on 9 assets at 0.10% real fee
          ALL 3 periods green: Feb-Mar, Jan-Mar, Jun25-Mar26
          174 filter combinations tested — NO filter improves base strategy
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

# Perp-style futures — DEC 2030 expiry, no monthly rolls needed
# Contract sizes confirmed from Coinbase API: future_product_details.contract_size
# Intraday margin rate ~10% confirmed from API
ASSETS = {
    "BTC":  {"perp":"BIP-20DEC30-CDE", "contract":0.01,   "margin_rate":0.10},
    "ETH":  {"perp":"ETP-20DEC30-CDE", "contract":0.10,   "margin_rate":0.10},
    "SOL":  {"perp":"SLP-20DEC30-CDE", "contract":5.0,    "margin_rate":0.10},
    "LINK": {"perp":"LNP-20DEC30-CDE", "contract":50.0,   "margin_rate":0.10},
    "HBAR": {"perp":"HEP-20DEC30-CDE", "contract":5000.0, "margin_rate":0.10},
    "SUI":  {"perp":"SUP-20DEC30-CDE", "contract":500.0,  "margin_rate":0.10},
    "XLM":  {"perp":"XLP-20DEC30-CDE", "contract":5000.0, "margin_rate":0.10},
    "BNB":  {"perp":"BNB-20DEC30-CDE", "contract":1.0,    "margin_rate":0.10},
}
ASSET_NAMES = list(ASSETS.keys())

# Fee: 0.10% per side — CONFIRMED from 4 live fills Aug 19 2026
FEE_PCT = 0.00100   # 0.10% per side confirmed real fee
MAX_CONTRACTS = 5   # Coinbase intraday position limit per asset

# RSI Momentum parameters — optimal from 174-combination backtest
RSI_PERIOD  = 14    # RSI period
RSI_ENTRY   = 60    # cross above → LONG, cross below (100-60=40) → SHORT
RSI_EXIT    = 45    # RSI drops below → exit LONG | rises above 55 → exit SHORT

def get_active_ticker(asset):
    """Returns perp ticker — no rolling needed, DEC 2030 expiry"""
    return ASSETS[asset]["perp"]

CANDLE_TF   = "FIFTEEN_MINUTE"  # 15min optimal from backtest
CANDLE_LIMIT= 150               # 150 candles = 37.5 hours lookback
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
    "balance": TOTAL_USDC, "buying_power": TOTAL_USDC, "weekly_pnl": 0.0, "total_pnl": 0.0,
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

def fetch_candles(asset, granularity=None, n_candles=None):
    """Fetch candles from perp ticker directly.
    granularity: ONE_HOUR for OpenRange, FIVE_MINUTE for AsianRange
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
        else:
            start = end - limit * 300
        resp  = client.get_candles(product_id, start=str(start),
                                   end=str(end), granularity=tf)
        if not resp.candles:
            log(f"WARNING {asset}: API returned 0 candles")
            return None
        candles = sorted([{
            "ts": int(c.start)*1000,
            "dt": datetime.fromtimestamp(int(c.start),tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "o":float(c.open),"h":float(c.high),
            "l":float(c.low),"c":float(c.close),"v":float(c.volume),
        } for c in resp.candles], key=lambda x:x["ts"])[-limit:]
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
# SIGNALS — OpenRange 1hour + AsianRange 5min
# Identical logic to backtest (proof_test_v2.py)
# ══════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════
# RSI CALCULATION
# ══════════════════════════════════════════════════════════════════
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
            "fail": f"no cross (RSI prev={prev_rsi:.1f} cur={cur_rsi:.1f})"
        }

    return d, None, None, {
        "strategy": "RSI-Mom",
        "rsi_prev": round(prev_rsi, 2),
        "rsi_cur":  round(cur_rsi, 2),
        "entry_candle": candles[i]["dt"],
    }


def should_exit(pos, candles):
    """
    RSI exit: LONG exits when RSI < 45, SHORT exits when RSI > 55
    Returns: True if should exit, False otherwise
    """
    if not candles or len(candles) < RSI_PERIOD + 2:
        return False
    closes = [float(c["c"]) for c in candles]
    rsi = calc_rsi(closes, RSI_PERIOD)
    cur_rsi = rsi[-2] if rsi[-2] is not None else rsi[-1]
    if cur_rsi is None:
        return False
    if pos["direction"] == "LONG"  and cur_rsi < RSI_EXIT:
        return True
    if pos["direction"] == "SHORT" and cur_rsi > (100 - RSI_EXIT):
        return True
    return False


def evaluate_openrange(candles_1h):
    """
    Opening Range Breakout on 1-hour candles.
    OR = first 3 candles of the UTC day (00:00, 01:00, 02:00).
    Signal: when current candle breaks above OR high → LONG
            when current candle breaks below OR low  → SHORT
    TP = entry ± OR_range × 2  |  Stop = entry ∓ OR_range
    Returns: (direction, tp, stop, info_dict) or (None, None, None, info)
    """
    if not candles_1h or len(candles_1h) < 5:
        return None, None, None, {"fail": "not enough 1h candles"}

    cur = candles_1h[-1]  # current (possibly forming) candle — use close as entry
    cur_dt = datetime.fromtimestamp(cur["ts"]/1000, tz=timezone.utc)

    # Find today's first 3 hourly candles (hours 0, 1, 2 UTC)
    day_start_ts = cur_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
    day_candles = [c for c in candles_1h if c["ts"] >= day_start_ts and c["ts"] < cur["ts"]]

    if len(day_candles) < 3:
        return None, None, None, {"fail": f"only {len(day_candles)} day candles so far"}

    first3 = day_candles[:3]
    or_high = max(c["h"] for c in first3)
    or_low  = min(c["l"] for c in first3)
    or_range = or_high - or_low

    if or_range <= 0:
        return None, None, None, {"fail": "OR range is zero"}

    entry = cur["c"]  # use close as signal price

    if entry > or_high:
        d = "LONG"
    elif entry < or_low:
        d = "SHORT"
    else:
        return None, None, None, {"fail": f"price {entry:.4f} inside OR [{or_low:.4f}-{or_high:.4f}]"}

    tp   = round_price(entry + or_range * 2) if d == "LONG" else round_price(entry - or_range * 2)
    stop = round_price(entry - or_range)     if d == "LONG" else round_price(entry + or_range)

    # Sanity checks
    if d == "LONG"  and (tp <= entry or stop >= entry): return None, None, None, {"fail": "LONG TP/stop invalid"}
    if d == "SHORT" and (tp >= entry or stop <= entry): return None, None, None, {"fail": "SHORT TP/stop invalid"}

    return d, tp, stop, {
        "strategy": "OpenRange",
        "or_high": round_price(or_high), "or_low": round_price(or_low),
        "or_range": round_price(or_range), "entry": round_price(entry),
        "tp": tp, "stop": stop, "rr": "2:1"
    }


def evaluate_asianrange(candles_5m):
    """
    Asian Range Breakout on 5-min candles.
    Asian session = 00:00-08:00 UTC.
    Signal fires only 08:00-12:00 UTC (London open window).
    Break of Asian session high → LONG, low → SHORT.
    TP = entry ± asian_range × 2  |  Stop = entry ∓ asian_range
    Returns: (direction, tp, stop, info_dict) or (None, None, None, info)
    """
    if not candles_5m or len(candles_5m) < 10:
        return None, None, None, {"fail": "not enough 5m candles"}

    cur = candles_5m[-1]
    cur_dt = datetime.fromtimestamp(cur["ts"]/1000, tz=timezone.utc)
    hour = cur_dt.hour

    # Only fire during London open window: 08:00-12:00 UTC
    if not (8 <= hour <= 12):
        return None, None, None, {"fail": f"outside London window (hour={hour} UTC)"}

    # Build Asian session candles: 00:00-08:00 UTC today
    day_start_ts   = cur_dt.replace(hour=0,  minute=0, second=0, microsecond=0).timestamp() * 1000
    london_open_ts = cur_dt.replace(hour=8,  minute=0, second=0, microsecond=0).timestamp() * 1000
    asian = [c for c in candles_5m
             if c["ts"] >= day_start_ts and c["ts"] < london_open_ts]

    if len(asian) < 3:
        return None, None, None, {"fail": f"only {len(asian)} Asian session candles"}

    as_high  = max(c["h"] for c in asian)
    as_low   = min(c["l"] for c in asian)
    as_range = as_high - as_low

    if as_range <= 0:
        return None, None, None, {"fail": "Asian range is zero"}

    entry = cur["c"]

    if entry > as_high:
        d = "LONG"
    elif entry < as_low:
        d = "SHORT"
    else:
        return None, None, None, {"fail": f"price inside Asian range [{as_low:.4f}-{as_high:.4f}]"}

    tp   = round_price(entry + as_range * 2) if d == "LONG" else round_price(entry - as_range * 2)
    stop = round_price(entry - as_range)     if d == "LONG" else round_price(entry + as_range)

    if d == "LONG"  and (tp <= entry or stop >= entry): return None, None, None, {"fail": "LONG TP/stop invalid"}
    if d == "SHORT" and (tp >= entry or stop <= entry): return None, None, None, {"fail": "SHORT TP/stop invalid"}

    return d, tp, stop, {
        "strategy": "AsianRange",
        "as_high": round_price(as_high), "as_low": round_price(as_low),
        "as_range": round_price(as_range), "entry": round_price(entry),
        "tp": tp, "stop": stop, "rr": "2:1", "hour_utc": hour
    }

# ══════════════════════════════════════════════════════════════════
# EXIT CHECK — fixed TP and Stop (matches backtest exactly)
# RSI exit handled by should_exit() function above

# ══════════════════════════════════════════════════════════════════
# ENTER / EXIT
# ══════════════════════════════════════════════════════════════════
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
    contracts  = min(MAX_CONTRACTS, max(1, int(per_slot / margin_per)))
    # Cap by buying power
    bp_contracts = max(1, int(buying_power / 100 / max(1, len(positions) + 1)))
    contracts = min(contracts, bp_contracts, MAX_CONTRACTS)

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
        "direction": direction, "entry": entry_price,
        "contracts": actual_cts, "size": actual_size,
        "strategy": "RSI-Mom", "entry_time": ts(),
        "rsi_entry": rsi_info.get("rsi_cur", 0),
    }
    add_audit(asset, f"📊 ENTER {direction}",
              f"RSI-Mom | entry=${entry_price:,.4f} | "
              f"rsi={rsi_info.get('rsi_cur',0):.1f} | "
              f"contracts={actual_cts} | size={actual_size} | LIVE",
              candle=candle)
    ntfy(f"ENTER {direction} {asset}",
         f"RSI-Mom | entry=${entry_price:,.4f} | RSI={rsi_info.get('rsi_cur',0):.1f} | {actual_cts}ct",
         priority="default")

def exit_position(asset, exit_price, exit_reason, candle):
    """
    Exit a position. exit_reason: "RSI_EXIT" or "MANUAL"
    """
    pos = positions.get(asset)
    if not pos: return
    pnl = round(
        (exit_price - pos["entry"]) * pos["size"] if pos["direction"] == "LONG"
        else (pos["entry"] - exit_price) * pos["size"], 4)
    side = "SELL" if pos["direction"] == "LONG" else "BUY"
    place_market_order(asset, side, pos["contracts"])
    record_tax(asset, pos["direction"], pos["entry"], exit_price,
               pos["size"], pnl, pos["entry_time"])
    with lock:
        state["total_pnl"]    = round(state["total_pnl"] + pnl, 4)
        state["weekly_pnl"]   = round(state["weekly_pnl"] + pnl, 4)
        state["balance"]      = round(TOTAL_USDC + state["total_pnl"], 4)
        state["total_trades"] += 1
        if pnl >= 0: state["wins"] += 1
    del positions[asset]
    emoji = "✅" if pnl >= 0 else "❌"
    add_audit(asset, f"{emoji} EXIT {exit_reason}",
              f"{pos['direction']} ${pos['entry']:,.4f} → ${exit_price:,.4f} | "
              f"P&L=${pnl:+,.4f} | strategy={pos.get('strategy','')}",
              candle=candle)
    ntfy(f"{emoji} EXIT {asset}",
         f"{pos['direction']} | entry=${pos['entry']:,.4f} → ${exit_price:,.4f} | P&L=${pnl:+,.2f} | {exit_reason}",
         priority="default" if pnl >= 0 else "high")
    # Refresh real balance from Coinbase after exit
    try:
        live_bal = get_live_balance()
        if live_bal > 0:
            with lock:
                state["balance"] = live_bal
                log(f"💰 Balance after exit: ${live_bal:,.2f} (Coinbase)")
    except Exception as e:
        log(f"Balance refresh error after exit: {e}")

# ══════════════════════════════════════════════════════════════════
# TRADING LOOP — Dual strategy: OpenRange 1hr + AsianRange 5min
# OpenRange:  runs every 5-min bucket, uses 1-hour candles
# AsianRange: runs every 5-min bucket 08:00-12:00 UTC, uses 5-min candles
# Exit: fixed TP and Stop checked every 5-min bucket
# ══════════════════════════════════════════════════════════════════
def trading_loop():
    global TOTAL_USDC
    live_bal = get_live_balance()
    if live_bal > 0:
        TOTAL_USDC = live_bal
    sync_open_positions()
    with lock:
        state["balance"] = TOTAL_USDC
    log("🚀 CB Trader v45 started")
    log("   Mode: 🔴 LIVE")
    log("   Strategy: RSI(14) Momentum Cross 60/45 | 15min candles")
    log(f"   Assets: {', '.join(ASSET_NAMES)}")
    log(f"   Fee: 0.10% confirmed | Backtest: $4,100-6,163/month")
    log(f"   Capital: ${TOTAL_USDC:,.2f} | Max contracts: {MAX_CONTRACTS}/asset")

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
                            if should_exit(pos, candles):
                                exit_price = float(cur["c"])
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

                        # ── PENDING ENTRY ──────────────────────────────
                        pend = pending_entry.get(asset)
                        if pend:
                            entry_price = float(cur["o"])
                            info = pend.get("info", {})
                            del pending_entry[asset]
                            enter_position(asset, pend["direction"], entry_price, cur, info)
                            if positions.get(asset):
                                save_sim_data(asset, current_bucket*1000, candles, info,
                                              f"ENTER_{pend['direction']}", position=dict(positions[asset]))
                            continue

                        # ── RSI MOMENTUM SIGNAL ────────────────────────
                        d, _, _, info = evaluate_signal(candles)
                        if d:
                            pending_entry[asset] = {"direction": d, "info": info,
                                                    "signal_ts": cur["ts"]}
                            add_audit(asset, f"🚨 RSI-Mom {d}",
                                      f"RSI prev={info.get('rsi_prev',0):.1f} → cur={info.get('rsi_cur',0):.1f}",
                                      candle=cur, indicators=info)
                            save_sim_data(asset, current_bucket*1000, candles, info,
                                          f"SIGNAL_{d}")
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

                # Refresh balance every 50 cycles
                if cycle_num % 50 == 0:
                    try:
                        live_bal = get_live_balance()
                        if live_bal > 0:
                            with lock: state["balance"] = live_bal
                    except: pass

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
    roll_status = "No roll needed — DEC 2030 expiry"

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
<title>CB Trader v45</title>
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
    RSI(14) Momentum Cross 60/45 · 15min candles<br>
    8 perp contracts · DEC 2030 perp-style · No rolls<br>
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
    Perp-style futures · DEC 2030 expiry · No monthly rolls<br>
    Fees: max(notional×0.02%, $0.15) per side<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>2026 Q1 Backtest</b><br>
    $566 account · $4,100-6,163/month · All 3 periods green<br>
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
