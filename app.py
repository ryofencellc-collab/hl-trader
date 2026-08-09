"""
HL TRADER v1
Strategy: EMA 5/13/34 + sep>=0.002 + vol>=0.3 + 8-bar breakout + 0.3% trail + ATR buffer
Exchange: HyperLiquid via CCXT
Candles:  HyperLiquid 5-min REST + WebSocket
"""

import threading, time, os, math, json, csv
from datetime import datetime, timezone
from flask import Flask, request, redirect, Response, session
import requests as req

# ── CONFIG ────────────────────────────────────────────────────────
PAPER_MODE   = True
PASSWORD     = os.environ.get("DASHBOARD_PASSWORD", "hl2026")
NTFY_TOPIC   = os.environ.get("NTFY_TOPIC", "hl-trader-lunchm0ney")
NTFY_URL     = f"https://ntfy.sh/{NTFY_TOPIC}"
HL_PRIVATE_KEY = os.environ.get("HL_PRIVATE_KEY",
    "0xdde0184ae92390a2b14c69d1e6b6f4b49d9f2d6bd2e800388aaa5381fb9a3b1f")
HL_MAIN_WALLET = os.environ.get("HL_MAIN_WALLET",
    "0xa90566c8d886CA63c1194101a7dA2Fa129D26B58")

# ── ASSETS ────────────────────────────────────────────────────────
ASSETS = {
    "BTC":  {"symbol": "BTC/USDC:USDC",  "contract": 0.001},
    "ETH":  {"symbol": "ETH/USDC:USDC",  "contract": 0.01},
    "SOL":  {"symbol": "SOL/USDC:USDC",  "contract": 0.1},
    "BNB":  {"symbol": "BNB/USDC:USDC",  "contract": 0.01},
    "DOGE": {"symbol": "DOGE/USDC:USDC", "contract": 10.0},
    "AVAX": {"symbol": "AVAX/USDC:USDC", "contract": 0.1},
    "XRP":  {"symbol": "XRP/USDC:USDC",  "contract": 1.0},
    "LINK": {"symbol": "LINK/USDC:USDC", "contract": 0.1},
    "LTC":  {"symbol": "LTC/USDC:USDC",  "contract": 0.01},
    "ADA":  {"symbol": "ADA/USDC:USDC",  "contract": 1.0},
    "UNI":  {"symbol": "UNI/USDC:USDC",  "contract": 0.1},
    "ATOM": {"symbol": "ATOM/USDC:USDC", "contract": 0.1},
    "DOT":  {"symbol": "DOT/USDC:USDC",  "contract": 0.1},
}
ASSET_NAMES = list(ASSETS.keys())

# ── STRATEGY PARAMS ───────────────────────────────────────────────
EMA_FAST    = 5
EMA_MID     = 13
EMA_SLOW    = 34
SEP_FILTER  = 0.002
VOL_FILTER  = 0.3
BRK_BARS    = 8
TRAIL_PCT   = 0.003
ATR_BUFFER  = 1.0
CANDLE_TF   = "5m"
CANDLE_LIMIT= 200
LEVERAGE    = 10
TOTAL_USDC  = float(os.environ.get("TOTAL_USDC", "99"))

DIAG_FILE = "/tmp/hl_diagnostic.json"
TAX_FILE  = "/tmp/hl_tax.csv"

# ── STATE ─────────────────────────────────────────────────────────
candle_cache      = {}
candle_cache_lock = threading.Lock()
positions         = {}
entry_times       = {}
last_candle       = {}
pending_entry     = {}
_processing       = set()
lock              = threading.Lock()

state = {
    "balance": TOTAL_USDC, "weekly_pnl": 0.0, "total_pnl": 0.0,
    "week": None, "cycle": 0, "paper_mode": PAPER_MODE,
    "ws_connected": False, "ws_last_candle": "never",
    "ntfy_last_sent": "never", "ntfy_errors": 0,
    "loop_last_run": "never", "loop_errors": 0,
    "trades": 0, "wins": 0,
}

# ── HELPERS ───────────────────────────────────────────────────────
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"[{ts()}] {msg}", flush=True)

NOISE = ["NO SIGNAL", "SAME CANDLE", "HOLDING", "WAITING"]

def add_audit(asset, event, detail, candle=None, indicators=None):
    entry = {"time": ts(), "asset": asset, "event": event, "detail": detail}
    if candle:     entry["candle"]     = candle
    if indicators: entry["indicators"] = indicators
    try:
        data = json.load(open(DIAG_FILE)) if os.path.exists(DIAG_FILE) else []
        data.insert(0, entry)
        if len(data) > 5000: data = data[:5000]
        json.dump(data, open(DIAG_FILE, "w"))
    except:
        pass
    if not any(n in event for n in NOISE):
        log(f"[{asset}] {event} -- {detail[:80]}")

def get_week():
    now = datetime.now(timezone.utc)
    return f"{now.year}-W{now.isocalendar()[1]:02d}"

def check_weekly_reset():
    wk = get_week()
    with lock:
        if state["week"] != wk:
            if state["week"]: log(f"📅 New week: {wk} -- P&L reset")
            state["week"]       = wk
            state["weekly_pnl"] = 0.0

def record_tax(asset, direction, entry_p, exit_p, size, pnl, entry_time):
    try:
        gross = pnl
        tax   = round(gross * 0.35, 4) if gross > 0 else 0.0
        net   = round(gross - tax, 4)
        row   = {"exit_time": ts(), "entry_time": entry_time, "asset": asset,
                 "direction": direction, "entry_price": f"{entry_p:.6f}",
                 "exit_price": f"{exit_p:.6f}", "size": f"{size}",
                 "gross_pnl": f"{gross:.4f}", "tax_35pct": f"{tax:.4f}",
                 "net_pnl": f"{net:.4f}"}
        write_header = not os.path.exists(TAX_FILE)
        with open(TAX_FILE, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if write_header: w.writeheader()
            w.writerow(row)
    except Exception as e:
        log(f"Tax record error: {e}")

def ntfy(title, body, tags="", priority="default"):
    try:
        r = req.post(NTFY_URL,
            data=body.encode("utf-8"),
            headers={"Title": title.encode("ascii","ignore").decode(),
                     "Priority": priority, "Tags": tags,
                     "Content-Type": "text/plain; charset=utf-8"},
            timeout=10)
        if r.status_code == 200:
            with lock: state["ntfy_last_sent"] = ts()
        else:
            with lock: state["ntfy_errors"] += 1
    except:
        with lock: state["ntfy_errors"] += 1

# ── HYPERLIQUID API ───────────────────────────────────────────────
def get_dex():
    import ccxt
    return ccxt.hyperliquid({
        "walletAddress": HL_MAIN_WALLET,
        "privateKey":    HL_PRIVATE_KEY,
        "timeout":       30000,
    })

def get_balance():
    try:
        dex = get_dex()
        bal = dex.fetch_balance()
        return float(bal["USDC"]["total"] or 0)
    except Exception as e:
        log(f"Balance fetch error: {e}")
        return None

def place_hl_order(asset, direction, contracts):
    if PAPER_MODE:
        oid = f"PAPER-{asset}-{int(time.time())}"
        log(f"📄 PAPER: {asset} {direction} {contracts} contracts -> {oid}")
        return oid
    try:
        dex    = get_dex()
        symbol = ASSETS[asset]["symbol"]
        side   = "buy" if direction == "LONG" else "sell"
        cs     = ASSETS[asset]["contract"]
        amount = contracts * cs
        markets   = dex.load_markets()
        mid_price = float(markets[symbol]["info"]["midPx"])
        order = dex.create_order(symbol, "market", side, amount, price=mid_price)
        oid   = order.get("id", f"HL-{asset}-{int(time.time())}")
        log(f"✅ HL order: {asset} {direction} {amount} @ market | oid={oid}")
        return oid
    except Exception as e:
        add_audit(asset, "⚠️ ORDER ERROR", str(e))
        return None

def close_hl_order(asset, direction, contracts):
    if PAPER_MODE:
        return f"PAPER-CLOSE-{asset}-{int(time.time())}"
    try:
        dex    = get_dex()
        symbol = ASSETS[asset]["symbol"]
        side   = "sell" if direction == "LONG" else "buy"
        cs     = ASSETS[asset]["contract"]
        amount = contracts * cs
        markets   = dex.load_markets()
        mid_price = float(markets[symbol]["info"]["midPx"])
        order = dex.create_order(symbol, "market", side, amount,
                                  price=mid_price,
                                  params={"reduceOnly": True})
        log(f"✅ HL close: {asset} {direction} closed")
        return order.get("id", "closed")
    except Exception as e:
        add_audit(asset, "⚠️ CLOSE ERROR", str(e))
        return None

def sync_positions():
    """Two-way sync: if HL doesn't have it, remove from app state"""
    if PAPER_MODE: return
    try:
        dex = get_dex()
        hl_pos = dex.fetch_positions()
        hl_open = {p["symbol"] for p in hl_pos if float(p.get("contracts", 0)) != 0}
        for asset in list(positions.keys()):
            if ASSETS[asset]["symbol"] not in hl_open:
                log(f"⚠️ {asset} in app but not on HL — removing from state")
                del positions[asset]
    except Exception as e:
        log(f"Sync error: {e}")

# ── CANDLES ───────────────────────────────────────────────────────
def fetch_candles_rest(asset):
    try:
        end_ms   = int(time.time() * 1000)
        start_ms = end_ms - CANDLE_LIMIT * 5 * 60 * 1000
        r = req.post("https://api.hyperliquid.xyz/info",
            json={"type": "candleSnapshot", "req": {
                "coin": asset, "interval": CANDLE_TF,
                "startTime": start_ms, "endTime": end_ms,
            }}, timeout=15)
        if r.status_code != 200:
            log(f"  {asset} candle fetch failed: HTTP {r.status_code} {r.text[:100]}")
            return None
        raw = r.json()
        if not raw:
            log(f"  {asset} candle fetch: empty response")
            return None
        candles = sorted([{
            "ts": int(c["t"]),
            "dt": datetime.fromtimestamp(int(c["t"])/1000,
                  tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "o": float(c["o"]), "h": float(c["h"]),
            "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"]),
        } for c in raw], key=lambda x: x["ts"])[-CANDLE_LIMIT:]
        with candle_cache_lock:
            candle_cache[asset] = candles
        return candles
    except Exception as e:
        add_audit(asset, "⚠️ CANDLE ERROR", str(e))
        return None

def fetch_candles(asset):
    with candle_cache_lock:
        cached = candle_cache.get(asset, [])
    if cached: return cached
    return fetch_candles_rest(asset)

# ── WEBSOCKET ─────────────────────────────────────────────────────
def start_websocket():
    import websocket as ws_lib
    import json as _j

    def on_message(ws, msg):
        try:
            data  = _j.loads(msg)
            if data.get("channel") != "candle": return
            c     = data.get("data", {})
            asset = c.get("s", "")
            if asset not in ASSET_NAMES: return
            candle = {
                "ts": int(c["t"]),
                "dt": datetime.fromtimestamp(int(c["t"])/1000,
                      tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"]),
            }
            with lock: state["ws_last_candle"] = candle["dt"]
            with candle_cache_lock:
                cache    = candle_cache.get(asset, [])
                existing = next((i for i,x in enumerate(cache)
                                 if x["ts"] == candle["ts"]), None)
                is_new   = existing is None
            if is_new:
                rest = fetch_candles_rest(asset)
                if not rest:
                    with candle_cache_lock:
                        cache = candle_cache.get(asset, [])
                        cache.append(candle)
                        cache.sort(key=lambda x: x["ts"])
                        if len(cache) > CANDLE_LIMIT: cache = cache[-CANDLE_LIMIT:]
                        candle_cache[asset] = cache
                threading.Thread(target=_ws_trigger_eval,
                                 args=(asset,), daemon=True).start()
        except Exception as e:
            log(f"WS message error: {e}")

    def on_error(ws, error):
        with lock: state["ws_connected"] = False
        log(f"WS error: {error}")

    def on_close(ws, *args):
        with lock: state["ws_connected"] = False
        log("WS closed -- reconnecting in 5s")
        time.sleep(5)
        start_websocket()

    def on_open(ws):
        with lock: state["ws_connected"] = True
        log(f"🔌 HL WebSocket connected -- subscribing {len(ASSET_NAMES)} assets")
        for asset in ASSET_NAMES:
            ws.send(_j.dumps({"method": "subscribe", "subscription": {
                "type": "candle", "coin": asset, "interval": CANDLE_TF
            }}))

    try:
        ws_lib.WebSocketApp(
            "wss://api.hyperliquid.xyz/ws",
            on_message=on_message, on_error=on_error,
            on_close=on_close, on_open=on_open,
        ).run_forever(ping_interval=30, ping_timeout=10)
    except Exception as e:
        log(f"WS failed: {e}")

# ── MATH ──────────────────────────────────────────────────────────
def round_price(p, sig=5):
    if p == 0: return 0.0
    mag = math.floor(math.log10(abs(p)))
    return round(p, max(0, sig-1-mag))

def ema(values, period):
    k = 2/(period+1); e = None; out = []
    for v in values:
        e = v if e is None else v*k + e*(1-k)
        out.append(e)
    return out

def sma(values, period):
    out = [None]*(period-1)
    for i in range(period-1, len(values)):
        out.append(sum(values[i-period+1:i+1])/period)
    return out

def atr_calc(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))
    if len(trs) < period: return [None]*len(closes)
    out = [None]*period; avg = sum(trs[:period])/period; out.append(avg)
    for i in range(period, len(trs)):
        avg = (avg*(period-1)+trs[i])/period; out.append(avg)
    while len(out) < len(closes): out.append(out[-1])
    return out

# ── SIGNAL ────────────────────────────────────────────────────────
def evaluate_signal(candles):
    if len(candles) < CANDLE_LIMIT:
        return None, None, {"fail": f"only {len(candles)} candles"}
    cl=[c["c"] for c in candles]; hi=[c["h"] for c in candles]
    lo=[c["l"] for c in candles]; vo=[c["v"] for c in candles]
    ef=ema(cl,EMA_FAST); em_=ema(cl,EMA_MID); es=ema(cl,EMA_SLOW)
    vs=sma(vo,20); i=-2
    sig_candle = candles[i]
    indic = {}
    if not (ef[i] and em_[i] and es[i]):
        return None, sig_candle, {"fail": "EMA not ready"}
    if   ef[i]>em_[i]>es[i]: d="LONG"
    elif ef[i]<em_[i]<es[i]: d="SHORT"
    else: return None, sig_candle, {"fail": "EMA not stacked"}
    sep=abs(ef[i]-es[i])/es[i] if es[i] else 0
    indic["sep"]=round(sep,5)
    if sep<SEP_FILTER:
        return None, sig_candle, {**indic,"fail":f"sep={sep:.5f}"}
    vr=vo[i]/vs[i] if vs[i] else 0
    indic["vol"]=round(vr,2)
    if vr<VOL_FILTER:
        return None, sig_candle, {**indic,"fail":f"vol={vr:.2f}x"}
    if d=="LONG":
        brk=max(hi[i-BRK_BARS:i]); indic["brk"]=brk
        if cl[i]<=brk:
            return None, sig_candle, {**indic,"fail":f"no brk"}
    else:
        brk=min(lo[i-BRK_BARS:i]); indic["brk"]=brk
        if cl[i]>=brk:
            return None, sig_candle, {**indic,"fail":f"no brk"}
    indic["pass"]=True
    return d, sig_candle, indic

# ── TRAIL ─────────────────────────────────────────────────────────
def check_trail(asset, pos, candle, atr_val):
    """Exact same logic as backtest — includes ATR buffer"""
    h=float(candle["h"]); l=float(candle["l"]); updated=False
    if pos["direction"]=="LONG":
        if h>pos["trail_peak"]:
            move=h-pos["trail_peak"]
            if atr_val==0 or move>atr_val*ATR_BUFFER:
                pos["trail_peak"]=h
                pos["trail_stop"]=round_price(h*(1-TRAIL_PCT))
                updated=True
        thresh=pos["trail_stop"]-(atr_val*ATR_BUFFER if atr_val else 0)
        if l<=thresh: return "EXIT", updated
    else:
        if l<pos["trail_peak"]:
            move=pos["trail_peak"]-l
            if atr_val==0 or move>atr_val*ATR_BUFFER:
                pos["trail_peak"]=l
                pos["trail_stop"]=round_price(l*(1+TRAIL_PCT))
                updated=True
        thresh=pos["trail_stop"]+(atr_val*ATR_BUFFER if atr_val else 0)
        if h>=thresh: return "EXIT", updated
    return "HOLD", updated

# ── EXIT ──────────────────────────────────────────────────────────
def exit_position(asset, exit_price, reason, candle):
    pos=positions.get(asset)
    if not pos: return
    pnl=round(
        (exit_price-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
        else (pos["entry"]-exit_price)*pos["size"], 4)
    if not PAPER_MODE:
        oid=close_hl_order(asset, pos["direction"], pos["contracts"])
        if not oid: log(f"⚠️ Close order failed: {asset} -- check manually")
    record_tax(asset, pos["direction"], pos["entry"], exit_price,
               pos["size"], pnl, entry_times.get(asset, ts()))
    with lock:
        state["total_pnl"]  = round(state["total_pnl"]+pnl, 4)
        state["weekly_pnl"] = round(state["weekly_pnl"]+pnl, 4)
        state["balance"]    = round(TOTAL_USDC+state["total_pnl"], 4)
        state["trades"]    += 1
        if pnl>=0: state["wins"]+=1
        del positions[asset]
        if asset in entry_times: del entry_times[asset]
    emoji = "✅" if pnl>=0 else "❌"
    add_audit(asset, f"{emoji} EXIT TRAIL",
              f"{pos['direction']} ${pos['entry']:,.4f} → ${exit_price:,.4f} | "
              f"P&L=${pnl:+,.4f}", candle=candle)
    ntfy(f"{asset} {pos['direction']} EXIT",
         f"${pos['entry']:,.4f} → ${exit_price:,.4f}\nP&L: ${pnl:+,.4f}\n"
         f"{'PAPER' if PAPER_MODE else 'LIVE'}")

# ── PROCESS ASSET ─────────────────────────────────────────────────
def _ws_trigger_eval(asset):
    if asset in _processing: return
    _processing.add(asset)
    try:
        _process_asset(asset)
    except Exception as e:
        log(f"Eval error {asset}: {e}")
    finally:
        _processing.discard(asset)

def _process_asset(asset):
    candles=fetch_candles(asset)
    if not candles or len(candles)<CANDLE_LIMIT: return
    cur_candle=candles[-1]; cur_ts=cur_candle["ts"]
    at=atr_calc([c["h"] for c in candles],[c["l"] for c in candles],
                [c["c"] for c in candles])
    av=at[-1] or 0

    # Check trail on open position
    pos=positions.get(asset)
    if pos:
        result,updated=check_trail(asset, pos, cur_candle, av)
        if updated:
            add_audit(asset, "🔄 TRAIL UPDATED",
                      f"trail_peak=${pos['trail_peak']:,.4f} | "
                      f"trail_stop=${pos['trail_stop']:,.4f}",
                      candle=cur_candle)
        if result=="EXIT":
            last_candle[asset]=cur_ts
            exit_position(asset, pos["trail_stop"], "trail", cur_candle)
        return

    # Check pending entry
    pend=pending_entry.get(asset)
    if pend:
        if cur_ts>pend["signal_ts"]:
            entry_price=float(cur_candle["o"])
            direction=pend["direction"]
            cs=ASSETS[asset]["contract"]
            cap=TOTAL_USDC/len(ASSET_NAMES)
            contracts=max(1,int((cap*LEVERAGE)/(entry_price*cs)))
            size=contracts*cs
            trail_stop=round_price(
                entry_price*(1-TRAIL_PCT) if direction=="LONG"
                else entry_price*(1+TRAIL_PCT))
            oid=place_hl_order(asset, direction, contracts)
            if oid is not None:
                positions[asset]={
                    "direction":direction,"entry":entry_price,
                    "size":size,"contracts":contracts,
                    "trail_peak":entry_price,"trail_stop":trail_stop,
                    "entry_time":ts()
                }
                entry_times[asset]=ts()
                add_audit(asset, f"📊 ENTER {direction}",
                          f"entry=${entry_price:,.4f} | contracts={contracts} | "
                          f"size={size} | trail_stop=${trail_stop:,.4f}",
                          candle=cur_candle)
                ntfy(f"{asset} {direction} ENTER",
                     f"Entry: ${entry_price:,.4f}\nSize: {size}\n"
                     f"{'PAPER' if PAPER_MODE else 'LIVE'}")
            del pending_entry[asset]
        return

    # Evaluate signal
    if positions.get(asset): return
    sig_ts=candles[-2]["ts"]
    if last_candle.get(asset)==sig_ts: return
    last_candle[asset]=sig_ts
    direction,sig_candle,indic=evaluate_signal(candles)
    if direction:
        pending_entry[asset]={
            "direction":direction,
            "signal_ts":sig_ts,
        }
        add_audit(asset, f"🚨 SIGNAL {direction}",
                  f"Waiting for next candle | signal_candle={sig_candle['dt']} | "
                  f"sep={indic.get('sep')} | vol={indic.get('vol')}x",
                  candle=sig_candle, indicators=indic)

# ── TRADING LOOP ──────────────────────────────────────────────────
def trading_loop():
    log(f"🚀 HL Trader v1 started")
    log(f"   Mode: {'📄 PAPER' if PAPER_MODE else '🔴 LIVE'}")
    log(f"   Assets: {', '.join(ASSET_NAMES)}")
    log(f"   Trail: {TRAIL_PCT*100}% | ATR buffer: {ATR_BUFFER}x")
    log(f"   Capital: ${TOTAL_USDC:,.2f} | Leverage: {LEVERAGE}x")
    if not PAPER_MODE:
        bal=get_balance()
        if bal:
            with lock: state["balance"]=round(bal,2)
            log(f"   Balance: ${state['balance']:,.2f}")
    cycle=0
    while True:
        try:
            cycle+=1
            with lock:
                state["cycle"]=cycle
                state["loop_last_run"]=ts()
            check_weekly_reset()
            if cycle%10==0: sync_positions()
            for asset in ASSET_NAMES:
                if asset not in _processing:
                    threading.Thread(target=_ws_trigger_eval,
                                     args=(asset,), daemon=True).start()
                    time.sleep(0.1)
        except Exception as e:
            with lock: state["loop_errors"]+=1
            log(f"Loop error: {e}")
        time.sleep(60)

# ── FLASK ─────────────────────────────────────────────────────────
app=Flask(__name__)
app.secret_key=os.urandom(24)

def auth(): return session.get("authed")==True

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method=="POST":
        if request.form.get("password")==PASSWORD:
            session["authed"]=True; return redirect("/")
        return "<p>Wrong password</p><a href='/login'>Try again</a>"
    return """<!DOCTYPE html><html><head><title>HL Trader</title>
    <style>body{background:#060D1A;color:#E0E6F0;font-family:sans-serif;
    display:flex;align-items:center;justify-content:center;height:100vh}
    input{background:#0D1421;border:1px solid #1A2236;color:#E0E6F0;
    padding:10px;border-radius:6px;margin:8px 0;width:200px}
    button{background:#00D68F;color:#000;border:none;padding:10px 20px;
    border-radius:6px;cursor:pointer;font-weight:700}</style></head>
    <body><form method=post style='text-align:center'>
    <div style='font-size:24px;font-weight:700;margin-bottom:16px'>HL Trader v1</div>
    <input type=password name=password placeholder='Password'><br>
    <button type=submit>Login</button></form></body></html>"""

@app.route("/logout")
def logout():
    session.clear(); return redirect("/login")

@app.route("/health")
def health():
    with candle_cache_lock:
        cc={a:{"candles":len(v),"last":v[-1]["dt"] if v else "none"}
            for a,v in candle_cache.items()}
    try:
        diag_data=json.load(open(DIAG_FILE)) if os.path.exists(DIAG_FILE) else []
        diag_info={"entries":len(diag_data),
                   "last_entry":diag_data[0]["time"] if diag_data else "never",
                   "size_kb":round(os.path.getsize(DIAG_FILE)/1024,1) if os.path.exists(DIAG_FILE) else 0,
                   "status":"✅ OK"}
    except:
        diag_info={"entries":0,"last_entry":"never","status":"❌ MISSING"}
    with lock:
        s=dict(state); n_open=len(positions)
    issues=[]
    if not s["ws_connected"]: issues.append("WebSocket disconnected")
    if s["ntfy_errors"]>10:   issues.append(f"{s['ntfy_errors']} ntfy errors")
    if s["loop_errors"]>5:    issues.append(f"{s['loop_errors']} loop errors")
    return Response(json.dumps({
        "overall":       "✅ ALL SYSTEMS OK" if not issues else f"❌ {len(issues)} issues",
        "mode":          {"paper_mode":s["paper_mode"],"status":"📄 PAPER" if s["paper_mode"] else "🔴 LIVE"},
        "candle_cache":  {"assets":cc,"status":"✅ OK"},
        "positions":     {"assets":list(positions.keys()),"open":n_open,"status":"✅ OK"},
        "websocket":     {"last_candle":s["ws_last_candle"],
                          "status":"✅ Connected" if s["ws_connected"] else "❌ Disconnected"},
        "trading_loop":  {"errors":s["loop_errors"],"last_run":s["loop_last_run"],
                          "status":"✅ OK" if s["loop_errors"]<5 else "⚠️ errors"},
        "diagnostic":    diag_info,
        "ntfy":          {"errors":s["ntfy_errors"],"last_sent":s["ntfy_last_sent"],
                          "status":"✅ OK" if s["ntfy_errors"]<5 else f"❌ {s['ntfy_errors']} errors"},
    }, indent=2), mimetype="application/json")

@app.route("/diagnostic-raw")
def diagnostic_raw():
    try:
        return Response(open(DIAG_FILE).read(), mimetype="application/json")
    except:
        return Response("[]", mimetype="application/json")

@app.route("/tax-export")
def tax_export():
    if not auth(): return redirect("/login")
    try:
        return Response(open(TAX_FILE).read(), mimetype="text/csv",
            headers={"Content-Disposition":"attachment;filename=hl_trades.csv"})
    except:
        return Response("No trades yet", mimetype="text/plain")

@app.route("/")
def dashboard():
    if not auth(): return redirect("/login")
    with lock:
        s=dict(state); pos=dict(positions)
    wr=round(s["wins"]/s["trades"]*100,1) if s["trades"] else 0
    bal=s["balance"]
    mode_color="#FFB800" if PAPER_MODE else "#00D68F"
    mode_label="📄 PAPER" if PAPER_MODE else "🔴 LIVE"
    wk_color="#00D68F" if s["weekly_pnl"]>=0 else "#FF4757"

    pos_html=""
    for asset,p in pos.items():
        pos_html+=f"""<div class="card">
          <b>{asset} {p["direction"]}</b><br>
          Entry: ${p["entry"]:,.4f}<br>
          Trail: ${p.get("trail_stop",0):,.4f} | Peak: ${p.get("trail_peak",0):,.4f}<br>
          Since: {p.get("entry_time","?")}
        </div>"""
    if not pos_html:
        pos_html="<div style='color:#4A5878;padding:16px'>No open positions</div>"

    try:
        audit_data=json.load(open(DIAG_FILE)) if os.path.exists(DIAG_FILE) else []
    except:
        audit_data=[]

    journal_html=""
    exits_html=""
    for a in [x for x in audit_data if not any(n in x.get("event","") for n in NOISE)][:50]:
        evt=a.get("event","")
        color=("#00D68F" if "ENTER" in evt else
               "#FF4757" if "EXIT" in evt
               else "#FFB800" if "SIGNAL" in evt else "#4A5878")
        journal_html+=(f"<div style='border-left:3px solid {color};"
                        f"padding:8px 12px;margin-bottom:6px;background:#0D1421'>"
                        f"<div style='font-size:10px;color:#4A5878'>{a['time']} | {a.get('asset','')}</div>"
                        f"<div style='font-weight:600;color:{color}'>{evt}</div>"
                        f"<div style='font-size:11px;color:#8892A4'>{a.get('detail','')[:100]}</div>"
                        f"</div>")

    assets_html=""
    for a_name in ASSET_NAMES:
        with candle_cache_lock:
            cache=candle_cache.get(a_name,[])
        price=cache[-1]["c"] if cache else 0
        last_dt=cache[-1]["dt"] if cache else "?"
        st="OPEN" if a_name in pos else "READY"
        sc="#00D68F" if a_name in pos else "#4A5878"
        assets_html+=(f"<div style='display:flex;justify-content:space-between;"
                       f"padding:6px 0;border-bottom:1px solid #1A2236;font-size:12px'>"
                       f"<b>{a_name}</b>"
                       f"<span style='color:#8892A4'>${price:,.4f}</span>"
                       f"<span style='color:#6B7A99'>{last_dt}</span>"
                       f"<span style='color:{sc}'>{st}</span></div>")

    return f"""<!DOCTYPE html>
<html><head>
  <title>HL Trader v1</title>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#060D1A;color:#E0E6F0;font-family:-apple-system,sans-serif;padding:16px;max-width:600px;margin:0 auto}}
    .card{{background:#0D1421;border:1px solid #1A2236;border-radius:8px;padding:14px;margin-bottom:8px}}
    .grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-bottom:12px}}
    @media(min-width:480px){{.grid{{grid-template-columns:repeat(4,1fr)}}}}
    .metric{{background:#0D1421;border:1px solid #1A2236;border-radius:8px;padding:10px;text-align:center}}
    .mv{{font-size:20px;font-weight:700;margin-top:4px}}
    .ml{{font-size:10px;color:#4A5878;text-transform:uppercase;letter-spacing:.8px}}
    .tabs{{display:flex;overflow-x:auto;gap:4px;margin-bottom:0}}
    .tab{{flex-shrink:0;padding:8px 14px;cursor:pointer;border-radius:6px 6px 0 0;
          font-size:12px;font-weight:600;background:#060D1A;color:#4A5878;border:1px solid #1A2236}}
    .tab.active{{background:#0D1421;color:#E0E6F0}}
    .sec{{display:none;background:#0D1421;border:1px solid #1A2236;
          border-radius:0 8px 8px 8px;padding:12px}}
    .sec.active{{display:block}}
  </style>
  <script>
    function show(id,el){{
      document.querySelectorAll('.sec').forEach(s=>s.classList.remove('active'));
      document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
      document.getElementById(id).classList.add('active');
      el.classList.add('active');
    }}
  </script>
</head><body>
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
    <div>
      <div style="font-size:20px;font-weight:700">HL Trader v1</div>
      <div style="font-size:12px;color:{mode_color};font-weight:700">{mode_label}</div>
    </div>
    <div style="text-align:right;font-size:11px;color:#4A5878">
      Cycle #{s["cycle"]}<br>
      <a href="/logout" style="color:#4A5878;text-decoration:none">logout</a>
    </div>
  </div>
  <div class="grid">
    <div class="metric"><div class="ml">Balance</div><div class="mv">${bal:,.2f}</div></div>
    <div class="metric"><div class="ml">Weekly P&L</div>
      <div class="mv" style="color:{wk_color}">${s["weekly_pnl"]:+,.2f}</div></div>
    <div class="metric"><div class="ml">Open</div><div class="mv">{len(pos)}</div></div>
    <div class="metric"><div class="ml">Trades</div>
      <div class="mv">{s["trades"]} <span style="font-size:12px;color:#4A5878">({wr}%)</span></div></div>
  </div>
  <div class="tabs">
    <span class="tab active" onclick="show('positions',this)">Positions</span>
    <span class="tab" onclick="show('journal',this)">Journal</span>
    <span class="tab" onclick="show('assets',this)">Assets</span>
    <span class="tab" onclick="show('diag',this)">Diag</span>
  </div>
  <div id="positions" class="sec active">{pos_html}</div>
  <div id="journal"   class="sec">{journal_html if journal_html else "<div style='color:#4A5878;padding:16px'>No events yet</div>"}</div>
  <div id="assets"    class="sec">{assets_html}</div>
  <div id="diag"      class="sec">
    <a href="/diagnostic-raw" style="color:#8892A4;font-size:12px">📥 Download Diagnostic JSON</a>
    &nbsp;|&nbsp;
    <a href="/tax-export" style="color:#8892A4;font-size:12px">📊 Export Tax CSV</a>
    &nbsp;|&nbsp;
    <a href="/health" style="color:#8892A4;font-size:12px">🔍 Health Check</a>
  </div>
</body></html>"""

# ── STARTUP ───────────────────────────────────────────────────────
log("📡 Pre-loading candles via HL REST (parallel)...")
def _preload(a):
    c=fetch_candles_rest(a)
    log(f"  {a}: {len(c) if c else 0} candles loaded")
threads=[threading.Thread(target=_preload,args=(a,),daemon=True) for a in ASSET_NAMES]
for t in threads: t.start()
for t in threads: t.join()
log("✅ All candles pre-loaded")

check_weekly_reset()
log(f"🚀 HL Trader v1 started")
log(f"   Mode: {'📄 PAPER' if PAPER_MODE else '🔴 LIVE'}")
log(f"   Assets: {', '.join(ASSET_NAMES)}")
log(f"   Trail: {TRAIL_PCT*100}% | ATR buffer: {ATR_BUFFER}x")
log(f"   Capital: ${TOTAL_USDC:,.2f} | Leverage: {LEVERAGE}x")

threading.Thread(target=start_websocket, daemon=True).start()
threading.Thread(target=trading_loop,    daemon=True).start()

port=int(os.environ.get("PORT",8080))
app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
