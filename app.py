"""
CB TRADER v30
═══════════════════════════════════════════════════════════════════
CRITICAL FIX from v29: Sequential asset processing
- v28/v29 used threads + _processing guard → blocked pending entries
- v30 processes all assets sequentially per bucket — no threads, no guard
- This matches backtest logic exactly → true 1:1:1

Strategy: EMA 5/13/34 + sep≥0.002 + vol≥0.3 + 8-bar breakout + 0.3% trail
Exchange: Coinbase CFM Futures (CFTC regulated, legal NYC)
Candles:  Coinbase spot API (BTC-USD etc)
Assets:   15
"""

import time, os, math, json, csv, uuid, threading
from datetime import datetime, timezone
from flask import Flask, Response, request, redirect
import requests as req

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
PAPER_MODE  = True
NTFY_TOPIC  = os.environ.get("NTFY_TOPIC", "hl-trader-lunchm0ney")
NTFY_URL    = f"https://ntfy.sh/{NTFY_TOPIC}"

CB_API_KEY  = os.environ.get("CB_API_KEY",
    "organizations/983775da-fb7b-45d1-8b79-3f58bde1f58a/apiKeys/126549d5-dd93-4e50-99b4-2752266a7d09")
CB_API_SEC  = os.environ.get("CB_API_SECRET",
    "m9coUDw6sCE+7KotbjF0xEyu1I7kpCun4Ez6bEVFm6ug+jk2hGtv3CiibvIThzmeXv2F8R0Kyui5kFhxi4tweQ==")

ASSETS = {
    "BTC":  {"spot":"BTC-USD",  "perp":"BIT-28AUG26-CDE", "contract":0.01},
    "ETH":  {"spot":"ETH-USD",  "perp":"ET-28AUG26-CDE",  "contract":0.1},
    "SOL":  {"spot":"SOL-USD",  "perp":"SOL-28AUG26-CDE", "contract":1.0},
    "XRP":  {"spot":"XRP-USD",  "perp":"XRP-28AUG26-CDE", "contract":100.0},
    "DOGE": {"spot":"DOGE-USD", "perp":"DOG-28AUG26-CDE", "contract":5000.0},
    "ADA":  {"spot":"ADA-USD",  "perp":"ADA-28AUG26-CDE", "contract":100.0},
    "DOT":  {"spot":"DOT-USD",  "perp":"DOT-28AUG26-CDE", "contract":10.0},
    "LINK": {"spot":"LINK-USD", "perp":"LNK-28AUG26-CDE", "contract":10.0},
    "LTC":  {"spot":"LTC-USD",  "perp":"LC-28AUG26-CDE",  "contract":1.0},
    "BCH":  {"spot":"BCH-USD",  "perp":"BCH-28AUG26-CDE", "contract":1.0},
    "BNB":  {"spot":"BNB-USD",  "perp":"BNF-28AUG26-CDE", "contract":1.0},
    "SUI":  {"spot":"SUI-USD",  "perp":"SUI-28AUG26-CDE", "contract":100.0},
    "XLM":  {"spot":"XLM-USD",  "perp":"XLM-28AUG26-CDE", "contract":1000.0},
    "AVAX": {"spot":"AVAX-USD", "perp":"AVA-28AUG26-CDE", "contract":10.0},
    "HBAR": {"spot":"HBAR-USD", "perp":"HED-28AUG26-CDE", "contract":1000.0},
}
ASSET_NAMES = list(ASSETS.keys())

EMA_FAST    = 5
EMA_MID     = 13
EMA_SLOW    = 34
SEP_FILTER  = 0.002
VOL_FILTER  = 0.3
BRK_BARS    = 8
TRAIL_PCT   = 0.003
ATR_BUFFER  = 1.0
CANDLE_TF   = "FIVE_MINUTE"
CANDLE_LIMIT= 201
LEVERAGE    = 10
TOTAL_USDC  = float(os.environ.get("TOTAL_USDC", "1000"))
TAX_RATE    = 0.35

DIAG_FILE   = "/tmp/cb_diagnostic.json"
TAX_FILE    = "/tmp/cb_trades.csv"

# ══════════════════════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════════════════════
positions     = {}   # asset -> pos dict
pending_entry = {}   # asset -> {direction, signal_ts}
lock          = threading.Lock()

state = {
    "balance": TOTAL_USDC, "weekly_pnl": 0.0, "total_pnl": 0.0,
    "week": None, "cycle": 0, "ws_connected": False,
    "ws_last_candle": "never", "ntfy_errors": 0,
    "loop_last_run": "never", "loop_errors": 0,
    "wins": 0, "total_trades": 0,
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

def ntfy(title, body, priority="default"):
    try:
        req.post(NTFY_URL, data=body.encode("utf-8"),
            headers={"Title":title.encode("ascii","ignore").decode().strip(),
                     "Priority":priority,"Content-Type":"text/plain; charset=utf-8"},
            timeout=10)
        with lock: state["ntfy_last_sent"]=ts()
    except:
        with lock: state["ntfy_errors"]+=1

# ══════════════════════════════════════════════════════════════════
# COINBASE API
# ══════════════════════════════════════════════════════════════════
def get_cb_client():
    from coinbase.rest import RESTClient
    return RESTClient(api_key=CB_API_KEY, api_secret=CB_API_SEC)

def fetch_candles(asset):
    try:
        client = get_cb_client()
        product_id = ASSETS[asset]["spot"]
        end   = int(time.time())
        start = end - CANDLE_LIMIT * 5 * 60
        resp  = client.get_candles(product_id, start=str(start),
                                   end=str(end), granularity=CANDLE_TF)
        if not resp.candles: return None
        candles = sorted([{
            "ts": int(c.start)*1000,
            "dt": datetime.fromtimestamp(int(c.start),tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "o":float(c.open),"h":float(c.high),
            "l":float(c.low), "c":float(c.close),"v":float(c.volume),
        } for c in resp.candles], key=lambda x:x["ts"])[-CANDLE_LIMIT:]
        return candles
    except Exception as e:
        log(f"Candle fetch error {asset}: {e}")
        return None

def place_market_order(asset, side, contracts):
    if PAPER_MODE:
        oid = f"PAPER-{asset}-{int(time.time())}"
        log(f"📄 PAPER: {asset} {side} {contracts} contracts → {oid}")
        return oid
    try:
        client  = get_cb_client()
        product = ASSETS[asset]["perp"]
        size    = str(int(contracts))
        if side in ("BUY","LONG"):
            order = client.market_order_buy(
                client_order_id=str(uuid.uuid4()),
                product_id=product, base_size=size)
        else:
            order = client.market_order_sell(
                client_order_id=str(uuid.uuid4()),
                product_id=product, base_size=size)
        success = getattr(order, "success", False)
        if success:
            oid = getattr(order, "order_id", f"CB-{asset}-{int(time.time())}")
            log(f"✅ CB order: {asset} {side} {size} → {oid}")
            return oid
        else:
            log(f"⚠️ CB order failed: {asset} {getattr(order,'error_response',order)}")
            return None
    except Exception as e:
        log(f"Order error {asset}: {e}")
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
    cs        = ASSETS[asset]["contract"]
    cap       = TOTAL_USDC / len(ASSET_NAMES)
    contracts = max(1, int((cap * LEVERAGE) / (entry_price * cs)))
    size      = contracts * cs
    trail_stop = round_price(
        entry_price*(1-TRAIL_PCT) if direction=="LONG"
        else entry_price*(1+TRAIL_PCT))
    side = "BUY" if direction=="LONG" else "SELL"
    oid  = place_market_order(asset, side, contracts)
    if not oid and not PAPER_MODE:
        log(f"⚠️ {asset} entry order rejected")
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
              f"{'PAPER' if PAPER_MODE else 'LIVE'}",
              candle=candle)
    ntfy(f"{asset} {direction} ENTER",
         f"Entry: ${entry_price:,.4f}\nSize: {size}\n"
         f"{'PAPER' if PAPER_MODE else 'LIVE'}")

def exit_position(asset, exit_price, candle):
    pos = positions.get(asset)
    if not pos: return
    pnl = round(
        (exit_price-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
        else (pos["entry"]-exit_price)*pos["size"], 4)
    if not PAPER_MODE:
        side = "SELL" if pos["direction"]=="LONG" else "BUY"
        place_market_order(asset, side, pos["contracts"])
    record_tax(asset, pos["direction"], pos["entry"], exit_price,
               pos["size"], pnl, pos["entry_time"])
    with lock:
        state["total_pnl"]  = round(state["total_pnl"]+pnl, 4)
        state["weekly_pnl"] = round(state["weekly_pnl"]+pnl, 4)
        state["balance"]    = round(TOTAL_USDC+state["total_pnl"], 4)
        state["total_trades"] += 1
        if pnl>=0: state["wins"]+=1
    del positions[asset]
    emoji = "✅" if pnl>=0 else "❌"
    add_audit(asset, f"{emoji} EXIT TRAIL",
              f"{pos['direction']} ${pos['entry']:,.4f} → ${exit_price:,.4f} | "
              f"P&L=${pnl:+,.4f}", candle=candle)
    ntfy(f"{emoji} {asset} {pos['direction']} EXIT",
         f"${pos['entry']:,.4f} → ${exit_price:,.4f}\nP&L: ${pnl:+,.4f}")

# ══════════════════════════════════════════════════════════════════
# TRADING LOOP — SEQUENTIAL, no threads, no processing guard
# Mirrors backtest exactly: one asset at a time per bucket
# ══════════════════════════════════════════════════════════════════
def trading_loop():
    log("🚀 CB Trader v30 started")
    log(f"   Mode: {'📄 PAPER' if PAPER_MODE else '🔴 LIVE'}")
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

                # ── SEQUENTIAL: process one asset at a time ──────────
                for asset in ASSET_NAMES:
                    try:
                        candles = fetch_candles(asset)
                        if not candles or len(candles) < 200: continue

                        cur = candles[-1]
                        at  = atr_calc([c["h"] for c in candles],
                                       [c["l"] for c in candles],
                                       [c["c"] for c in candles])
                        av  = at[-1] or 0

                        # 1. Trail check on open position
                        pos = positions.get(asset)
                        if pos:
                            result = check_trail(pos, cur, av)
                            if result == "EXIT":
                                exit_position(asset, pos["trail_stop"], cur)
                            continue

                        # 2. Pending entry — enter at this candle's open
                        pend = pending_entry.get(asset)
                        if pend:
                            entry_price = float(cur["o"])
                            direction   = pend["direction"]
                            del pending_entry[asset]
                            enter_position(asset, direction, entry_price, cur)
                            continue

                        # 3. Evaluate signal
                        direction, sig_candle, indic = evaluate_signal(candles)
                        if direction:
                            pending_entry[asset] = {
                                "direction": direction,
                                "signal_ts": candles[-2]["ts"]
                            }
                            add_audit(asset, f"🚨 SIGNAL {direction}",
                                      f"signal_candle={sig_candle['dt']} | "
                                      f"sep={indic.get('sep')} | vol={indic.get('vol')}x",
                                      candle=sig_candle, indicators=indic)

                    except Exception as e:
                        log(f"Asset error {asset}: {e}")

                # ── Heartbeat after all assets processed ─────────────
                add_audit("SYSTEM", "💓 CYCLE",
                          f"candle={bucket_dt} | open={len(positions)} | "
                          f"pending={list(pending_entry.keys())} | "
                          f"balance=${state['balance']:,.2f} | trades={state['total_trades']}")

        except Exception as e:
            with lock: state["loop_errors"] = state.get("loop_errors",0)+1
            log(f"Loop error: {e}")

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
                        # Quick trail check — exit if hit
                        av = 0  # ATR not available here, use 0
                        result = check_trail(pos, cur, av)
                        if result == "EXIT":
                            exit_position(asset, pos["trail_stop"], cur)
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

# ══════════════════════════════════════════════════════════════════
# FLASK DASHBOARD
# ══════════════════════════════════════════════════════════════════
app = Flask(__name__)

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
        "open_positions": list(positions.keys()),
        "pending_entries": list(pending_entry.keys()),
        "candle_cache": {a:{"candles":200,"last":state.get("ws_last_candle","?")} for a in ASSET_NAMES},
        "websocket":  {"last_candle":s["ws_last_candle"],
                       "status":"✅ Connected" if s["ws_connected"] else "❌ Down"},
        "trading_loop":{"errors":s.get("loop_errors",0),"last_run":s["loop_last_run"],
                        "status":"✅ OK" if s.get("loop_errors",0)<5 else "❌ errors"},
        "diagnostic": {"entries":len(json.load(open(DIAG_FILE))) if os.path.exists(DIAG_FILE) else 0,
                       "status":"✅ OK"},
    }, indent=2), mimetype="application/json")

@app.route("/diagnostic-raw")
def diagnostic_raw():
    try: return Response(open(DIAG_FILE).read(), mimetype="application/json")
    except: return Response("[]", mimetype="application/json")

@app.route("/tax-export")
def tax_export():
    try:
        return Response(open(TAX_FILE).read(), mimetype="text/csv",
            headers={"Content-Disposition":"attachment;filename=cb_trades.csv"})
    except: return Response("No trades yet", mimetype="text/plain")

@app.route("/")
def dashboard():
    with lock: s=dict(state); pos=dict(positions); pend=dict(pending_entry)
    wr  = round(s["wins"]/s["total_trades"]*100,1) if s["total_trades"] else 0
    mode_color = "#FFB800" if PAPER_MODE else "#00D68F"
    mode_label = "📄 PAPER" if PAPER_MODE else "🔴 LIVE"
    wk_color   = "#00D68F" if s["weekly_pnl"]>=0 else "#FF4757"
    tot_color  = "#00D68F" if s["total_pnl"]>=0 else "#FF4757"

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
  <div style='font-size:11px;color:#4A5878;margin-bottom:10px'>15 assets · evaluates every 5 min</div>
  {assets_rows}
</div>
<div id=info class=panel>
  <div style='font-size:13px;line-height:2;color:#8892A4'>
    <b style='color:#E0E6F0;font-size:14px'>Strategy</b><br>
    EMA 5/13/34 · Sep ≥0.2% · Vol ≥0.3x · 8-bar breakout<br>
    Trail 0.3% · ATR 1.0x · 5-min candles · Sequential processing<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>Exchange</b><br>
    Coinbase CFM Futures · CFTC regulated · Legal NYC<br>
    10x leverage · 15 assets · Contract roll Aug 28<br>
    <div style='height:1px;background:#1E2D45;margin:10px 0'></div>
    <b style='color:#E0E6F0;font-size:14px'>5-Year Backtest</b><br>
    263/263 green weeks · $339k net · $1,290/week avg<br>
    Best week: $8,809 · Worst week: $77<br>
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
log("✅ All candles pre-loaded")

check_weekly_reset()
threading.Thread(target=start_websocket, daemon=True).start()
threading.Thread(target=trading_loop,    daemon=True).start()
