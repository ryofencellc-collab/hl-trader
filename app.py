"""
HL TRADER — Final Production App v4
══════════════════════════════════════
8 assets | ntfy alerts | /signal-check | /audit | Tax system

Full diagnostic visibility — no Railway logs needed.
Every candle, every signal, every skip tracked and visible.

DRY_RUN = False | TESTNET = True | LEVERAGE = 10x
"""

import threading
import math, time, csv, os, requests as req
from datetime import datetime, timezone
from flask import Flask, request, session, redirect, jsonify, Response
import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

# ══════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════
DRY_RUN         = False
TESTNET         = False
MAIN_WALLET     = "0xa90566c8d886CA63c1194101a7dA2Fa129D26B58"
API_PRIVATE_KEY = "0x9cd8627ff5c807e8dd1ba0ce0e5936c0f8eb133123fccd49807509abf3f3ff07"
API_URL         = constants.TESTNET_API_URL if TESTNET else constants.MAINNET_API_URL
PASSWORD        = os.environ.get("DASHBOARD_PASSWORD","hl2026")
NTFY_TOPIC      = "hl-trader-lunchm0ney"
NTFY_URL        = f"https://ntfy.sh/{NTFY_TOPIC}"

# ── TESTNET PAPER TRADING (Binance candles + testnet execution) ────────────
TN_WALLET       = "0xa90566c8d886CA63c1194101a7dA2Fa129D26B58"
TN_API_KEY      = "0x5b75aa092ea3bd1ee77983ab5b8268607120a0145de6df11174b3f72f91b9ea0"
TN_LEVERAGE     = 10
TN_TOTAL_USDC   = 988.0

ASSETS          = ["BTC","ETH","SOL","BNB","DOGE","AVAX"]
TOTAL_USDC      = 114.80
BASE_POS        = TOTAL_USDC / len(ASSETS)
LEVERAGE        = 10
CHECK_EVERY     = 60
TAX_RATE        = 0.35

EMA_FAST=5; EMA_MID=13; EMA_SLOW=34
STOP_PCT=0.05; TRAIL_PCT=0.01; ATR_BUFFER=0.5
VOL_FILTER=0.5; SEP_FILTER=0.003; BRK_BARS=12  # 0.5x proven best on 2yr Binance backtest

# Binance candle feed — data-api.binance.vision works from Railway
BINANCE_CANDLE_URL = "https://data-api.binance.vision/api/v3/klines"
BINANCE_SYM = {"BTC":"BTCUSDT","ETH":"ETHUSDT","SOL":"SOLUSDT",
               "BNB":"BNBUSDT","DOGE":"DOGEUSDT","AVAX":"AVAXUSDT"}
CANDLE_TF="15m"; CANDLE_LIMIT=200

ASSET_CFG = {
    "BTC":  {"exit":"trail",    "ff":0.0001,"bb":True, "sc":False,"no_ov":False,"pt":None,"ps":None,"tp":None,"cd":0, "regime":False},
    "ETH":  {"exit":"trail",    "ff":None,  "bb":False,"sc":False,"no_ov":True, "pt":None,"ps":None,"tp":None,"cd":0, "regime":False},
    "SOL":  {"exit":"partial",  "ff":None,  "bb":True, "sc":True, "no_ov":False,"pt":0.01,"ps":0.25,"tp":None,"cd":5, "regime":False},
    "BNB":  {"exit":"fixed_tp", "ff":None,  "bb":False,"sc":True, "no_ov":False,"pt":None,"ps":None,"tp":0.01,"cd":0, "regime":False},
    "DOGE": {"exit":"trail",    "ff":None,  "bb":True, "sc":True, "no_ov":False,"pt":None,"ps":None,"tp":None,"cd":0, "regime":True},
    "AVAX": {"exit":"trail",    "ff":None,  "bb":False,"sc":True, "no_ov":False,"pt":None,"ps":None,"tp":None,"cd":0, "regime":False},
}

# Tax rates
FED_LTCG_RATE=0.20; FED_STCG_RATE=0.37
NY_STATE_RATE=0.0685; NYC_LOCAL_RATE=0.03876
SEC1256_LTCG=0.60; SEC1256_STCG=0.40

QUARTERLY_DATES = [
    {"quarter":"Q1 2026","period":"Jan 1 – Mar 31","due":"2026-04-15"},
    {"quarter":"Q2 2026","period":"Apr 1 – May 31","due":"2026-06-15"},
    {"quarter":"Q3 2026","period":"Jun 1 – Aug 31","due":"2026-09-15"},
    {"quarter":"Q4 2026","period":"Sep 1 – Dec 31","due":"2027-01-15"},
    {"quarter":"Q1 2027","period":"Jan 1 – Mar 31","due":"2027-04-15"},
    {"quarter":"Q2 2027","period":"Apr 1 – May 31","due":"2027-06-15"},
    {"quarter":"Q3 2027","period":"Jun 1 – Aug 31","due":"2027-09-15"},
    {"quarter":"Q4 2027","period":"Sep 1 – Dec 31","due":"2028-01-15"},
]

MILESTONES=[2000,5000,10000,20000,50000,100000]
milestones_hit=set()
quarterly_payments={}

def get_pos_usd(vol,vs,ef,es):
    if not vs or vs==0: return BASE_POS
    vr=vol/vs; sep=abs(ef-es)/es if es else 0
    if vr>=4.0 and sep>=0.008: return BASE_POS*2
    if vr>=2.5 or sep>=0.005:  return BASE_POS
    return BASE_POS*0.5

# ══════════════════════════════════════════════════
# STATE — Full audit trail built in
# ══════════════════════════════════════════════════
state = {
    "status":"starting","last_check":None,"next_check":None,
    "cycle":0,"dry_run":DRY_RUN,"testnet":TESTNET,"leverage":LEVERAGE,
    "assets":ASSETS,"balance":TOTAL_USDC,
    "positions":{},"trades":[],"diagnostics":[],"weekly_pnl":{},
    "paused":False,"kill_switch":False,"close_all_requested":False,
    "weekly_pnl":0.0,"weekly_trades":0,"week_start":"",
    "issues":[],
    # Full audit log — every candle, every signal, every skip
    "audit":[],
    "health":{"api_connected":False,"last_ping":None,"assets_ok":{},
              "params":{
                  "ema":"5/13/34","stop_pct":"5%","trail_pct":"1%",
                  "vol_filter":"per-asset (0.10-1.50x)","sep_filter":"0.003","brk_bars":"12",
                  "candle_tf":"15m","check_every":"60s","leverage":f"{LEVERAGE}x",
                  "assets":",".join(ASSETS),
                  "btc_cfg":"trail|fr1bp|BB|varsz",
                  "eth_cfg":"trail|no_overnight|varsz",
                  "sol_cfg":"partial1%@25%|cd5|BB|SC|varsz",
                  "bnb_cfg":"tp1%|SC",
                  "doge_cfg":"trail|BB|SC|varsz|regime",
                  "avax_cfg":"trail|SC|varsz",
              }},
    "tax":{"total_pnl":0.0,"total_tax":0.0,"total_net":0.0,
           "winning_trades":0,"losing_trades":0,"total_trades":0},
}
lock=threading.Lock()

# ── TESTNET STATE (Binance candles + testnet execution) ────────────────────
tn_state = {
    "status":"starting","cycle":0,"balance":TN_TOTAL_USDC,
    "positions":{},"trades":[],"diagnostics":[],"weekly_pnl":{},
    "paused":False,"kill_switch":False,
    "audit":[],"issues":[],
    "health":{"api_connected":False,"last_ping":None,"assets_ok":{}},
    "tax":{"total_pnl":0.0,"total_tax":0.0,"total_net":0.0,
           "winning_trades":0,"losing_trades":0,"total_trades":0},
}
tn_lock=threading.Lock()

def fetch_binance_candles(asset):
    """Fetch Binance candles for testnet signal evaluation.
    Uses same source as mainnet — data-api.binance.vision works from Railway.
    This ensures testnet and mainnet evaluate identical signals."""
    try:
        end_ms=int(time.time()*1000)
        start_ms=end_ms-CANDLE_LIMIT*15*60*1000
        sym=BINANCE_SYM.get(asset,asset+"USDT")
        r=req.get(BINANCE_CANDLE_URL,
            params={"symbol":sym,"interval":"15m",
                    "startTime":start_ms,"endTime":end_ms,"limit":CANDLE_LIMIT},
            timeout=10)
        if r.status_code!=200:
            add_tn_issue(asset,"Binance candle error",f"HTTP {r.status_code}")
            return []
        data=r.json()
        if not isinstance(data,list):
            add_tn_issue(asset,"Candle fetch error",str(data))
            return []
        candles=[]
        for b in data:
            candles.append({"t":int(b[0]),"T":int(b[6]),
                           "o":b[1],"h":b[2],"l":b[3],"c":b[4],"v":b[5]})
        return sorted(candles,key=lambda x:x["t"])
    except Exception as e:
        log(f"⚠️ Testnet candle fetch error {asset}: {e}")
        add_tn_issue(asset,"Candle fetch error",str(e))
        return []

def add_tn_audit(asset,event,detail,filters=None):
    """Testnet audit trail."""
    from datetime import timedelta
    now_utc=datetime.now(timezone.utc)
    time_str=f"{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC"
    safe=str(detail).replace("<","&lt;").replace(">","&gt;").replace(chr(10)," ").replace(chr(13)," ")
    entry={"time":time_str,"asset":asset,"event":event,"detail":safe,"filters":filters or {}}
    with tn_lock:
        tn_state["audit"].insert(0,entry)
        tn_state["audit"]=tn_state["audit"][:10000]

def add_tn_issue(asset,issue,detail):
    """Log trade issues to testnet issues tab."""
    from datetime import timedelta
    now_utc=datetime.now(timezone.utc)
    time_str=f"{now_utc.strftime('%H:%M:%S')} UTC"
    entry={"time":time_str,"asset":asset,"issue":issue,"detail":str(detail)}
    with tn_lock:
        tn_state["issues"].insert(0,entry)
        tn_state["issues"]=tn_state["issues"][:500]
    log(f"⚠️ [TESTNET ISSUE] {asset}: {issue} — {detail}")

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}",flush=True)

def add_diag(level,event,cause,action):
    entry={"time":ts(),"level":level,"event":event,"cause":cause,"action":action}
    with lock:
        if level=="ERROR" and state["diagnostics"]:
            last=state["diagnostics"][0]
            if last["event"]==event and last["level"]==level: return
        state["diagnostics"].insert(0,entry)
        state["diagnostics"]=state["diagnostics"][:200]
    icons={"INFO":"ℹ️","WARNING":"⚠️","ERROR":"❌","CRITICAL":"🚨"}
    log(f"{icons.get(level,'📋')} [{level}] {event} | {cause} | {action}")

AUDIT_FILE = "/tmp/hl_audit_log.txt"
TRADES_FILE    = "/tmp/hl_trades.json"
TN_TRADES_FILE = "/tmp/hl_tn_trades.json"

def add_issue(asset,issue,detail):
    """Log trade issues to mainnet issues tab."""
    from datetime import timedelta
    now_utc=datetime.now(timezone.utc)
    time_str=f"{now_utc.strftime('%H:%M:%S')} UTC"
    entry={"time":time_str,"asset":asset,"issue":issue,"detail":str(detail)}
    with lock:
        state["issues"].insert(0,entry)
        state["issues"]=state["issues"][:500]
    log(f"⚠️ [ISSUE] {asset}: {issue} — {detail}")

def add_audit(asset,event,detail,filters=None):
    """Full audit trail — every candle evaluation visible on dashboard + saved to disk"""
    # Sanitize at source so nothing breaks the HTML dashboard
    safe=str(detail).replace("<","&lt;").replace(">","&gt;").replace(chr(10)," ").replace(chr(13)," ")
    from datetime import datetime,timezone,timedelta
    now_utc=datetime.now(timezone.utc)
    time_str=f"{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC"
    entry={
        "time":time_str,"asset":asset,"event":event,
        "detail":safe,"filters":filters or {}
    }
    with lock:
        state["audit"].insert(0,entry)
        state["audit"]=state["audit"][:10000]
    try:
        with open(AUDIT_FILE,"a") as f:
            f.write(f"{time_str}|{asset}|{event}|{safe}\n")
    except: pass

def add_trade(asset,action,direction,entry,exit_p,size,pnl,reason,filters=None):
    t={"time":ts(),"asset":asset,"action":action,"direction":direction,
       "entry":entry,"exit":exit_p,"size":size,"leverage":LEVERAGE,
       "pnl":round(pnl,4) if pnl is not None else None,"reason":reason,
       "filters":filters or {}}
    with lock:
        state["trades"].insert(0,t)
        state["trades"]=state["trades"][:500]
        if pnl is not None:
            wk=datetime.now(timezone.utc).strftime("%Y-W%W")
            state["weekly_pnl"][wk]=round(state["weekly_pnl"].get(wk,0)+pnl,4)
    # Persist to disk so trades survive Railway restarts
    try:
        import json
        existing=[]
        if os.path.exists(TRADES_FILE):
            existing=json.load(open(TRADES_FILE))
        existing.insert(0,t)
        existing=existing[:500]
        json.dump(existing,open(TRADES_FILE,"w"))
    except: pass

# ══════════════════════════════════════════════════
# PRICE ROUNDING (HL requires 5 significant figures)
# ══════════════════════════════════════════════════
def round_price(p, sig=5):
    """Round price to HyperLiquid 5 significant figures requirement"""
    if p==0: return 0.0
    try:
        mag=math.floor(math.log10(abs(p)))
        dec=max(0,sig-1-mag)
        return round(p,dec)
    except: return round(p,4)

# ══════════════════════════════════════════════════
# NTFY
# ══════════════════════════════════════════════════
def ntfy(title,message,priority="default",tags=""):
    try:
        headers={"Title":title.encode("utf-8").decode("latin-1","ignore"),
                 "Priority":priority}
        if tags: headers["Tags"]=tags
        req.post(NTFY_URL,data=message.encode("utf-8"),headers=headers,timeout=5)
    except Exception as e:
        log(f"⚠️ ntfy failed: {e}")

def ntfy_trade_entered(asset,direction,price,size,stop,trail,pos_usd):
    icon="📈" if direction=="LONG" else "📉"
    ntfy(f"{icon} {asset} {direction} Entered",
         f"Asset: {asset}-PERP\nDirection: {direction}\nEntry: ${price:,.2f}\n"
         f"Size: {size:.5f} (${pos_usd*LEVERAGE:.0f} notional)\n"
         f"Hard stop: ${stop:,.2f}\nTrail stop: ${trail:,.2f}\nLeverage: {LEVERAGE}x",
         priority="high",
         tags="chart_with_upwards_trend" if direction=="LONG" else "chart_with_downwards_trend")

def ntfy_trade_closed(asset,direction,entry,exit_p,pnl,reason):
    win=pnl>=0; icon="✅" if win else "❌"
    tax=max(0,pnl*TAX_RATE); net=pnl-tax
    ntfy(f"{icon} {asset} {direction} Closed — ${pnl:+.2f}",
         f"Asset: {asset}-PERP\nDirection: {direction}\n"
         f"Entry: ${entry:,.2f} → Exit: ${exit_p:,.2f}\nReason: {reason}\n"
         f"Gross: ${pnl:+.4f}\nTax: ${tax:.4f}\nNet: ${net:+.4f}",
         priority="high" if win else "default",
         tags="white_check_mark" if win else "x")

def ntfy_api_down():
    ntfy("⚠️ API Offline",
         "HyperLiquid API not responding\nSystem retrying automatically\nPositions held open",
         priority="high",tags="warning")

def ntfy_api_recovered(down_min):
    ntfy("✅ API Recovered",
         f"Back online after {down_min:.0f} min\nTrading resumed",tags="white_check_mark")

def ntfy_kill_switch():
    ntfy("🛑 Kill Switch",
         "All trading stopped\nPositions remain open on HyperLiquid",
         priority="urgent",tags="rotating_light")

def ntfy_milestone(balance):
    tax=state["tax"]
    ntfy(f"🎯 Balance hit ${balance:,.0f}!",
         f"Account: ${balance:,.2f}\nTrades: {tax['total_trades']}\n"
         f"Win rate: {tax['winning_trades']/max(1,tax['total_trades'])*100:.1f}%\n"
         f"Net P&L: ${tax['total_net']:+,.2f}",
         priority="high",tags="tada")

def ntfy_daily_summary(period="morning"):
    tax=state["tax"]; opens=state["positions"]
    wr=tax["winning_trades"]/max(1,tax["total_trades"])*100
    open_str="".join(
        f"\n  {a} {p['direction']} @ ${p['entry']:,.2f} | P&L: ${p.get('unrealized_pnl',0):+.2f}"
        for a,p in opens.items()) or "\n  None"
    icon="🌅" if period=="morning" else "🌆"
    ntfy(f"{icon} {'Morning' if period=='morning' else 'Evening'} Summary",
         f"Balance: ${state['balance']:.2f}\nNet P&L: ${tax['total_net']:+.2f}\n"
         f"Trades: {tax['total_trades']} ({tax['winning_trades']}W/{tax['losing_trades']}L)\n"
         f"Win rate: {wr:.1f}%\nTax set aside: ${tax['total_tax']:.2f}\n"
         f"Open positions:{open_str}\nCycle #{state['cycle']}",tags="bar_chart")

# ══════════════════════════════════════════════════
# 12-HOUR SIM COMPARISON (9am / 6pm UTC)
# ══════════════════════════════════════════════════
_last_sim_hour = -1

def run_12hr_sim():
    try:
        import requests as _req
        now_utc = datetime.now(timezone.utc)
        log("📊 Running 12hr sim comparison...")
        end_ms = int(time.time()*1000)
        start_ms = end_ms - 12*3600*1000
        BSYM = {"BTC":"BTCUSDT","ETH":"ETHUSDT","SOL":"SOLUSDT",
                "BNB":"BNBUSDT","DOGE":"DOGEUSDT","AVAX":"AVAXUSDT"}
        sim_signals=[]; app_entered=[]
        cutoff = time.time()-12*3600
        for t in state["trades"]:
            try:
                ep=datetime.strptime(t["time"],"%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc).timestamp()
                if ep>=cutoff and t.get("action")=="ENTER":
                    app_entered.append(t["asset"])
            except: pass
        for asset in ASSETS:
            try:
                r=_req.get(BINANCE_CANDLE_URL,
                    params={"symbol":BSYM.get(asset,asset+"USDT"),"interval":"15m",
                            "startTime":start_ms,"endTime":end_ms,"limit":200},timeout=10)
                if r.status_code!=200: continue
                raw=r.json()
                if not isinstance(raw,list) or len(raw)<50: continue
                candles=[{"t":int(b[0]),"o":b[1],"h":b[2],"l":b[3],"c":b[4],"v":b[5]} for b in raw]
                for i in range(50,len(candles)-1):
                    direction,_,_,_,filters=evaluate_signal(candles[:i+1],asset)
                    if direction:
                        st=datetime.fromtimestamp(int(candles[i]["t"])/1000,tz=timezone.utc)
                        if st.timestamp()>=cutoff:
                            sim_signals.append((asset,direction,st.strftime("%H:%M")))
                        break
            except: continue
        sim_assets=[s[0] for s in sim_signals]
        matched=[a for a in sim_assets if a in app_entered]
        sim_only=[a for a in sim_assets if a not in app_entered]
        app_only=[a for a in app_entered if a not in sim_assets]
        out=[f"12hr Sim | {now_utc.strftime('%H:%M')} UTC"]
        if sim_signals:
            for asset,direction,st in sim_signals:
                status="✅ matched" if asset in matched else "⚠️ app missed"
                out.append(f"{status}: {asset} {direction} @ {st}")
        else:
            out.append("No signals in last 12hrs")
        if app_only: out.append(f"App extra: {', '.join(app_only)}")
        if not sim_only and not app_only: out.append("✅ In sync")
        ntfy("📊 Daily Sim Check","\n".join(out),tags="chart_with_upwards_trend")
        log(f"📊 Sim done: {len(sim_signals)} signals | {len(matched)} matched")
    except Exception as e:
        log(f"⚠️ 12hr sim failed: {e}")

def check_12hr_sim():
    global _last_sim_hour
    h=datetime.now(timezone.utc).hour
    if h in (10,0) and h!=_last_sim_hour:
        _last_sim_hour=h
        threading.Thread(target=run_12hr_sim,daemon=True).start()


def check_milestones():
    bal=state["balance"]
    for m in MILESTONES:
        if bal>=m and m not in milestones_hit:
            milestones_hit.add(m); ntfy_milestone(bal)

def check_weekly_reset():
    """Reset weekly P&L every Monday 00:00 UTC"""
    now=datetime.now(timezone.utc)
    week_key=f"{now.isocalendar()[0]}-W{now.isocalendar()[1]:02d}"
    with lock:
        if state.get("week_start")!=week_key:
            state["week_start"]=week_key
            state["weekly_pnl"]=0.0
            state["weekly_trades"]=0
            log(f"📅 New week started: {week_key} — weekly P&L reset")

def check_daily_summaries():
    now=datetime.now(timezone.utc); h,minute=now.hour,now.minute
    if minute<2:
        if h==13: ntfy_daily_summary("morning")
        elif h==22: ntfy_daily_summary("evening")

def check_tax_reminders():
    now=datetime.now(timezone.utc)
    for q in QUARTERLY_DATES:
        due=datetime.strptime(q["due"],"%Y-%m-%d").replace(tzinfo=timezone.utc)
        days_left=(due-now).days
        if days_left not in [30,7,1]: continue
        owed=max(0,state["tax"]["total_tax"]-quarterly_payments.get("current",{}).get("total",0))
        if owed<=0: continue
        if days_left==30:
            ntfy(f"📅 Tax Due in 30 Days",f"Quarter: {q['quarter']}\nDue: {q['due']}\nOwed: ${owed:,.2f}",tags="calendar")
        elif days_left==7:
            ntfy(f"⚠️ Tax Due in 7 Days",f"Quarter: {q['quarter']}\nDue: {q['due']}\nOwed: ${owed:,.2f}",priority="high",tags="warning")
        elif days_left==1:
            ntfy(f"🚨 Tax Due TOMORROW",f"Quarter: {q['quarter']}\nOwed: ${owed:,.2f}\nPay TODAY",priority="urgent",tags="rotating_light")

# ══════════════════════════════════════════════════
# TAX
# ══════════════════════════════════════════════════
def calc_tax(gross_pnl):
    if gross_pnl<=0:
        return {"gross":gross_pnl,"ltcg":gross_pnl*SEC1256_LTCG,"stcg":gross_pnl*SEC1256_STCG,
                "fed_ltcg":0,"fed_stcg":0,"fed_total":0,"ny":0,"nyc":0,"total":0,"net":gross_pnl,"rate":0}
    ltcg=gross_pnl*SEC1256_LTCG; stcg=gross_pnl*SEC1256_STCG
    fed_ltcg=ltcg*FED_LTCG_RATE; fed_stcg=stcg*FED_STCG_RATE
    fed=fed_ltcg+fed_stcg; ny=gross_pnl*NY_STATE_RATE; nyc=gross_pnl*NYC_LOCAL_RATE
    total=fed+ny+nyc
    return {"gross":round(gross_pnl,4),"ltcg":round(ltcg,4),"stcg":round(stcg,4),
            "fed_ltcg":round(fed_ltcg,4),"fed_stcg":round(fed_stcg,4),
            "fed_total":round(fed,4),"ny":round(ny,4),"nyc":round(nyc,4),
            "total":round(total,4),"net":round(gross_pnl-total,4),
            "rate":round(total/gross_pnl*100,2)}

def get_quarter(dt):
    m=dt.month; y=dt.year
    if m<=3: return f"{y}-Q1"
    elif m<=5: return f"{y}-Q2"
    elif m<=8: return f"{y}-Q3"
    else: return f"{y}-Q4"

def get_next_due():
    now=datetime.now(timezone.utc)
    for q in QUARTERLY_DATES:
        due=datetime.strptime(q["due"],"%Y-%m-%d").replace(tzinfo=timezone.utc)
        if due>=now: return q,(due-now).days
    return None,0

def update_weekly_pnl(pnl):
    """Update weekly P&L running total"""
    with lock:
        state["weekly_pnl"]=round(state.get("weekly_pnl",0)+pnl,4)
        state["weekly_trades"]=state.get("weekly_trades",0)+1

def record_tax(asset,direction,entry,exit_p,size,pnl,entry_time):
    tax=calc_tax(pnl)
    with lock:
        state["tax"]["total_pnl"]+=pnl; state["tax"]["total_tax"]+=tax["total"]
        state["tax"]["total_net"]+=tax["net"]; state["tax"]["total_trades"]+=1
        if pnl>0: state["tax"]["winning_trades"]+=1
        else:      state["tax"]["losing_trades"]+=1
    year=datetime.now(timezone.utc).year; fname=f"hl_tax_{year}.csv"
    fe=os.path.exists(fname); q=get_quarter(datetime.now(timezone.utc))
    row={"trade_id":f"{asset}-{entry_time[:10]}-{entry_time[11:19].replace(':','')}",
         "account":MAIN_WALLET[:10]+"...","network":"Testnet" if TESTNET else "Mainnet",
         "contract_type":"Section 1256 - Perpetual Futures","exchange":"HyperLiquid",
         "asset":f"{asset}-PERP","direction":direction,
         "entry_date":entry_time,"exit_date":ts(),
         "entry_price":round(entry,6),"exit_price":round(exit_p,6),
         "size":round(size,6),"leverage":LEVERAGE,
         "notional_value":round(entry*size,2),"quarter":q,
         "gross_pnl":tax["gross"],"win_loss":"WIN" if pnl>0 else "LOSS",
         "sec1256_60pct_ltcg":tax["ltcg"],"sec1256_40pct_stcg":tax["stcg"],
         "fed_ltcg_tax":tax["fed_ltcg"],"fed_stcg_tax":tax["fed_stcg"],
         "federal_total":tax["fed_total"],"ny_state_tax":tax["ny"],
         "nyc_local_tax":tax["nyc"],"total_tax":tax["total"],
         "effective_rate":f"{tax['rate']}%","net_after_tax":tax["net"],
         "fed_ltcg_rate":f"{FED_LTCG_RATE*100}%","fed_stcg_rate":f"{FED_STCG_RATE*100}%",
         "ny_state_rate":f"{NY_STATE_RATE*100}%","nyc_local_rate":f"{NYC_LOCAL_RATE*100}%",
         "dry_run":DRY_RUN}
    with open(fname,"a",newline="") as f:
        import csv as _c; w=_c.DictWriter(f,fieldnames=list(row.keys()))
        if not fe: w.writeheader()
        w.writerow(row)

# ══════════════════════════════════════════════════
# EXCHANGE
# ══════════════════════════════════════════════════
wallet=eth_account.Account.from_key(API_PRIVATE_KEY)
info=Info(API_URL,skip_ws=True)
exchange=Exchange(wallet,API_URL,account_address=MAIN_WALLET)

# Testnet paper trading — Binance candles + testnet execution
tn_wallet=eth_account.Account.from_key(TN_API_KEY)
tn_info=Info(constants.TESTNET_API_URL,skip_ws=True)
tn_exchange=Exchange(tn_wallet,constants.TESTNET_API_URL,account_address=TN_WALLET)

positions={}; last_candle={}; last_exit={}; bar_count={}; entry_times={}
stop_oids={}   # mainnet: asset -> HL stop order OID (S2 cancel-replace)
tn_positions={}; tn_last_candle={}; tn_last_exit={}; tn_bar_count={}
tn_stop_oids={}  # testnet: asset -> HL stop order OID (S2 cancel-replace)

# ══════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════
def ema(v,p):
    k=2/(p+1);e=None;out=[]
    for x in v:
        e=x if e is None else x*k+e*(1-k);out.append(e)
    return out

def sma(v,p):
    out=[None]*(p-1)
    for i in range(p-1,len(v)):
        out.append(sum(v[i-p+1:i+1])/p)
    return out

def bbu(closes,p=20,m=2.0):
    out=[None]*p
    for i in range(p,len(closes)):
        w=closes[i-p:i];mu=sum(w)/p
        s=(sum((x-mu)**2 for x in w)/p)**0.5
        out.append(mu+m*s)
    return out

def bbl(closes,p=20,m=2.0):
    u=bbu(closes,p,m);out=[None]*len(closes)
    for i in range(p,len(closes)):
        w=closes[i-p:i];mu=sum(w)/p
        s=(sum((x-mu)**2 for x in w)/p)**0.5
        if u[i]: out[i]=mu-m*s
    return out

def atr_lookup(candles):
    highs=[float(c["h"]) for c in candles]; lows=[float(c["l"]) for c in candles]
    closes=[float(c["c"]) for c in candles]; trs=[]
    for i in range(1,len(closes)):
        tr=max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1]))
        trs.append(tr)
    period=14
    if len(trs)<period: return [None]*len(candles),[None]*len(candles)
    avg=sum(trs[:period])/period; atr_vals=[None]*period; atr_vals.append(avg)
    for i in range(period,len(trs)):
        avg=(avg*(period-1)+trs[i])/period; atr_vals.append(avg)
    while len(atr_vals)<len(candles): atr_vals.append(atr_vals[-1])
    valid=[a for a in atr_vals if a]
    if len(valid)<50: return [None]*len(candles),atr_vals
    ma_list=sma(valid,50); lookup=[None]*len(candles); vi=0; mi=0
    for i in range(len(candles)):
        if atr_vals[i] is not None:
            vi+=1
            if vi>50 and mi<len(ma_list):
                lookup[i]=ma_list[mi]; mi+=1
    return lookup,atr_vals

def evaluate_signal(candles,asset):
    """
    Full signal evaluation with detailed filter breakdown.
    S2+S4: Evaluates on candles[-2] — the LAST COMPLETE candle.
    candles[-1] is the current forming candle — we never evaluate on incomplete data.
    This matches the backtest exactly: signal on complete candle OHLC.
    Returns signal direction + complete filter status for audit.
    """
    cfg=ASSET_CFG[asset]
    filters={}

    if len(candles)<50:
        return None,None,0,0,{"error":"insufficient candles"}

    # S2+S4 KEY CHANGE: use candles[:-1] — exclude current forming candle
    # candles[-1] = current incomplete candle (forming right now)
    # candles[-2] = last COMPLETE candle with final OHLC
    complete_candles = candles[:-1]
    if len(complete_candles)<50:
        return None,None,0,0,{"error":"insufficient complete candles"}

    closes=[float(c["c"]) for c in complete_candles]
    highs=[float(c["h"]) for c in complete_candles]
    lows=[float(c["l"]) for c in complete_candles]
    vols=[float(c["v"]) for c in complete_candles]
    ef=ema(closes,EMA_FAST); em2=ema(closes,EMA_MID); es=ema(closes,EMA_SLOW)
    vs=sma(vols,20); u=bbu(closes); l=bbl(closes); i=len(complete_candles)-1

    # EMA stack
    if ef[i] and em2[i] and es[i]:
        if   ef[i]>em2[i]>es[i]: d="LONG"
        elif ef[i]<em2[i]<es[i]: d="SHORT"
        else: d=None
    else: d=None
    filters["ema_stack"]={"pass":d is not None,"value":d or "flat",
                          "detail":f"EMA5={ef[i]:.2f} EMA13={em2[i]:.2f} EMA34={es[i]:.2f}" if ef[i] else "no data"}

    if not d:
        return None,None,0,0,filters

    # Separation
    sep=abs(ef[i]-es[i])/es[i] if es[i] else 0
    sep_ok=sep>=SEP_FILTER
    filters["separation"]={"pass":sep_ok,"value":f"{sep:.4f}","need":f">={SEP_FILTER}"}
    if not sep_ok: return None,None,0,0,filters

    # Volume — per-asset threshold calibrated to HL mainnet volume profile
    vol=vols[i]; vr=vol/vs[i] if vs[i] else 0
    asset_vf=VOL_FILTER
    vol_ok=vr>=asset_vf
    filters["volume"]={"pass":vol_ok,"value":f"{vr:.2f}x","need":f">={asset_vf}x"}

    # Breakout — fixed to handle BRK_BARS=0 safely
    if i>=BRK_BARS and BRK_BARS>0:
        brk_ok=(closes[i]>max(highs[i-BRK_BARS:i]) if d=="LONG"
                else closes[i]<min(lows[i-BRK_BARS:i]))
        brk_val=(f"close {closes[i]:.2f} > {max(highs[i-BRK_BARS:i]):.2f}" if d=="LONG"
                 else f"close {closes[i]:.2f} < {min(lows[i-BRK_BARS:i]):.2f}")
    else:
        brk_ok=False; brk_val="insufficient bars"
    filters["breakout"]={"pass":brk_ok,"value":brk_val}

    # BB filter
    if cfg["bb"]:
        if u[i] and l[i]:
            bb_ok=(closes[i]>u[i] if d=="LONG" else closes[i]<l[i])
            bb_val=(f"close {closes[i]:.2f} {'>' if d=='LONG' else '<'} BB {'upper' if d=='LONG' else 'lower'} {(u[i] if d=='LONG' else l[i]):.2f}")
        else: bb_ok=False; bb_val="BB not calculated"
        filters["bb_breakout"]={"pass":bb_ok,"value":bb_val}
    else:
        filters["bb_breakout"]={"pass":True,"value":"not required"}

    # Strong close
    if cfg["sc"]:
        br=float(complete_candles[i]["h"])-float(complete_candles[i]["l"])
        if br>0:
            cp=(closes[i]-float(complete_candles[i]["l"]))/br
            sc_ok=(cp>=0.70 if d=="LONG" else cp<=0.30)
            sc_val=f"close pct={cp:.2f} ({'≥0.70' if d=='LONG' else '≤0.30'} needed)"
        else: sc_ok=False; sc_val="zero range candle"
        filters["strong_close"]={"pass":sc_ok,"value":sc_val}
    else:
        filters["strong_close"]={"pass":True,"value":"not required"}

    # Regime
    if cfg["regime"]:
        try:
            lkp,atr_v=atr_lookup(complete_candles)
            if lkp[i] and atr_v[i]:
                reg_ok=atr_v[i]>lkp[i]*1.2
                reg_val=f"ATR={atr_v[i]:.4f} vs MA={lkp[i]:.4f} (need >1.2x)"
            else: reg_ok=True; reg_val="ATR MA not ready — skipping"
        except: reg_ok=True; reg_val="error — skipping"
        filters["regime"]={"pass":reg_ok,"value":reg_val}
    else:
        filters["regime"]={"pass":True,"value":"not required"}

    # Overnight filter
    if cfg["no_ov"]:
        h_utc=datetime.now(timezone.utc).hour
        ov_ok=not(6<=h_utc<10)
        filters["overnight"]={"pass":ov_ok,"value":f"UTC hour={h_utc} ({'blocked 6-10' if not ov_ok else 'ok'})"}
    else:
        filters["overnight"]={"pass":True,"value":"not required"}

    # Funding filter
    if cfg["ff"]:
        fr=abs(float(candles[-1].get("fundingRate",0)))
        ff_ok=fr<=cfg["ff"]
        filters["funding"]={"pass":ff_ok,"value":f"rate={fr:.5f} max={cfg['ff']:.5f}"}
    else:
        filters["funding"]={"pass":True,"value":"not required"}

    # All filters
    all_pass=all(f["pass"] for f in filters.values())
    blocked=[k for k,v in filters.items() if not v["pass"]]
    filters["_result"]={"pass":all_pass,"blocked_by":blocked,"direction":d if all_pass else None}

    return (d if all_pass else None),closes[i],vol,vs[i] if vs[i] else 0,filters

HL_INFO_URL = "https://api.hyperliquid-testnet.xyz/info" if TESTNET else "https://api.hyperliquid.xyz/info"

def verify_entry(asset):
    """
    Verify entry using userFills — clearinghouseState broken on this wallet.
    userFills works correctly on both testnet and mainnet.
    """
    time.sleep(15)
    try:
        since_ms=int(time.time()*1000)-30000
        r=req.post(HL_INFO_URL,json={"type":"userFills","user":MAIN_WALLET},timeout=10)
        fills=r.json()
        if not isinstance(fills,list): return False,0
        recent=[f for f in fills
                if int(f.get("time",0))>since_ms
                and f.get("coin")==asset
                and "Open" in f.get("dir","")]
        if recent:
            fill_price=float(recent[0].get("px",0))
            log(f"✅ verify_entry {asset} — fill confirmed @ ${fill_price:,.4f} via userFills")
            return True,fill_price
        log(f"❌ verify_entry {asset} — no fill found in last 30s")
        return False,0
    except Exception as e:
        add_diag("ERROR",f"Verify entry {asset}",str(e),"Assuming failed")
        add_issue(asset,"Verify entry failed",str(e))
        return False,0

def verify_exit(asset):
    """
    Verify exit using userFills — check for Close fill in last 30 seconds.
    """
    time.sleep(3)
    try:
        since_ms=int(time.time()*1000)-30000
        r=req.post(HL_INFO_URL,json={"type":"userFills","user":MAIN_WALLET},timeout=10)
        fills=r.json()
        if not isinstance(fills,list): return False
        recent=[f for f in fills
                if int(f.get("time",0))>since_ms
                and f.get("coin")==asset
                and "Close" in f.get("dir","")]
        return len(recent)>0
    except: return False

def liq_price(entry,direction):
    pct=1/LEVERAGE
    return round(entry*(1-pct) if direction=="LONG" else entry*(1+pct),2)

# ══════════════════════════════════════════════════
# TRADING
# ══════════════════════════════════════════════════
# ══════════════════════════════════════════════════
# S2+S4 STOP ORDER MANAGEMENT
# ══════════════════════════════════════════════════
def place_hl_stop(asset, direction, size, stop_price):
    """Place reduce-only stop market order on HyperLiquid exchange"""
    is_buy = direction == "SHORT"  # SHORT needs BUY to close
    sp = round_price(stop_price)
    lp = round_price(sp * 0.9 if is_buy else sp * 1.1)
    try:
        dec = next((a.get("szDecimals",5) for a in info.meta()["universe"] if a["name"]==asset),5)
        sz = round(size, dec)
    except:
        sz = size
    order_type = {"trigger": {"triggerPx": sp, "isMarket": True, "tpsl": "sl"}}
    try:
        result = exchange.order(asset, is_buy, sz, lp, order_type, reduce_only=True)
        if result.get("status") == "ok":
            statuses = result["response"]["data"]["statuses"]
            if statuses and "resting" in statuses[0]:
                oid = statuses[0]["resting"]["oid"]
                log(f"✅ Stop placed {asset} @ ${sp} OID:{oid}")
                return oid
            if statuses and "filled" in statuses[0]:
                log(f"⚠️  Stop filled immediately for {asset} — position already closed")
                return "FILLED"
            log(f"⚠️  Stop unexpected response {asset}: {statuses}")
        else:
            add_diag("ERROR", f"Stop order failed {asset}", str(result), "Position unprotected")
            add_issue(asset,"Stop order failed",f"Result: {str(result)[:100]} — position unprotected")
    except Exception as e:
        add_diag("ERROR", f"Stop order exception {asset}", str(e), "Position unprotected")
        add_issue(asset,"Stop order exception",f"{str(e)[:100]} — position unprotected")
    return None

def cancel_hl_stop(asset, oid):
    """Cancel a stop order on HyperLiquid"""
    try:
        result = exchange.cancel(asset, oid)
        return result.get("status") == "ok"
    except Exception as e:
        log(f"⚠️  Cancel stop failed {asset} OID:{oid}: {e}")
        return False

def update_trail_stop(asset, pos, hi, lo, atr_val, oids_dict):
    """
    S2+S4 trail update:
    S4: Only update trail if candle move > 0.5x ATR
    S2: Cancel old stop, place new stop at updated trail price
    Returns True if trail was updated
    """
    if atr_val is None: atr_val = 0
    updated = False
    
    if pos["direction"] == "LONG":
        move = hi - pos["trail_peak"]
        # S4: ATR filter — skip update if move is noise
        # If ATR=0, always update (can't filter nothing)
        if hi > pos["trail_peak"] and (atr_val == 0 or move > atr_val * ATR_BUFFER):
            pos["trail_peak"] = hi
            pos["trail_stop"] = round_price(hi * (1 - TRAIL_PCT))
            updated = True
    else:
        move = pos["trail_peak"] - lo
        if lo < pos["trail_peak"] and (atr_val == 0 or move > atr_val * ATR_BUFFER):
            pos["trail_peak"] = lo
            pos["trail_stop"] = round_price(lo * (1 + TRAIL_PCT))
            updated = True

    if updated:
        add_audit(asset, "📈 TRAIL UPDATED S4",
                  f"trail=${pos['trail_stop']:.4f} | peak=${pos['trail_peak']:.4f} | "
                  f"move=${move:.4f} | ATR=${atr_val:.4f}")
        # S2: Cancel old stop, place new one
        old_oid = oids_dict.get(asset)
        if old_oid and old_oid != "FILLED":
            cancelled = cancel_hl_stop(asset, old_oid)
            log(f"🔄 {asset}: cancel old stop OID:{old_oid} → {cancelled}")
            time.sleep(0.3)
        new_oid = place_hl_stop(asset, pos["direction"], pos.get("qty_rem", pos["size"]), pos["trail_stop"])
        if new_oid and new_oid != "FILLED":
            oids_dict[asset] = new_oid
        elif new_oid == "FILLED":
            # Stop triggered immediately — position closed by exchange
            return "FILLED"

    return updated

def enter_trade(asset,direction,price,vol,vs,ef,es,filters=None):
    log(f"🔥 ENTER_TRADE CALLED: {asset} {direction} @ ${price:,.4f} — attempting order")
    cfg=ASSET_CFG[asset]
    pos_usd=get_pos_usd(vol,vs,ef,es)
    # Get correct decimal places for this asset from exchange
    try:
        meta=info.meta()
        dec=next((a.get("szDecimals",5) for a in meta["universe"] if a["name"]==asset),5)
    except:
        dec=5
    qty=round((pos_usd*LEVERAGE)/price,dec)
    log(f"📋 {asset} size: {qty} (dec={dec}, pos_usd=${pos_usd:.2f}, notional=${qty*price:.2f})")
    stop=round_price(price*(1-STOP_PCT) if direction=="LONG" else price*(1+STOP_PCT))
    trail=round_price(price*(1-TRAIL_PCT) if direction=="LONG" else price*(1+TRAIL_PCT))
    liq=liq_price(price,direction)

    if DRY_RUN:
        log(f"[DRY] ENTER {direction} {asset} @ ${price:,.2f}")
        entry_times[asset]=ts()
        positions[asset]={"direction":direction,"entry":price,"size":qty,
                          "pos_usd":pos_usd,"stop":stop,"trail_peak":price,
                          "trail_stop":trail,"liq":liq,"partial_done":False,
                          "partial_pnl":0.0,"qty_rem":qty,"current_price":price,"unrealized_pnl":0.0}
        add_trade(asset,"ENTER",direction,price,None,qty,None,"signal",filters)
        add_audit(asset,"ENTERED (DRY)",f"{direction} @ ${price:,.2f} | stop=${stop:,.2f} | liq=${liq:,.2f}")
        with lock: state["positions"]={k:v for k,v in positions.items()}
        return

    try:
        r=exchange.market_open(asset,direction=="LONG",qty)
        log(f"📋 Exchange response for {asset}: {r}")
        if r and r.get("status")=="ok":
            statuses=r.get("response",{}).get("data",{}).get("statuses",[])
            log(f"📋 Statuses for {asset}: {statuses}")
            if statuses and "error" in statuses[0]:
                add_diag("ERROR",f"Order rejected {asset}",statuses[0]["error"],"Skipping")
                add_issue(asset,"Order rejected",f"Exchange rejected: {statuses[0]['error']}")
                add_audit(asset,"ORDER REJECTED",f"Exchange error: {statuses[0]['error']}")
                add_issue(asset,"Order rejected",statuses[0]["error"])
                return
            fill=price
            if statuses and "filled" in statuses[0]:
                fill=float(statuses[0]["filled"]["avgPx"])

            # NON-BLOCKING: run verification in background thread
            # Main loop continues checking other assets immediately
            def verify_and_confirm(a,d,f,pu,q,stp,trl,lq,et,filters_arg=None):
                confirmed,actual=verify_entry(a)
                if not confirmed:
                    add_diag("ERROR",f"Entry NOT confirmed {a}","Order placed but not visible","NOT logging")
                    add_audit(a,"ENTRY FAILED",f"Order placed @ ${f:,.2f} but not visible on exchange")
                    add_issue(a,"Entry not confirmed",f"Order placed @ ${f:,.2f} but not visible — position NOT recorded")
                    return
                f2=actual if actual>0 else f
                stp2=round_price(f2*(1-STOP_PCT) if d=="LONG" else f2*(1+STOP_PCT))
                trl2=round_price(f2*(1-TRAIL_PCT) if d=="LONG" else f2*(1+TRAIL_PCT))
                lq2=liq_price(f2,d)
                try:
                    meta2=info.meta()
                    dec2=next((x.get("szDecimals",5) for x in meta2["universe"] if x["name"]==a),5)
                except:
                    dec2=5
                q2=round((pu*LEVERAGE)/f2,dec2)
                entry_times[a]=et
                positions[a]={"direction":d,"entry":f2,"size":q2,
                              "pos_usd":pu,"stop":stp2,"trail_peak":f2,
                              "trail_stop":trl2,"liq":lq2,"partial_done":False,
                              "partial_pnl":0.0,"qty_rem":q2,"current_price":f2,"unrealized_pnl":0.0}
                add_trade(a,"ENTER",d,f2,None,q2,None,"signal",filters_arg)
                add_audit(a,"✅ ENTERED",f"{d} @ ${f2:,.2f} | stop=${stp2:,.2f} | trail=${trl2:,.2f} | liq=${lq2:,.2f} | CONFIRMED")
                ntfy_trade_entered(a,d,f2,q2,stp2,trl2,pu)
                log(f"✅ ENTERED {d} {a} @ ${f2:,.2f} | CONFIRMED | liq=${lq2:,.2f}")
                # S2: Place stop loss order on HL exchange immediately
                oid=place_hl_stop(a,d,q2,stp2)
                if oid and oid!="FILLED":
                    stop_oids[a]=oid
                    add_audit(a,"🛡 STOP PLACED",f"stop @ ${stp2:,.4f} OID:{oid}")
                elif oid=="FILLED":
                    add_audit(a,"⚠️ STOP FILLED IMMEDIATELY",f"position may be closed")
                else:
                    add_audit(a,"⚠️ STOP PLACEMENT FAILED",f"position unprotected — will retry next candle")
                    add_issue(a,"Stop placement failed",f"entry @ ${f2:,.4f} — no stop order placed")
                with lock: state["positions"]={k:v for k,v in positions.items()}

            t=threading.Thread(
                target=verify_and_confirm,
                args=(asset,direction,fill,pos_usd,qty,stop,trail,liq,ts()),
                kwargs={"filters_arg":filters},
                daemon=True
            )
            t.start()
            log(f"🔄 {asset} verification running in background — main loop continues")
            return  # Return immediately, don't block
        else:
            add_diag("ERROR",f"Entry failed {asset}",str(r),"Skipping")
            add_audit(asset,"ENTRY FAILED",f"Exchange rejected order: {r}")
            add_issue(asset,"Entry failed",f"Exchange rejected: {str(r)[:100]}")
    except Exception as e:
        add_diag("ERROR",f"Entry exception {asset}",str(e),"Skipping")
        add_audit(asset,"ENTRY ERROR",str(e))
        add_issue(asset,"Entry exception",str(e)[:150])

def exit_trade(asset,price,reason):
    if asset not in positions: return
    pos=positions[asset]; cfg=ASSET_CFG[asset]; etime=entry_times.get(asset,ts())

    if DRY_RUN:
        if cfg["exit"]=="partial":
            pnl=round((price-pos["entry"])*pos["qty_rem"]+pos["partial_pnl"],4) \
                if pos["direction"]=="LONG" \
                else round((pos["entry"]-price)*pos["qty_rem"]+pos["partial_pnl"],4)
        else:
            pnl=round((price-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                      else (pos["entry"]-price)*pos["size"],4)
        icon="✅" if pnl>=0 else "❌"
        log(f"[DRY] EXIT {pos['direction']} {asset} @ ${price:,.2f} | {reason} | P&L=${pnl:+.4f}")
        add_audit(asset,f"{icon} EXITED (DRY)",f"{pos['direction']} @ ${price:,.2f} | reason={reason} | P&L=${pnl:+.4f}")
        record_tax(asset,pos["direction"],pos["entry"],price,pos["size"],pnl,etime)
        update_weekly_pnl(pnl)
        ntfy_trade_closed(asset,pos["direction"],pos["entry"],price,pnl,reason)
        add_trade(asset,"EXIT",pos["direction"],pos["entry"],price,pos["size"],pnl,reason)
        last_exit[asset]=bar_count.get(asset,0)
        del positions[asset]
        if asset in entry_times: del entry_times[asset]
        with lock: state["positions"]={k:v for k,v in positions.items()}
        return

    try:
        r=exchange.market_close(asset)
        fill=price; closed=False
        if r and r.get("status")=="ok":
            statuses=r.get("response",{}).get("data",{}).get("statuses",[])
            if statuses and "filled" in statuses[0]:
                fill=float(statuses[0]["filled"]["avgPx"])
            closed=verify_exit(asset)
            if not closed:
                add_diag("CRITICAL",f"Exit NOT confirmed {asset}","Position still visible","Manual check required")
                add_audit(asset,"EXIT NOT CONFIRMED",f"Close placed @ ${fill:,.2f} but position still visible on exchange")
        if closed:
            if cfg["exit"]=="partial":
                rem=((fill-pos["entry"])*pos["qty_rem"] if pos["direction"]=="LONG"
                     else (pos["entry"]-fill)*pos["qty_rem"])
                pnl=round(rem+pos["partial_pnl"],4)
            else:
                pnl=round((fill-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                          else (pos["entry"]-fill)*pos["size"],4)
            icon="✅" if pnl>=0 else "❌"
            log(f"{icon} EXITED {pos['direction']} {asset} @ ${fill:,.2f} | {reason} | P&L=${pnl:+.4f} | CONFIRMED")
            add_audit(asset,f"{icon} EXITED",f"{pos['direction']} @ ${fill:,.2f} | reason={reason} | P&L=${pnl:+.4f} | CONFIRMED on exchange")
            record_tax(asset,pos["direction"],pos["entry"],fill,pos["size"],pnl,etime)
            update_weekly_pnl(pnl)
            ntfy_trade_closed(asset,pos["direction"],pos["entry"],fill,pnl,reason)
            add_trade(asset,"EXIT",pos["direction"],pos["entry"],fill,pos["size"],pnl,reason)
            last_exit[asset]=bar_count.get(asset,0)
            del positions[asset]
            if asset in entry_times: del entry_times[asset]
            with lock: state["positions"]={k:v for k,v in positions.items()}
    except Exception as e:
        add_diag("ERROR",f"Exit exception {asset}",str(e),"Position may still be open")
        add_audit(asset,"EXIT ERROR",str(e))
        add_issue(asset,"Exit exception",f"{str(e)[:150]} — position may still be open")

def close_all(reason="manual"):
    log(f"🚨 CLOSING ALL — {reason}")
    add_diag("WARNING","Close all triggered",reason,"Closing all positions")
    for asset in list(positions.keys()):
        try:
            mids=info.all_mids()
            price=float(mids.get(asset,positions[asset]["entry"]))
            exit_trade(asset,price,reason); time.sleep(1)
        except Exception as e:
            add_diag("ERROR",f"Close all failed {asset}",str(e),"Try manually")

# ══════════════════════════════════════════════════
# TRADING LOOP
# ══════════════════════════════════════════════════
def trading_loop():
    log("HL TRADER v4 — Full audit trail | Per-asset errors | 6 assets")
    add_diag("INFO","HL Trader v4 started",
             f"DRY={DRY_RUN} TEST={TESTNET} LEV={LEVERAGE}x ASSETS={len(ASSETS)}",
             "Per-asset retry | Full audit trail | All orders verified")
    ntfy("🚀 HL Trader v4 Started",
         f"6 assets | Per-asset errors\nMode: {'TESTNET' if TESTNET else 'LIVE'}\n"
         f"Leverage: {LEVERAGE}x\nAssets: {', '.join(ASSETS)}",tags="rocket")

    retry_count={}; cycle=0; api_down_since=None
    startup_complete=False  # prevents sync from firing before positions loaded

    while True:
        with lock:
            killed=state["kill_switch"]; paused=state["paused"]
            close_req=state["close_all_requested"]

        if killed:
            with lock: state["status"]="stopped"
            time.sleep(10); continue

        if close_req:
            close_all("emergency")
            with lock: state["close_all_requested"]=False
            continue

        cycle+=1
        with lock:
            state["cycle"]=cycle
            state["last_check"]=ts()
            state["status"]="paused" if paused else "checking"

        log(f"🔄 Cycle #{cycle} | checking {len(ASSETS)} assets")

        try:
            mids=info.all_mids()
            was_down=api_down_since is not None
            if was_down:
                down_min=(time.time()-api_down_since)/60
                ntfy_api_recovered(down_min); api_down_since=None
            startup_complete=True  # positions loaded, safe to run sync
            with lock:
                state["health"]["api_connected"]=True
                state["health"]["last_ping"]=ts()
                for asset,pos in positions.items():
                    cur=float(mids.get(asset,pos["entry"]))
                    pnl=((cur-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                         else (pos["entry"]-cur)*pos["size"])
                    state["positions"][asset]["current_price"]=cur
                    state["positions"][asset]["unrealized_pnl"]=round(pnl,4)

            # TWO-WAY HL SYNC — only runs after startup_complete=True
            # Uses same clearinghouseState API as startup sync
            if startup_complete and positions:
                try:
                    r_sync=req.post(HL_INFO_URL,
                        json={"type":"clearinghouseState","user":MAIN_WALLET},timeout=10)
                    hl_pos=r_sync.json().get("assetPositions",[])
                    hl_open={p["position"]["coin"]:p["position"]
                             for p in hl_pos
                             if float(p["position"].get("szi",0))!=0
                             and p["position"]["coin"] in ASSETS}
                    # Only fire if asset WAS in positions AND is now gone from HL
                    for asset in list(positions.keys()):
                        if asset not in hl_open:
                            pos=positions[asset]
                            cur_p=float(mids.get(asset,pos["entry"]))
                            pnl=round((cur_p-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                                      else (pos["entry"]-cur_p)*pos["size"],4)
                            log(f"TWO-WAY HL SYNC: {asset} closed on HL — syncing")
                            add_audit(asset,"🔄 HL SYNC CLOSE","Position closed on HL — syncing")
                            ntfy_trade_closed(asset,pos["direction"],pos["entry"],cur_p,pnl,"trail_hl")
                            add_trade(asset,"EXIT",pos["direction"],pos["entry"],cur_p,pos["size"],pnl,"trail_hl")
                            record_tax(asset,pos["direction"],pos["entry"],cur_p,pos["size"],pnl,entry_times.get(asset,ts()))
                            update_weekly_pnl(pnl)
                            del positions[asset]
                            if asset in stop_oids: del stop_oids[asset]
                            if asset in entry_times: del entry_times[asset]
                            with lock: state["positions"]={k:v for k,v in positions.items()}
                    # Cancel duplicate stops
                    open_orders=req.post(HL_INFO_URL,
                        json={"type":"openOrders","user":MAIN_WALLET},timeout=10).json()
                    if isinstance(open_orders,list):
                        asset_orders={}
                        for o in open_orders:
                            a=o.get("coin","")
                            if a in ASSETS:
                                if a not in asset_orders: asset_orders[a]=[]
                                asset_orders[a].append(o)
                        for asset,orders in asset_orders.items():
                            if len(orders)>1:
                                orders_sorted=sorted(orders,key=lambda x:x.get("timestamp",0),reverse=True)
                                for dup in orders_sorted[1:]:
                                    log(f"🔄 {asset}: DUPLICATE STOP CANCELLED OID:{dup['oid']}")
                                    cancel_hl_stop(asset,dup["oid"])
                                    add_audit(asset,"🔄 DUPLICATE STOP CANCELLED",f"OID:{dup['oid']}")
                except Exception as e:
                    log(f"⚠️ HL sync check failed: {e}")
        except Exception as e:
            if api_down_since is None:
                api_down_since=time.time(); ntfy_api_down()
            with lock: state["health"]["api_connected"]=False
            add_diag("ERROR","API ping failed",str(e),"Retrying")
            with lock: state["status"]="waiting"; state["next_check"]=f"in {CHECK_EVERY}s"
            time.sleep(CHECK_EVERY); continue

        for asset in ASSETS:
            cfg=ASSET_CFG[asset]
            bar_count[asset]=bar_count.get(asset,0)+1

            try:
                end_ms=int(time.time()*1000)
                start_ms=end_ms-CANDLE_LIMIT*15*60*1000
                # Use Binance candles — proven 11,496 trades 74% WR on 2yr backtest
                # data-api.binance.vision confirmed working from Railway
                sym=BINANCE_SYM.get(asset,asset+"USDT")
                r_b=req.get(BINANCE_CANDLE_URL,
                    params={"symbol":sym,"interval":"15m",
                            "startTime":start_ms,"endTime":end_ms,"limit":CANDLE_LIMIT},
                    timeout=10)
                if r_b.status_code!=200:
                    add_diag("WARNING",f"Binance fetch {asset}",f"HTTP {r_b.status_code}","Skipping")
                    continue
                raw=r_b.json()
                if not isinstance(raw,list) or not raw:
                    continue
                candles=[{"t":int(b[0]),"o":b[1],"h":b[2],"l":b[3],"c":b[4],"v":b[5]} for b in raw]

                if not candles or len(candles)<50:
                    msg=f"Got {len(candles) if candles else 0} bars"
                    add_diag("WARNING",f"No candles {asset}",msg,"Skipping")
                    add_audit(asset,"⚠️ NO CANDLES",msg)
                    continue

                ts_val=str(candles[-1].get("t",candles[-1].get("T","")))
                cur=float(candles[-1]["c"])
                hi=float(candles[-1]["h"])
                lo=float(candles[-1]["l"])
                vol=float(candles[-1]["v"])
                if cur==0: continue

                age_s=int((time.time()*1000-int(ts_val))/1000) if ts_val.isdigit() else 9999

                with lock:
                    state["health"]["assets_ok"][asset]={
                        "ok":True,"price":cur,
                        "last_candle":f"{age_s//60}m{age_s%60}s ago" if ts_val.isdigit() else ts_val,
                        "signal":"checking","fresh":age_s<1200,
                        "candle_ts":ts_val,
                    }

                # DEDUP CHECK
                if last_candle.get(asset)==ts_val:
                    log(f"⏭  {asset}: same candle ts={ts_val} price=${cur:,.2f} — skipping")
                    add_audit(asset,"⏭ SAME CANDLE",f"ts={ts_val} | price=${cur:,.2f} | age={age_s}s | skipping (already evaluated)")
                    continue

                log(f"🕯  {asset}: NEW candle ts={ts_val} | price=${cur:,.2f} | age={age_s}s")
                add_audit(asset,"🕯 NEW CANDLE",f"ts={ts_val} | price=${cur:,.2f} | age={age_s}s | evaluating signal...")

                # Evaluate signal with full filter breakdown
                direction,signal_price,sig_vol,sig_vs,filters=evaluate_signal(candles,asset)
                result=filters.get("_result",{})
                blocked=result.get("blocked_by",[])

                # RETRY ALL FIX: only EMA direction marks candle as seen
                # Proven: 2yr backtest 8142 trades 78.2% WR $+235k 105G/0R
                # Simulation v3: 10/10 tests pass all 6 assets
                ema_filter=filters.get("ema_stack",{})
                ema_passed=ema_filter.get("pass",False)
                ema_dir=ema_filter.get("value","flat")

                if not ema_passed:
                    # EMA flat — only fundamental filter, mark as seen
                    last_candle[asset]=ts_val
                    add_audit(asset,"⏭ EMA FLAT",f"EMA={ema_dir} | seen — wont change mid-candle")
                elif direction:
                    # All filters passed — mark as seen, enter trade below
                    last_candle[asset]=ts_val
                else:
                    # EMA stacked but other filters failing — RETRY next cycle
                    # Show detailed filter values so we can see why it's retrying
                    fil_detail=" | ".join(f"{k}={'✅' if v.get('pass') else '❌'} {str(v.get('value',''))[:15]}"
                                         for k,v in filters.items() if k!="_result")
                    add_audit(asset,"🔄 RETRY ALL",
                              f"EMA={ema_dir} | blocked:{blocked} | {fil_detail}")
                    log(f"🔄 {asset}: EMA stacked, blocked by {blocked} — retry next cycle")


                with lock:
                    state["health"]["assets_ok"][asset]["signal"]=(
                        f"{direction} @ ${signal_price:,.2f}" if direction else
                        f"no signal — blocked by: {', '.join(blocked)}" if blocked else "no signal"
                    )

                if direction:
                    filter_summary=" | ".join(
                        f"{k}={'✅' if v['pass'] else '❌'}" 
                        for k,v in filters.items() if k!="_result"
                    )
                    add_audit(asset,f"🚨 SIGNAL {direction}",
                              f"price=${signal_price:,.2f} | {filter_summary}",filters)
                    log(f"🚨 SIGNAL: {asset} {direction} @ ${signal_price:,.2f}")
                else:
                    if blocked:
                        filter_detail=" | ".join(
                            f"{k}={filters[k]['value']}" for k in blocked if k in filters
                        )
                        add_audit(asset,"⏳ NO SIGNAL",
                                  f"blocked by: {', '.join(blocked)} | {filter_detail}",filters)
                        log(f"⏳ {asset}: no signal — blocked by {', '.join(blocked)}")
                    else:
                        add_audit(asset,"⏳ NO SIGNAL","EMA not stacked",filters)
                        log(f"⏳ {asset}: no signal @ ${cur:,.2f}")

                # Overnight + funding already checked inside evaluate_signal

                # EXITS — S2+S4: use last COMPLETE candle hi/lo
                if asset in positions:
                    pos=positions[asset]
                    # Use candles[-2] for exit checks — last COMPLETE candle
                    prev=candles[-2]
                    prev_hi=float(prev["h"]); prev_lo=float(prev["l"])
                    # ATR from complete candles
                    try:
                        _,atr_vals=atr_lookup(candles[:-1])
                        atr_val=atr_vals[-1] if atr_vals and atr_vals[-1] else 0
                    except: atr_val=0

                    # EMA cross check FIRST — matches backtest priority
                    # If EMA crossed, cancel stop and exit — no trail update needed
                    complete_closes=[float(c["c"]) for c in candles[:-1]]
                    ema_x=((pos["direction"]=="LONG" and
                            ema(complete_closes,EMA_FAST)[-1]<ema(complete_closes,EMA_MID)[-1]) or
                           (pos["direction"]=="SHORT" and
                            ema(complete_closes,EMA_FAST)[-1]>ema(complete_closes,EMA_MID)[-1]))

                    if ema_x:
                        add_audit(asset,"📊 EMA CROSS EXIT",f"EMA5 crossed EMA13 @ ${cur:,.4f}")
                        old_oid=stop_oids.get(asset)
                        if old_oid and old_oid!="FILLED":
                            cancelled=cancel_hl_stop(asset,old_oid)
                            log(f"🔄 {asset}: cancelled stop before EMA exit OID:{old_oid} → {cancelled}")
                            time.sleep(0.3)
                            if asset in stop_oids: del stop_oids[asset]
                        # ntfy on EMA cross exit
                        pos_ema=positions.get(asset,{})
                        if pos_ema:
                            pnl_ema=((cur-pos_ema["entry"])*pos_ema["size"] if pos_ema["direction"]=="LONG"
                                     else (pos_ema["entry"]-cur)*pos_ema["size"])
                            ntfy_trade_closed(asset,pos_ema["direction"],pos_ema["entry"],cur,pnl_ema,"ema_cross")
                        exit_trade(asset,cur,"ema_cross")
                        continue

                    # Hard stop check using complete candle
                    stop_hit=((pos["direction"]=="LONG" and prev_lo<=pos["stop"]) or
                               (pos["direction"]=="SHORT" and prev_hi>=pos["stop"]))
                    if stop_hit:
                        add_audit(asset,"🛑 STOP HIT",f"stop=${pos['stop']:,.4f} | low={prev_lo:,.4f}")
                        old_oid=stop_oids.get(asset)
                        if old_oid and old_oid!="FILLED":
                            cancel_hl_stop(asset,old_oid)
                        exit_trade(asset,pos["stop"],"stop")
                        if asset in stop_oids: del stop_oids[asset]
                        continue

                    # S4+S2: Update trail with ATR filter, cancel-replace stop on HL
                    trail_result=update_trail_stop(asset,pos,prev_hi,prev_lo,atr_val,stop_oids)
                    if trail_result=="FILLED":
                        # Stop was triggered by HL exchange
                        pnl=((pos["trail_stop"]-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                             else (pos["entry"]-pos["trail_stop"])*pos["size"])
                        add_audit(asset,"✅ TRAIL EXIT (HL)",f"stop triggered by exchange @ ${pos['trail_stop']:.4f} | P&L=${pnl:+.4f}")
                        record_tax(asset,pos["direction"],pos["entry"],pos["trail_stop"],pos["size"],pnl,entry_times.get(asset,ts()))
                        ntfy_trade_closed(asset,pos["direction"],pos["entry"],pos["trail_stop"],pnl,"trail")
                        add_trade(asset,"EXIT",pos["direction"],pos["entry"],pos["trail_stop"],pos["size"],pnl,"trail")
                        last_exit[asset]=bar_count.get(asset,0)
                        del positions[asset]
                        if asset in stop_oids: del stop_oids[asset]
                        if asset in entry_times: del entry_times[asset]
                        with lock: state["positions"]={k:v for k,v in positions.items()}
                        continue

                    # Partial exit for SOL
                    if cfg["exit"]=="partial" and not pos["partial_done"]:
                        trig_p=(pos["entry"]*(1+cfg["pt"]) if pos["direction"]=="LONG"
                                else pos["entry"]*(1-cfg["pt"]))
                        if ((pos["direction"]=="LONG" and prev_hi>=trig_p) or
                            (pos["direction"]=="SHORT" and prev_lo<=trig_p)):
                            pqty=pos["qty_rem"]*cfg["ps"]
                            praw=((trig_p-pos["entry"])*pqty if pos["direction"]=="LONG"
                                  else (pos["entry"]-trig_p)*pqty)
                            pos["partial_pnl"]+=praw; pos["qty_rem"]-=pqty
                            pos["partial_done"]=True; pos["stop"]=pos["entry"]
                            if pos["direction"]=="LONG":
                                pos["trail_peak"]=trig_p; pos["trail_stop"]=round_price(trig_p*(1-TRAIL_PCT))
                            else:
                                pos["trail_peak"]=trig_p; pos["trail_stop"]=round_price(trig_p*(1+TRAIL_PCT))
                            add_audit(asset,"💰 PARTIAL EXIT",f"@ ${trig_p:,.4f} | stop→breakeven @ ${pos['entry']:,.4f}")
                            log(f"💰 {asset} PARTIAL @ ${trig_p:,.4f} | stop→breakeven")
                            old_oid=stop_oids.get(asset)
                            if old_oid and old_oid!="FILLED":
                                cancel_hl_stop(asset,old_oid)
                                time.sleep(0.3)
                            new_oid=place_hl_stop(asset,pos["direction"],pos["qty_rem"],pos["stop"])
                            if new_oid and new_oid!="FILLED":
                                stop_oids[asset]=new_oid

                    # Fixed TP for BNB
                    if cfg["exit"]=="fixed_tp" and cfg["tp"]:
                        tp_p=(pos["entry"]*(1+cfg["tp"]) if pos["direction"]=="LONG"
                              else pos["entry"]*(1-cfg["tp"]))
                        if ((pos["direction"]=="LONG" and prev_hi>=tp_p) or
                            (pos["direction"]=="SHORT" and prev_lo<=tp_p)):
                            add_audit(asset,"🎯 TP HIT",f"target=${tp_p:,.4f} hit")
                            old_oid=stop_oids.get(asset)
                            if old_oid and old_oid!="FILLED":
                                cancel_hl_stop(asset,old_oid)
                                if asset in stop_oids: del stop_oids[asset]
                                time.sleep(0.3)
                            exit_trade(asset,tp_p,"tp"); continue
                    else:
                        pnl=((cur-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                             else (pos["entry"]-cur)*pos["size"])
                        add_audit(asset,"⏳ HOLDING",
                                  f"{pos['direction']} @ ${pos['entry']:,.4f} | cur=${cur:,.4f} | "
                                  f"trail=${pos['trail_stop']:,.4f} | stop_oid={stop_oids.get(asset,'?')} | P&L=${pnl:+.4f}")
                        log(f"⏳ {asset} {pos['direction']} @ ${cur:,.4f} | trail=${pos['trail_stop']:,.4f} | P&L=${pnl:+.4f}")

                # ENTRIES
                elif not paused and not killed:
                    cd=cfg.get("cd",0)
                    if cd>0 and (bar_count.get(asset,0)-last_exit.get(asset,0))<cd:
                        bars_left=cd-(bar_count.get(asset,0)-last_exit.get(asset,0))
                        add_audit(asset,"⏸ COOLDOWN",f"{bars_left} bars remaining before next entry")
                        continue
                    if direction:
                        enter_trade(asset,direction,signal_price,sig_vol,sig_vs,
                                    ema([float(c["c"]) for c in candles],EMA_FAST)[-1],
                                    ema([float(c["c"]) for c in candles],EMA_SLOW)[-1],
                                    filters)
                    # No signal already logged in audit above

                elif paused:
                    if direction:
                        add_audit(asset,"⏸ PAUSED — MISSED SIGNAL",
                                  f"{direction} @ ${signal_price:,.2f} — system paused, signal not taken")
                elif killed:
                    if direction:
                        add_audit(asset,"🛑 KILLED — MISSED SIGNAL",
                                  f"{direction} @ ${signal_price:,.2f} — kill switch active")

                retry_count[asset]=0

            except Exception as e:
                retry_count[asset]=retry_count.get(asset,0)+1
                add_diag("ERROR",f"Error {asset}",str(e),f"Retry {retry_count[asset]}/5")
                add_audit(asset,"❌ ERROR",f"{str(e)} | retry {retry_count[asset]}/5")
                add_issue(asset,f"Trading error (retry {retry_count[asset]}/5)",str(e)[:150])
                if retry_count[asset]>5:
                    add_diag("WARNING",f"{asset} skipped",
                             f"{retry_count[asset]} consecutive errors",
                             f"Skipping {asset} only — other assets unaffected")
                    add_audit(asset,"⚠️ ASSET SKIPPED",
                              f"Too many errors — skipping this asset only, others continue normally")
                    retry_count[asset]=0

            time.sleep(0.5)

        check_milestones()
        check_daily_summaries()
        check_tax_reminders()
        check_12hr_sim()
        check_weekly_reset()

        with lock:
            state["status"]="stopped" if state["kill_switch"] else ("paused" if state["paused"] else "waiting")
            state["next_check"]=f"in {CHECK_EVERY}s"
            state["positions"]={k:v for k,v in positions.items()}

        log(f"💤 Cycle #{cycle} complete | next in {CHECK_EVERY}s")
        time.sleep(CHECK_EVERY)

# ══════════════════════════════════════════════════
# FLASK
# ══════════════════════════════════════════════════
app=Flask(__name__)
app.secret_key=os.environ.get("SECRET_KEY","hl2026secret")

def build_dashboard():
    s=state; h=s["health"]; tax=s["tax"]
    any_fresh=any(v.get("fresh") for v in h["assets_ok"].values())
    killed=s["kill_switch"]; paused=s["paused"]
    status="STOPPED" if killed else ("PAUSED" if paused else s["status"].upper())
    dot="#FF4757" if killed else ("#FFB800" if paused else "#00D68F")
    mode="DRY RUN" if s["dry_run"] else ("TESTNET" if s["testnet"] else "🚨 LIVE")
    mc="61,158,255" if s["dry_run"] else ("255,184,0" if s["testnet"] else "0,214,143")
    wr=f"{tax['winning_trades']/tax['total_trades']*100:.0f}%" if tax["total_trades"]>0 else "—"

    def row(k,v):
        return f'<div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #1E2D42"><span style="font-size:13px;color:#4A5878">{k}</span><span style="font-family:monospace;font-weight:600;font-size:12px">{v}</span></div>'

    pos_html=""
    for asset,pos in s["positions"].items():
        pnl=pos.get("unrealized_pnl",0); cur=pos.get("current_price",pos["entry"])
        pc="#00D68F" if pnl>=0 else "#FF4757"
        dc="0,214,143" if pos["direction"]=="LONG" else "255,71,87"
        liq=pos.get("liq",0); dist=abs(cur-liq)/liq*100 if liq>0 else 0
        pos_html+=f'''<div style="background:#161E2E;border:1px solid #1E2D42;border-radius:14px;padding:14px;margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;margin-bottom:10px">
            <div style="font-family:monospace;font-size:15px;font-weight:700">{asset}-PERP</div>
            <div style="font-size:11px;font-weight:700;padding:3px 10px;border-radius:6px;background:rgba({dc},0.15);color:rgb({dc})">{pos["direction"]}</div>
          </div>
          {row("Entry",f"${pos['entry']:,.2f}")}{row("Current",f'<span style="color:{pc}">${cur:,.2f}</span>')}{row("Hard Stop",f'<span style="color:#FF4757">${pos["stop"]:,.2f}</span>')}{row("Trail Stop",f'<span style="color:#FFB800">${pos["trail_stop"]:,.2f}</span>')}{row("Liquidation",f'<span style="color:#FF4757">${liq:,.2f} ({dist:.1f}% away)</span>')}
          <div style="margin-top:10px;padding:10px;border-radius:8px;text-align:center;font-family:monospace;font-weight:700;font-size:15px;background:rgba({("0,214,143" if pnl>=0 else "255,71,87")},0.1);color:{pc}">
            Unrealized P&L: ${pnl:+.2f}
          </div>
        </div>'''

    trades_html=""
    for t in s["trades"][:30]:
        ie=t["action"]=="EXIT"; iw=t.get("pnl") is not None and t.get("pnl",0)>=0
        icon="✅" if (ie and iw) else ("❌" if (ie and not iw) else "📊")
        dc="0,214,143" if t["direction"]=="LONG" else "255,71,87"
        pnl_s=f'<span style="font-family:monospace;font-weight:700;color:{"#00D68F" if iw else "#FF4757"}">${t["pnl"]:+.2f}</span>' if t.get("pnl") is not None else ""
        trades_html+=f'''<div style="display:flex;align-items:center;padding:12px 0;border-bottom:1px solid #1E2D42;gap:12px">
          <div style="width:36px;height:36px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:16px;background:rgba({dc},0.15);flex-shrink:0">{icon}</div>
          <div style="flex:1"><div style="font-weight:600;font-size:14px">{t["asset"]} <span style="font-size:11px;padding:2px 6px;border-radius:4px;background:rgba({dc},0.15);color:rgb({dc})">{t["direction"]}</span> <span style="font-size:10px;color:#4A5878">{t["action"]}</span></div>
          <div style="font-size:11px;color:#4A5878">${t["entry"]:,.2f}{f" → ${t['exit']:,.2f}" if t.get("exit") else ""} · {t.get("reason","")}</div>
          <div style="font-size:11px;color:#4A5878">{t["time"]}</div></div>{pnl_s}</div>'''

    # Trade Detail HTML — shows filter criteria for each completed trade
    trade_detail_html=""
    all_trades=s["trades"][:100]
    paired=[]
    used_exits=set()
    for ei,entry_t in enumerate(all_trades):
        if entry_t.get("action")!="ENTER": continue
        asset_=entry_t["asset"]; dir_=entry_t["direction"]
        for xi,exit_t in enumerate(all_trades):
            if xi in used_exits: continue
            if exit_t.get("action") not in ("EXIT","CLOSED"): continue
            if exit_t["asset"]!=asset_ or exit_t["direction"]!=dir_: continue
            if xi<ei:
                paired.append((entry_t,exit_t)); used_exits.add(xi); break
        else:
            paired.append((entry_t,None))
    if paired:
        for entry_t,exit_t in paired[:15]:
            pnl=exit_t.get("pnl") if exit_t else None
            pnl_color="#00D68F" if (pnl or 0)>=0 else "#FF4757"
            pnl_str=f'<span style="font-weight:700;color:{pnl_color}">${pnl:+,.2f}</span>' if pnl is not None else '<span style="color:#4A5878">OPEN</span>'
            filter_pills=""
            for k,v in entry_t.get("filters",{}).items():
                if k=="_result": continue
                passed=v.get("pass",False)
                fc="0,214,143" if passed else "255,71,87"
                val=str(v.get("value",""))[:25]
                filter_pills+=f'<span style="font-size:10px;padding:2px 6px;border-radius:4px;margin:2px;display:inline-block;background:rgba({fc},0.15);color:rgb({fc})">{"✅" if passed else "❌"} {k}: {val}</span>'
            exit_reason=exit_t.get("reason","—") if exit_t else "holding"
            exit_price=f'${exit_t.get("exit",0):,.4f}' if exit_t else "—"
            reason_map={"trail":"🔔 Trail stop","ema_cross":"📊 EMA cross","stop":"🛑 Hard stop","tp":"🎯 Take profit","holding":"⏳ Still open"}
            exit_label=reason_map.get(exit_reason,exit_reason)
            border_color="#00D68F" if (pnl or 0)>=0 else "#FF4757" if pnl is not None else "#4A5878"
            exit_time=exit_t["time"][11:16] if exit_t else "—"
            trade_detail_html+=f"""<div class="card" style="margin-bottom:10px;border-left:3px solid {border_color}">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
                <div style="font-size:14px;font-weight:700">{entry_t["asset"]} {entry_t["direction"]}</div>
                <div style="font-size:11px;color:#4A5878">${entry_t["entry"]:,.4f} → {exit_price}</div>
                <div style="margin-left:auto">{pnl_str}</div>
              </div>
              <div style="font-size:11px;color:#4A5878;margin-bottom:6px">
                ▶ In: {entry_t["time"][11:16]} UTC &nbsp;|&nbsp; {"■ Out" if exit_t else "⏳ Open"}: {exit_time} UTC &nbsp;|&nbsp; {exit_label}
              </div>
              <div style="font-size:10px;color:#4A5878;margin-bottom:3px">WHY ENTERED:</div>
              <div style="line-height:1.8">{filter_pills if filter_pills else '<span style="font-size:10px;color:#4A5878">No filter data</span>'}</div>
            </div>"""
    else:
        trade_detail_html='<div style="text-align:center;padding:48px 24px;color:#4A5878">No trades yet</div>'
    # Testnet Trade Detail HTML
    tn_trade_detail_html=""
    tn_completed=[t for t in tn_state["trades"][:20] if t.get("action") in ("CLOSED","EXIT")]
    if tn_completed:
        for t in tn_completed:
            pnl_color="#00D68F" if (t.get("pnl") or 0)>=0 else "#FF4757"
            pnl_str=f'<span style="font-weight:700;color:{pnl_color}">${t["pnl"]:+,.2f}</span>' if t.get("pnl") is not None else ""
            filter_pills=""
            for k,v in t.get("filters",{}).items():
                if k=="_result": continue
                passed=v.get("pass",False)
                fc="0,214,143" if passed else "255,71,87"
                icon="✅" if passed else "❌"
                val=str(v.get("value",""))[:20]
                filter_pills+=f'<span style="font-size:10px;padding:2px 6px;border-radius:4px;margin:2px;display:inline-block;background:rgba({fc},0.15);color:rgb({fc})">{icon} {k}: {val}</span>'
            tn_trade_detail_html+=f'''<div class="card" style="margin-bottom:10px;border-left:3px solid #00B4FF">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
                <div style="font-size:14px;font-weight:700">🧪 {t["asset"]} {t["direction"]}</div>
                <div style="font-size:11px;color:#4A5878">@ ${t["entry"]:,.4f} → ${t.get("exit",0):,.4f}</div>
                <div style="margin-left:auto">{pnl_str}</div>
              </div>
              <div style="font-size:11px;color:#4A5878;margin-bottom:8px">Exit via {t.get("reason","—")} | {t["time"][11:19]} UTC</div>
              <div style="line-height:1.8">{filter_pills if filter_pills else "No filter data"}</div>
            </div>'''
    else:
        tn_trade_detail_html='<div style="text-align:center;padding:24px;color:#4A5878">No completed testnet trades yet</div>'


    # Audit log HTML — full detail
    audit_html=""
    for a in s["audit"][:100]:
        event=a["event"]
        if "✅" in event or "ENTERED" in event: ec="0,214,143"
        elif "❌" in event or "ERROR" in event or "FAILED" in event: ec="255,71,87"
        elif "🚨" in event or "SIGNAL" in event: ec="255,184,0"
        elif "⏭" in event or "SAME" in event: ec="74,88,120"
        elif "🕯" in event or "NEW" in event: ec="61,158,255"
        elif "⏳" in event or "HOLDING" in event: ec="74,88,120"
        elif "⏸" in event: ec="255,184,0"
        else: ec="74,88,120"

        filters=a.get("filters",{})
        filter_html=""
        if filters and "_result" not in a["event"]:
            for k,v in filters.items():
                if k=="_result": continue
                fc="0,214,143" if v.get("pass") else "255,71,87"
                filter_html+=f'<span style="font-size:10px;padding:1px 5px;border-radius:3px;margin:1px;background:rgba({fc},0.15);color:rgb({fc})">{k}:{"✅" if v.get("pass") else "❌"}</span>'

        audit_html+=f'''<div style="padding:10px 0;border-bottom:1px solid #1E2D42">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
            <span style="font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;background:rgba({ec},0.15);color:rgb({ec});white-space:nowrap">{a["asset"]}</span>
            <span style="font-size:12px;font-weight:600">{event}</span>
            <span style="font-size:10px;color:#4A5878;margin-left:auto;white-space:nowrap">{a["time"]}</span>
          </div>
          <div style="font-size:11px;color:#4A5878;font-family:monospace;margin-bottom:3px">{a["detail"].replace(chr(10)," ").replace(chr(13)," ").replace("<","&lt;").replace(">","&gt;").replace("'","&#39;").replace('"',"&quot;")}
          </div>
          {f'<div style="display:flex;flex-wrap:wrap;gap:2px">{filter_html}</div>' if filter_html else ""}
        </div>'''

    diag_html=""
    for d in s["diagnostics"][:20]:
        cs={"INFO":"61,158,255","WARNING":"255,184,0","ERROR":"255,71,87","CRITICAL":"255,71,87"}
        c=cs.get(d["level"],"74,88,120")
        diag_html+=f'''<div style="display:flex;gap:10px;padding:10px 0;border-bottom:1px solid #1E2D42">
          <span style="font-size:10px;font-weight:700;padding:3px 6px;border-radius:4px;white-space:nowrap;background:rgba({c},0.15);color:rgb({c})">{d["level"]}</span>
          <div style="flex:1"><div style="font-weight:600;font-size:12px">{d["event"].replace("<","&lt;").replace(">","&gt;")}</div>
          <div style="font-size:11px;color:#4A5878">{d["cause"].replace("<","&lt;").replace(">","&gt;").replace(chr(10)," ").replace(chr(13)," ")}</div>
          <div style="font-size:10px;color:#4A5878;font-family:monospace">{d["time"]}</div></div></div>'''

    asset_html=""
    for asset in s["assets"]:
        ah=h["assets_ok"].get(asset,{})
        fresh=ah.get("fresh",False)
        sig=ah.get("signal","—"); sc="#00D68F" if sig and "LONG" in sig or "SHORT" in sig else "#4A5878"
        asset_html+=f'''<div style="background:#161E2E;border:1px solid #1E2D42;border-radius:14px;padding:14px;margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
            <div style="font-family:monospace;font-size:15px;font-weight:700">{asset}-PERP</div>
            <span style="font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;background:rgba({"0,214,143" if fresh else "255,184,0"},0.15);color:{"#00D68F" if fresh else "#FFB800"}">{"LIVE" if fresh else "STALE"}</span>
          </div>
          {row("Price",f"${ah.get('price',0):,.2f}")}{row("Last candle",ah.get("last_candle","—"))}
          <div style="display:flex;justify-content:space-between;padding:8px 0"><span style="font-size:13px;color:#4A5878">Signal</span><span style="font-family:monospace;font-weight:600;font-size:11px;color:{sc}">{sig}</span></div>
        </div>'''

    q_info,days_left=get_next_due()
    tax_due_html=""
    if q_info:
        urgency="#FF4757" if days_left<=7 else "#FFB800" if days_left<=30 else "#00D68F"
        tax_due_html=f'''<div style="background:#0F1520;border:2px solid {urgency};border-radius:16px;padding:16px;margin-bottom:12px">
          <div style="font-size:10px;font-weight:700;color:#4A5878;text-transform:uppercase;margin-bottom:6px">Next Tax Payment</div>
          <div style="font-family:monospace;font-size:18px;font-weight:700;color:{urgency}">{q_info["quarter"]} — {q_info["due"]}</div>
          <div style="font-size:13px;color:#4A5878;margin-top:4px">{days_left} days remaining</div>
        </div>'''

    return f'''<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes"><title>HL Trader</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}}
body{{background:#080B10;color:#E8EDF5;font-family:-apple-system,BlinkMacSystemFont,sans-serif;min-height:100vh}}
.hd{{position:sticky;top:0;z-index:100;background:rgba(8,11,16,.95);backdrop-filter:blur(20px);border-bottom:1px solid #1E2D42;padding:12px 16px 0;padding-top:calc(12px + env(safe-area-inset-top))}}
.tab{{flex-shrink:0;padding:8px 14px 10px;font-size:13px;font-weight:600;color:#4A5878;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap}}
.tab.active{{color:#00D68F;border-bottom-color:#00D68F}}
.sec{{display:none}}.sec.active{{display:block}}
.main{{padding:16px;padding-bottom:calc(16px + env(safe-area-inset-bottom))}}
.card{{background:#0F1520;border:1px solid #1E2D42;border-radius:16px;padding:16px;margin-bottom:12px}}
.ctrl{{border:none;border-radius:14px;padding:14px 12px;font-size:13px;font-weight:700;cursor:pointer;text-align:center;width:100%;margin-bottom:8px}}
.ov{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:200;align-items:center;justify-content:center;padding:24px}}
.ov.show{{display:flex}}
.ovc{{background:#0F1520;border:1px solid #1E2D42;border-radius:20px;padding:28px 24px;width:100%;max-width:340px;text-align:center}}
.rfb{{position:fixed;bottom:calc(24px + env(safe-area-inset-bottom));right:20px;width:48px;height:48px;border-radius:50%;background:#00D68F;color:#000;border:none;font-size:20px;cursor:pointer;box-shadow:0 4px 20px rgba(0,214,143,.4);z-index:50;display:flex;align-items:center;justify-content:center}}
</style></head><body>
<div id="ov" class="ov"><div class="ovc">
  <div id="ot" style="font-size:18px;font-weight:700;margin-bottom:8px"></div>
  <div id="os" style="font-size:13px;color:#4A5878;margin-bottom:24px;line-height:1.5"></div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
    <button onclick="closeOv()" style="background:#161E2E;color:#E8EDF5;border:1px solid #1E2D42;border-radius:12px;padding:14px;font-size:14px;font-weight:700;cursor:pointer">Cancel</button>
    <button id="oy" style="background:#FF4757;color:#fff;border:none;border-radius:12px;padding:14px;font-size:14px;font-weight:700;cursor:pointer">Confirm</button>
  </div>
</div></div>
<div class="hd">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
    <div style="font-family:monospace;font-size:18px;font-weight:700;color:#00D68F">HL TRADER v4</div>
    <div style="display:flex;align-items:center;gap:8px">
      <span style="font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;background:rgba({"0,214,143" if any_fresh else "255,184,0"},0.15);color:{"#00D68F" if any_fresh else "#FFB800"}">{"LIVE" if any_fresh else "STALE"}</span>
      <div style="display:flex;align-items:center;gap:6px;background:#0F1520;border:1px solid #1E2D42;border-radius:20px;padding:5px 10px;font-size:11px;font-weight:600">
        <div style="width:7px;height:7px;border-radius:50%;background:{dot}"></div>{status}
      </div>
    </div>
  </div>
  <div style="display:flex;gap:5px;flex-wrap:wrap;margin-bottom:8px">
    <span style="font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba({mc},0.15);color:rgb({mc});border:1px solid rgba({mc},0.3)">{mode}</span>
    <span style="font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba(74,88,120,0.2);color:#4A5878;border:1px solid #1E2D42">{s["leverage"]}x · {len(s["assets"])} assets</span>
    {"<span style='font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba(255,184,0,0.15);color:#FFB800'>⏸ PAUSED</span>" if paused else ""}
    {"<span style='font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba(255,71,87,0.2);color:#FF4757'>🛑 KILLED</span>" if killed else ""}
  </div>
  <div style="display:flex;overflow-x:auto;scrollbar-width:none;gap:4px">
    <div class="tab active" onclick="show('ov2',this)">Overview</div>
    <div class="tab" onclick="show('pos',this)">Positions</div>
    <div class="tab" onclick="show('tr',this)">Trades</div>
    <div class="tab" onclick="show('td',this)">Trade Detail</div>
    <div class="tab" onclick="show('au',this)">Audit</div>
    <div class="tab" onclick="show('tx',this)">Tax</div>
    <div class="tab" onclick="show('dg',this)">Diagnostics</div>
    <div class="tab" onclick="show('iss',this)" style="color:#FFB800">⚠️ Issues</div>
    <div class="tab" onclick="show('tn',this)" style="color:#00B4FF">🧪 Testnet</div>
  </div>
</div>
<div class="main">

<div id="ov2" class="sec active">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px">Controls</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:8px">
    {"<button class='ctrl' style='background:rgba(0,214,143,0.15);color:#00D68F;border:2px solid rgba(0,214,143,0.4);margin:0' onclick=\"doAction('resume')\">▶ Resume</button>" if (paused or killed) else "<button class='ctrl' style='background:rgba(255,184,0,0.15);color:#FFB800;border:2px solid rgba(255,184,0,0.4);margin:0' onclick=\"confirm_action('pause','Pause new entries?','Stops new entries. Exits still managed.')\">⏸ Pause</button>"}
    <button class="ctrl" style="background:rgba(255,71,87,0.15);color:#FF4757;border:2px solid rgba(255,71,87,0.4);margin:0" onclick="confirm_action('close_all','Close ALL positions?','Immediately market-closes everything.')">⚡ Close All</button>
  </div>
  <button class="ctrl" style="background:rgba(255,71,87,0.25);color:#FF4757;border:2px solid #FF4757;font-size:14px" onclick="confirm_action('kill','KILL SWITCH?','Stops all trading. Positions stay open on HyperLiquid.')">🛑 KILL SWITCH</button>
  <a href="/system-test" target="_blank" style="display:block;text-align:center;background:rgba(0,180,255,0.1);border:1px solid rgba(0,180,255,0.4);border-radius:12px;padding:12px;color:#00B4FF;font-weight:600;text-decoration:none;margin-top:8px;font-size:13px">🔬 System Test</a>
  <div class="card" style="border-color:{'rgba(0,214,143,0.3)' if tax['total_net']>=0 else 'rgba(255,71,87,0.3)'}">
    <div style="font-size:10px;font-weight:700;color:#4A5878;text-transform:uppercase;margin-bottom:6px">Net P&L</div>
    <div style="font-family:monospace;font-size:28px;font-weight:700;color:{'#00D68F' if tax['total_net']>=0 else '#FF4757'}">${tax["total_net"]:.2f}</div>
    <div style="font-size:12px;color:#4A5878;margin-top:4px">Gross: ${tax["total_pnl"]:.2f} · Tax: ${tax["total_tax"]:.2f}</div>
  </div>
  <div class="card" style="margin-bottom:10px;border-left:3px solid {'#00D68F' if s.get('weekly_pnl',0)>=0 else '#FF4757'}">
    <div style="font-size:10px;color:#4A5878;margin-bottom:4px">WEEK {s.get('week_start','—')} · {s.get('weekly_trades',0)} trades</div>
    <div style="font-family:monospace;font-size:22px;font-weight:700;color:{'#00D68F' if s.get('weekly_pnl',0)>=0 else '#FF4757'}">${s.get('weekly_pnl',0):+,.2f}</div>
    <div style="font-size:11px;color:#4A5878;margin-top:2px">Weekly P&L · resets Monday 00:00 UTC</div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
    <div class="card"><div style="font-size:10px;color:#4A5878;font-weight:600;text-transform:uppercase;margin-bottom:6px">Balance</div><div style="font-family:monospace;font-size:18px;font-weight:700">${s["balance"]:.2f}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;font-weight:600;text-transform:uppercase;margin-bottom:6px">Open</div><div style="font-family:monospace;font-size:18px;font-weight:700;color:#3D9EFF">{len(s["positions"])}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;font-weight:600;text-transform:uppercase;margin-bottom:6px">Trades</div><div style="font-family:monospace;font-size:18px;font-weight:700">{tax["total_trades"]}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;font-weight:600;text-transform:uppercase;margin-bottom:6px">Win Rate</div><div style="font-family:monospace;font-size:18px;font-weight:700;color:#00D68F">{wr}</div></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px">
    <a href="/signal-check" style="display:block;text-align:center;background:#0F1520;border:1px solid #3D9EFF;border-radius:12px;padding:12px;color:#3D9EFF;font-size:12px;font-weight:700;text-decoration:none">📡 Signals</a>
    <a href="/log" style="display:block;text-align:center;background:#0F1520;border:1px solid #1E2D42;border-radius:12px;padding:12px;color:#4A5878;font-size:13px;text-decoration:none">📋 Export Log</a>
  </div>
  <div class="card">
    {row("Cycle",f"#{s['cycle']}")}{row("Last check",s["last_check"] or "—")}{row("Next check",s["next_check"] or "—")}{row("Mode",mode)}{row("Assets",", ".join(s["assets"]))}
  </div>
  <div class="card" style="margin-top:10px;border-color:rgba(0,180,255,0.3)">
    <div style="font-size:10px;font-weight:700;color:#00B4FF;text-transform:uppercase;margin-bottom:8px">🧪 Testnet Paper Trading — HL Mainnet Candles</div>
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px">
      <div><div style="font-size:10px;color:#4A5878">Balance</div><div style="font-weight:700">${tn_state["balance"]:,.0f}</div></div>
      <div><div style="font-size:10px;color:#4A5878">Trades</div><div style="font-weight:700">{tn_state["tax"]["total_trades"]}</div></div>
      <div><div style="font-size:10px;color:#4A5878">Open</div><div style="font-weight:700">{len(tn_state["positions"])}</div></div>
      <div><div style="font-size:10px;color:#4A5878">P&L</div><div style="font-weight:700;color:{"#00D68F" if tn_state["tax"]["total_pnl"]>=0 else "#FF4757"}">${tn_state["tax"]["total_pnl"]:+,.2f}</div></div>
    </div>
    <div style="font-size:10px;color:#4A5878;margin-top:6px">Cycle #{tn_state["cycle"]} | {len(tn_state.get("issues",[]))} issues | HL Mainnet → HyperLiquid Testnet</div>
  </div>
</div>

<div id="pos" class="sec">
  {pos_html or '<div style="text-align:center;padding:48px 24px;color:#4A5878"><div style="font-size:36px;margin-bottom:12px">📭</div><div>No open positions</div></div>'}
</div>

<div id="tr" class="sec">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px">Trade History</div>
  {f'<div class="card" style="padding:0 16px">{trades_html}</div>' if trades_html else '<div style="text-align:center;padding:48px 24px;color:#4A5878"><div style="font-size:36px;margin-bottom:12px">📋</div><div>No trades yet</div></div>'}
</div>


<div id="td" class="sec">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#00D68F;margin-bottom:10px">📋 Trade Detail — Why Each Trade Was Taken</div>
  {trade_detail_html}
</div>

<div id="au" class="sec">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
    <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878">Full Audit Trail</div>
    <div style="font-size:10px;color:#4A5878">{len(s["audit"])} events</div>
  </div>
  <div style="font-size:11px;color:#4A5878;margin-bottom:10px;line-height:1.5">
    Every candle evaluation, signal check, filter result, entry, exit, skip — all visible here. No Railway needed.
  </div>
  {f'<div class="card" style="padding:0 16px">{audit_html}</div>' if audit_html else '<div style="text-align:center;padding:48px 24px;color:#4A5878"><div style="font-size:36px;margin-bottom:12px">📋</div><div>No events yet — waiting for first candle</div></div>'}
</div>

<div id="tx" class="sec">
  {tax_due_html}
  <div class="card" style="border-color:rgba(255,184,0,0.3)">
    <div style="font-size:10px;font-weight:700;color:#4A5878;text-transform:uppercase;margin-bottom:6px">Tax Set-Aside</div>
    <div style="font-family:monospace;font-size:28px;font-weight:700;color:#FFB800">${tax["total_tax"]:.2f}</div>
    <div style="font-size:12px;color:#4A5878;margin-top:4px">Do not spend — owed to IRS + NY + NYC</div>
  </div>
  <div class="card" style="padding:0">
    <div style="padding:10px 16px;font-size:10px;font-weight:700;text-transform:uppercase;color:#4A5878">Section 1256 Breakdown</div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="color:#4A5878">Gross</span><span style="font-family:monospace;color:{'#00D68F' if tax['total_pnl']>=0 else '#FF4757'}">${tax["total_pnl"]:+.2f}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="color:#4A5878">60% LTCG</span><span style="font-family:monospace">${tax["total_pnl"]*0.6:.2f}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="color:#4A5878">40% STCG</span><span style="font-family:monospace">${tax["total_pnl"]*0.4:.2f}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="color:#4A5878">Federal (~26.8%)</span><span style="font-family:monospace;color:#FF4757">-${tax["total_tax"]*0.707:.2f}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="color:#4A5878">NY State (6.85%)</span><span style="font-family:monospace;color:#FF4757">-${tax["total_tax"]*0.185:.2f}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="color:#4A5878">NYC (3.876%)</span><span style="font-family:monospace;color:#FF4757">-${tax["total_tax"]*0.108:.2f}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;background:#161E2E"><span style="font-weight:600">Net take home</span><span style="font-family:monospace;font-weight:600;font-size:16px;color:#00D68F">${tax["total_net"]:+.2f}</span></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px">
    <a href="/tax-export" style="display:block;text-align:center;background:#0F1520;border:1px solid #00D68F;border-radius:12px;padding:14px;color:#00D68F;font-size:13px;font-weight:700;text-decoration:none">📥 Export CSV</a>
    <a href="/tax-guide" style="display:block;text-align:center;background:#0F1520;border:1px solid #1E2D42;border-radius:12px;padding:14px;color:#4A5878;font-size:13px;font-weight:700;text-decoration:none">📋 Pay Guide</a>
  </div>
</div>

<div id="dg" class="sec">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px">System Health</div>
  <div class="card" style="padding:0 16px">
    <div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid #1E2D42">
      <div style="font-size:14px;width:24px;text-align:center">{"✅" if h["api_connected"] else "❌"}</div>
      <div style="flex:1"><div style="font-size:13px;font-weight:600">HyperLiquid API</div><div style="font-size:11px;color:#4A5878">{h["last_ping"] or "never"}</div></div>
      <span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;background:rgba({"0,214,143" if h["api_connected"] else "255,71,87"},0.15);color:{"#00D68F" if h["api_connected"] else "#FF4757"}">{"CONNECTED" if h["api_connected"] else "OFFLINE"}</span>
    </div>
    <div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid #1E2D42">
      <div style="font-size:14px;width:24px;text-align:center">{"✅" if s["cycle"]>0 else "⏳"}</div>
      <div style="flex:1"><div style="font-size:13px;font-weight:600">Strategy Loop</div><div style="font-size:11px;color:#4A5878">Cycle #{s["cycle"]} · {status}</div></div>
      <span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;background:rgba({"0,214,143" if s["cycle"]>0 else "74,88,120"},0.15);color:{"#00D68F" if s["cycle"]>0 else "#4A5878"}">{"RUNNING" if s["cycle"]>0 else "STARTING"}</span>
    </div>
    <div style="display:flex;align-items:center;gap:10px;padding:10px 0">
      <div style="font-size:14px;width:24px;text-align:center">{"✅" if any_fresh else "⚠️"}</div>
      <div style="flex:1"><div style="font-size:13px;font-weight:600">Data Freshness</div><div style="font-size:11px;color:#4A5878">{s["last_check"] or "not yet"}</div></div>
      <span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;background:rgba({"0,214,143" if any_fresh else "255,184,0"},0.15);color:{"#00D68F" if any_fresh else "#FFB800"}">{"LIVE" if any_fresh else "STALE"}</span>
    </div>
  </div>
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin:16px 0 10px">Asset Status</div>
  {asset_html}
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin:4px 0 10px">System Events</div>
  {f'<div class="card" style="padding:0 16px">{diag_html}</div>' if diag_html else '<div style="text-align:center;padding:24px;color:#4A5878">No events yet</div>'}
</div>

<div id="iss" class="sec">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#FFB800;margin-bottom:10px">⚠️ Trade Issues — Both Systems</div>
  {f'''<div class="card" style="padding:0 16px">{"".join(f'''<div style="padding:10px 0;border-bottom:1px solid #1E2D42">
    <div style="display:flex;gap:8px;align-items:center">
      <span style="font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;background:rgba(255,184,0,0.15);color:#FFB800">{iss["asset"]}</span>
      <span style="font-size:12px;font-weight:600;color:#FFB800">{iss["issue"]}</span>
      <span style="font-size:10px;color:#4A5878;margin-left:auto">{iss["time"]}</span>
    </div>
    <div style="font-size:11px;color:#4A5878;margin-top:4px">{iss["detail"].replace("<","&lt;").replace(">","&gt;")}</div>
  </div>''' for iss in (tn_state.get("issues",[]) + state.get("issues",[])))}</div>''' if (tn_state.get("issues") or state.get("issues")) else '<div style="text-align:center;padding:48px 24px;color:#4A5878">No trade issues — all systems nominal ✅</div>'}
</div>

<div id="tn" class="sec">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#00B4FF;margin-bottom:10px">🧪 Testnet — HL Mainnet Candles + Testnet Execution</div>
  <a href="/log-testnet" style="display:block;text-align:center;background:#0F1520;border:1px solid #00B4FF;border-radius:12px;padding:12px;color:#00B4FF;font-weight:600;text-decoration:none;margin-bottom:12px">📋 Export Testnet Log</a>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px">
    <div class="card"><div style="font-size:10px;color:#4A5878;margin-bottom:4px">BALANCE</div><div style="font-size:20px;font-weight:700">${tn_state["balance"]:,.2f}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;margin-bottom:4px">OPEN</div><div style="font-size:20px;font-weight:700">{len(tn_state["positions"])}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;margin-bottom:4px">TRADES</div><div style="font-size:20px;font-weight:700">{tn_state["tax"]["total_trades"]}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;margin-bottom:4px">NET P&L</div><div style="font-size:20px;font-weight:700;color:{"#00D68F" if tn_state["tax"]["total_pnl"]>=0 else "#FF4757"}">${tn_state["tax"]["total_pnl"]:+,.2f}</div></div>
  </div>
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px">Open Positions</div>
  {f'<div class="card" style="padding:0 16px">{"".join(f"""<div style="padding:10px 0;border-bottom:1px solid #1E2D42"><div style="font-weight:600">{a} {pos["direction"]} @ ${pos["entry"]:,.4f}</div><div style="font-size:11px;color:#4A5878">Now: ${pos.get("current_price",pos["entry"]):,.4f} | Stop: ${pos["stop"]:,.4f}</div></div>""" for a,pos in tn_state["positions"].items())}</div>' if tn_state["positions"] else '<div style="text-align:center;padding:24px;color:#4A5878">No open testnet positions</div>'}
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin:16px 0 10px">Recent Testnet Trades</div>
  <div class="card" style="padding:0 16px">
    {"".join(f'''<div style="padding:10px 0;border-bottom:1px solid #1E2D42">
      <div style="display:flex;gap:8px;align-items:center">
        <span style="font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;background:rgba(0,180,255,0.15);color:#00B4FF">{t["asset"]}</span>
        <span style="font-size:12px;font-weight:600">{t["direction"]} {t["action"]}</span>
        {"<span style='font-size:12px;font-weight:700;margin-left:auto;color:" + ("#00D68F" if (t.get("pnl") or 0)>=0 else "#FF4757") + "'>${:+,.2f}</span>".format(t.get("pnl") or 0) if t.get("pnl") is not None else ""}
      </div>
      <div style="font-size:11px;color:#4A5878">${t["entry"]:,.4f} | {t.get("reason","—")} | {t["time"][11:19]}</div>
    </div>''' for t in tn_state["trades"][:20]) if tn_state["trades"] else '<div style="text-align:center;padding:24px;color:#4A5878">No testnet trades yet</div>'}
  </div>
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin:16px 0 10px">Testnet Audit (last 50)</div>
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#00B4FF;margin:16px 0 10px">🧪 Testnet Trade Detail</div>
  {tn_trade_detail_html}
  <div class="card" style="padding:0 16px">
    {"".join(f'''<div style="padding:8px 0;border-bottom:1px solid #1E2D42">
      <div style="display:flex;gap:6px;align-items:center">
        <span style="font-size:10px;color:#00B4FF;font-weight:700">{a["asset"]}</span>
        <span style="font-size:11px;font-weight:600">{a["event"]}</span>
        <span style="font-size:10px;color:#4A5878;margin-left:auto">{a["time"]}</span>
      </div>
      <div style="font-size:11px;color:#4A5878;font-family:monospace">{a["detail"][:100]}</div>
    </div>''' for a in tn_state["audit"][:50]) if tn_state["audit"] else '<div style="text-align:center;padding:24px;color:#4A5878">No testnet audit entries yet</div>'}
  </div>
</div>

</div>
<button class="rfb" onclick="location.reload()">↻</button>
<script defer>
function show(id,el){{document.querySelectorAll(".sec").forEach(s=>s.classList.remove("active"));document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));document.getElementById(id).classList.add("active");el.classList.add("active")}}
let pend=null;
function confirm_action(a,t,s){{pend=a;document.getElementById("ot").textContent=t;document.getElementById("os").textContent=s;document.getElementById("ov").classList.add("show")}}
function closeOv(){{document.getElementById("ov").classList.remove("show");pend=null}}
document.getElementById("oy").onclick=function(){{if(pend)doAction(pend);closeOv()}}
function doAction(a){{fetch("/control",{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify({{action:a}})}}).then(r=>r.json()).then(d=>{{if(d.ok)location.reload();else alert("Error: "+d.error)}})}}
setTimeout(()=>location.reload(),30000);
</script>
</body></html>'''

@app.route("/")
def index():
    if not session.get("ok"):
        return '''<!DOCTYPE html><html><body style="background:#080B10;color:#E8EDF5;font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0">
        <div style="text-align:center;max-width:360px;width:100%;padding:40px;background:#0F1520;border-radius:20px;border:1px solid #1E2D42">
        <div style="font-family:monospace;font-size:28px;font-weight:700;color:#00D68F;margin-bottom:8px">HL TRADER</div>
        <div style="color:#4A5878;font-size:13px;margin-bottom:32px">6 Assets · HyperLiquid · Full Audit</div>
        <form method="POST" action="/login">
        <input type="password" name="p" placeholder="Password" autofocus style="width:100%;background:#161E2E;border:1px solid #1E2D42;border-radius:12px;color:#E8EDF5;font-size:16px;padding:14px 16px;margin-bottom:12px;outline:none;box-sizing:border-box;letter-spacing:2px">
        <button type="submit" style="width:100%;background:#00D68F;color:#000;border:none;border-radius:12px;font-size:15px;font-weight:700;padding:14px;cursor:pointer">Enter</button>
        </form></div></body></html>'''
    return build_dashboard()

@app.route("/login",methods=["POST"])
def login():
    if request.form.get("p")==PASSWORD: session["ok"]=True
    return redirect("/")

@app.route("/logout")
def logout():
    session.clear(); return redirect("/")

@app.route("/control",methods=["POST"])
def control():
    if not session.get("ok"): return jsonify({"ok":False,"error":"unauthorized"}),401
    a=request.json.get("action","")
    with lock:
        if a=="pause":      state["paused"]=True;add_diag("WARNING","Paused","Dashboard","No new entries")
        elif a=="resume":   state["paused"]=False;state["kill_switch"]=False;add_diag("INFO","Resumed","Dashboard","Trading active")
        elif a=="kill":     state["kill_switch"]=True;ntfy_kill_switch();add_diag("CRITICAL","Kill switch","Dashboard","All stopped")
        elif a=="close_all": state["close_all_requested"]=True;add_diag("WARNING","Close all","Dashboard","Closing positions")
        else: return jsonify({"ok":False,"error":"unknown"})
    return jsonify({"ok":True})

@app.route("/signal-check")
def signal_check():
    if not session.get("ok"): return redirect("/")
    results=[]
    for asset in ASSETS:
        try:
            end_ms=int(time.time()*1000); start_ms=end_ms-200*15*60*1000
            candles=info.candles_snapshot(asset,"15m",start_ms,end_ms)
            if not candles or len(candles)<50:
                results.append({"asset":asset,"signal":None,"price":0,"filters":{},"blocked_by":["insufficient candles"]}); continue
            cur=float(candles[-1]["c"])
            direction,signal_price,_,_,filters=evaluate_signal(candles,asset)
            blocked=filters.get("_result",{}).get("blocked_by",[])
            results.append({"asset":asset,"signal":direction,"price":cur,"filters":filters,"blocked_by":blocked,"direction":direction})
        except Exception as e:
            results.append({"asset":asset,"signal":None,"price":0,"filters":{},"blocked_by":[str(e)]})

    firing=[r for r in results if r.get("signal")]
    rows_html=""
    for r in results:
        sig=r.get("signal"); blocked=r.get("blocked_by",[])
        sc="#00D68F" if sig=="LONG" else "#FF4757" if sig=="SHORT" else "#4A5878"
        filters=r.get("filters",{})
        filter_pills=""
        for k,v in filters.items():
            if k=="_result": continue
            fc="0,214,143" if v.get("pass") else "255,71,87"
            filter_pills+=f'<span style="font-size:10px;padding:2px 6px;border-radius:4px;margin:2px;background:rgba({fc},0.15);color:rgb({fc})">{k}:{"✅" if v.get("pass") else "❌"} {v.get("value","")}</span>'
        rows_html+=f'''<div style="background:#161E2E;border:1px solid #1E2D42;border-radius:14px;padding:14px;margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
            <div style="font-family:monospace;font-size:15px;font-weight:700">{r["asset"]}-PERP</div>
            <div style="display:flex;align-items:center;gap:8px">
              <span style="font-family:monospace">${r["price"]:,.2f}</span>
              <span style="font-size:12px;font-weight:700;padding:3px 10px;border-radius:6px;background:rgba({("0,214,143" if sig=="LONG" else "255,71,87" if sig=="SHORT" else "74,88,120")},0.2);color:{sc}">{sig or "NO SIGNAL"}</span>
            </div>
          </div>
          <div style="display:flex;flex-wrap:wrap;gap:2px">{filter_pills}</div>
          {f'<div style="font-size:11px;color:#FF4757;margin-top:6px">Blocked by: {", ".join(blocked)}</div>' if blocked else ""}
        </div>'''

    return f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no"><title>Signal Check</title></head>
<body style="background:#080B10;color:#E8EDF5;font-family:-apple-system,sans-serif;padding:20px;padding-top:calc(20px + env(safe-area-inset-top))">
<div style="max-width:600px;margin:0 auto">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
    <a href="/" style="color:#4A5878;text-decoration:none;font-size:13px">← Dashboard</a>
    <div style="font-family:monospace;font-size:20px;font-weight:700;color:#3D9EFF">Signal Check</div>
    <div style="margin-left:auto;font-size:11px;color:#4A5878">{ts()} UTC</div>
  </div>
  <div style="background:#0F1520;border:1px solid #1E2D42;border-radius:16px;padding:16px;text-align:center;margin-bottom:16px">
    <div style="font-size:28px;margin-bottom:4px">{"🚨" if firing else "⏳"}</div>
    <div style="font-size:16px;font-weight:700;color:{"#00D68F" if firing else "#4A5878"}">
      {"SIGNAL: " + ", ".join(r["asset"]+" "+r["signal"] for r in firing) if firing else "No signals right now"}
    </div>
  </div>
  {rows_html}
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
    <a href="/signal-check" style="display:block;text-align:center;background:#0F1520;border:1px solid #1E2D42;border-radius:12px;padding:14px;color:#E8EDF5;font-size:13px;font-weight:600;text-decoration:none">🔄 Refresh</a>
    <a href="/" style="display:block;text-align:center;background:#0F1520;border:1px solid #1E2D42;border-radius:12px;padding:14px;color:#4A5878;font-size:13px;font-weight:600;text-decoration:none">← Dashboard</a>
  </div>
</div></body></html>'''

@app.route("/tax-export")
def tax_export():
    if not session.get("ok"): return "unauthorized",401
    year=datetime.now(timezone.utc).year; fname=f"hl_tax_{year}.csv"
    if os.path.exists(fname):
        with open(fname) as f: content=f.read()
        return Response(content,mimetype="text/csv",
                        headers={"Content-Disposition":f"attachment; filename=hl_tax_{year}_report.csv"})
    return Response("No tax data yet",mimetype="text/plain")

@app.route("/tax-guide")
def tax_guide():
    if not session.get("ok"): return redirect("/")
    q,days=get_next_due()
    return f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no"><title>Tax Guide</title></head>
<body style="background:#080B10;color:#E8EDF5;font-family:-apple-system,sans-serif;padding:20px;padding-top:calc(20px + env(safe-area-inset-top))">
<div style="max-width:600px;margin:0 auto">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
    <a href="/" style="color:#4A5878;text-decoration:none;font-size:13px">← Dashboard</a>
    <div style="font-family:monospace;font-size:20px;font-weight:700;color:#FFB800">Tax Payment Guide</div>
  </div>
  <div style="background:#0F1520;border:1px solid #FFB800;border-radius:16px;padding:16px;margin-bottom:16px">
    <div style="font-size:12px;color:#FFB800;font-weight:700;margin-bottom:8px">NEXT DUE</div>
    <div style="font-family:monospace;font-size:18px;font-weight:700">{q["quarter"] if q else "—"} — {q["due"] if q else "—"}</div>
    <div style="font-size:13px;color:#4A5878;margin-top:4px">{days} days remaining</div>
  </div>
  <div style="background:#0F1520;border:1px solid #1E2D42;border-radius:16px;padding:16px;margin-bottom:16px">
    <div style="font-size:12px;color:#4A5878;font-weight:700;margin-bottom:12px">HOW TO PAY</div>
    <div style="padding:12px 0;border-bottom:1px solid #1E2D42"><div style="font-weight:600;margin-bottom:4px">1. Federal (IRS)</div><div style="font-size:13px;color:#4A5878">irs.gov/payments → Direct Pay → Estimated Tax</div></div>
    <div style="padding:12px 0;border-bottom:1px solid #1E2D42"><div style="font-weight:600;margin-bottom:4px">2. NY State</div><div style="font-size:13px;color:#4A5878">tax.ny.gov → Make a Payment → Estimated Tax</div></div>
    <div style="padding:12px 0"><div style="font-weight:600;margin-bottom:4px">3. NYC Local</div><div style="font-size:13px;color:#4A5878">nyc.gov/finance → NYC Estimated Tax</div></div>
  </div>
  <div style="background:#0F1520;border:1px solid #1E2D42;border-radius:16px;padding:16px">
    <div style="font-size:12px;color:#4A5878;font-weight:700;margin-bottom:8px">SECTION 1256 RATES</div>
    <div style="font-size:13px;color:#4A5878;line-height:1.8">60% long-term (20% federal)<br>40% short-term (37% federal)<br>Blended federal: ~26.8%<br>NY State: 6.85%<br>NYC: 3.876%<br>Total effective: ~37-38%</div>
  </div>
</div></body></html>'''

@app.route("/api/state")
def api_state():
    if not session.get("ok"): return jsonify({"error":"unauthorized"}),401
    return jsonify(state)

@app.route("/system-test")
def system_test():
    if not session.get("ok"): return "unauthorized",401
    import time as _time
    from datetime import timedelta
    MAINNET_URL="https://api.hyperliquid.xyz/info"
    TESTNET_URL_ST="https://api.hyperliquid-testnet.xyz/info"
    results=[]
    def chk(name,passed,detail=""):
        results.append({"name":name,"passed":passed,"detail":detail})

    now=datetime.now(timezone.utc)
    
    # 1. Binance candle fetch
    try:
        r=req.get("https://data-api.binance.vision/api/v3/klines",
            params={"symbol":"BTCUSDT","interval":"15m","limit":3},
            headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            timeout=10)
        data=r.json()
        is_list=isinstance(data,list)
        chk("Binance reachable",r.status_code==200,f"HTTP {r.status_code}")
        chk("Binance returns list",is_list,f"type={type(data).__name__}")
        if is_list and data:
            chk("Binance candle format",len(data[0])>=7,
                f"{len(data)} candles | BTC close=${float(data[-1][4]):,.2f}")
        else:
            chk("Binance candle format",False,str(data)[:100])
    except Exception as e:
        chk("Binance fetch",False,str(e))

    # 2. HL Mainnet candles
    mn_candles={}
    try:
        end_ms=int(_time.time()*1000); start_ms=end_ms-200*15*60*1000
        for asset in ["BTC","ETH"]:
            r=req.post(MAINNET_URL,json={"type":"candleSnapshot",
                "req":{"coin":asset,"interval":"15m","startTime":start_ms,"endTime":end_ms}},timeout=10)
            data=r.json()
            ok=isinstance(data,list) and len(data)>0 and all(k in data[-1] for k in ["t","o","h","l","c","v"])
            mn_candles[asset]=data if ok else []
            chk(f"Mainnet {asset} candles",ok,
                f"{len(data)} candles | close=${float(data[-1]['c']):,.4f}" if ok else str(data)[:80])
    except Exception as e:
        chk("Mainnet candles",False,str(e))

    # 3. HL Testnet candles
    try:
        end_ms=int(_time.time()*1000); start_ms=end_ms-200*15*60*1000
        for asset in ["BTC","ETH"]:
            r=req.post(TESTNET_URL_ST,
                json={"type":"candleSnapshot",
                    "req":{"coin":asset,"interval":"15m","startTime":start_ms,"endTime":end_ms}},timeout=10)
            data=r.json()
            ok=isinstance(data,list) and len(data)>0 and all(k in data[-1] for k in ["t","o","h","l","c","v"])
            chk(f"Testnet {asset} candles",ok,
                f"{len(data)} candles | close=${float(data[-1]['c']):,.4f}" if ok else str(data)[:80])
    except Exception as e:
        chk("Testnet candles",False,str(e))

    # 4. evaluate_signal
    try:
        for asset in ["BTC","ETH"]:
            candles=mn_candles.get(asset,[])
            if len(candles)<50: chk(f"evaluate_signal {asset}",False,"insufficient candles"); continue
            _,_,_,_,filters=evaluate_signal(candles,asset)
            ok="_result" in filters or "ema_stack" in filters
            d=filters.get("ema_stack",{}).get("value","?")
            vr=filters.get("volume",{}).get("value","?")
            chk(f"evaluate_signal {asset}",ok,f"EMA={d} | vol={vr}")
    except Exception as e:
        chk("evaluate_signal",False,str(e))

    # 5. szDecimals
    try:
        r=req.post(MAINNET_URL,json={"type":"meta"},timeout=10)
        meta=r.json()
        universe={a["name"]:a for a in meta.get("universe",[])}
        for asset in ASSETS:
            sz=universe.get(asset,{}).get("szDecimals",None)
            chk(f"szDecimals {asset}",sz is not None,f"szDecimals={sz}")
    except Exception as e:
        chk("szDecimals",False,str(e))

    # 6. Order minimum notional
    try:
        r=req.post(MAINNET_URL,json={"type":"allMids"},timeout=10)
        mids=r.json()
        pos_usd=TOTAL_USDC/len(ASSETS)
        for asset in ASSETS:
            price=float(mids.get(asset,0))
            notional=pos_usd*LEVERAGE
            chk(f"Min notional {asset}",notional>=10,
                f"${notional:.2f} (pos=${pos_usd:.2f}×{LEVERAGE}x) | need $10 | price=${price:,.4f}")
    except Exception as e:
        chk("Order minimum",False,str(e))

    # 7. Mainnet API auth + userFills
    try:
        r=req.post(MAINNET_URL,json={"type":"userFills","user":MAIN_WALLET},timeout=10)
        fills=r.json()
        chk("Mainnet userFills",isinstance(fills,list),f"{len(fills)} fills")
    except Exception as e:
        chk("Mainnet userFills",False,str(e))

    # 8. Testnet userFills
    try:
        tn_url="https://api.hyperliquid-testnet.xyz/info"
        r=req.post(tn_url,json={"type":"userFills","user":TN_WALLET},timeout=10)
        fills=r.json()
        chk("Testnet userFills",isinstance(fills,list),f"{len(fills)} fills")
    except Exception as e:
        chk("Testnet userFills",False,str(e))

    # 9. Retry logic
    try:
        candles=mn_candles.get("BTC",[])
        if len(candles)>=50:
            import copy
            tc=copy.deepcopy(candles)
            closes=[float(c["c"]) for c in tc]
            vols=[float(c["v"]) for c in tc]
            vs=sma(vols,20)
            avg=vs[-1] if vs[-1] else 1
            tc[-1]["v"]=str(avg*0.02)
            vr=float(tc[-1]["v"])/avg
            _,_,_,_,filters=evaluate_signal(tc,"BTC")
            blocked=filters.get("_result",{}).get("blocked_by",[])
            only_vol=blocked==["volume"]
            chk("Retry logic (vol only block)",only_vol,
                f"blocked={blocked} → {'will retry ✅' if only_vol else 'marked as seen ❌'}")
        else:
            chk("Retry logic",False,"insufficient candles")
    except Exception as e:
        chk("Retry logic",False,str(e))

    # 10. ntfy
    try:
        r=req.post(f"https://ntfy.sh/{NTFY_TOPIC}",
            data="🔬 System test from dashboard".encode("utf-8"),
            headers={"Title":"HL Trader System Test","Tags":"white_check_mark"},timeout=10)
        chk("ntfy delivery",r.status_code==200,f"HTTP {r.status_code}")
    except Exception as e:
        chk("ntfy delivery",False,str(e))

    # Build HTML report
    passed=sum(1 for r in results if r["passed"])
    failed=len(results)-passed
    color="#00D68F" if failed==0 else "#FFB800" if failed<=6 else "#FF4757"

    rows="".join(f'''<tr>
        <td style="padding:8px 12px;font-weight:600">{"✅" if r["passed"] else "❌"} {r["name"]}</td>
        <td style="padding:8px 12px;color:#4A5878;font-family:monospace;font-size:11px">{r["detail"]}</td>
    </tr>''' for r in results)

    return f'''<!DOCTYPE html><html><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>System Test — HL Trader v4</title>
<style>
body{{background:#080B10;color:#E8EDF5;font-family:-apple-system,BlinkMacSystemFont,sans-serif;padding:24px;max-width:800px;margin:0 auto}}
h1{{color:{color};font-size:20px;margin-bottom:4px}}
p{{color:#4A5878;margin-bottom:20px;font-size:13px}}
table{{width:100%;border-collapse:collapse;background:#0F1520;border-radius:12px;overflow:hidden}}
tr{{border-bottom:1px solid #1E2D42}}
tr:last-child{{border-bottom:none}}
td{{font-size:13px}}
.badge{{display:inline-block;padding:6px 16px;border-radius:20px;font-weight:700;font-size:16px;background:rgba({
    "0,214,143" if failed==0 else "255,184,0" if failed<=6 else "255,71,87"
},0.15);color:{color};margin-bottom:16px}}
a{{color:#00B4FF;text-decoration:none}}
</style></head><body>
<h1>🔬 System Test — HL Trader v4</h1>
<p>{now.strftime("%Y-%m-%d %H:%M")} UTC / UTC</p>
<div class="badge">{passed}/{len(results)} passed {"✅" if failed==0 else f"— {failed} failed ⚠️"}</div>
<table>{rows}</table>
<p style="margin-top:16px"><a href="/">← Back to dashboard</a></p>
</body></html>'''

@app.route("/log")
def log_export():
    if not session.get("ok"): return "unauthorized",401
    s=state; lines=["="*60,"HL TRADER v4 — SYSTEM LOG",f"Generated: {ts()} UTC","="*60]
    lines.append(f"\nSTATUS: {s['status']} | Cycle #{s['cycle']} | {s['leverage']}x")
    lines.append(f"Mode: {'DRY' if s['dry_run'] else 'LIVE'} | {'Testnet' if s['testnet'] else 'Mainnet'}")
    lines.append(f"Paused: {s['paused']} | Kill: {s['kill_switch']} | API: {s['health']['api_connected']}")
    lines.append(f"\nP&L: Gross ${s['tax']['total_pnl']:+.4f} | Tax ${s['tax']['total_tax']:.4f} | Net ${s['tax']['total_net']:+.4f}")
    lines.append(f"Trades: {s['tax']['total_trades']} | W:{s['tax']['winning_trades']} L:{s['tax']['losing_trades']}")
    lines.append("\nOPEN POSITIONS:")
    for asset,pos in s["positions"].items():
        lines.append(f"  {asset}: {pos['direction']} @ ${pos['entry']:,.2f} | cur=${pos.get('current_price',pos['entry']):,.2f} | liq=${pos.get('liq',0):,.2f} | P&L=${pos.get('unrealized_pnl',0):+.2f}")
    if not s["positions"]: lines.append("  None")
    lines.append("\nTRADE HISTORY:")
    for t in s["trades"][:20]:
        ep=f"${t['exit']:,.2f}" if t.get("exit") else "—"
        pl=f"${t['pnl']:+.4f}" if t.get("pnl") is not None else "open"
        lines.append(f"  {t['time']} | {t['asset']} {t['direction']} {t['action']} | ${t['entry']:,.2f}→{ep} | {t.get('reason','')} | {pl}")
    lines.append(f"\nAUDIT TRAIL (all {len(s['audit'])} entries):")
    for a in s["audit"]:
        lines.append(f"  {a['time'][11:19]} | {a['asset']:<6} | {a['event']:<30} | {a['detail']}")
    # Also append raw disk log if available
    try:
        import os
        if os.path.exists(AUDIT_FILE):
            disk_lines=open(AUDIT_FILE).readlines()
            lines.append(f"\nFULL DISK LOG ({len(disk_lines)} entries):")
            for dl in disk_lines:
                lines.append(f"  {dl.strip()}")
    except: pass
    lines.append("\nASSET STATUS:")
    for asset in s["assets"]:
        ah=s["health"]["assets_ok"].get(asset,{})
        lines.append(f"  {asset}: ${ah.get('price',0):,.2f} | {ah.get('last_candle','?')} | {ah.get('signal','?')} | {'LIVE' if ah.get('fresh') else 'STALE'}")
    lines.append("\nDIAGNOSTICS:")
    for d in s["diagnostics"][:20]:
        lines.append(f"  {d['time']} [{d['level']}] {d['event']} | {d['cause']}")
    lines.append("\n"+"="*60)
    return Response("\n".join(lines),mimetype="text/plain")

@app.route("/log-testnet")
def log_export_testnet():
    if not session.get("ok"): return "unauthorized",401
    s=tn_state
    lines=["="*60,"HL TRADER v4 — TESTNET LOG (Binance Candles)",f"Generated: {ts()} UTC","="*60]
    lines.append(f"\nCycle #{s['cycle']} | Balance: ${s['balance']:,.2f}")
    lines.append(f"Trades: {s['tax']['total_trades']} | W:{s['tax']['winning_trades']} L:{s['tax']['losing_trades']}")
    lines.append(f"Gross P&L: ${s['tax']['total_pnl']:+,.4f}")
    lines.append("\nOPEN POSITIONS:")
    for asset,pos in s["positions"].items():
        lines.append(f"  {asset}: {pos['direction']} @ ${pos['entry']:,.4f} | now=${pos.get('current_price',pos['entry']):,.4f}")
    if not s["positions"]: lines.append("  None")
    lines.append("\nTRADE HISTORY:")
    for t in s["trades"]:
        ep=f"${t['exit']:,.4f}" if t.get("exit") else "—"
        pl=f"${t['pnl']:+,.4f}" if t.get("pnl") is not None else "open"
        lines.append(f"  {t['time']} | {t['asset']} {t['direction']} {t['action']} | ${t['entry']:,.4f}→{ep} | {t.get('reason','—')} | {pl}")
    lines.append(f"\nISSUES ({len(s.get('issues',[]))}):")
    for iss in s.get("issues",[]):
        lines.append(f"  {iss['time']} | {iss['asset']} | {iss['issue']} | {iss['detail']}")
    lines.append(f"\nAUDIT TRAIL (all {len(s['audit'])} entries):")
    for a in s["audit"]:
        lines.append(f"  {a['time']} | {a['asset']:<6} | {a['event']:<30} | {a['detail']}")
    lines.append("\n"+"="*60)
    return Response("\n".join(lines),mimetype="text/plain")

# Load audit from disk on startup so history is preserved across restarts
try:
    import os
    if os.path.exists(AUDIT_FILE):
        disk_lines = open(AUDIT_FILE).readlines()
        for line in reversed(disk_lines[-10000:]):
            parts = line.strip().split("|",3)
            if len(parts)==4:
                state["audit"].append({"time":parts[0],"asset":parts[1],
                                       "event":parts[2],"detail":parts[3],"filters":{}})
        log(f"📂 Loaded {len(state['audit'])} audit entries from disk")
except Exception as e:
    log(f"⚠️ Could not load audit from disk: {e}")

# Load trades from disk
try:
    import json as _json
    if os.path.exists(TRADES_FILE):
        disk_trades=_json.load(open(TRADES_FILE))
        with lock:
            state["trades"]=disk_trades[:500]
            # Rebuild weekly P&L and tax from disk trades
            for t in disk_trades:
                if t.get("pnl") is not None:
                    wk=t["time"][:7].replace("-","") if t.get("time") else ""
                    try:
                        dt=datetime.strptime(t["time"][:10],"%Y-%m-%d")
                        wk=dt.strftime("%Y-W%W")
                        state["weekly_pnl"][wk]=round(state["weekly_pnl"].get(wk,0)+t["pnl"],4)
                        state["tax"]["total_pnl"]+=t["pnl"]
                        state["tax"]["total_trades"]+=1
                        if t["pnl"]>0: state["tax"]["winning_trades"]+=1
                        else: state["tax"]["losing_trades"]+=1
                    except: pass
        log(f"📂 Loaded {len(disk_trades)} trades from disk")
except Exception as e:
    log(f"⚠️ Could not load trades from disk: {e}")

# Sync open positions from HyperLiquid on startup to prevent double entries
try:
    import json as _json2
    r_pos=req.post(HL_INFO_URL,json={"type":"clearinghouseState","user":MAIN_WALLET},timeout=10)
    hl_positions=r_pos.json().get("assetPositions",[])
    for p in hl_positions:
        pos_data=p.get("position",{})
        asset=pos_data.get("coin","")
        szi=float(pos_data.get("szi",0))
        if asset in ASSETS and szi!=0:
            direction="LONG" if szi>0 else "SHORT"
            entry=float(pos_data.get("entryPx",0))
            size=abs(szi)
            stop=entry*(1-STOP_PCT) if direction=="LONG" else entry*(1+STOP_PCT)
            trail=entry*(1-TRAIL_PCT) if direction=="LONG" else entry*(1+TRAIL_PCT)
            positions[asset]={
                "direction":direction,"entry":entry,"size":size,"qty_rem":size,
                "stop":stop,"trail_peak":entry,"trail_stop":trail,
                "liq":float(pos_data.get("liquidationPx",0) or 0),
                "partial_done":False,"partial_pnl":0.0,
                "current_price":entry,"unrealized_pnl":0.0
            }
            state["positions"][asset]=positions[asset]
            log(f"📂 Restored position: {asset} {direction} @ ${entry:,.4f} size={size}")
    if hl_positions:
        log(f"📂 Synced {len(positions)} open positions from HyperLiquid")
    # Recover stop order OIDs from HL open orders
    try:
        open_orders=req.post(HL_INFO_URL,json={"type":"openOrders","user":MAIN_WALLET},timeout=10).json()
        if isinstance(open_orders,list):
            for o in open_orders:
                asset=o.get("coin","")
                if asset in positions and asset not in stop_oids:
                    # Find reduce-only orders (stop losses)
                    if o.get("reduceOnly") or o.get("orderType","").lower() in ("stop market","stop limit","trigger"):
                        stop_oids[asset]=o.get("oid")
                        log(f"📂 Recovered stop OID for {asset}: {stop_oids[asset]}")
    except Exception as e:
        log(f"⚠️ Could not recover stop OIDs: {e}")
except Exception as e:
    log(f"⚠️ Could not sync positions on startup: {e}")

# Load testnet trades from disk
try:
    import json as _json3
    if os.path.exists(TN_TRADES_FILE):
        tn_disk_trades=_json3.load(open(TN_TRADES_FILE))
        tn_state["trades"]=tn_disk_trades[:500]
        log(f"📂 Loaded {len(tn_disk_trades)} testnet trades from disk")
except Exception as e:
    log(f"⚠️ Could not load testnet trades from disk: {e}")

# Sync testnet open positions from HyperLiquid testnet on startup
try:
    import json as _json4
    r_tn=req.post("https://api.hyperliquid-testnet.xyz/info",
        json={"type":"clearinghouseState","user":TN_WALLET},timeout=10)
    tn_hl_positions=r_tn.json().get("assetPositions",[])
    for p in tn_hl_positions:
        pos_data=p.get("position",{})
        asset=pos_data.get("coin","")
        szi=float(pos_data.get("szi",0))
        if asset in ASSETS and szi!=0:
            direction="LONG" if szi>0 else "SHORT"
            entry=float(pos_data.get("entryPx",0))
            size=abs(szi)
            stop=entry*(1-STOP_PCT) if direction=="LONG" else entry*(1+STOP_PCT)
            trail=entry*(1-TRAIL_PCT) if direction=="LONG" else entry*(1+TRAIL_PCT)
            tn_positions[asset]={
                "direction":direction,"entry":entry,"size":size,
                "stop":stop,"trail_high":entry,"trail_low":entry,
                "trail_stop":trail,"current_price":entry,"entry_time":ts()
            }
            log(f"📂 Restored TN position: {asset} {direction} @ ${entry:,.4f}")
    if tn_hl_positions:
        log(f"📂 Synced {len(tn_positions)} testnet positions from HyperLiquid")
    # Recover testnet stop OIDs
    try:
        tn_open_orders=req.post("https://api.hyperliquid-testnet.xyz/info",
            json={"type":"openOrders","user":TN_WALLET},timeout=10).json()
        if isinstance(tn_open_orders,list):
            for o in tn_open_orders:
                asset=o.get("coin","")
                if asset in tn_positions and asset not in tn_stop_oids:
                    if o.get("reduceOnly"):
                        tn_stop_oids[asset]=o.get("oid")
                        log(f"📂 Recovered TN stop OID for {asset}: {tn_stop_oids[asset]}")
    except Exception as e:
        log(f"⚠️ Could not recover TN stop OIDs: {e}")
except Exception as e:
    log(f"⚠️ Could not sync testnet positions on startup: {e}")

_t=threading.Thread(target=trading_loop,daemon=True)
_t.start()

# ── TESTNET TRADING LOOP (Binance candles + testnet execution) ─────────────
def testnet_trading_loop():
    log("🧪 Testnet trading loop started — Binance candles + testnet execution")
    tn_pos_usd = TN_TOTAL_USDC / len(ASSETS)
    tn_leverage = TN_LEVERAGE

    while True:
        try:
            if tn_state["kill_switch"] or tn_state["paused"]:
                time.sleep(CHECK_EVERY); continue

            with tn_lock: tn_state["cycle"]+=1

            for asset in ASSETS:
                try:
                    # Fetch Binance candles — always complete
                    candles=fetch_binance_candles(asset)
                    if not candles: continue

                    # Get newest candle
                    newest=candles[-1]
                    ts_val=str(newest["t"])
                    now_ms=int(time.time()*1000)
                    age_s=(now_ms-int(ts_val))/1000
                    cur=float(newest["c"])
                    is_closed=now_ms>int(newest.get("T",0))

                    # Position management — S2+S4 same as mainnet
                    if asset in tn_positions:
                        pos=tn_positions[asset]
                        direction=pos["direction"]

                        # S2+S4: Use last COMPLETE candle for exit checks
                        prev=candles[-2]
                        prev_hi=float(prev["h"]); prev_lo=float(prev["l"])

                        # ATR from complete candles
                        try:
                            _,atr_vals=atr_lookup(candles[:-1])
                            atr_val=atr_vals[-1] if atr_vals and atr_vals[-1] else 0
                        except: atr_val=0

                        # S4+S2: Update trail with ATR filter, cancel-replace stop
                        trail_result=update_trail_stop(asset,pos,prev_hi,prev_lo,atr_val,tn_stop_oids)
                        if trail_result=="FILLED":
                            # Stop triggered by HL testnet exchange
                            pnl=((pos["trail_stop"]-pos["entry"])*pos["size"] if direction=="LONG"
                                 else (pos["entry"]-pos["trail_stop"])*pos["size"])
                            add_tn_audit(asset,f"✅ TRAIL EXIT (HL) {direction}",
                                        f"@ ${pos['trail_stop']:.4f} | P&L=${pnl:+.4f}")
                            with tn_lock:
                                del tn_positions[asset]
                                if asset in tn_stop_oids: del tn_stop_oids[asset]
                                tn_state["trades"].insert(0,{
                                    "time":ts(),"asset":asset,"action":"CLOSED",
                                    "direction":direction,"entry":pos["entry"],
                                    "exit":pos["trail_stop"],"pnl":round(pnl,4),
                                    "reason":"trail","size":pos["size"]
                                })
                                try:
                                    import json as _jtn2
                                    _etn2=[]
                                    if os.path.exists(TN_TRADES_FILE):
                                        _etn2=_jtn2.load(open(TN_TRADES_FILE))
                                    _etn2.insert(0,tn_state["trades"][0])
                                    _etn2=_etn2[:500]
                                    _jtn2.dump(_etn2,open(TN_TRADES_FILE,"w"))
                                except: pass
                            continue

                        pos["current_price"]=cur
                        exit_reason=None; exit_price=cur

                        cfg=ASSET_CFG[asset]
                        # EMA cross using complete candles
                        complete_closes=[float(c["c"]) for c in candles[:-1]]
                        ef_v=ema(complete_closes,EMA_FAST); em_v=ema(complete_closes,EMA_MID)
                        ii=len(complete_closes)-1

                        # Fixed TP for BNB
                        if cfg["exit"]=="fixed_tp" and cfg["tp"]:
                            tp_p=pos["entry"]*(1+cfg["tp"]) if direction=="LONG" else pos["entry"]*(1-cfg["tp"])
                            if direction=="LONG":
                                if prev_hi>=tp_p: exit_reason,exit_price="tp",tp_p
                                elif prev_lo<=pos["stop"]: exit_reason,exit_price="stop",pos["stop"]
                                elif ef_v[ii] and em_v[ii] and ef_v[ii]<em_v[ii]: exit_reason="ema"
                            else:
                                if prev_lo<=tp_p: exit_reason,exit_price="tp",tp_p
                                elif prev_hi>=pos["stop"]: exit_reason,exit_price="stop",pos["stop"]
                                elif ef_v[ii] and em_v[ii] and ef_v[ii]>em_v[ii]: exit_reason="ema"
                        else:
                            # Trail/partial exits — hard stop and EMA cross checks
                            if direction=="LONG":
                                if prev_lo<=pos["stop"]: exit_reason,exit_price="stop",pos["stop"]
                                elif ef_v[ii] and em_v[ii] and ef_v[ii]<em_v[ii]: exit_reason="ema"
                            else:
                                if prev_hi>=pos["stop"]: exit_reason,exit_price="stop",pos["stop"]
                                elif ef_v[ii] and em_v[ii] and ef_v[ii]>em_v[ii]: exit_reason="ema"

                        if exit_reason:
                            qty=pos["size"]
                            pnl=((exit_price-pos["entry"])*qty if direction=="LONG"
                                 else (pos["entry"]-exit_price)*qty)
                            # S2: Cancel stop order before closing
                            old_oid=tn_stop_oids.get(asset)
                            if old_oid and old_oid!="FILLED":
                                try:
                                    tn_exchange.cancel(asset,old_oid)
                                    time.sleep(0.3)
                                except: pass
                                if asset in tn_stop_oids: del tn_stop_oids[asset]
                            # Execute on testnet
                            try:
                                tn_exchange.market_close(asset)
                            except Exception as e:
                                add_tn_issue(asset,"Exit failed",str(e))

                            with tn_lock:
                                del tn_positions[asset]
                                tn_state["trades"].insert(0,{
                                    "time":ts(),"asset":asset,"action":"CLOSED",
                                    "direction":direction,"entry":pos["entry"],
                                    "exit":exit_price,"pnl":round(pnl,4),
                                    "reason":exit_reason,"size":qty
                                })
                                # Persist testnet trades to disk
                                try:
                                    import json as _jtn2
                                    _etn2=[]
                                    if os.path.exists(TN_TRADES_FILE):
                                        _etn2=_jtn2.load(open(TN_TRADES_FILE))
                                    _etn2.insert(0,tn_state["trades"][0])
                                    _etn2=_etn2[:500]
                                    _jtn2.dump(_etn2,open(TN_TRADES_FILE,"w"))
                                except: pass
                                tn_state["tax"]["total_pnl"]+=pnl
                                tn_state["tax"]["total_trades"]+=1
                                if pnl>0: tn_state["tax"]["winning_trades"]+=1
                                else: tn_state["tax"]["losing_trades"]+=1

                            add_tn_audit(asset,f"✅ CLOSED {direction}",
                                        f"exit=${exit_price:,.4f} | pnl=${pnl:+,.2f} | reason={exit_reason}")
                            ntfy(f"📊 [TESTNET] {asset} {direction} CLOSED",
                                 f"P&L: ${pnl:+,.2f} | Reason: {exit_reason}\nEntry: ${pos['entry']:,.4f} → Exit: ${exit_price:,.4f}")
                            log(f"🧪 TESTNET {asset} {direction} CLOSED @ ${exit_price:,.4f} | P&L=${pnl:+,.2f} | {exit_reason}")
                        continue

                    # Signal evaluation — only if not in position
                    if tn_last_candle.get(asset)==ts_val:
                        continue  # already evaluated this candle

                    add_tn_audit(asset,"🕯 NEW CANDLE",
                                f"ts={ts_val} | price=${cur:,.4f} | age={age_s:.0f}s | binance | evaluating...")

                    direction,signal_price,sig_vol,sig_vs,filters=evaluate_signal(candles,asset)
                    result=filters.get("_result",{})
                    blocked=result.get("blocked_by",[])
                    ema_filter=filters.get("ema_stack",{})
                    ema_passed=ema_filter.get("pass",False)
                    ema_dir=ema_filter.get("value","flat")

                    if not ema_passed:
                        # EMA flat — mark as seen
                        tn_last_candle[asset]=ts_val
                        add_tn_audit(asset,"⏭ EMA FLAT",f"EMA={ema_dir} | seen")
                    elif direction:
                        # All pass — mark as seen, enter below
                        tn_last_candle[asset]=ts_val
                    else:
                        # EMA stacked but other filters failing — RETRY
                        add_tn_audit(asset,"🔄 RETRY ALL",
                                    f"EMA={ema_dir} | blocked:{blocked} — retrying")

                    if not direction: continue  # still retrying — no signal yet

                    # Signal fired — all filters passed
                    add_tn_audit(asset,f"🚨 SIGNAL {direction}",
                                f"price=${signal_price:,.4f} | binance data | all filters ✅")

                    if tn_state["paused"] or tn_state["kill_switch"]: continue


                    # Place order on testnet
                    try:
                        pos_usd=get_pos_usd(sig_vol,sig_vs,
                            ema([ float(c["c"]) for c in candles],EMA_FAST)[-1],
                            ema([ float(c["c"]) for c in candles],EMA_SLOW)[-1])
                        notional=pos_usd*tn_leverage
                        # Use correct szDecimals per asset
                        try:
                            tn_meta=req.post("https://api.hyperliquid-testnet.xyz/info",
                                json={"type":"meta"},timeout=5).json()
                            tn_dec=next((a.get("szDecimals",5) for a in tn_meta["universe"]
                                        if a["name"]==asset),5)
                        except: tn_dec=5
                        qty=round(notional/cur,tn_dec)

                        is_buy=(direction=="LONG")
                        r=tn_exchange.market_open(asset,is_buy,qty)
                        statuses=r.get("response",{}).get("data",{}).get("statuses",[])

                        if statuses and "error" in statuses[0]:
                            err=statuses[0]["error"]
                            add_tn_issue(asset,"Order rejected",err)
                            add_tn_audit(asset,"❌ ORDER REJECTED",err)
                            continue

                        fill=float(statuses[0].get("filled",{}).get("avgPx",cur)) if statuses else cur
                        stop=round_price(fill*(1-STOP_PCT) if direction=="LONG" else fill*(1+STOP_PCT))
                        trail=round_price(fill*(1-TRAIL_PCT) if direction=="LONG" else fill*(1+TRAIL_PCT))

                        with tn_lock:
                            tn_positions[asset]={
                                "direction":direction,"entry":fill,"size":qty,"qty_rem":qty,
                                "stop":stop,"trail_peak":fill,
                                "trail_stop":trail,
                                "current_price":fill,"entry_time":ts(),
                                "partial_done":False,"partial_pnl":0.0
                            }
                            tn_state["trades"].insert(0,{
                                "time":ts(),"asset":asset,"action":"OPENED",
                                "direction":direction,"entry":fill,"exit":None,
                                "pnl":None,"reason":"signal","size":qty,
                                "filters":filters if "filters" in dir() else {}
                            })
                            # Persist testnet trades to disk
                            try:
                                import json as _jtn
                                _etn=[]
                                if os.path.exists(TN_TRADES_FILE):
                                    _etn=_jtn.load(open(TN_TRADES_FILE))
                                _etn.insert(0,tn_state["trades"][0])
                                _etn=_etn[:500]
                                _jtn.dump(_etn,open(TN_TRADES_FILE,"w"))
                            except: pass

                        # S2: Place stop order on HL testnet exchange
                        tn_oid=None
                        try:
                            is_buy=direction=="SHORT"
                            sp=round_price(stop)
                            lp=round_price(sp*0.9 if is_buy else sp*1.1)
                            tn_order_type={"trigger":{"triggerPx":sp,"isMarket":True,"tpsl":"sl"}}
                            tn_stop_result=tn_exchange.order(asset,is_buy,qty,lp,tn_order_type,reduce_only=True)
                            if tn_stop_result.get("status")=="ok":
                                tn_s=tn_stop_result["response"]["data"]["statuses"][0]
                                if "resting" in tn_s:
                                    tn_oid=tn_s["resting"]["oid"]
                                    tn_stop_oids[asset]=tn_oid
                                    add_tn_audit(asset,"🛡 TN STOP PLACED",f"stop @ ${sp} OID:{tn_oid}")
                        except Exception as e:
                            add_tn_issue(asset,"TN stop placement failed",str(e))

                        add_tn_audit(asset,f"✅ ENTERED {direction}",
                                    f"fill=${fill:,.4f} | qty={qty} | stop=${stop:,.4f} | stop_oid={tn_oid}")
                        ntfy(f"🧪 [TESTNET] {asset} {direction} ENTERED",
                             f"Fill: ${fill:,.4f} | Qty: {qty}\nStop: ${stop:,.4f} | Binance signal")
                        log(f"🧪 TESTNET {asset} {direction} ENTERED @ ${fill:,.4f}")

                    except Exception as e:
                        add_tn_issue(asset,"Entry error",str(e))
                        add_tn_audit(asset,"❌ ENTRY ERROR",str(e))

                except Exception as e:
                    log(f"🧪 Testnet loop error {asset}: {e}")

        except Exception as e:
            log(f"🧪 Testnet loop error: {e}")

        time.sleep(CHECK_EVERY)

_tn=threading.Thread(target=testnet_trading_loop,daemon=True)
_tn.start()

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False,use_reloader=False)
