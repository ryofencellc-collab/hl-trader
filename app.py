"""
HL TRADER — Final Production App v3
══════════════════════════════════════
8 assets | ntfy alerts | /signal-check | /audit | Tax system

Full diagnostic visibility — no Railway logs needed.
Every candle, every signal, every skip tracked and visible.

DRY_RUN = False | TESTNET = True | LEVERAGE = 10x
"""

import threading, time, csv, os, requests as req
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
TESTNET         = True
MAIN_WALLET     = "0xa90566c8d886CA63c1194101a7dA2Fa129D26B58"
API_PRIVATE_KEY = "0x5b75aa092ea3bd1ee77983ab5b8268607120a0145de6df11174b3f72f91b9ea0"
API_URL         = constants.TESTNET_API_URL if TESTNET else constants.MAINNET_API_URL
PASSWORD        = os.environ.get("DASHBOARD_PASSWORD","hl2026")
NTFY_TOPIC      = "hl-trader-lunchm0ney"
NTFY_URL        = f"https://ntfy.sh/{NTFY_TOPIC}"

ASSETS          = ["BTC","ETH","SOL","BNB","DOGE","AVAX"]
TOTAL_USDC      = 1998.0
BASE_POS        = TOTAL_USDC / len(ASSETS)
LEVERAGE        = 10
CHECK_EVERY     = 60
TAX_RATE        = 0.35

EMA_FAST=5; EMA_MID=13; EMA_SLOW=34
STOP_PCT=0.05; TRAIL_PCT=0.01
VOL_FILTER=1.5; SEP_FILTER=0.003; BRK_BARS=12
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
    # Full audit log — every candle, every signal, every skip
    "audit":[],
    "health":{"api_connected":False,"last_ping":None,"assets_ok":{},
              "params":{
                  "ema":"5/13/34","stop_pct":"5%","trail_pct":"1%",
                  "vol_filter":"1.5x","sep_filter":"0.003","brk_bars":"12",
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

AUDIT_FILE="/tmp/hl_audit.csv"

def add_audit(asset,event,detail,filters=None):
    """Full audit trail — every candle evaluation visible on dashboard + saved to disk"""
    entry={
        "time":ts(),"asset":asset,"event":event,
        "detail":detail,"filters":filters or {}
    }
    with lock:
        state["audit"].insert(0,entry)
        state["audit"]=state["audit"][:2000]
    # Persist to disk so it survives restarts
    try:
        with open(AUDIT_FILE,"a") as f:
            f.write(f"{entry['time']}|{asset}|{event}|{detail}\n")
    except: pass

def load_audit_from_disk():
    """Load audit trail from disk on startup"""
    try:
        if not os.path.exists(AUDIT_FILE): return
        lines=open(AUDIT_FILE).readlines()
        for line in reversed(lines[-2000:]):
            parts=line.strip().split("|",3)
            if len(parts)==4:
                state["audit"].append({"time":parts[0],"asset":parts[1],
                                       "event":parts[2],"detail":parts[3],"filters":{}})
        log(f"📂 Loaded {len(state['audit'])} audit entries from disk")
    except Exception as e:
        log(f"⚠️ Could not load audit from disk: {e}")

def add_trade(asset,action,direction,entry,exit_p,size,pnl,reason):
    t={"time":ts(),"asset":asset,"action":action,"direction":direction,
       "entry":entry,"exit":exit_p,"size":size,"leverage":LEVERAGE,
       "pnl":round(pnl,4) if pnl is not None else None,"reason":reason}
    with lock:
        state["trades"].insert(0,t)
        state["trades"]=state["trades"][:500]
        if pnl is not None:
            wk=datetime.now(timezone.utc).strftime("%Y-W%W")
            state["weekly_pnl"][wk]=round(state["weekly_pnl"].get(wk,0)+pnl,4)

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

def check_milestones():
    bal=state["balance"]
    for m in MILESTONES:
        if bal>=m and m not in milestones_hit:
            milestones_hit.add(m); ntfy_milestone(bal)

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

positions={}; last_candle={}; last_exit={}; bar_count={}; entry_times={}

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
    Returns signal direction + complete filter status for audit.
    """
    cfg=ASSET_CFG[asset]
    filters={}

    if len(candles)<50:
        return None,None,0,0,{"error":"insufficient candles"}

    closes=[float(c["c"]) for c in candles]
    highs=[float(c["h"]) for c in candles]
    lows=[float(c["l"]) for c in candles]
    vols=[float(c["v"]) for c in candles]
    ef=ema(closes,EMA_FAST); em2=ema(closes,EMA_MID); es=ema(closes,EMA_SLOW)
    vs=sma(vols,20); u=bbu(closes); l=bbl(closes); i=len(candles)-1

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

    # Volume
    vol=vols[i]; vr=vol/vs[i] if vs[i] else 0
    vol_ok=vr>=VOL_FILTER
    filters["volume"]={"pass":vol_ok,"value":f"{vr:.2f}x","need":f">={VOL_FILTER}x"}

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
        br=float(candles[i]["h"])-float(candles[i]["l"])
        if br>0:
            cp=(closes[i]-float(candles[i]["l"]))/br
            sc_ok=(cp>=0.70 if d=="LONG" else cp<=0.30)
            sc_val=f"close pct={cp:.2f} ({'≥0.70' if d=='LONG' else '≤0.30'} needed)"
        else: sc_ok=False; sc_val="zero range candle"
        filters["strong_close"]={"pass":sc_ok,"value":sc_val}
    else:
        filters["strong_close"]={"pass":True,"value":"not required"}

    # Regime
    if cfg["regime"]:
        try:
            lkp,atr_v=atr_lookup(candles)
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
        add_diag("ERROR",f"Verify entry {asset}",str(e),"Assuming failed"); return False,0

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
def enter_trade(asset,direction,price,vol,vs,ef,es):
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
    stop=round(price*(1-STOP_PCT) if direction=="LONG" else price*(1+STOP_PCT),2)
    trail=round(price*(1-TRAIL_PCT) if direction=="LONG" else price*(1+TRAIL_PCT),2)
    liq=liq_price(price,direction)

    if DRY_RUN:
        log(f"[DRY] ENTER {direction} {asset} @ ${price:,.2f}")
        entry_times[asset]=ts()
        positions[asset]={"direction":direction,"entry":price,"size":qty,
                          "pos_usd":pos_usd,"stop":stop,"trail_peak":price,
                          "trail_stop":trail,"liq":liq,"partial_done":False,
                          "partial_pnl":0.0,"qty_rem":qty,"current_price":price,"unrealized_pnl":0.0}
        add_trade(asset,"ENTER",direction,price,None,qty,None,"signal")
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
                add_audit(asset,"ORDER REJECTED",f"Exchange error: {statuses[0]['error']}")
                return
            fill=price
            if statuses and "filled" in statuses[0]:
                fill=float(statuses[0]["filled"]["avgPx"])

            # NON-BLOCKING: run verification in background thread
            # Main loop continues checking other assets immediately
            def verify_and_confirm(a,d,f,pu,q,stp,trl,lq,et):
                confirmed,actual=verify_entry(a)
                if not confirmed:
                    add_diag("ERROR",f"Entry NOT confirmed {a}","Order placed but not visible","NOT logging")
                    add_audit(a,"ENTRY FAILED",f"Order placed @ ${f:,.2f} but not visible on exchange")
                    return
                f2=actual if actual>0 else f
                stp2=round(f2*(1-STOP_PCT) if d=="LONG" else f2*(1+STOP_PCT),2)
                trl2=round(f2*(1-TRAIL_PCT) if d=="LONG" else f2*(1+TRAIL_PCT),2)
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
                add_trade(a,"ENTER",d,f2,None,q2,None,"signal")
                add_audit(a,"✅ ENTERED",f"{d} @ ${f2:,.2f} | stop=${stp2:,.2f} | trail=${trl2:,.2f} | liq=${lq2:,.2f} | CONFIRMED")
                ntfy_trade_entered(a,d,f2,q2,stp2,trl2,pu)
                log(f"✅ ENTERED {d} {a} @ ${f2:,.2f} | CONFIRMED | liq=${lq2:,.2f}")
                with lock: state["positions"]={k:v for k,v in positions.items()}

            t=threading.Thread(
                target=verify_and_confirm,
                args=(asset,direction,fill,pos_usd,qty,stop,trail,liq,ts()),
                daemon=True
            )
            t.start()
            log(f"🔄 {asset} verification running in background — main loop continues")
            return  # Return immediately, don't block
        else:
            add_diag("ERROR",f"Entry failed {asset}",str(r),"Skipping")
            add_audit(asset,"ENTRY FAILED",f"Exchange rejected order: {r}")
    except Exception as e:
        add_diag("ERROR",f"Entry exception {asset}",str(e),"Skipping")
        add_audit(asset,"ENTRY ERROR",str(e))

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
            ntfy_trade_closed(asset,pos["direction"],pos["entry"],fill,pnl,reason)
            add_trade(asset,"EXIT",pos["direction"],pos["entry"],fill,pos["size"],pnl,reason)
            last_exit[asset]=bar_count.get(asset,0)
            del positions[asset]
            if asset in entry_times: del entry_times[asset]
            with lock: state["positions"]={k:v for k,v in positions.items()}
    except Exception as e:
        add_diag("ERROR",f"Exit exception {asset}",str(e),"Position may still be open")
        add_audit(asset,"EXIT ERROR",str(e))

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
    log("HL TRADER v3 — Full audit trail | Per-asset errors | 6 assets")
    add_diag("INFO","HL Trader v3 started",
             f"DRY={DRY_RUN} TEST={TESTNET} LEV={LEVERAGE}x ASSETS={len(ASSETS)}",
             "Per-asset retry | Full audit trail | All orders verified")
    ntfy("🚀 HL Trader v3 Started",
         f"6 assets | Per-asset errors\nMode: {'TESTNET' if TESTNET else 'LIVE'}\n"
         f"Leverage: {LEVERAGE}x\nAssets: {', '.join(ASSETS)}",tags="rocket")

    retry_count={}; cycle=0; api_down_since=None

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
            with lock:
                state["health"]["api_connected"]=True
                state["health"]["last_ping"]=ts()
                for asset,pos in positions.items():
                    cur=float(mids.get(asset,pos["entry"]))
                    pnl=((cur-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                         else (pos["entry"]-cur)*pos["size"])
                    state["positions"][asset]["current_price"]=cur
                    state["positions"][asset]["unrealized_pnl"]=round(pnl,4)
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
                candles=info.candles_snapshot(asset,CANDLE_TF,start_ms,end_ms)

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
                last_candle[asset]=ts_val

                # Evaluate signal with full filter breakdown
                direction,signal_price,sig_vol,sig_vs,filters=evaluate_signal(candles,asset)
                result=filters.get("_result",{})
                blocked=result.get("blocked_by",[])

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

                # Overnight filter
                if cfg["no_ov"] and 6<=datetime.now(timezone.utc).hour<10:
                    add_audit(asset,"⏸ OVERNIGHT SKIP",f"UTC hour={datetime.now(timezone.utc).hour} (blocked 6-10)")
                    log(f"⏸  {asset}: overnight skip"); continue

                # Funding filter
                if cfg["ff"]:
                    fr=abs(float(candles[-1].get("fundingRate",0)))
                    if fr>cfg["ff"]:
                        add_audit(asset,"⏸ FUNDING SKIP",f"rate={fr:.5f} > max={cfg['ff']:.5f}")
                        log(f"⏸  {asset}: funding too high"); continue

                # EXITS
                if asset in positions:
                    pos=positions[asset]
                    if pos["direction"]=="LONG" and hi>pos["trail_peak"]:
                        pos["trail_peak"]=hi; pos["trail_stop"]=round(hi*(1-TRAIL_PCT),2)
                        add_audit(asset,"📈 TRAIL UPDATED",f"new trail=${pos['trail_stop']:,.2f}")
                    elif pos["direction"]=="SHORT" and lo<pos["trail_peak"]:
                        pos["trail_peak"]=lo; pos["trail_stop"]=round(lo*(1+TRAIL_PCT),2)
                        add_audit(asset,"📉 TRAIL UPDATED",f"new trail=${pos['trail_stop']:,.2f}")

                    if cfg["exit"]=="partial" and not pos["partial_done"]:
                        trig_p=(pos["entry"]*(1+cfg["pt"]) if pos["direction"]=="LONG"
                                else pos["entry"]*(1-cfg["pt"]))
                        if ((pos["direction"]=="LONG" and hi>=trig_p) or
                            (pos["direction"]=="SHORT" and lo<=trig_p)):
                            pqty=pos["qty_rem"]*cfg["ps"]
                            praw=((trig_p-pos["entry"])*pqty if pos["direction"]=="LONG"
                                  else (pos["entry"]-trig_p)*pqty)
                            pos["partial_pnl"]+=praw; pos["qty_rem"]-=pqty
                            pos["partial_done"]=True; pos["stop"]=pos["entry"]
                            if pos["direction"]=="LONG":
                                pos["trail_peak"]=trig_p; pos["trail_stop"]=round(trig_p*(1-TRAIL_PCT),2)
                            else:
                                pos["trail_peak"]=trig_p; pos["trail_stop"]=round(trig_p*(1+TRAIL_PCT),2)
                            add_audit(asset,"💰 PARTIAL EXIT",f"@ ${trig_p:,.2f} | stop→breakeven @ ${pos['entry']:,.2f}")
                            log(f"💰 {asset} PARTIAL @ ${trig_p:,.2f} | stop→breakeven")

                    if cfg["exit"]=="fixed_tp" and cfg["tp"]:
                        tp_p=(pos["entry"]*(1+cfg["tp"]) if pos["direction"]=="LONG"
                              else pos["entry"]*(1-cfg["tp"]))
                        if ((pos["direction"]=="LONG" and hi>=tp_p) or
                            (pos["direction"]=="SHORT" and lo<=tp_p)):
                            add_audit(asset,"🎯 TP HIT",f"target=${tp_p:,.2f} hit")
                            exit_trade(asset,tp_p,"tp"); continue

                    stop_hit=((pos["direction"]=="LONG" and lo<=pos["stop"]) or
                               (pos["direction"]=="SHORT" and hi>=pos["stop"]))
                    trail_hit=((pos["direction"]=="LONG" and lo<=pos["trail_stop"]) or
                                (pos["direction"]=="SHORT" and hi>=pos["trail_stop"]))
                    ema_x=((pos["direction"]=="LONG" and ema(
                        [float(c["c"]) for c in candles],EMA_FAST)[-1]<
                        ema([float(c["c"]) for c in candles],EMA_MID)[-1]) or
                        (pos["direction"]=="SHORT" and ema(
                        [float(c["c"]) for c in candles],EMA_FAST)[-1]>
                        ema([float(c["c"]) for c in candles],EMA_MID)[-1]))

                    if stop_hit:
                        add_audit(asset,"🛑 STOP HIT",f"stop=${pos['stop']:,.2f} | low={lo:,.2f}")
                        exit_trade(asset,pos["stop"],"stop")
                    elif trail_hit:
                        add_audit(asset,"🔔 TRAIL HIT",f"trail=${pos['trail_stop']:,.2f} | low={lo:,.2f}")
                        exit_trade(asset,pos["trail_stop"],"trail")
                    elif ema_x:
                        add_audit(asset,"📊 EMA CROSS EXIT",f"EMA5 crossed EMA13 @ ${cur:,.2f}")
                        exit_trade(asset,cur,"ema_cross")
                    else:
                        pnl=((cur-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                             else (pos["entry"]-cur)*pos["size"])
                        add_audit(asset,"⏳ HOLDING",
                                  f"{pos['direction']} @ ${pos['entry']:,.2f} | cur=${cur:,.2f} | "
                                  f"trail=${pos['trail_stop']:,.2f} | liq=${pos['liq']:,.2f} | P&L=${pnl:+.4f}")
                        log(f"⏳ {asset} {pos['direction']} @ ${cur:,.2f} | trail=${pos['trail_stop']:,.2f} | P&L=${pnl:+.4f}")

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
                                    ema([float(c["c"]) for c in candles],EMA_SLOW)[-1])
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
            <span style="font-size:10px;color:#4A5878;margin-left:auto;white-space:nowrap">{a["time"][11:19]}</span>
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
    <div style="font-family:monospace;font-size:18px;font-weight:700;color:#00D68F">HL TRADER v3</div>
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
    <div class="tab" onclick="show('au',this)">Audit</div>
    <div class="tab" onclick="show('tx',this)">Tax</div>
    <div class="tab" onclick="show('dg',this)">Diagnostics</div>
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
  <button class="ctrl" style="background:rgba(0,180,255,0.15);color:#00B4FF;border:2px solid rgba(0,180,255,0.4);margin-top:8px" onclick="runExecTest()">🧪 Execution Test</button>
  <div class="card" style="border-color:{'rgba(0,214,143,0.3)' if tax['total_net']>=0 else 'rgba(255,71,87,0.3)'}">
    <div style="font-size:10px;font-weight:700;color:#4A5878;text-transform:uppercase;margin-bottom:6px">Net P&L</div>
    <div style="font-family:monospace;font-size:28px;font-weight:700;color:{'#00D68F' if tax['total_net']>=0 else '#FF4757'}">${tax["total_net"]:.2f}</div>
    <div style="font-size:12px;color:#4A5878;margin-top:4px">Gross: ${tax["total_pnl"]:.2f} · Tax: ${tax["total_tax"]:.2f}</div>
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
</div>

<div id="pos" class="sec">
  {pos_html or '<div style="text-align:center;padding:48px 24px;color:#4A5878"><div style="font-size:36px;margin-bottom:12px">📭</div><div>No open positions</div></div>'}
</div>

<div id="tr" class="sec">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px">Trade History</div>
  {f'<div class="card" style="padding:0 16px">{trades_html}</div>' if trades_html else '<div style="text-align:center;padding:48px 24px;color:#4A5878"><div style="font-size:36px;margin-bottom:12px">📋</div><div>No trades yet</div></div>'}
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

</div>
<button class="rfb" onclick="location.reload()">↻</button>
<script>
function show(id,el){{document.querySelectorAll(".sec").forEach(s=>s.classList.remove("active"));document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));document.getElementById(id).classList.add("active");el.classList.add("active");localStorage.setItem("hl_tab",id);}}
function restoreTab(){{var t=localStorage.getItem("hl_tab");if(!t)return;var sec=document.getElementById(t);if(!sec)return;document.querySelectorAll(".sec").forEach(function(s){{s.classList.remove("active")}});document.querySelectorAll(".tab").forEach(function(tb){{tb.classList.remove("active")}});sec.classList.add("active");document.querySelectorAll(".tab").forEach(function(tb){{if(tb.getAttribute("onclick")&&tb.getAttribute("onclick").indexOf(t)>-1)tb.classList.add("active")}});}}let pend=null;
function confirm_action(a,t,s){{pend=a;document.getElementById("ot").textContent=t;document.getElementById("os").textContent=s;document.getElementById("ov").classList.add("show")}}
function closeOv(){{document.getElementById("ov").classList.remove("show");pend=null}}
function doAction(a){{fetch("/control",{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify({{action:a}})}}).then(r=>r.json()).then(d=>{{if(d.ok)location.reload();else alert("Error: "+d.error)}})}}
function runExecTest(){{
  if(!confirm("Run execution test? Places a real small order on each asset and immediately closes it. Confirms entry, verify, and exit cycle."))return;
  fetch("/exec-test",{{method:"POST"}}).then(r=>r.json()).then(d=>{{
    alert("Execution test result:\n\n"+d.result);
    location.reload();
  }}).catch(e=>alert("Error: "+e));
}}
document.addEventListener("DOMContentLoaded",function(){{
  var oy=document.getElementById("oy");
  if(oy)oy.onclick=function(){{if(pend)doAction(pend);closeOv()}};
  restoreTab();
}});
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

@app.route("/exec-test",methods=["POST"])
def exec_test():
    if not session.get("ok"): return jsonify({"error":"unauthorized"}),401
    results=[]; test_size={"BTC":0.00032,"ETH":0.011,"SOL":0.13,"BNB":0.018,"DOGE":280,"AVAX":3.1}
    for asset in ASSETS:
        if asset in positions:
            results.append(f"{asset}: SKIP — position already open")
            continue
        try:
            # Enter
            sz=test_size.get(asset,1)
            r=exchange.market_open(asset,True,sz)
            statuses=r.get("response",{}).get("data",{}).get("statuses",[])
            if not statuses or "error" in statuses[0]:
                results.append(f"{asset}: ENTRY FAILED — {statuses[0].get('error','unknown') if statuses else 'no response'}")
                continue
            fill=float(statuses[0].get("filled",{}).get("avgPx",0))
            time.sleep(3)
            # Verify via userFills
            since_ms=int(time.time()*1000)-10000
            fr=req.post(HL_INFO_URL,json={"type":"userFills","user":MAIN_WALLET},timeout=10)
            fills=fr.json()
            recent=[f for f in fills if int(f.get("time",0))>since_ms and f.get("coin")==asset and "Open" in f.get("dir","")]
            verified=len(recent)>0
            # Exit
            exchange.market_close(asset)
            time.sleep(2)
            status="✅ FULL CYCLE" if verified else "⚠️ ENTRY PLACED BUT NOT VERIFIED"
            results.append(f"{asset}: {status} — entry@${fill:,.4f} | fills={len(recent)}")
            add_audit(asset,"🧪 EXEC TEST",f"{status} @ ${fill:,.4f}")
        except Exception as e:
            results.append(f"{asset}: ERROR — {e}")
        time.sleep(1)
    result_str="\n".join(results)
    log(f"🧪 Execution test complete:\n{result_str}")
    return jsonify({"result":result_str})

@app.route("/log")
def log_export():
    if not session.get("ok"): return "unauthorized",401
    s=state; lines=["="*60,"HL TRADER v3 — SYSTEM LOG",f"Generated: {ts()} UTC","="*60]
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
    lines.append("\nASSET STATUS:")
    for asset in s["assets"]:
        ah=s["health"]["assets_ok"].get(asset,{})
        lines.append(f"  {asset}: ${ah.get('price',0):,.2f} | {ah.get('last_candle','?')} | {ah.get('signal','?')} | {'LIVE' if ah.get('fresh') else 'STALE'}")
    lines.append("\nDIAGNOSTICS:")
    for d in s["diagnostics"][:20]:
        lines.append(f"  {d['time']} [{d['level']}] {d['event']} | {d['cause']}")
    lines.append("\n"+"="*60)
    return Response("\n".join(lines),mimetype="text/plain")

load_audit_from_disk()
_t=threading.Thread(target=trading_loop,daemon=True)
_t.start()

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False,use_reloader=False)
