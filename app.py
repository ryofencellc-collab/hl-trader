"""
HL TRADER — Production App v2
══════════════════════════════
Single process: trading loop + Flask dashboard
Key fix: verifies every order on exchange before logging

DRY_RUN = False | TESTNET = True | LEVERAGE = 10x
"""

import threading, time, csv, os
from datetime import datetime, timezone
from flask import Flask, render_template_string, request, session, redirect, url_for, jsonify, Response
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
PASSWORD        = os.environ.get("DASHBOARD_PASSWORD", "hl2026")

ASSETS          = ["BTC", "ETH", "SOL", "BNB"]
TOTAL_USDC      = 999.0
BASE_POS        = TOTAL_USDC / len(ASSETS)
LEVERAGE        = 10
CHECK_EVERY     = 60
TAX_RATE        = 0.35

EMA_FAST=5; EMA_MID=13; EMA_SLOW=34
STOP_PCT=0.05; TRAIL_PCT=0.01
VOL_FILTER=1.5; SEP_FILTER=0.003; BRK_BARS=12
CANDLE_TF="15m"; CANDLE_LIMIT=200

ASSET_CFG = {
    "BTC": {"exit":"trail","ff":0.0001,"bb":True, "sc":False,"no_ov":False,"pt":None,"ps":None,"tp":None,"cd":0},
    "ETH": {"exit":"trail","ff":None,  "bb":False,"sc":False,"no_ov":True, "pt":None,"ps":None,"tp":None,"cd":0},
    "SOL": {"exit":"partial","ff":None,"bb":True, "sc":True, "no_ov":False,"pt":0.01,"ps":0.25,"tp":None,"cd":5},
    "BNB": {"exit":"fixed_tp","ff":None,"bb":False,"sc":True,"no_ov":False,"pt":None,"ps":None,"tp":0.01,"cd":0},
}

def get_pos_usd(vol,vs,ef,es):
    if not vs or vs==0: return BASE_POS
    vr=vol/vs; sep=abs(ef-es)/es if es else 0
    if vr>=4.0 and sep>=0.008: return BASE_POS*2
    if vr>=2.5 or sep>=0.005:  return BASE_POS
    return BASE_POS*0.5

# ══════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════
state = {
    "status":"starting","last_check":None,"next_check":None,
    "cycle":0,"dry_run":DRY_RUN,"testnet":TESTNET,
    "leverage":LEVERAGE,"assets":ASSETS,"balance":998.93,
    "positions":{},"trades":[],"diagnostics":[],"weekly_pnl":{},
    "paused":False,"kill_switch":False,"close_all_requested":False,
    "health":{
        "api_connected":False,"last_ping":None,"assets_ok":{},
        "params":{
            "ema":"5/13/34","stop_pct":"5%","trail_pct":"1%",
            "vol_filter":"1.5x","sep_filter":"0.003","brk_bars":"12",
            "candle_tf":"15m","check_every":"60s","leverage":f"{LEVERAGE}x",
            "assets":"BTC,ETH,SOL,BNB",
            "btc_cfg":"trail|fr1bp|BB|varsz",
            "eth_cfg":"trail|no_overnight|varsz",
            "sol_cfg":"partial1%@25%|cd5|BB|SC|varsz",
            "bnb_cfg":"tp1%|SC",
        }
    },
    "tax":{"total_pnl":0.0,"total_tax":0.0,"total_net":0.0,
           "winning_trades":0,"losing_trades":0,"total_trades":0},
}
lock = threading.Lock()

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)

def add_diag(level, event, cause, action):
    icons={"INFO":"ℹ️","WARNING":"⚠️","ERROR":"❌","CRITICAL":"🚨"}
    entry={"time":ts(),"level":level,"event":event,"cause":cause,"action":action}
    with lock:
        if level=="ERROR" and state["diagnostics"]:
            last=state["diagnostics"][0]
            if last["event"]==event and last["level"]==level:
                return
        state["diagnostics"].insert(0,entry)
        state["diagnostics"]=state["diagnostics"][:200]
    log(f"{icons.get(level,'📋')} [{level}] {event} | {cause} | {action}")

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

def record_tax(asset,direction,entry,exit_p,size,pnl):
    tax=max(0,pnl*TAX_RATE); net=pnl-tax
    with lock:
        state["tax"]["total_pnl"]    +=pnl
        state["tax"]["total_tax"]    +=tax
        state["tax"]["total_net"]    +=net
        state["tax"]["total_trades"] +=1
        if pnl>0: state["tax"]["winning_trades"]+=1
        else:      state["tax"]["losing_trades"] +=1
    fe=os.path.exists("hl_tax.csv")
    with open("hl_tax.csv","a",newline="") as f:
        import csv as _csv
        w=_csv.DictWriter(f,fieldnames=["time","asset","direction","entry",
            "exit","size","leverage","gross","tax","net","dry_run"])
        if not fe: w.writeheader()
        w.writerow({"time":ts(),"asset":asset,"direction":direction,
                    "entry":entry,"exit":exit_p,"size":size,"leverage":LEVERAGE,
                    "gross":round(pnl,4),"tax":round(tax,4),
                    "net":round(net,4),"dry_run":DRY_RUN})

# ══════════════════════════════════════════════════
# EXCHANGE
# ══════════════════════════════════════════════════
wallet   = eth_account.Account.from_key(API_PRIVATE_KEY)
info     = Info(API_URL, skip_ws=True)
exchange = Exchange(wallet, API_URL, account_address=MAIN_WALLET)

positions   = {}
last_candle = {}
last_exit   = {}
bar_count   = {}

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
    u=bbu(closes,p,m); out=[None]*len(closes)
    for i in range(p,len(closes)):
        w=closes[i-p:i];mu=sum(w)/p
        s=(sum((x-mu)**2 for x in w)/p)**0.5
        if u[i]: out[i]=mu-m*s
    return out

def check_signal(candles,asset):
    if len(candles)<50: return None,None,0,0
    cfg=ASSET_CFG[asset]
    closes=[float(c["c"]) for c in candles]
    highs=[float(c["h"]) for c in candles]
    lows=[float(c["l"]) for c in candles]
    vols=[float(c["v"]) for c in candles]
    ef=ema(closes,EMA_FAST); em2=ema(closes,EMA_MID); es=ema(closes,EMA_SLOW)
    vs=sma(vols,20); u=bbu(closes); l=bbl(closes); i=len(candles)-1
    if   ef[i] and em2[i] and es[i] and ef[i]>em2[i]>es[i]: d="LONG"
    elif ef[i] and em2[i] and es[i] and ef[i]<em2[i]<es[i]: d="SHORT"
    else: return None,None,0,0
    if es[i] and abs(ef[i]-es[i])/es[i]<SEP_FILTER: return None,None,0,0
    vol=vols[i]
    if vs[i] and vol<vs[i]*VOL_FILTER: return None,None,0,0
    if i>=BRK_BARS:
        if d=="LONG"  and closes[i]<=max(highs[i-BRK_BARS:i]): return None,None,0,0
        if d=="SHORT" and closes[i]>=min(lows[i-BRK_BARS:i]):  return None,None,0,0
    if cfg["bb"]:
        if not u[i] or not l[i]: return None,None,0,0
        if d=="LONG"  and float(candles[i]["c"])<=u[i]: return None,None,0,0
        if d=="SHORT" and float(candles[i]["c"])>=l[i]: return None,None,0,0
    if cfg["sc"]:
        br=float(candles[i]["h"])-float(candles[i]["l"])
        if br>0:
            cp=(float(candles[i]["c"])-float(candles[i]["l"]))/br
            if d=="LONG"  and cp<0.70: return None,None,0,0
            if d=="SHORT" and cp>0.30: return None,None,0,0
    return d,closes[i],vol,vs[i] if vs[i] else 0

def verify_entry(asset):
    """Wait 5s then confirm position exists on exchange"""
    time.sleep(5)
    try:
        s=info.user_state(MAIN_WALLET)
        for p in s.get("assetPositions",[]):
            if p["position"]["coin"]==asset and float(p["position"]["szi"])!=0:
                return True, float(p["position"]["entryPx"])
        return False, 0
    except Exception as e:
        add_diag("ERROR",f"Verify entry {asset}",str(e),"Assuming failed")
        return False, 0

def verify_exit(asset):
    """Wait 3s then confirm position is closed on exchange"""
    time.sleep(3)
    try:
        s=info.user_state(MAIN_WALLET)
        still=[p for p in s.get("assetPositions",[])
               if p["position"]["coin"]==asset and float(p["position"]["szi"])!=0]
        return len(still)==0
    except:
        return False

# ══════════════════════════════════════════════════
# TRADING
# ══════════════════════════════════════════════════
def enter_trade(asset,direction,price,vol,vs,ef,es):
    cfg=ASSET_CFG[asset]
    pos_usd=get_pos_usd(vol,vs,ef,es)
    qty=round((pos_usd*LEVERAGE)/price,6)
    stop=round(price*(1-STOP_PCT) if direction=="LONG" else price*(1+STOP_PCT),2)
    trail=round(price*(1-TRAIL_PCT) if direction=="LONG" else price*(1+TRAIL_PCT),2)

    if DRY_RUN:
        log(f"[DRY] ENTER {direction} {asset} @ ${price:,.2f} | size={qty}")
        positions[asset]={"direction":direction,"entry":price,"size":qty,
                          "pos_usd":pos_usd,"stop":stop,"trail_peak":price,
                          "trail_stop":trail,"partial_done":False,
                          "partial_pnl":0.0,"qty_rem":qty,
                          "current_price":price,"unrealized_pnl":0.0}
        add_trade(asset,"ENTER",direction,price,None,qty,None,"signal")
        with lock: state["positions"]={k:v for k,v in positions.items()}
        return

    try:
        r=exchange.market_open(asset,direction=="LONG",qty)
        if r and r.get("status")=="ok":
            statuses=r.get("response",{}).get("data",{}).get("statuses",[])
            fill=price
            if statuses and "filled" in statuses[0]:
                fill=float(statuses[0]["filled"]["avgPx"])

            # CRITICAL: verify on exchange before logging
            confirmed,actual_entry=verify_entry(asset)
            if not confirmed:
                add_diag("ERROR",f"Entry NOT confirmed {asset}",
                         "Order placed but position not visible on exchange",
                         "NOT logging as entered — will retry on next signal")
                return

            fill=actual_entry if actual_entry>0 else fill
            stop=round(fill*(1-STOP_PCT) if direction=="LONG" else fill*(1+STOP_PCT),2)
            trail=round(fill*(1-TRAIL_PCT) if direction=="LONG" else fill*(1+TRAIL_PCT),2)
            qty2=round((pos_usd*LEVERAGE)/fill,6)
            positions[asset]={"direction":direction,"entry":fill,"size":qty2,
                              "pos_usd":pos_usd,"stop":stop,"trail_peak":fill,
                              "trail_stop":trail,"partial_done":False,
                              "partial_pnl":0.0,"qty_rem":qty2,
                              "current_price":fill,"unrealized_pnl":0.0}
            add_trade(asset,"ENTER",direction,fill,None,qty2,None,"signal")
            log(f"✅ ENTERED {direction} {asset} @ ${fill:,.2f} | CONFIRMED on exchange")
            with lock: state["positions"]={k:v for k,v in positions.items()}
        else:
            add_diag("ERROR",f"Entry failed {asset}",str(r),"Skipping")
    except Exception as e:
        add_diag("ERROR",f"Entry exception {asset}",str(e),"Skipping")

def exit_trade(asset,price,reason):
    if asset not in positions: return
    pos=positions[asset]; cfg=ASSET_CFG[asset]

    if DRY_RUN:
        if cfg["exit"]=="partial":
            pnl=round((price-pos["entry"])*pos["qty_rem"]+pos["partial_pnl"],4) \
                if pos["direction"]=="LONG" \
                else round((pos["entry"]-price)*pos["qty_rem"]+pos["partial_pnl"],4)
        else:
            pnl=round((price-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                      else (pos["entry"]-price)*pos["size"],4)
        log(f"[DRY] EXIT {pos['direction']} {asset} @ ${price:,.2f} | {reason} | P&L=${pnl:+.4f}")
        record_tax(asset,pos["direction"],pos["entry"],price,pos["size"],pnl)
        add_trade(asset,"EXIT",pos["direction"],pos["entry"],price,pos["size"],pnl,reason)
        last_exit[asset]=bar_count.get(asset,0)
        del positions[asset]
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
                add_diag("CRITICAL",f"Exit NOT confirmed {asset}",
                         "Close order placed but position still visible",
                         "Manual check required on HyperLiquid")
        elif r is None:
            closed=verify_exit(asset)
            if not closed:
                add_diag("CRITICAL",f"Exit failed {asset}",
                         "None response + position still open",
                         "Manual intervention required")
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
            record_tax(asset,pos["direction"],pos["entry"],fill,pos["size"],pnl)
            add_trade(asset,"EXIT",pos["direction"],pos["entry"],fill,pos["size"],pnl,reason)
            last_exit[asset]=bar_count.get(asset,0)
            del positions[asset]
            with lock: state["positions"]={k:v for k,v in positions.items()}
    except Exception as e:
        add_diag("ERROR",f"Exit exception {asset}",str(e),"Position may still be open")

def close_all(reason="manual"):
    log(f"🚨 CLOSING ALL — {reason}")
    add_diag("WARNING","Close all triggered",reason,"Closing all positions")
    for asset in list(positions.keys()):
        try:
            mids=info.all_mids()
            price=float(mids.get(asset,positions[asset]["entry"]))
            exit_trade(asset,price,reason)
            time.sleep(1)
        except Exception as e:
            add_diag("ERROR",f"Close all failed {asset}",str(e),"Try manually")

# ══════════════════════════════════════════════════
# TRADING LOOP
# ══════════════════════════════════════════════════
def trading_loop():
    log("HL TRADER v2 started — all orders verified on exchange")
    add_diag("INFO","HL Trader v2 started",
             f"DRY={DRY_RUN} TEST={TESTNET} LEV={LEVERAGE}x",
             "Fix: verify entry/exit on exchange before logging")

    retry_count=0; cycle=0

    while True:
        with lock:
            killed=state["kill_switch"]
            paused=state["paused"]
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

        try:
            mids=info.all_mids()
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
                    add_diag("WARNING",f"No candles {asset}",
                             f"Got {len(candles) if candles else 0}","Skipping")
                    continue

                ts_val=str(candles[-1].get("t",candles[-1].get("T","")))
                cur=float(candles[-1]["c"])
                hi=float(candles[-1]["h"])
                lo=float(candles[-1]["l"])
                vol=float(candles[-1]["v"])
                if cur==0: continue

                age_s=int((time.time()*1000-int(ts_val))/1000) if ts_val.isdigit() else 9999
                closes=[float(c["c"]) for c in candles]
                vols=[float(c["v"]) for c in candles]
                ef=ema(closes,EMA_FAST); em2=ema(closes,EMA_MID); es=ema(closes,EMA_SLOW)
                vs_arr=sma(vols,20); vs=vs_arr[-1] if vs_arr[-1] else 0
                direction,signal_price,_,_=check_signal(candles,asset)

                with lock:
                    state["health"]["assets_ok"][asset]={
                        "ok":True,"price":cur,
                        "last_candle":f"{age_s//60}m{age_s%60}s ago" if ts_val.isdigit() else ts_val,
                        "signal":f"{direction} @ ${signal_price:,.2f}" if direction else "no signal",
                        "fresh":age_s<1200,
                    }

                if last_candle.get(asset)==ts_val:
                    continue
                last_candle[asset]=ts_val

                if cfg["no_ov"] and 6<=datetime.now(timezone.utc).hour<10:
                    log(f"⏸  {asset}: overnight skip"); continue

                if cfg["ff"]:
                    fr=abs(float(candles[-1].get("fundingRate",0)))
                    if fr>cfg["ff"]:
                        log(f"⏸  {asset}: funding too high"); continue

                # EXITS
                if asset in positions:
                    pos=positions[asset]
                    if pos["direction"]=="LONG" and hi>pos["trail_peak"]:
                        pos["trail_peak"]=hi; pos["trail_stop"]=round(hi*(1-TRAIL_PCT),2)
                        log(f"📈 {asset} trail → ${pos['trail_stop']:,.2f}")
                    elif pos["direction"]=="SHORT" and lo<pos["trail_peak"]:
                        pos["trail_peak"]=lo; pos["trail_stop"]=round(lo*(1+TRAIL_PCT),2)
                        log(f"📉 {asset} trail → ${pos['trail_stop']:,.2f}")

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
                            log(f"💰 {asset} PARTIAL @ ${trig_p:,.2f} | stop→breakeven")

                    if cfg["exit"]=="fixed_tp" and cfg["tp"]:
                        tp_p=(pos["entry"]*(1+cfg["tp"]) if pos["direction"]=="LONG"
                              else pos["entry"]*(1-cfg["tp"]))
                        if ((pos["direction"]=="LONG" and hi>=tp_p) or
                            (pos["direction"]=="SHORT" and lo<=tp_p)):
                            exit_trade(asset,tp_p,"tp"); continue

                    stop_hit=((pos["direction"]=="LONG" and lo<=pos["stop"]) or
                               (pos["direction"]=="SHORT" and hi>=pos["stop"]))
                    trail_hit=((pos["direction"]=="LONG" and lo<=pos["trail_stop"]) or
                                (pos["direction"]=="SHORT" and hi>=pos["trail_stop"]))
                    ema_x=((pos["direction"]=="LONG" and ef[-1]<em2[-1]) or
                            (pos["direction"]=="SHORT" and ef[-1]>em2[-1]))

                    if stop_hit:    exit_trade(asset,pos["stop"],"stop")
                    elif trail_hit: exit_trade(asset,pos["trail_stop"],"trail")
                    elif ema_x:     exit_trade(asset,cur,"ema_cross")
                    else:
                        pnl=((cur-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                             else (pos["entry"]-cur)*pos["size"])
                        log(f"⏳ {asset} {pos['direction']} @ ${cur:,.2f} | trail=${pos['trail_stop']:,.2f} | P&L=${pnl:+.4f}")

                # ENTRIES
                elif not paused and not killed:
                    cd=cfg.get("cd",0)
                    if cd>0 and (bar_count.get(asset,0)-last_exit.get(asset,0))<cd:
                        continue
                    if direction:
                        log(f"🚨 SIGNAL: {asset} {direction} @ ${signal_price:,.2f}")
                        enter_trade(asset,direction,signal_price,vol,vs,ef[-1],es[-1])
                    else:
                        log(f"⏳ {asset}: no signal @ ${cur:,.2f}")

                retry_count=0

            except Exception as e:
                retry_count+=1
                add_diag("ERROR",f"Error {asset}",str(e),f"Retry {retry_count}/5")
                if retry_count>5:
                    add_diag("CRITICAL","Too many errors",f"{retry_count}","Pausing 5min")
                    time.sleep(300); retry_count=0

            time.sleep(0.5)

        with lock:
            state["status"]="stopped" if state["kill_switch"] else ("paused" if state["paused"] else "waiting")
            state["next_check"]=f"in {CHECK_EVERY}s"
            state["positions"]={k:v for k,v in positions.items()}

        log(f"💤 Next check in {CHECK_EVERY}s")
        time.sleep(CHECK_EVERY)

# ══════════════════════════════════════════════════
# FLASK
# ══════════════════════════════════════════════════
app=Flask(__name__)
app.secret_key=os.environ.get("SECRET_KEY","hl2026secret")

@app.route("/")
def index():
    if not session.get("ok"):
        return '''<!DOCTYPE html><html><body style="background:#080B10;color:#E8EDF5;font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0">
        <div style="text-align:center;max-width:360px;width:100%;padding:40px;background:#0F1520;border-radius:20px;border:1px solid #1E2D42">
        <div style="font-family:monospace;font-size:28px;font-weight:700;color:#00D68F;margin-bottom:8px">HL TRADER v2</div>
        <div style="color:#4A5878;font-size:13px;margin-bottom:32px">HyperLiquid Strategy Dashboard</div>
        <form method="POST" action="/login">
        <input type="password" name="p" placeholder="Password" autofocus style="width:100%;background:#161E2E;border:1px solid #1E2D42;border-radius:12px;color:#E8EDF5;font-size:16px;padding:14px 16px;margin-bottom:12px;outline:none;box-sizing:border-box;letter-spacing:2px">
        <button type="submit" style="width:100%;background:#00D68F;color:#000;border:none;border-radius:12px;font-size:15px;font-weight:700;padding:14px;cursor:pointer">Enter</button>
        </form></div></body></html>'''
    return build_dashboard()

def build_dashboard():
    s=state; h=s["health"]; tax=s["tax"]
    any_fresh=any(v.get("fresh") for v in h["assets_ok"].values())
    killed=s["kill_switch"]; paused=s["paused"]
    status="STOPPED" if killed else ("PAUSED" if paused else s["status"].upper())
    dot="#FF4757" if killed else ("#FFB800" if paused else "#00D68F")

    def row(k,v): return f'<div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #1E2D42"><span style="font-size:13px;color:#4A5878">{k}</span><span style="font-family:monospace;font-weight:600;font-size:12px">{v}</span></div>'

    positions_html=""
    for asset,pos in s["positions"].items():
        pnl=pos.get("unrealized_pnl",0)
        cur=pos.get("current_price",pos["entry"])
        pc="#00D68F" if pnl>=0 else "#FF4757"
        dc="0,214,143" if pos["direction"]=="LONG" else "255,71,87"
        positions_html+=f'''<div style="background:#161E2E;border:1px solid #1E2D42;border-radius:14px;padding:14px;margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;margin-bottom:10px">
            <div style="font-family:monospace;font-size:15px;font-weight:700">{asset}-PERP</div>
            <div style="font-size:11px;font-weight:700;padding:3px 10px;border-radius:6px;background:rgba({dc},0.15);color:rgb({dc})">{pos["direction"]}</div>
          </div>
          {row("Entry",f"${pos['entry']:,.2f}")}{row("Current",f'<span style="color:{pc}">${cur:,.2f}</span>')}{row("Hard Stop",f'<span style="color:#FF4757">${pos["stop"]:,.2f}</span>')}{row("Trail Stop",f'<span style="color:#FFB800">${pos["trail_stop"]:,.2f}</span>')}
          <div style="margin-top:10px;padding:10px;border-radius:8px;text-align:center;font-family:monospace;font-weight:700;font-size:15px;background:rgba({("0,214,143" if pnl>=0 else "255,71,87")},0.1);color:{pc};border:1px solid rgba({("0,214,143" if pnl>=0 else "255,71,87")},0.3)">
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

    diag_html=""
    for d in s["diagnostics"][:30]:
        cs={"INFO":"61,158,255","WARNING":"255,184,0","ERROR":"255,71,87","CRITICAL":"255,71,87"}
        c=cs.get(d["level"],"74,88,120")
        diag_html+=f'''<div style="display:flex;gap:10px;padding:12px 0;border-bottom:1px solid #1E2D42">
          <span style="font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;white-space:nowrap;margin-top:2px;background:rgba({c},0.15);color:rgb({c})">{d["level"]}</span>
          <div style="flex:1"><div style="font-weight:600;font-size:13px">{d["event"]}</div>
          <div style="font-size:11px;color:#4A5878">{d["cause"]}</div>
          <div style="font-size:11px;color:#3D9EFF">→ {d["action"]}</div>
          <div style="font-size:10px;color:#4A5878;font-family:monospace">{d["time"]}</div></div></div>'''

    asset_html=""
    for asset in s["assets"]:
        ah=h["assets_ok"].get(asset,{})
        fresh=ah.get("fresh",False)
        sig=ah.get("signal","—"); sc="#00D68F" if sig and sig!="no signal" else "#4A5878"
        asset_html+=f'''<div style="background:#161E2E;border:1px solid #1E2D42;border-radius:14px;padding:14px;margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
            <div style="font-family:monospace;font-size:15px;font-weight:700">{asset}-PERP</div>
            <span style="font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;background:rgba({"0,214,143" if fresh else "255,184,0"},0.15);color:{"#00D68F" if fresh else "#FFB800"}">{"LIVE" if fresh else "STALE"}</span>
          </div>
          {row("Price",f"${ah.get('price',0):,.2f}")}{row("Last candle",ah.get("last_candle","—"))}
          <div style="display:flex;justify-content:space-between;padding:8px 0"><span style="font-size:13px;color:#4A5878">Signal</span><span style="font-family:monospace;font-weight:600;color:{sc}">{sig}</span></div>
        </div>'''

    params_html=""
    exp={"ema":"5/13/34","stop_pct":"5%","trail_pct":"1%","vol_filter":"1.5x",
         "sep_filter":"0.003","brk_bars":"12","candle_tf":"15m","check_every":"60s",
         "leverage":f"{LEVERAGE}x","assets":"BTC,ETH,SOL,BNB",
         "btc_cfg":"trail|fr1bp|BB|varsz","eth_cfg":"trail|no_overnight|varsz",
         "sol_cfg":"partial1%@25%|cd5|BB|SC|varsz","bnb_cfg":"tp1%|SC"}
    for k,e in exp.items():
        v=h["params"].get(k,"—"); ok=v==e
        params_html+=f'''<div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid #1E2D42">
          <div style="font-size:14px;width:24px;text-align:center">{"✅" if ok else "⚠️"}</div>
          <div style="flex:1"><div style="font-size:13px;font-weight:600">{k}</div><div style="font-size:11px;color:#4A5878">Expected: {e}</div></div>
          <span style="font-size:10px;font-weight:700;padding:2px 8px;border-radius:4px;background:rgba({"0,214,143" if ok else "255,184,0"},0.15);color:{"#00D68F" if ok else "#FFB800"}">{v}</span>
        </div>'''

    mode="DRY RUN" if s["dry_run"] else ("TESTNET" if s["testnet"] else "🚨 LIVE")
    mc="61,158,255" if s["dry_run"] else ("255,184,0" if s["testnet"] else "0,214,143")
    wr=f"{tax['winning_trades']/tax['total_trades']*100:.0f}%" if tax["total_trades"]>0 else "—"
    wrc="0,214,143" if tax["total_trades"]>0 and tax["winning_trades"]/tax["total_trades"]>=0.6 else "255,184,0"

    return f'''<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>HL Trader v2</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}}
body{{background:#080B10;color:#E8EDF5;font-family:-apple-system,BlinkMacSystemFont,sans-serif;min-height:100vh;padding-bottom:env(safe-area-inset-bottom)}}
.hd{{position:sticky;top:0;z-index:100;background:rgba(8,11,16,.95);backdrop-filter:blur(20px);border-bottom:1px solid #1E2D42;padding:12px 16px 0;padding-top:calc(12px + env(safe-area-inset-top))}}
.tab{{flex-shrink:0;padding:8px 14px 10px;font-size:13px;font-weight:600;color:#4A5878;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap}}
.tab.active{{color:#00D68F;border-bottom-color:#00D68F}}
.sec{{display:none}}.sec.active{{display:block}}
.main{{padding:16px}}
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
    <div style="font-family:monospace;font-size:18px;font-weight:700;color:#00D68F">HL TRADER v2</div>
    <div style="display:flex;align-items:center;gap:8px">
      <span style="font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;background:rgba({"0,214,143" if any_fresh else "255,184,0"},0.15);color:{"#00D68F" if any_fresh else "#FFB800"}">{"LIVE" if any_fresh else "STALE"}</span>
      <div style="display:flex;align-items:center;gap:6px;background:#0F1520;border:1px solid #1E2D42;border-radius:20px;padding:5px 10px;font-size:11px;font-weight:600">
        <div style="width:7px;height:7px;border-radius:50%;background:{dot}"></div>{status}
      </div>
    </div>
  </div>
  <div style="display:flex;gap:5px;flex-wrap:wrap;margin-bottom:8px">
    <span style="font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba({mc},0.15);color:rgb({mc});border:1px solid rgba({mc},0.3)">{mode}</span>
    <span style="font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba(74,88,120,0.2);color:#4A5878;border:1px solid #1E2D42">{s["leverage"]}x</span>
    <span style="font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba(74,88,120,0.2);color:#4A5878;border:1px solid #1E2D42">EMA 5/13/34</span>
    <span style="font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba(74,88,120,0.2);color:#4A5878;border:1px solid #1E2D42">BTC·ETH·SOL·BNB</span>
    {"<span style='font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba(255,184,0,0.15);color:#FFB800;border:1px solid rgba(255,184,0,0.3)'>⏸ PAUSED</span>" if paused else ""}
    {"<span style='font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;background:rgba(255,71,87,0.2);color:#FF4757;border:1px solid rgba(255,71,87,0.4)'>🛑 KILLED</span>" if killed else ""}
  </div>
  <div style="display:flex;overflow-x:auto;scrollbar-width:none;gap:4px">
    <div class="tab active" onclick="show('ov2',this)">Overview</div>
    <div class="tab" onclick="show('pos',this)">Positions</div>
    <div class="tab" onclick="show('tr',this)">Trades</div>
    <div class="tab" onclick="show('tx',this)">Tax</div>
    <div class="tab" onclick="show('dg',this)">Diagnostics</div>
  </div>
</div>
<div class="main">

<div id="ov2" class="sec active">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px">Emergency Controls</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:8px">
    {"<button class='ctrl' style='background:rgba(0,214,143,0.15);color:#00D68F;border:2px solid rgba(0,214,143,0.4);margin:0' onclick=\"doAction('resume')\">▶ Resume</button>" if (paused or killed) else "<button class='ctrl' style='background:rgba(255,184,0,0.15);color:#FFB800;border:2px solid rgba(255,184,0,0.4);margin:0' onclick=\"confirm_action('pause','Pause new entries?','Stops new entries. Exits still managed automatically.')\">⏸ Pause</button>"}
    <button class="ctrl" style="background:rgba(255,71,87,0.15);color:#FF4757;border:2px solid rgba(255,71,87,0.4);margin:0" onclick="confirm_action('close_all','Close ALL positions?','Immediately market-closes everything. Use for news events.')">⚡ Close All</button>
  </div>
  <button class="ctrl" style="background:rgba(255,71,87,0.25);color:#FF4757;border:2px solid #FF4757;font-size:14px" onclick="confirm_action('kill','KILL SWITCH — Stop Everything?','No new entries, no exit management. Positions stay open on HyperLiquid.')">🛑 KILL SWITCH</button>
  <div class="card" style="border-color:{'rgba(0,214,143,0.3)' if tax['total_net']>=0 else 'rgba(255,71,87,0.3)'}">
    <div style="font-size:10px;font-weight:700;color:#4A5878;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px">Net P&L (after 35% tax)</div>
    <div style="font-family:monospace;font-size:28px;font-weight:700;color:{'#00D68F' if tax['total_net']>=0 else '#FF4757'}">${tax["total_net"]:.2f}</div>
    <div style="font-size:12px;color:#4A5878;margin-top:4px">Gross: ${tax["total_pnl"]:.2f} · Tax: ${tax["total_tax"]:.2f}</div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
    <div class="card"><div style="font-size:10px;color:#4A5878;font-weight:600;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px">Balance</div><div style="font-family:monospace;font-size:18px;font-weight:700">${s["balance"]:.2f}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;font-weight:600;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px">Open</div><div style="font-family:monospace;font-size:18px;font-weight:700;color:#3D9EFF">{len(s["positions"])}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;font-weight:600;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px">Trades</div><div style="font-family:monospace;font-size:18px;font-weight:700">{tax["total_trades"]}</div></div>
    <div class="card"><div style="font-size:10px;color:#4A5878;font-weight:600;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px">Win Rate</div><div style="font-family:monospace;font-size:18px;font-weight:700;color:rgb({wrc})">{wr}</div></div>
  </div>
  <a href="/log" style="display:block;text-align:center;background:#0F1520;border:1px solid #1E2D42;border-radius:12px;padding:12px;color:#4A5878;font-size:13px;text-decoration:none;margin-bottom:12px">📋 Export Log</a>
  <div class="card">
    <div style="font-size:10px;font-weight:700;color:#4A5878;text-transform:uppercase;letter-spacing:.8px;margin-bottom:10px">System Info</div>
    {row("Cycle",f"#{s['cycle']}")}{row("Last check",s["last_check"] or "—")}{row("Next check",s["next_check"] or "—")}{row("Mode",mode)}
  </div>
</div>

<div id="pos" class="sec">
  {positions_html or '<div style="text-align:center;padding:48px 24px;color:#4A5878"><div style="font-size:36px;margin-bottom:12px">📭</div><div>No open positions</div><div style="font-size:12px;margin-top:6px">Waiting for signals...</div></div>'}
</div>

<div id="tr" class="sec">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px">Trade History</div>
  {f'<div class="card" style="padding:0 16px">{trades_html}</div>' if trades_html else '<div style="text-align:center;padding:48px 24px;color:#4A5878"><div style="font-size:36px;margin-bottom:12px">📋</div><div>No trades yet</div></div>'}
</div>

<div id="tx" class="sec">
  <div class="card" style="border-color:rgba(255,184,0,0.3)">
    <div style="font-size:10px;font-weight:700;color:#4A5878;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px">Tax Set-Aside (35%)</div>
    <div style="font-family:monospace;font-size:28px;font-weight:700;color:#FFB800">${tax["total_tax"]:.2f}</div>
    <div style="font-size:12px;color:#4A5878;margin-top:4px">Do not spend — owed to IRS</div>
  </div>
  <div class="card" style="padding:0">
    <div style="padding:10px 16px;font-size:10px;font-weight:700;text-transform:uppercase;color:#4A5878">P&L Breakdown</div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="font-size:13px;color:#4A5878">Gross</span><span style="font-family:monospace;font-weight:600;color:{'#00D68F' if tax['total_pnl']>=0 else '#FF4757'}">${tax["total_pnl"]:+.2f}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="font-size:13px;color:#4A5878">Tax (35%)</span><span style="font-family:monospace;font-weight:600;color:#FF4757">-${tax["total_tax"]:.2f}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;background:#161E2E"><span style="font-size:13px;font-weight:600">Net</span><span style="font-family:monospace;font-weight:600;font-size:16px;color:#00D68F">${tax["total_net"]:+.2f}</span></div>
    <div style="padding:10px 16px;font-size:10px;font-weight:700;text-transform:uppercase;color:#4A5878">Stats</div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="color:#4A5878">Total</span><span style="font-family:monospace;font-weight:600">{tax["total_trades"]}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px;border-bottom:1px solid #1E2D42"><span style="color:#4A5878">Wins</span><span style="font-family:monospace;font-weight:600;color:#00D68F">{tax["winning_trades"]}</span></div>
    <div style="display:flex;justify-content:space-between;padding:13px 16px"><span style="color:#4A5878">Losses</span><span style="font-family:monospace;font-weight:600;color:#FF4757">{tax["losing_trades"]}</span></div>
  </div>
</div>

<div id="dg" class="sec">
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin-bottom:10px">System Health</div>
  <div class="card" style="padding:0 16px">
    <div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid #1E2D42">
      <div style="font-size:14px;width:24px;text-align:center">{"✅" if h["api_connected"] else "❌"}</div>
      <div style="flex:1"><div style="font-size:13px;font-weight:600">HyperLiquid API</div><div style="font-size:11px;color:#4A5878;font-family:monospace">{h["last_ping"] or "never"}</div></div>
      <span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;background:rgba({"0,214,143" if h["api_connected"] else "255,71,87"},0.15);color:{"#00D68F" if h["api_connected"] else "#FF4757"}">{"CONNECTED" if h["api_connected"] else "OFFLINE"}</span>
    </div>
    <div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid #1E2D42">
      <div style="font-size:14px;width:24px;text-align:center">{"✅" if s["cycle"]>0 else "⏳"}</div>
      <div style="flex:1"><div style="font-size:13px;font-weight:600">Strategy Worker</div><div style="font-size:11px;color:#4A5878;font-family:monospace">Cycle #{s["cycle"]} · {status}</div></div>
      <span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;background:rgba({"255,71,87" if killed else "255,184,0" if paused else "0,214,143"},0.15);color:{"#FF4757" if killed else "#FFB800" if paused else "#00D68F"}">{"STOPPED" if killed else "PAUSED" if paused else "RUNNING" if s["cycle"]>0 else "STARTING"}</span>
    </div>
    <div style="display:flex;align-items:center;gap:10px;padding:10px 0">
      <div style="font-size:14px;width:24px;text-align:center">{"✅" if any_fresh else "⚠️"}</div>
      <div style="flex:1"><div style="font-size:13px;font-weight:600">Data Freshness</div><div style="font-size:11px;color:#4A5878;font-family:monospace">{s["last_check"] or "not yet"}</div></div>
      <span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;background:rgba({"0,214,143" if any_fresh else "255,184,0"},0.15);color:{"#00D68F" if any_fresh else "#FFB800"}">{"LIVE" if any_fresh else "STALE"}</span>
    </div>
  </div>
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin:16px 0 10px">Strategy Parameters</div>
  <div class="card" style="padding:0 16px">{params_html}</div>
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin:16px 0 10px">Asset Status</div>
  {asset_html}
  <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#4A5878;margin:4px 0 10px">Event Log</div>
  {f'<div class="card" style="padding:0 16px">{diag_html}</div>' if diag_html else '<div style="text-align:center;padding:48px 24px;color:#4A5878"><div style="font-size:36px;margin-bottom:12px">✅</div><div>No events yet</div></div>'}
</div>

</div>
<button class="rfb" onclick="location.reload()">↻</button>
<script>
function show(id,el){{document.querySelectorAll(".sec").forEach(s=>s.classList.remove("active"));document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));document.getElementById(id).classList.add("active");el.classList.add("active")}}
let pend=null;
function confirm_action(a,t,s){{pend=a;document.getElementById("ot").textContent=t;document.getElementById("os").textContent=s;document.getElementById("ov").classList.add("show")}}
function closeOv(){{document.getElementById("ov").classList.remove("show");pend=null}}
document.getElementById("oy").onclick=function(){{if(pend)doAction(pend);closeOv()}}
function doAction(a){{fetch("/control",{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify({{action:a}})}}).then(r=>r.json()).then(d=>{{if(d.ok)location.reload();else alert("Error: "+d.error)}})}}
setTimeout(()=>location.reload(),30000);
</script>
</body></html>'''

@app.route("/login",methods=["POST"])
def login():
    if request.form.get("p")==PASSWORD:
        session["ok"]=True
    return redirect("/")

@app.route("/logout")
def logout():
    session.clear(); return redirect("/")

@app.route("/control",methods=["POST"])
def control():
    if not session.get("ok"): return jsonify({"ok":False,"error":"unauthorized"}),401
    a=request.json.get("action","")
    with lock:
        if a=="pause":     state["paused"]=True;add_diag("WARNING","Paused","Dashboard","No new entries")
        elif a=="resume":  state["paused"]=False;state["kill_switch"]=False;add_diag("INFO","Resumed","Dashboard","Trading active")
        elif a=="kill":    state["kill_switch"]=True;add_diag("CRITICAL","Kill switch","Dashboard","All trading stopped")
        elif a=="close_all": state["close_all_requested"]=True;add_diag("WARNING","Close all","Dashboard","Closing positions")
        else: return jsonify({"ok":False,"error":"unknown"})
    return jsonify({"ok":True})

@app.route("/api/state")
def api_state():
    if not session.get("ok"): return jsonify({"error":"unauthorized"}),401
    return jsonify(state)

@app.route("/log")
def log_export():
    if not session.get("ok"): return "unauthorized",401
    s=state; lines=["="*60,"HL TRADER v2 — SYSTEM LOG",f"Generated: {ts()} UTC","="*60]
    lines.append(f"\nSTATUS: {s['status']} | Cycle #{s['cycle']} | {s['leverage']}x | {'DRY RUN' if s['dry_run'] else 'LIVE'} | {'Testnet' if s['testnet'] else 'Mainnet'}")
    lines.append(f"Paused: {s['paused']} | Kill: {s['kill_switch']} | API: {s['health']['api_connected']}")
    lines.append(f"\nP&L: Gross ${s['tax']['total_pnl']:+.4f} | Tax ${s['tax']['total_tax']:.4f} | Net ${s['tax']['total_net']:+.4f}")
    lines.append(f"Trades: {s['tax']['total_trades']} | Wins: {s['tax']['winning_trades']} | Losses: {s['tax']['losing_trades']}")
    lines.append("\nOPEN POSITIONS:")
    for asset,pos in s["positions"].items():
        lines.append(f"  {asset}: {pos['direction']} @ ${pos['entry']:,.2f} | cur=${pos.get('current_price',pos['entry']):,.2f} | P&L=${pos.get('unrealized_pnl',0):+.2f}")
    if not s["positions"]: lines.append("  None")
    lines.append("\nTRADE HISTORY (last 20):")
    for t in s["trades"][:20]:
        exit_str=f"${t['exit']:,.2f}" if t.get('exit') else "—"
        pnl_str=f"${t['pnl']:+.4f}" if t.get('pnl') is not None else "open"
        lines.append(f"  {t['time']} | {t['asset']} {t['direction']} {t['action']} | ${t['entry']:,.2f}→{exit_str} | {t.get('reason','')} | {pnl_str}")
    lines.append("\nASSET STATUS:")
    for asset in s["assets"]:
        ah=s["health"]["assets_ok"].get(asset,{})
        lines.append(f"  {asset}: ${ah.get('price',0):,.2f} | {ah.get('last_candle','?')} | {ah.get('signal','?')} | {'LIVE' if ah.get('fresh') else 'STALE'}")
    lines.append("\nDIAGNOSTICS (last 20):")
    for d in s["diagnostics"][:20]:
        lines.append(f"  {d['time']} [{d['level']}] {d['event']} | {d['cause']}")
    lines.append("\n"+"="*60)
    return Response("\n".join(lines),mimetype="text/plain")

_t=threading.Thread(target=trading_loop,daemon=True)
_t.start()

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False,use_reloader=False)
