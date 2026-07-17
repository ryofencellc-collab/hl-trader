"""
HYPERLIQUID TRADER — Final Validated System
═══════════════════════════════════════════
Params from 62,208 config mega backtest + OOS validation

Per-asset validated configs:
  BTC: trail | funding filter 1bp | BB breakout | variable sizing
  ETH: trail | skip overnight (6-10 UTC) | variable sizing
  SOL: partial 1%@25% | cooldown 5 bars | BB | strong close | variable sizing
  BNB: fixed TP 1% | strong close

Universal: variable sizing (weak=$500, med=$1000, strong=$2000)

DRY_RUN = True  → logs signals, no orders
DRY_RUN = False → live orders on testnet
TESTNET = False → real money on mainnet

Dashboard: /
Log export: /log  (copy/paste text summary)
"""

import threading, time, csv, os
from datetime import datetime, timezone
from flask import Flask, render_template_string, request, session, redirect, url_for, jsonify, Response
import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

# ══════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════
DRY_RUN         = True
TESTNET         = True
MAIN_WALLET     = "0xa90566c8d886CA63c1194101a7dA2Fa129D26B58"
API_PRIVATE_KEY = "0x5b75aa092ea3bd1ee77983ab5b8268607120a0145de6df11174b3f72f91b9ea0"
API_URL         = constants.TESTNET_API_URL if TESTNET else constants.MAINNET_API_URL
PASSWORD        = os.environ.get("DASHBOARD_PASSWORD", "hl2026")

ASSETS          = ["BTC", "ETH", "SOL", "BNB"]
TOTAL_USDC      = 999.0
BASE_POSITION   = TOTAL_USDC / len(ASSETS)   # ~$249.75
ACTIVE_LEVERAGE = 3
CHECK_INTERVAL  = 60
TAX_RATE        = 0.35

# Base params (locked)
EMA_FAST=5; EMA_MID=13; EMA_SLOW=34
STOP_PCT=0.05; TRAIL_PCT=0.01
VOL_FILTER=1.5; SEP_FILTER=0.003; BRK_BARS=12
CANDLE_TF="15m"; CANDLE_LIMIT=200

# Per-asset validated configs
ASSET_CFG = {
    "BTC": {
        "exit":            "trail",
        "funding_filter":  0.0001,   # skip if |funding| > 1bp
        "use_bb":          True,
        "use_sc":          False,
        "no_overnight":    False,
        "partial_trigger": None,
        "partial_size":    None,
        "tp_pct":          None,
        "cooldown":        0,
    },
    "ETH": {
        "exit":            "trail",
        "funding_filter":  None,
        "use_bb":          False,
        "use_sc":          False,
        "no_overnight":    True,     # skip 06:00-10:00 UTC (2AM-6AM ET)
        "partial_trigger": None,
        "partial_size":    None,
        "tp_pct":          None,
        "cooldown":        0,
    },
    "SOL": {
        "exit":            "partial",
        "funding_filter":  None,
        "use_bb":          True,
        "use_sc":          True,     # strong close required
        "no_overnight":    False,
        "partial_trigger": 0.01,     # exit 25% at 1% gain
        "partial_size":    0.25,
        "tp_pct":          None,
        "cooldown":        5,        # 5-bar cooldown after exit
    },
    "BNB": {
        "exit":            "fixed_tp",
        "funding_filter":  None,
        "use_bb":          False,
        "use_sc":          True,     # strong close required
        "no_overnight":    False,
        "partial_trigger": None,
        "partial_size":    None,
        "tp_pct":          0.01,     # fixed 1% take profit
        "cooldown":        0,
    },
}

# Variable sizing thresholds
def get_pos_usd(vol, vs, ef, es):
    if not vs or vs == 0: return BASE_POSITION
    vr  = vol / vs
    sep = abs(ef - es) / es if es else 0
    if vr >= 4.0 and sep >= 0.008: return BASE_POSITION * 2   # strong
    if vr >= 2.5 or sep >= 0.005:  return BASE_POSITION       # medium
    return BASE_POSITION * 0.5                                  # weak

# ══════════════════════════════════════════════════════════
# SHARED STATE
# ══════════════════════════════════════════════════════════
state = {
    "status": "starting", "last_check": None, "next_check": None,
    "cycle": 0, "dry_run": DRY_RUN, "testnet": TESTNET,
    "leverage": ACTIVE_LEVERAGE, "assets": ASSETS, "balance": 998.93,
    "positions": {}, "trades": [], "diagnostics": [], "weekly_pnl": {},
    "health": {
        "api_connected": False, "last_ping": None, "assets_ok": {},
        "params": {
            "ema": "5/13/34", "stop_pct": "5%", "trail_pct": "1%",
            "vol_filter": "1.5x", "sep_filter": "0.003", "brk_bars": "12",
            "candle_tf": "15m", "check_every": "60s",
            "leverage": f"{ACTIVE_LEVERAGE}x", "assets": ",".join(ASSETS),
            "btc_cfg": "trail|fr1bp|BB|varsz",
            "eth_cfg": "trail|no_overnight|varsz",
            "sol_cfg": "partial1%@25%|cd5|BB|SC|varsz",
            "bnb_cfg": "tp1%|SC",
        }
    },
    "tax": {
        "total_pnl": 0.0, "total_tax": 0.0, "total_net": 0.0,
        "winning_trades": 0, "losing_trades": 0, "total_trades": 0,
    },
}
state_lock = threading.Lock()

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")

def add_diag(level, event, cause, action):
    icons={"INFO":"ℹ️","WARNING":"⚠️","ERROR":"❌","CRITICAL":"🚨"}
    entry={"time":ts(),"level":level,"event":event,"cause":cause,"action":action}
    with state_lock:
        # Only log first occurrence of repeated errors to reduce noise
        if level=="ERROR" and state["diagnostics"]:
            last=state["diagnostics"][0]
            if last["event"]==event and last["level"]==level:
                return  # suppress repeated identical errors
        state["diagnostics"].insert(0,entry)
        state["diagnostics"]=state["diagnostics"][:200]
    log(f"{icons.get(level,'📋')} [{level}] {event} | {cause} | → {action}")

def add_trade_log(asset,action,direction,entry,exit_p,size,pnl,reason):
    trade={"time":ts(),"asset":asset,"action":action,"direction":direction,
           "entry":entry,"exit":exit_p,"size":size,"leverage":ACTIVE_LEVERAGE,
           "pnl":round(pnl,4) if pnl is not None else None,"reason":reason}
    with state_lock:
        state["trades"].insert(0,trade)
        state["trades"]=state["trades"][:500]
        if pnl is not None:
            wk=datetime.now(timezone.utc).strftime("%Y-W%W")
            state["weekly_pnl"][wk]=round(state["weekly_pnl"].get(wk,0)+pnl,4)

def record_tax(asset,direction,entry,exit_p,size,pnl):
    tax=max(0,pnl*TAX_RATE); net=pnl-tax
    with state_lock:
        state["tax"]["total_pnl"]    +=pnl
        state["tax"]["total_tax"]    +=tax
        state["tax"]["total_net"]    +=net
        state["tax"]["total_trades"] +=1
        if pnl>0: state["tax"]["winning_trades"]+=1
        else:      state["tax"]["losing_trades"] +=1
    row={"time":ts(),"asset":asset,"direction":direction,
         "entry":entry,"exit":exit_p,"size":size,"leverage":ACTIVE_LEVERAGE,
         "gross_pnl":round(pnl,4),"tax_35pct":round(tax,4),"net_pnl":round(net,4),
         "dry_run":DRY_RUN}
    fe=os.path.exists("hl_tax_tracker.csv")
    with open("hl_tax_tracker.csv","a",newline="") as f:
        import csv as _csv
        w=_csv.DictWriter(f,fieldnames=list(row.keys()))
        if not fe: w.writeheader()
        w.writerow(row)
    log(f"💰 TAX | Gross ${pnl:+.4f} | Tax ${tax:.4f} | Net ${net:+.4f}")

# ══════════════════════════════════════════════════════════
# EXCHANGE
# ══════════════════════════════════════════════════════════
wallet   = eth_account.Account.from_key(API_PRIVATE_KEY)
info     = Info(API_URL, skip_ws=True)
exchange = Exchange(wallet, API_URL, account_address=MAIN_WALLET)

# ══════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════
def ema_calc(values,p):
    k=2/(p+1);e=None;out=[]
    for v in values:
        e=v if e is None else v*k+e*(1-k);out.append(e)
    return out

def sma_calc(values,p):
    out=[None]*(p-1)
    for i in range(p-1,len(values)):
        out.append(sum(values[i-p+1:i+1])/p)
    return out

def bb_upper(closes,period=20,mult=2.0):
    out=[None]*period
    for i in range(period,len(closes)):
        w=closes[i-period:i]; m=sum(w)/period
        s=(sum((x-m)**2 for x in w)/period)**0.5
        out.append(m+mult*s)
    return out

def bb_lower(closes,period=20,mult=2.0):
    bbu=bb_upper(closes,period,mult)
    sma=[None]*period
    for i in range(period,len(closes)):
        w=closes[i-period:i]; m=sum(w)/period
        sma.append(m)
    out=[None]*len(closes)
    for i in range(period,len(closes)):
        if sma[i] and bbu[i]:
            out[i]=2*sma[i]-bbu[i]
    return out

def check_signal(candles, asset):
    if len(candles)<50: return None,None,0,0
    cfg=ASSET_CFG[asset]
    closes=[float(c["c"]) for c in candles]
    highs=[float(c["h"]) for c in candles]
    lows=[float(c["l"]) for c in candles]
    vols=[float(c["v"]) for c in candles]
    ef=ema_calc(closes,EMA_FAST); em=ema_calc(closes,EMA_MID); es=ema_calc(closes,EMA_SLOW)
    vs=sma_calc(vols,20)
    bbu=bb_upper(closes); bbl=bb_lower(closes)
    i=len(candles)-1

    # EMA stack
    if   ef[i] and em[i] and es[i] and ef[i]>em[i]>es[i]: d="LONG"
    elif ef[i] and em[i] and es[i] and ef[i]<em[i]<es[i]: d="SHORT"
    else: return None,None,0,0

    # Separation
    if es[i] and abs(ef[i]-es[i])/es[i]<SEP_FILTER: return None,None,0,0

    # Volume
    vol=vols[i]
    if vs[i] and vol<vs[i]*VOL_FILTER: return None,None,0,0

    # Breakout
    if i>=BRK_BARS:
        if d=="LONG"  and closes[i]<=max(highs[i-BRK_BARS:i]): return None,None,0,0
        if d=="SHORT" and closes[i]>=min(lows[i-BRK_BARS:i]):  return None,None,0,0

    # BB filter (BTC and SOL)
    if cfg["use_bb"]:
        if not bbu[i] or not bbl[i]: return None,None,0,0
        if d=="LONG"  and candles[i]["c"]<=bbu[i]: return None,None,0,0
        if d=="SHORT" and candles[i]["c"]>=bbl[i]: return None,None,0,0

    # Strong close (SOL and BNB)
    if cfg["use_sc"]:
        bar_range=float(candles[i]["h"])-float(candles[i]["l"])
        if bar_range>0:
            cp=(float(candles[i]["c"])-float(candles[i]["l"]))/bar_range
            if d=="LONG"  and cp<0.70: return None,None,0,0
            if d=="SHORT" and cp>0.30: return None,None,0,0

    return d, closes[i], vol, vs[i] if vs[i] else 0

# ══════════════════════════════════════════════════════════
# POSITION TRACKING
# ══════════════════════════════════════════════════════════
positions   = {}
last_candle = {}
last_exit   = {}   # asset -> bar index of last exit (for cooldown)
bar_count   = {}   # asset -> total bars seen

# ══════════════════════════════════════════════════════════
# TRADING
# ══════════════════════════════════════════════════════════
def enter_trade(asset, direction, price, vol, vs, ef, es):
    cfg = ASSET_CFG[asset]
    pos_usd  = get_pos_usd(vol, vs, ef, es)
    qty      = (pos_usd * ACTIVE_LEVERAGE) / price
    stop     = round(price*(1-STOP_PCT) if direction=="LONG" else price*(1+STOP_PCT), 2)
    trail    = round(price*(1-TRAIL_PCT) if direction=="LONG" else price*(1+TRAIL_PCT), 2)

    if DRY_RUN:
        log(f"[DRY] ENTER {direction} {asset} @ ${price:,.2f} | "
            f"size={qty:.4f} | notional=${pos_usd*ACTIVE_LEVERAGE:.0f} | "
            f"stop=${stop:,.2f} | trail=${trail:,.2f} | cfg={cfg['exit']}")
        positions[asset]={
            "direction":direction,"entry":price,"size":qty,"pos_usd":pos_usd,
            "stop":stop,"trail_peak":price,"trail_stop":trail,
            "partial_done":False,"partial_pnl":0.0,"qty_rem":qty,
        }
        add_trade_log(asset,"ENTER",direction,price,None,qty,None,"signal")
        with state_lock: state["positions"]={k:v for k,v in positions.items()}
        return

    try:
        r=exchange.market_open(asset,direction=="LONG",round(qty,4))
        if r and r.get("status")=="ok":
            statuses=r.get("response",{}).get("data",{}).get("statuses",[])
            fill=price
            if statuses and "filled" in statuses[0]:
                fill=float(statuses[0]["filled"]["avgPx"])
            stop=round(fill*(1-STOP_PCT) if direction=="LONG" else fill*(1+STOP_PCT),2)
            trail=round(fill*(1-TRAIL_PCT) if direction=="LONG" else fill*(1+TRAIL_PCT),2)
            qty2=(pos_usd*ACTIVE_LEVERAGE)/fill
            positions[asset]={
                "direction":direction,"entry":fill,"size":qty2,"pos_usd":pos_usd,
                "stop":stop,"trail_peak":fill,"trail_stop":trail,
                "partial_done":False,"partial_pnl":0.0,"qty_rem":qty2,
            }
            add_trade_log(asset,"ENTER",direction,fill,None,qty2,None,"signal")
            log(f"✅ ENTERED {direction} {asset} @ ${fill:,.2f}")
            with state_lock: state["positions"]={k:v for k,v in positions.items()}
        else:
            add_diag("ERROR",f"Entry failed {asset}",str(r),"Skipping")
    except Exception as e:
        add_diag("ERROR",f"Entry exception {asset}",str(e),"Skipping")

def exit_trade(asset, price, reason):
    if asset not in positions: return
    pos=positions[asset]
    cfg=ASSET_CFG[asset]

    if cfg["exit"]=="partial":
        rem_raw=((price-pos["entry"])*pos["qty_rem"] if pos["direction"]=="LONG"
                 else (pos["entry"]-price)*pos["qty_rem"])
        pnl=round(rem_raw+pos["partial_pnl"],4)
    else:
        pnl=round((price-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                  else (pos["entry"]-price)*pos["size"],4)

    if DRY_RUN:
        icon="✅" if pnl>=0 else "❌"
        log(f"[DRY] EXIT {pos['direction']} {asset} @ ${price:,.2f} | {reason} | P&L=${pnl:+.4f} {icon}")
        record_tax(asset,pos["direction"],pos["entry"],price,pos["size"],pnl)
        add_trade_log(asset,"EXIT",pos["direction"],pos["entry"],price,pos["size"],pnl,reason)
        last_exit[asset]=bar_count.get(asset,0)
        del positions[asset]
        with state_lock: state["positions"]={k:v for k,v in positions.items()}
        return

    try:
        r=exchange.market_close(asset)
        fill=price; closed=False
        if r and r.get("status")=="ok":
            statuses=r.get("response",{}).get("data",{}).get("statuses",[])
            if statuses and "filled" in statuses[0]:
                fill=float(statuses[0]["filled"]["avgPx"])
            closed=True
        elif r is None:
            time.sleep(3)
            s_check=info.user_state(MAIN_WALLET)
            still=[p for p in s_check.get("assetPositions",[])
                   if p["position"]["coin"]==asset and float(p["position"]["szi"])!=0]
            closed=len(still)==0
            if not closed:
                add_diag("CRITICAL",f"Exit failed {asset}","None response + still open","Manual check required")
        if closed:
            if cfg["exit"]=="partial":
                rem_raw=((fill-pos["entry"])*pos["qty_rem"] if pos["direction"]=="LONG"
                         else (pos["entry"]-fill)*pos["qty_rem"])
                pnl=round(rem_raw+pos["partial_pnl"],4)
            else:
                pnl=round((fill-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                          else (pos["entry"]-fill)*pos["size"],4)
            log(f"{'✅' if pnl>=0 else '❌'} EXITED {pos['direction']} {asset} @ ${fill:,.2f} | {reason} | P&L=${pnl:+.4f}")
            record_tax(asset,pos["direction"],pos["entry"],fill,pos["size"],pnl)
            add_trade_log(asset,"EXIT",pos["direction"],pos["entry"],fill,pos["size"],pnl,reason)
            last_exit[asset]=bar_count.get(asset,0)
        del positions[asset]
        with state_lock: state["positions"]={k:v for k,v in positions.items()}
    except Exception as e:
        add_diag("ERROR",f"Exit exception {asset}",str(e),"Position may still be open")

# ══════════════════════════════════════════════════════════
# TRADING LOOP
# ══════════════════════════════════════════════════════════
def trading_loop():
    print("\n"+"="*60)
    print("  HYPERLIQUID — FINAL VALIDATED SYSTEM")
    print(f"  {'DRY RUN' if DRY_RUN else 'LIVE'} | {'TESTNET' if TESTNET else 'MAINNET'}")
    print(f"  BTC:trail+fr+BB+varsz | ETH:trail+noON+varsz")
    print(f"  SOL:partial1%+cd5+BB+SC+varsz | BNB:tp1%+SC")
    print("="*60+"\n")

    add_diag("INFO","Final validated system started",
             f"DRY={DRY_RUN} TEST={TESTNET} LEV={ACTIVE_LEVERAGE}x",
             "Running every 60s with per-asset configs")

    retry_count=0; cycle=0

    while True:
        cycle+=1
        with state_lock:
            state["cycle"]=cycle
            state["last_check"]=ts()
            state["status"]="checking"

        log(f"── CYCLE {cycle} ──────────────────────────────")

        # API ping
        try:
            mids=info.all_mids()
            with state_lock:
                state["health"]["api_connected"]=True
                state["health"]["last_ping"]=ts()
        except Exception as e:
            with state_lock: state["health"]["api_connected"]=False
            add_diag("ERROR","API ping failed",str(e),"Retrying next cycle")
            with state_lock:
                state["status"]="waiting"
                state["next_check"]=f"in {CHECK_INTERVAL}s"
            time.sleep(CHECK_INTERVAL); continue

        for asset in ASSETS:
            cfg=ASSET_CFG[asset]
            bar_count[asset]=bar_count.get(asset,0)+1

            try:
                end_ms=int(time.time()*1000)
                start_ms=end_ms-CANDLE_LIMIT*15*60*1000
                candles=info.candles_snapshot(asset,CANDLE_TF,start_ms,end_ms)

                if not candles or len(candles)<50:
                    add_diag("WARNING",f"Insufficient candles {asset}",
                             f"Got {len(candles) if candles else 0}","Skipping")
                    continue

                candle_ts=str(candles[-1].get("t",candles[-1].get("T","")))
                cur=float(candles[-1]["c"])
                hi =float(candles[-1]["h"])
                lo =float(candles[-1]["l"])
                vol=float(candles[-1]["v"])
                if cur==0: continue

                # Data freshness
                candle_age_s=0
                if candle_ts.isdigit():
                    candle_age_s=int((time.time()*1000-int(candle_ts))/1000)
                    candle_age_str=f"{candle_age_s//60}m{candle_age_s%60}s ago"
                else:
                    candle_age_str=candle_ts

                # Time filter (ETH: skip overnight)
                if cfg["no_overnight"]:
                    hour_utc=datetime.now(timezone.utc).hour
                    if 6<=hour_utc<10:
                        log(f"⏸  {asset}: overnight hours (skip 6-10 UTC)")
                        continue

                # Compute indicators for asset status
                closes=[float(c["c"]) for c in candles]
                vols=[float(c["v"]) for c in candles]
                ef=ema_calc(closes,EMA_FAST)
                em=ema_calc(closes,EMA_MID)
                es=ema_calc(closes,EMA_SLOW)
                vs_arr=sma_calc(vols,20)
                vs=vs_arr[-1] if vs_arr[-1] else 0

                direction,signal_price,_,_=check_signal(candles,asset)

                with state_lock:
                    state["health"]["assets_ok"][asset]={
                        "ok":True,"price":cur,
                        "last_candle":candle_age_str,
                        "signal":f"{direction} @ ${signal_price:,.2f}" if direction else "no signal",
                        "fresh": candle_age_s < 1200,  # fresh if <20min old
                    }

                # Candle dedup
                if last_candle.get(asset)==candle_ts:
                    log(f"⏳ {asset}: same candle @ ${cur:,.2f}")
                    continue
                last_candle[asset]=candle_ts

                # Funding filter
                if cfg["funding_filter"] and candles[-1].get("fundingRate"):
                    fr=abs(float(candles[-1].get("fundingRate",0)))
                    if fr>cfg["funding_filter"]:
                        log(f"⏸  {asset}: funding too high ({fr:.5f} > {cfg['funding_filter']})")
                        continue

                # ── EXITS ─────────────────────────────────────────────────────
                if asset in positions:
                    pos=positions[asset]

                    # Update trailing stop
                    if pos["direction"]=="LONG" and hi>pos["trail_peak"]:
                        pos["trail_peak"]=hi; pos["trail_stop"]=round(hi*(1-TRAIL_PCT),2)
                        log(f"📈 {asset} trail → ${pos['trail_stop']:,.2f}")
                    elif pos["direction"]=="SHORT" and lo<pos["trail_peak"]:
                        pos["trail_peak"]=lo; pos["trail_stop"]=round(lo*(1+TRAIL_PCT),2)
                        log(f"📉 {asset} trail → ${pos['trail_stop']:,.2f}")

                    # Partial exit check (SOL)
                    if cfg["exit"]=="partial" and not pos["partial_done"]:
                        pt=cfg["partial_trigger"]; ps=cfg["partial_size"]
                        trig_p=(pos["entry"]*(1+pt) if pos["direction"]=="LONG"
                                else pos["entry"]*(1-pt))
                        hit=((pos["direction"]=="LONG" and hi>=trig_p) or
                             (pos["direction"]=="SHORT" and lo<=trig_p))
                        if hit:
                            pqty=pos["qty_rem"]*ps
                            praw=((trig_p-pos["entry"])*pqty if pos["direction"]=="LONG"
                                  else (pos["entry"]-trig_p)*pqty)
                            pos["partial_pnl"]+=praw; pos["qty_rem"]-=pqty
                            pos["partial_done"]=True; pos["stop"]=pos["entry"]
                            if pos["direction"]=="LONG":
                                pos["trail_peak"]=trig_p; pos["trail_stop"]=round(trig_p*(1-TRAIL_PCT),2)
                            else:
                                pos["trail_peak"]=trig_p; pos["trail_stop"]=round(trig_p*(1+TRAIL_PCT),2)
                            log(f"💰 {asset} PARTIAL EXIT {int(ps*100)}% @ ${trig_p:,.2f} | stop→breakeven")

                    # Fixed TP (BNB)
                    if cfg["exit"]=="fixed_tp" and cfg["tp_pct"]:
                        tp_p=(pos["entry"]*(1+cfg["tp_pct"]) if pos["direction"]=="LONG"
                              else pos["entry"]*(1-cfg["tp_pct"]))
                        if ((pos["direction"]=="LONG" and hi>=tp_p) or
                            (pos["direction"]=="SHORT" and lo<=tp_p)):
                            exit_trade(asset,tp_p,"tp"); continue

                    # Stop/trail exits
                    stop_hit=((pos["direction"]=="LONG" and lo<=pos["stop"]) or
                               (pos["direction"]=="SHORT" and hi>=pos["stop"]))
                    trail_hit=((pos["direction"]=="LONG" and lo<=pos["trail_stop"]) or
                                (pos["direction"]=="SHORT" and hi>=pos["trail_stop"]))
                    ema_cross=((pos["direction"]=="LONG" and ef[-1]<em[-1]) or
                                (pos["direction"]=="SHORT" and ef[-1]>em[-1]))

                    if stop_hit:    exit_trade(asset,pos["stop"],"stop")
                    elif trail_hit: exit_trade(asset,pos["trail_stop"],"trail")
                    elif ema_cross: exit_trade(asset,cur,"ema_cross")
                    else:
                        pnl=((cur-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                             else (pos["entry"]-cur)*pos["size"])
                        log(f"⏳ {asset} {pos['direction']} @ ${cur:,.2f} | "
                            f"trail=${pos['trail_stop']:,.2f} | P&L=${pnl:+.4f}")

                # ── ENTRIES ────────────────────────────────────────────────────
                else:
                    # Cooldown check
                    cd=cfg.get("cooldown",0)
                    if cd>0 and (bar_count.get(asset,0)-last_exit.get(asset,0))<cd:
                        log(f"⏸  {asset}: cooldown ({bar_count.get(asset,0)-last_exit.get(asset,0)}/{cd} bars)")
                        continue

                    if direction:
                        log(f"🚨 SIGNAL: {asset} {direction} @ ${signal_price:,.2f}")
                        enter_trade(asset,direction,signal_price,vol,vs,ef[-1],es[-1])
                    else:
                        log(f"⏳ {asset}: no signal @ ${cur:,.2f}")

                retry_count=0

            except Exception as e:
                retry_count+=1
                add_diag("ERROR",f"Error on {asset}",str(e),f"Retry {retry_count}/5")
                if retry_count>5:
                    add_diag("CRITICAL","Too many errors",f"{retry_count} consecutive","Pausing 5min")
                    time.sleep(300); retry_count=0

            time.sleep(0.5)

        with state_lock:
            state["status"]="waiting"
            state["next_check"]=f"in {CHECK_INTERVAL}s"
            state["positions"]={k:v for k,v in positions.items()}

        log(f"💤 Next check in {CHECK_INTERVAL}s")
        time.sleep(CHECK_INTERVAL)

# ══════════════════════════════════════════════════════════
# FLASK APP
# ══════════════════════════════════════════════════════════
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY","hl2026secret")

DASH = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>HL Trader</title>
<style>
@import url("https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap");
:root{--bg:#080B10;--surface:#0F1520;--surface2:#161E2E;--border:#1E2D42;--green:#00D68F;--red:#FF4757;--gold:#FFB800;--blue:#3D9EFF;--text:#E8EDF5;--muted:#4A5878;--mono:"JetBrains Mono",monospace;}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;}
body{background:var(--bg);color:var(--text);font-family:"Inter",sans-serif;min-height:100vh;padding-bottom:env(safe-area-inset-bottom);}
.lw{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;}
.lc{background:var(--surface);border:1px solid var(--border);border-radius:20px;padding:40px 32px;width:100%;max-width:360px;text-align:center;}
.ll{font-family:var(--mono);font-size:28px;font-weight:700;color:var(--green);margin-bottom:8px;}
.ls{color:var(--muted);font-size:13px;margin-bottom:32px;}
.li{width:100%;background:var(--surface2);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:16px;padding:14px 16px;margin-bottom:12px;outline:none;font-family:var(--mono);letter-spacing:2px;}
.li:focus{border-color:var(--green);}
.lb{width:100%;background:var(--green);color:#000;border:none;border-radius:12px;font-size:15px;font-weight:700;padding:14px;cursor:pointer;}
.le{color:var(--red);font-size:13px;margin-top:12px;}
.hd{position:sticky;top:0;z-index:100;background:rgba(8,11,16,0.95);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);border-bottom:1px solid var(--border);padding:12px 16px 0;padding-top:calc(12px + env(safe-area-inset-top));}
.hr2{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;}
.hl2{font-family:var(--mono);font-size:18px;font-weight:700;color:var(--green);}
.sp{display:flex;align-items:center;gap:6px;background:var(--surface);border:1px solid var(--border);border-radius:20px;padding:5px 10px;font-size:11px;font-weight:600;}
.dot{width:7px;height:7px;border-radius:50%;}
.dg{background:var(--green);animation:pulse 2s infinite;}
.dy{background:var(--gold);}
.dr2{background:var(--red);}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:0.4;}}
.fresh{font-size:10px;padding:2px 7px;border-radius:4px;font-weight:600;}
.fresh-ok{background:rgba(0,214,143,0.15);color:var(--green);}
.fresh-warn{background:rgba(255,184,0,0.15);color:var(--gold);}
.fresh-err{background:rgba(255,71,87,0.15);color:var(--red);}
.bdg{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:8px;}
.b{font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;letter-spacing:0.5px;}
.bb{background:rgba(61,158,255,0.15);color:var(--blue);border:1px solid rgba(61,158,255,0.3);}
.bg2{background:rgba(0,214,143,0.15);color:var(--green);border:1px solid rgba(0,214,143,0.3);}
.bgo{background:rgba(255,184,0,0.15);color:var(--gold);border:1px solid rgba(255,184,0,0.3);}
.bm{background:rgba(74,88,120,0.2);color:var(--muted);border:1px solid var(--border);}
.tabs{display:flex;overflow-x:auto;scrollbar-width:none;gap:4px;}
.tabs::-webkit-scrollbar{display:none;}
.tab{flex-shrink:0;padding:8px 14px 10px;font-size:13px;font-weight:600;color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap;}
.tab.active{color:var(--green);border-bottom-color:var(--green);}
.main{padding:16px;}
.sec{display:none;}.sec.active{display:block;}
.card{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:16px;margin-bottom:12px;}
.cl{font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;}
.cv{font-family:var(--mono);font-size:28px;font-weight:700;line-height:1;}
.cs{font-size:12px;color:var(--muted);margin-top:4px;}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;}
.sc2{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:14px;}
.sl{font-size:10px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:0.6px;margin-bottom:6px;}
.sv{font-family:var(--mono);font-size:18px;font-weight:700;}
.row{display:flex;justify-content:space-between;align-items:center;padding:11px 0;border-bottom:1px solid var(--border);}
.row:last-child{border-bottom:none;}
.rk{font-size:13px;color:var(--muted);}
.rv{font-family:var(--mono);font-weight:600;font-size:13px;}
.stitle{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:var(--muted);margin-bottom:10px;margin-top:4px;}
.green{color:var(--green);}.red{color:var(--red);}.gold{color:var(--gold);}.blue{color:var(--blue);}.muted{color:var(--muted);}
.ac{background:var(--surface2);border:1px solid var(--border);border-radius:14px;padding:14px;margin-bottom:10px;}
.ah{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;}
.an{font-family:var(--mono);font-size:15px;font-weight:700;}
.as2{font-size:11px;font-weight:700;padding:3px 10px;border-radius:6px;}
.aok{background:rgba(0,214,143,0.15);color:var(--green);}
.aer{background:rgba(255,71,87,0.15);color:var(--red);}
.awk{background:rgba(255,184,0,0.15);color:var(--gold);}
.pos-pnl{font-family:var(--mono);font-size:14px;font-weight:700;margin-top:8px;padding:8px;border-radius:8px;text-align:center;}
.tr{display:flex;align-items:center;padding:12px 0;border-bottom:1px solid var(--border);gap:12px;}
.tr:last-child{border-bottom:none;}
.ti{width:36px;height:36px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0;}
.tiw{background:rgba(0,214,143,0.15);}
.til{background:rgba(255,71,87,0.15);}
.tio{background:rgba(61,158,255,0.15);}
.tif{flex:1;min-width:0;}
.ta{font-weight:600;font-size:14px;display:flex;align-items:center;gap:6px;}
.tt{font-size:11px;color:var(--muted);margin-top:2px;}
.tp2{font-family:var(--mono);font-weight:700;font-size:15px;text-align:right;}
.dr3{display:flex;gap:10px;padding:12px 0;border-bottom:1px solid var(--border);align-items:flex-start;}
.dr3:last-child{border-bottom:none;}
.db{font-size:10px;font-weight:700;padding:3px 7px;border-radius:6px;white-space:nowrap;margin-top:2px;}
.dI{background:rgba(61,158,255,0.15);color:var(--blue);}
.dW{background:rgba(255,184,0,0.15);color:var(--gold);}
.dE{background:rgba(255,71,87,0.15);color:var(--red);}
.dC{background:rgba(255,71,87,0.25);color:var(--red);border:1px solid var(--red);}
.dbody{flex:1;min-width:0;}
.dev{font-weight:600;font-size:13px;margin-bottom:2px;}
.dca{font-size:11px;color:var(--muted);margin-bottom:2px;}
.dac{font-size:11px;color:var(--blue);}
.dtm{font-size:10px;color:var(--muted);margin-top:3px;font-family:var(--mono);}
.txr{display:flex;justify-content:space-between;align-items:center;padding:13px 16px;border-bottom:1px solid var(--border);}
.txr:last-child{border-bottom:none;}
.txk{font-size:13px;color:var(--muted);}
.txv{font-family:var(--mono);font-weight:600;font-size:14px;}
.hr3{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border);}
.hr3:last-child{border-bottom:none;}
.hi{font-size:14px;width:24px;text-align:center;flex-shrink:0;}
.hb{flex:1;}
.hn{font-size:13px;font-weight:600;}
.hd3{font-size:11px;color:var(--muted);margin-top:2px;font-family:var(--mono);}
.hs{font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;}
.hok{background:rgba(0,214,143,0.15);color:var(--green);}
.hwn{background:rgba(255,184,0,0.15);color:var(--gold);}
.her{background:rgba(255,71,87,0.15);color:var(--red);}
.empty{text-align:center;padding:48px 24px;color:var(--muted);}
.ei{font-size:36px;margin-bottom:12px;}
.wb{display:flex;align-items:flex-end;gap:4px;height:80px;padding:0 4px;margin-top:12px;}
.wbw{flex:1;display:flex;flex-direction:column;align-items:center;gap:4px;}
.wb2{width:100%;border-radius:4px 4px 0 0;min-height:4px;}
.wbp{background:var(--green);opacity:0.8;}
.wbn{background:var(--red);opacity:0.8;}
.wbl{font-size:9px;color:var(--muted);font-family:var(--mono);}
.log-btn{display:block;text-align:center;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:12px;color:var(--muted);font-size:13px;text-decoration:none;margin-bottom:12px;}
.rfb{position:fixed;bottom:calc(24px + env(safe-area-inset-bottom));right:20px;width:48px;height:48px;border-radius:50%;background:var(--green);color:#000;border:none;font-size:20px;display:flex;align-items:center;justify-content:center;cursor:pointer;box-shadow:0 4px 20px rgba(0,214,143,0.4);z-index:50;}
</style>
</head>
<body>
{% if not li %}
<div class="lw"><div class="lc">
  <div class="ll">HL TRADER</div>
  <div class="ls">HyperLiquid Strategy Dashboard</div>
  <form method="POST" action="/login">
    <input class="li" type="password" name="password" placeholder="Password" autofocus>
    <button class="lb" type="submit">Enter</button>
    {% if err %}<div class="le">{{ err }}</div>{% endif %}
  </form>
</div></div>
{% else %}
{% set s=st %}{% set h=s.health %}{% set tax=s.tax %}
{% set any_fresh=s.health.assets_ok.values()|selectattr("fresh")|list|length > 0 %}
<div class="hd">
  <div class="hr2">
    <div class="hl2">HL TRADER</div>
    <div style="display:flex;align-items:center;gap:8px;">
      {% if any_fresh %}
        <span class="fresh fresh-ok">LIVE</span>
      {% else %}
        <span class="fresh fresh-warn">STALE</span>
      {% endif %}
      <div class="sp">
        {% if s.status in ["checking","waiting","running"] %}<div class="dot dg"></div>
        {% elif s.status=="stopped" %}<div class="dot dr2"></div>
        {% else %}<div class="dot dy"></div>{% endif %}
        {{ s.status|upper }}
      </div>
    </div>
  </div>
  <div class="bdg">
    {% if s.dry_run %}<span class="b bb">DRY RUN</span>
    {% elif s.testnet %}<span class="b bgo">TESTNET</span>
    {% else %}<span class="b bg2">● LIVE</span>{% endif %}
    <span class="b bm">{{ s.leverage }}x</span>
    <span class="b bm">EMA 5/13/34</span>
    <span class="b bm">BTC·ETH·SOL·BNB</span>
    <span class="b bm">Every 60s</span>
  </div>
  <div class="tabs">
    <div class="tab active" onclick="show('ov',this)">Overview</div>
    <div class="tab" onclick="show('pos',this)">Positions</div>
    <div class="tab" onclick="show('tr',this)">Trades</div>
    <div class="tab" onclick="show('tx',this)">Tax</div>
    <div class="tab" onclick="show('dg',this)">Diagnostics</div>
  </div>
</div>
<div class="main">

<!-- OVERVIEW -->
<div id="ov" class="sec active">
  <div class="card" style="border-color:{% if tax.total_net>=0 %}rgba(0,214,143,0.3){% else %}rgba(255,71,87,0.3){% endif %}">
    <div class="cl">Net P&L (after 35% tax)</div>
    <div class="cv {% if tax.total_net>=0 %}green{% else %}red{% endif %}">${{ "%.2f"|format(tax.total_net) }}</div>
    <div class="cs">Gross: ${{ "%.2f"|format(tax.total_pnl) }} · Tax: ${{ "%.2f"|format(tax.total_tax) }}</div>
  </div>
  <div class="g2">
    <div class="sc2"><div class="sl">Balance</div><div class="sv">${{ "%.2f"|format(s.balance) }}</div></div>
    <div class="sc2"><div class="sl">Open</div><div class="sv blue">{{ s.positions|length }}</div></div>
    <div class="sc2"><div class="sl">Trades</div><div class="sv">{{ tax.total_trades }}</div></div>
    <div class="sc2"><div class="sl">Win Rate</div>
      {% if tax.total_trades>0 %}
      <div class="sv {% if tax.winning_trades/tax.total_trades>=0.6 %}green{% else %}gold{% endif %}">
        {{ "%.0f"|format(tax.winning_trades/tax.total_trades*100) }}%
      </div>
      {% else %}<div class="sv muted">—</div>{% endif %}
    </div>
  </div>
  {% if s.weekly_pnl %}
  <div class="card"><div class="cl">Weekly P&L</div>
    {% set wv=s.weekly_pnl.values()|list %}{% set mx=namespace(v=1) %}
    {% for v in wv %}{% if v|abs>mx.v %}{% set mx.v=v|abs %}{% endif %}{% endfor %}
    <div class="wb">{% for wk,val in s.weekly_pnl.items()|list %}
      {% set hh=([4,(val|abs/mx.v*70)|int]|max) %}
      <div class="wbw"><div class="wb2 {% if val>=0 %}wbp{% else %}wbn{% endif %}" style="height:{{hh}}px"></div>
      <div class="wbl">W{{loop.index}}</div></div>
    {% endfor %}</div>
  </div>{% endif %}
  <a href="/log" class="log-btn">📋 Export log — copy/paste to share</a>
  <div class="card">
    <div class="cl">System Info</div>
    <div class="row"><span class="rk">Cycle</span><span class="rv">#{{ s.cycle }}</span></div>
    <div class="row"><span class="rk">Last check</span><span class="rv">{{ s.last_check or "—" }}</span></div>
    <div class="row"><span class="rk">Next check</span><span class="rv">{{ s.next_check or "—" }}</span></div>
    <div class="row"><span class="rk">Mode</span><span class="rv">{{ "DRY RUN" if s.dry_run else ("TESTNET" if s.testnet else "LIVE") }}</span></div>
    <div class="row"><span class="rk">Network</span><span class="rv">{{ "Testnet" if s.testnet else "Mainnet" }}</span></div>
  </div>
</div>

<!-- POSITIONS -->
<div id="pos" class="sec">
  {% if s.positions %}{% for asset,pos in s.positions.items() %}
  {% set cur_price=h.assets_ok.get(asset,{}).get("price",pos.entry) %}
  {% set pnl=(cur_price-pos.entry)*pos.size if pos.direction=="LONG" else (pos.entry-cur_price)*pos.size %}
  <div class="ac">
    <div class="ah">
      <div class="an">{{ asset }}-PERP</div>
      <div class="as2 {% if pos.direction=='LONG' %}aok{% else %}aer{% endif %}">{{ pos.direction }}</div>
    </div>
    <div class="row"><span class="rk">Entry</span><span class="rv">${{ "{:,.2f}".format(pos.entry) }}</span></div>
    <div class="row"><span class="rk">Current</span><span class="rv {% if pnl>=0 %}green{% else %}red{% endif %}">${{ "{:,.2f}".format(cur_price) }}</span></div>
    <div class="row"><span class="rk">Hard Stop</span><span class="rv red">${{ "{:,.2f}".format(pos.stop) }}</span></div>
    <div class="row"><span class="rk">Trail Stop</span><span class="rv gold">${{ "{:,.2f}".format(pos.trail_stop) }}</span></div>
    <div class="row"><span class="rk">Size</span><span class="rv">{{ "%.4f"|format(pos.size) }}</span></div>
    <div class="row" style="border:0"><span class="rk">Unrealized P&L</span>
      <span class="rv {% if pnl>=0 %}green{% else %}red{% endif %}">${{ "%+.2f"|format(pnl) }}</span>
    </div>
  </div>{% endfor %}
  {% else %}<div class="empty"><div class="ei">📭</div><div>No open positions</div></div>{% endif %}
</div>

<!-- TRADES -->
<div id="tr" class="sec">
  <div class="stitle">Trade History</div>
  {% if s.trades %}<div class="card" style="padding:0 16px">
    {% for t in s.trades[:50] %}{% set ie=t.action=="EXIT" %}{% set iw=t.pnl is not none and t.pnl>=0 %}
    <div class="tr">
      <div class="ti {% if not ie %}tio{% elif iw %}tiw{% else %}til{% endif %}">{% if not ie %}📊{% elif iw %}✅{% else %}❌{% endif %}</div>
      <div class="tif">
        <div class="ta">{{ t.asset }}
          <span style="font-size:11px;padding:2px 6px;border-radius:4px;{% if t.direction=='LONG' %}background:rgba(0,214,143,0.15);color:var(--green){% else %}background:rgba(255,71,87,0.15);color:var(--red){% endif %}">{{ t.direction }}</span>
          <span style="font-size:10px;color:var(--muted)">{{ t.action }}</span>
        </div>
        <div class="tt">${{ "{:,.2f}".format(t.entry) }}{% if t.exit %} → ${{ "{:,.2f}".format(t.exit) }}{% endif %} · {{ t.reason or "" }}</div>
        <div class="tt">{{ t.time }}</div>
      </div>
      {% if t.pnl is not none %}<div class="tp2 {% if iw %}green{% else %}red{% endif %}">${{ "%+.2f"|format(t.pnl) }}</div>{% endif %}
    </div>{% endfor %}
  </div>
  {% else %}<div class="empty"><div class="ei">📋</div><div>No trades yet</div></div>{% endif %}
</div>

<!-- TAX -->
<div id="tx" class="sec">
  <div class="card" style="border-color:rgba(255,184,0,0.3)">
    <div class="cl">Tax Set-Aside (35%)</div>
    <div class="cv gold">${{ "%.2f"|format(tax.total_tax) }}</div>
    <div class="cs">Do not spend — owed to IRS</div>
  </div>
  <div class="card" style="padding:0">
    <div style="padding:10px 16px;font-size:10px;font-weight:700;text-transform:uppercase;color:var(--muted)">P&L</div>
    <div class="txr"><span class="txk">Gross</span><span class="txv {% if tax.total_pnl>=0 %}green{% else %}red{% endif %}">${{ "%+.2f"|format(tax.total_pnl) }}</span></div>
    <div class="txr"><span class="txk">Tax (35%)</span><span class="txv red">-${{ "%.2f"|format(tax.total_tax) }}</span></div>
    <div class="txr" style="background:var(--surface2)"><span class="txk" style="font-weight:600;color:var(--text)">Net</span><span class="txv green" style="font-size:16px">${{ "%+.2f"|format(tax.total_net) }}</span></div>
    <div style="padding:10px 16px;font-size:10px;font-weight:700;text-transform:uppercase;color:var(--muted)">Stats</div>
    <div class="txr"><span class="txk">Trades</span><span class="txv">{{ tax.total_trades }}</span></div>
    <div class="txr"><span class="txk">Wins</span><span class="txv green">{{ tax.winning_trades }}</span></div>
    <div class="txr"><span class="txk">Losses</span><span class="txv red">{{ tax.losing_trades }}</span></div>
    {% if tax.total_trades>0 %}<div class="txr"><span class="txk">Win rate</span><span class="txv">{{ "%.1f"|format(tax.winning_trades/tax.total_trades*100) }}%</span></div>{% endif %}
  </div>
  <div style="font-size:11px;color:var(--muted);text-align:center;padding:16px 0;line-height:1.6">Section 1256 · Estimate only · Consult CPA<br>NY+NYC ~11% additional</div>
</div>

<!-- DIAGNOSTICS -->
<div id="dg" class="sec">
  <div class="stitle">System Health</div>
  <div class="card" style="padding:0 16px">
    <div class="hr3">
      <div class="hi">{% if h.api_connected %}✅{% else %}❌{% endif %}</div>
      <div class="hb"><div class="hn">HyperLiquid API</div><div class="hd3">Last ping: {{ h.last_ping or "never" }}</div></div>
      <span class="hs {% if h.api_connected %}hok{% else %}her{% endif %}">{% if h.api_connected %}CONNECTED{% else %}OFFLINE{% endif %}</span>
    </div>
    <div class="hr3">
      <div class="hi">{% if s.cycle>0 %}✅{% else %}⏳{% endif %}</div>
      <div class="hb"><div class="hn">Strategy Worker</div><div class="hd3">Cycle #{{ s.cycle }} · {{ s.status }}</div></div>
      <span class="hs {% if s.cycle>0 %}hok{% else %}hwn{% endif %}">{% if s.cycle>0 %}RUNNING{% else %}STARTING{% endif %}</span>
    </div>
    <div class="hr3">
      <div class="hi">{% if any_fresh %}✅{% else %}⚠️{% endif %}</div>
      <div class="hb"><div class="hn">Data Freshness</div><div class="hd3">{{ s.last_check or "not yet" }}</div></div>
      <span class="hs {% if any_fresh %}hok{% else %}hwn{% endif %}">{% if any_fresh %}LIVE{% else %}STALE{% endif %}</span>
    </div>
  </div>

  <div class="stitle" style="margin-top:16px">Strategy Parameters</div>
  <div class="card" style="padding:0 16px">
    {% for key,val,exp in [
      ("EMA",h.params.ema,"5/13/34"),("Stop",h.params.stop_pct,"5%"),
      ("Trail",h.params.trail_pct,"1%"),("Vol Filter",h.params.vol_filter,"1.5x"),
      ("EMA Sep",h.params.sep_filter,"0.003"),("Breakout",h.params.brk_bars,"12"),
      ("Candle TF",h.params.candle_tf,"15m"),("Check",h.params.check_every,"60s"),
      ("Leverage",h.params.leverage,"3x"),("Assets",h.params.assets,"BTC,ETH,SOL,BNB"),
      ("BTC cfg",h.params.btc_cfg,"trail|fr1bp|BB|varsz"),
      ("ETH cfg",h.params.eth_cfg,"trail|no_overnight|varsz"),
      ("SOL cfg",h.params.sol_cfg,"partial1%@25%|cd5|BB|SC|varsz"),
      ("BNB cfg",h.params.bnb_cfg,"tp1%|SC"),
    ] %}
    <div class="hr3">
      <div class="hi">{% if val==exp %}✅{% else %}⚠️{% endif %}</div>
      <div class="hb"><div class="hn">{{ key }}</div><div class="hd3">Expected: {{ exp }}</div></div>
      <span class="hs {% if val==exp %}hok{% else %}hwn{% endif %}" style="font-size:10px">{{ val }}</span>
    </div>{% endfor %}
  </div>

  <div class="stitle" style="margin-top:16px">Asset Status</div>
  {% for asset in s.assets %}{% set ah=h.assets_ok.get(asset,{}) %}
  <div class="ac">
    <div class="ah">
      <div class="an">{{ asset }}-PERP</div>
      <div style="display:flex;gap:6px;align-items:center;">
        {% if ah.get("fresh") %}<span class="fresh fresh-ok">LIVE</span>{% else %}<span class="fresh fresh-warn">STALE</span>{% endif %}
        <div class="as2 {% if ah.get('ok') %}aok{% elif ah.get('ok')==false %}aer{% else %}awk{% endif %}">
          {% if ah.get('ok') %}OK{% elif ah.get('ok')==false %}ERROR{% else %}CHECKING{% endif %}
        </div>
      </div>
    </div>
    <div class="row"><span class="rk">Price</span><span class="rv">${{ "{:,.2f}".format(ah.get('price',0)) if ah.get('price') else "—" }}</span></div>
    <div class="row"><span class="rk">Last candle</span><span class="rv">{{ ah.get('last_candle','—') }}</span></div>
    <div class="row" style="border:0"><span class="rk">Signal</span>
      <span class="rv {% if ah.get('signal') and ah.get('signal')!='no signal' %}green{% else %}muted{% endif %}">{{ ah.get('signal','—') }}</span>
    </div>
  </div>{% endfor %}

  {% set errs=s.diagnostics|selectattr("level","in",["ERROR","CRITICAL"])|list %}
  {% if errs %}<div style="background:rgba(255,71,87,0.1);border:1px solid rgba(255,71,87,0.3);border-radius:12px;padding:12px 16px;margin:12px 0;font-size:13px;color:var(--red);font-weight:600">⚠️ {{ errs|length }} error(s)</div>{% endif %}

  <div class="stitle" style="margin-top:4px">Event Log</div>
  {% if s.diagnostics %}<div class="card" style="padding:0 16px">
    {% for d in s.diagnostics[:50] %}
    <div class="dr3">
      <span class="db d{{d.level[0]}}">{{ d.level }}</span>
      <div class="dbody">
        <div class="dev">{{ d.event }}</div>
        <div class="dca">{{ d.cause }}</div>
        <div class="dac">→ {{ d.action }}</div>
        <div class="dtm">{{ d.time }}</div>
      </div>
    </div>{% endfor %}
  </div>
  {% else %}<div class="empty"><div class="ei">✅</div><div>No events yet</div></div>{% endif %}
</div>
</div>
<button class="rfb" onclick="location.reload()">↻</button>
<script>
function show(id,el){document.querySelectorAll(".sec").forEach(s=>s.classList.remove("active"));document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));document.getElementById(id).classList.add("active");el.classList.add("active");}
setTimeout(()=>location.reload(),30000);
</script>
{% endif %}
</body></html>'''

@app.route("/")
def index():
    if not session.get("li"):
        return render_template_string(DASH,li=False,err=None,st=None)
    return render_template_string(DASH,li=True,st=state)

@app.route("/login",methods=["POST"])
def login():
    if request.form.get("password")==PASSWORD:
        session["li"]=True; return redirect(url_for("index"))
    return render_template_string(DASH,li=False,err="Wrong password",st=None)

@app.route("/logout")
def logout():
    session.clear(); return redirect(url_for("index"))

@app.route("/api/state")
def api_state():
    if not session.get("li"): return jsonify({"error":"unauthorized"}),401
    return jsonify(state)

@app.route("/log")
def log_export():
    if not session.get("li"): return "unauthorized",401
    s=state; lines=[]
    lines.append("="*60)
    lines.append("HL TRADER — SYSTEM LOG")
    lines.append(f"Generated: {ts()} UTC")
    lines.append("="*60)
    lines.append(f"\n── SYSTEM STATUS")
    lines.append(f"Status:        {s['status']}")
    lines.append(f"Mode:          {'DRY RUN' if s['dry_run'] else 'LIVE'}")
    lines.append(f"Network:       {'Testnet' if s['testnet'] else 'Mainnet'}")
    lines.append(f"Cycle:         #{s['cycle']}")
    lines.append(f"Last check:    {s['last_check']}")
    lines.append(f"Next check:    {s['next_check']}")
    lines.append(f"Leverage:      {s['leverage']}x")
    lines.append(f"API connected: {s['health']['api_connected']}")
    lines.append(f"Last ping:     {s['health']['last_ping']}")
    lines.append(f"\n── P&L SUMMARY")
    tax=s['tax']
    lines.append(f"Total trades:  {tax['total_trades']}")
    lines.append(f"Wins:          {tax['winning_trades']}")
    lines.append(f"Losses:        {tax['losing_trades']}")
    wr=tax['winning_trades']/tax['total_trades']*100 if tax['total_trades'] else 0
    lines.append(f"Win rate:      {wr:.1f}%")
    lines.append(f"Gross P&L:     ${tax['total_pnl']:+.4f}")
    lines.append(f"Tax (35%):     ${tax['total_tax']:.4f}")
    lines.append(f"Net P&L:       ${tax['total_net']:+.4f}")
    lines.append(f"\n── OPEN POSITIONS")
    if s['positions']:
        for asset,pos in s['positions'].items():
            cur=s['health']['assets_ok'].get(asset,{}).get('price',pos['entry'])
            upnl=(cur-pos['entry'])*pos['size'] if pos['direction']=="LONG" else (pos['entry']-cur)*pos['size']
            lines.append(f"{asset}: {pos['direction']} entry=${pos['entry']:,.2f} cur=${cur:,.2f} stop=${pos['stop']:,.2f} trail=${pos['trail_stop']:,.2f} size={pos['size']:.4f} unrealized=${upnl:+.2f}")
    else:
        lines.append("None")
    lines.append(f"\n── TRADE HISTORY (last 20)")
    for t in s['trades'][:20]:
        pnl_str=f"${t['pnl']:+.4f}" if t['pnl'] is not None else "open"
        exit_str=f"${t['exit']:,.2f}" if t['exit'] else "—"
        lines.append(f"{t['time']} | {t['asset']} {t['direction']} {t['action']} | ${t['entry']:,.2f}→{exit_str} | {t['reason']} | {pnl_str}")
    lines.append(f"\n── ASSET STATUS")
    for asset in s['assets']:
        ah=s['health']['assets_ok'].get(asset,{})
        fresh="LIVE" if ah.get('fresh') else "STALE"
        lines.append(f"{asset}: ${ah.get('price',0):,.2f} | {ah.get('last_candle','?')} | {ah.get('signal','?')} | {fresh}")
    lines.append(f"\n── WEEKLY P&L")
    for wk,pnl in sorted(s['weekly_pnl'].items()):
        lines.append(f"{wk}: ${pnl:+.2f}")
    lines.append(f"\n── DIAGNOSTICS (last 20)")
    for d in s['diagnostics'][:20]:
        lines.append(f"{d['time']} [{d['level']}] {d['event']} | {d['cause']}")
    lines.append("\n"+"="*60)
    return Response("\n".join(lines),mimetype="text/plain")

# ══════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════
_trader_thread=threading.Thread(target=trading_loop,daemon=True)
_trader_thread.start()

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False,use_reloader=False)
