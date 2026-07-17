"""
HYPERLIQUID STRATEGY ENGINE v2
════════════════════════════════
Validated: 93,312 optimizer runs + OOS + walk-forward
EMA 5/13/34 | Stop 5% | Trail 1% | Vol 1.5x | Breakout 12bar
Assets: BTC, ETH, SOL, BNB | 15min candles | checks every 60s

Backtest results (2yr, $250/asset, 3x leverage):
  Portfolio: 100% green weeks | $251/wk median | 11-14 wks to $20k
"""

import time, csv, os
from datetime import datetime, timezone
import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
import state as S

# ══════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════
DRY_RUN         = True    # ← False to place real orders
TESTNET         = True    # ← False for real money (mainnet)

MAIN_WALLET     = "0xa90566c8d886CA63c1194101a7dA2Fa129D26B58"
API_PRIVATE_KEY = "0x5b75aa092ea3bd1ee77983ab5b8268607120a0145de6df11174b3f72f91b9ea0"
API_URL         = constants.TESTNET_API_URL if TESTNET else constants.MAINNET_API_URL

ASSETS          = ["BTC", "ETH", "SOL", "BNB"]
TOTAL_USDC      = 999.0
POSITION_USD    = TOTAL_USDC / len(ASSETS)   # ~$249.75 per asset
ACTIVE_LEVERAGE = 3
CHECK_INTERVAL  = 60      # seconds
TAX_RATE        = 0.35

# Strategy params — validated
EMA_FAST = 5;   EMA_MID = 13;   EMA_SLOW = 34
STOP_PCT = 0.05; TRAIL_PCT = 0.01
VOL_FILTER = 1.5; SEP_FILTER = 0.003; BRK_BARS = 12
CANDLE_TF = "15m"; CANDLE_LIMIT = 200

# ══════════════════════════════════════════════════════════
# SETUP
# ══════════════════════════════════════════════════════════
wallet   = eth_account.Account.from_key(API_PRIVATE_KEY)
info     = Info(API_URL, skip_ws=True)
exchange = Exchange(wallet, API_URL, account_address=MAIN_WALLET)

st = S.load()
st.update({
    "dry_run":  DRY_RUN,
    "testnet":  TESTNET,
    "leverage": ACTIVE_LEVERAGE,
    "status":   "starting",
    "assets":   ASSETS,
    "balance":  998.93,
})
# Set confirmed params in health
st["health"]["params"] = {
    "ema":         "5/13/34",
    "stop_pct":    "5%",
    "trail_pct":   "1%",
    "vol_filter":  "1.5x",
    "sep_filter":  "0.003",
    "brk_bars":    "12",
    "candle_tf":   "15m",
    "check_every": "60s",
    "leverage":    f"{ACTIVE_LEVERAGE}x",
    "assets":      ",".join(ASSETS),
}
S.save(st)

positions   = {}
last_candle = {}
retry_count = 0

# ══════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════
def ema_calc(values, p):
    k=2/(p+1); e=None; out=[]
    for v in values:
        e=v if e is None else v*k+e*(1-k)
        out.append(e)
    return out

def sma_calc(values, p):
    out=[None]*(p-1)
    for i in range(p-1,len(values)):
        out.append(sum(values[i-p+1:i+1])/p)
    return out

def check_signal(candles):
    if len(candles)<50: return None,None
    closes=[float(c["c"]) for c in candles]
    highs =[float(c["h"]) for c in candles]
    lows  =[float(c["l"]) for c in candles]
    vols  =[float(c["v"]) for c in candles]
    ef=ema_calc(closes,EMA_FAST); em=ema_calc(closes,EMA_MID); es=ema_calc(closes,EMA_SLOW)
    vs=sma_calc(vols,20); i=len(candles)-1
    if   ef[i] and em[i] and es[i] and ef[i]>em[i]>es[i]: d="LONG"
    elif ef[i] and em[i] and es[i] and ef[i]<em[i]<es[i]: d="SHORT"
    else: return None,None
    if es[i] and abs(ef[i]-es[i])/es[i]<SEP_FILTER: return None,None
    if vs[i] and vols[i]<vs[i]*VOL_FILTER: return None,None
    if i>=BRK_BARS:
        if d=="LONG"  and closes[i]<=max(highs[i-BRK_BARS:i]): return None,None
        if d=="SHORT" and closes[i]>=min(lows[i-BRK_BARS:i]):  return None,None
    return d,closes[i]

# ══════════════════════════════════════════════════════════
# SIZE
# ══════════════════════════════════════════════════════════
def get_sz_dec(asset):
    try:
        meta=info.meta()
        for a in meta["universe"]:
            if a["name"]==asset: return a.get("szDecimals",4)
    except: pass
    return 4

def calc_size(asset,price):
    dec=get_sz_dec(asset); f=10**dec
    sz=int((POSITION_USD*ACTIVE_LEVERAGE)/price*f)/f
    while sz*price<11: sz=int(sz*1.5*f)/f
    return sz

# ══════════════════════════════════════════════════════════
# TAX TRACKER
# ══════════════════════════════════════════════════════════
def record_tax(asset,direction,entry,exit_p,size,pnl):
    tax=max(0,pnl*TAX_RATE); net=pnl-tax
    st["tax"]["total_pnl"]     +=pnl
    st["tax"]["total_tax"]     +=tax
    st["tax"]["total_net"]     +=net
    st["tax"]["total_trades"]  +=1
    if pnl>0: st["tax"]["winning_trades"]+=1
    else:      st["tax"]["losing_trades"] +=1
    row={"time":ts(),"asset":asset,"direction":direction,
         "entry":entry,"exit":exit_p,"size":size,"leverage":ACTIVE_LEVERAGE,
         "gross_pnl":round(pnl,4),"tax_35pct":round(tax,4),"net_pnl":round(net,4),
         "running_pnl":round(st["tax"]["total_pnl"],4),
         "running_tax":round(st["tax"]["total_tax"],4),
         "running_net":round(st["tax"]["total_net"],4),"dry_run":DRY_RUN}
    fe=os.path.exists("hl_tax_tracker.csv")
    with open("hl_tax_tracker.csv","a",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(row.keys()))
        if not fe: w.writeheader()
        w.writerow(row)
    log(f"💰 TAX | Gross ${pnl:+.4f} | Tax ${tax:.4f} | Net ${net:+.4f}")
    log(f"📊 TOTAL | P&L ${st['tax']['total_pnl']:+.2f} | "
        f"Owed ${st['tax']['total_tax']:.2f} | Net ${st['tax']['total_net']:+.2f}")

# ══════════════════════════════════════════════════════════
# DIAGNOSTICS
# ══════════════════════════════════════════════════════════
def diag(level,event,cause,action):
    icons={"INFO":"ℹ️","WARNING":"⚠️","ERROR":"❌","CRITICAL":"🚨"}
    S.add_diagnostic(st,level,event,cause,action)
    log(f"{icons.get(level,'📋')} [{level}] {event} | {cause} | → {action}")

# ══════════════════════════════════════════════════════════
# TRADING
# ══════════════════════════════════════════════════════════
def enter_trade(asset,direction,price):
    size      =calc_size(asset,price)
    stop      =round(price*(1-STOP_PCT) if direction=="LONG" else price*(1+STOP_PCT),2)
    trail_stop=round(price*(1-TRAIL_PCT) if direction=="LONG" else price*(1+TRAIL_PCT),2)
    if DRY_RUN:
        log(f"[DRY] ENTER {direction} {asset} @ ${price:,.2f} | "
            f"size={size} | notional=${size*price:.2f} | "
            f"stop=${stop:,.2f} | trail=${trail_stop:,.2f} | {ACTIVE_LEVERAGE}x")
        positions[asset]={"direction":direction,"entry":price,"size":size,
                          "stop":stop,"trail_peak":price,"trail_stop":trail_stop}
        S.add_trade(st,asset,"ENTER",direction,price,None,size,ACTIVE_LEVERAGE,None,"signal")
        st["positions"]={k:v for k,v in positions.items()}; S.save(st); return
    try:
        r=exchange.market_open(asset,direction=="LONG",size)
        if r and r.get("status")=="ok":
            statuses=r.get("response",{}).get("data",{}).get("statuses",[])
            fill=price
            if statuses and "filled" in statuses[0]:
                fill=float(statuses[0]["filled"]["avgPx"])
            stop      =round(fill*(1-STOP_PCT) if direction=="LONG" else fill*(1+STOP_PCT),2)
            trail_stop=round(fill*(1-TRAIL_PCT) if direction=="LONG" else fill*(1+TRAIL_PCT),2)
            positions[asset]={"direction":direction,"entry":fill,"size":size,
                              "stop":stop,"trail_peak":fill,"trail_stop":trail_stop}
            S.add_trade(st,asset,"ENTER",direction,fill,None,size,ACTIVE_LEVERAGE,None,"signal")
            log(f"✅ ENTERED {direction} {asset} @ ${fill:,.2f} | stop=${stop:,.2f} | trail=${trail_stop:,.2f}")
            st["positions"]={k:v for k,v in positions.items()}; S.save(st)
        else:
            diag("ERROR",f"Entry failed {asset}",str(r),"Skipping — retry next signal")
    except Exception as e:
        diag("ERROR",f"Entry exception {asset}",str(e),"Skipping — retry next signal")

def exit_trade(asset,price,reason):
    if asset not in positions: return
    pos=positions[asset]
    pnl=round((price-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
              else (pos["entry"]-price)*pos["size"],4)
    if DRY_RUN:
        log(f"[DRY] EXIT {pos['direction']} {asset} @ ${price:,.2f} | {reason} | P&L=${pnl:+.4f}")
        record_tax(asset,pos["direction"],pos["entry"],price,pos["size"],pnl)
        S.add_trade(st,asset,"EXIT",pos["direction"],pos["entry"],price,pos["size"],ACTIVE_LEVERAGE,pnl,reason)
        del positions[asset]; st["positions"]={k:v for k,v in positions.items()}; S.save(st); return
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
                diag("CRITICAL",f"Exit failed {asset}","Response None + position still open",
                     "Manual check required on HyperLiquid dashboard")
        if closed:
            pnl=round((fill-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                      else (pos["entry"]-fill)*pos["size"],4)
            log(f"{'✅' if pnl>=0 else '❌'} EXITED {pos['direction']} {asset} @ ${fill:,.2f} | {reason} | P&L=${pnl:+.4f}")
            record_tax(asset,pos["direction"],pos["entry"],fill,pos["size"],pnl)
            S.add_trade(st,asset,"EXIT",pos["direction"],pos["entry"],fill,pos["size"],ACTIVE_LEVERAGE,pnl,reason)
        del positions[asset]; st["positions"]={k:v for k,v in positions.items()}; S.save(st)
    except Exception as e:
        diag("ERROR",f"Exit exception {asset}",str(e),"Position may still be open — check dashboard")

# ══════════════════════════════════════════════════════════
# RECONCILE ON STARTUP
# ══════════════════════════════════════════════════════════
def reconcile():
    if DRY_RUN: return
    try:
        s=info.user_state(MAIN_WALLET)
        for p in s.get("assetPositions",[]):
            pos=p["position"]; asset=pos["coin"]; size=float(pos["szi"])
            if size==0 or asset not in ASSETS: continue
            if asset not in positions:
                entry=float(pos.get("entryPx",0))
                direction="LONG" if size>0 else "SHORT"
                stop =round(entry*(1-STOP_PCT) if direction=="LONG" else entry*(1+STOP_PCT),2)
                trail=round(entry*(1-TRAIL_PCT) if direction=="LONG" else entry*(1+TRAIL_PCT),2)
                positions[asset]={"direction":direction,"entry":entry,"size":abs(size),
                                  "stop":stop,"trail_peak":entry,"trail_stop":trail}
                diag("WARNING",f"Reconciled {asset}",f"{direction} @ ${entry:,.2f} found on exchange",
                     "Position reloaded into memory")
        st["positions"]={k:v for k,v in positions.items()}; S.save(st)
    except Exception as e:
        diag("ERROR","Reconciliation failed",str(e),"Starting with empty positions")

# ══════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")

def print_status(cycle):
    mode="DRY RUN 🔵" if DRY_RUN else ("TESTNET 🟡" if TESTNET else "LIVE 🟢")
    print(f"\n{'='*60}")
    print(f"  CYCLE {cycle} | {ts()} UTC | {mode}")
    print(f"  Assets: {', '.join(ASSETS)} | Leverage: {ACTIVE_LEVERAGE}x")
    print(f"  Open: {len(positions)} | Trades: {st['tax']['total_trades']} | Net P&L: ${st['tax']['total_net']:+.2f}")
    for asset,pos in positions.items():
        print(f"    {pos['direction']} {asset} @ ${pos['entry']:,.2f} | trail=${pos['trail_stop']:,.2f}")
    print(f"{'='*60}\n")

# ══════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════
def run():
    global retry_count
    print("\n"+"="*60)
    print("  HYPERLIQUID STRATEGY ENGINE v2")
    print(f"  {'DRY RUN — no orders' if DRY_RUN else 'LIVE TRADING'} | {'TESTNET' if TESTNET else '🚨 MAINNET'}")
    print(f"  Assets: {', '.join(ASSETS)} | Leverage: {ACTIVE_LEVERAGE}x")
    print(f"  EMA {EMA_FAST}/{EMA_MID}/{EMA_SLOW} | Stop {int(STOP_PCT*100)}% | Trail {int(TRAIL_PCT*100)}%")
    print(f"  Vol {VOL_FILTER}x | Sep {SEP_FILTER} | BRK {BRK_BARS}bar | {CANDLE_TF}")
    print(f"  Checks every {CHECK_INTERVAL}s on completed 15min candles")
    print("="*60+"\n")

    reconcile()
    diag("INFO","Strategy engine v2 started",
         f"DRY_RUN={DRY_RUN} TESTNET={TESTNET} LEV={ACTIVE_LEVERAGE}x ASSETS={ASSETS}",
         "Running — monitoring all assets every 60 seconds")

    cycle=0
    while True:
        cycle+=1
        st["cycle"]     =cycle
        st["last_check"]=ts()
        st["status"]    ="checking"
        st["positions"] ={k:v for k,v in positions.items()}
        S.save(st)
        print_status(cycle)

        # Ping API to confirm connection
        try:
            mids=info.all_mids()
            st["health"]["api_connected"]=True
            st["health"]["last_ping"]=ts()
        except Exception as e:
            st["health"]["api_connected"]=False
            diag("ERROR","API ping failed",str(e),"Will retry next cycle")
            st["status"]="waiting"; st["next_check"]=f"in {CHECK_INTERVAL}s"
            S.save(st); time.sleep(CHECK_INTERVAL); continue

        for asset in ASSETS:
            try:
                end_ms  =int(time.time()*1000)
                start_ms=end_ms-CANDLE_LIMIT*15*60*1000
                candles =info.candles_snapshot(asset,CANDLE_TF,start_ms,end_ms)

                if not candles:
                    diag("WARNING",f"No candles {asset}","API returned empty","Skipping")
                    st["health"]["assets_ok"][asset]={"ok":False,"price":0,
                        "last_candle":"no data","signal":"no data"}
                    continue
                if len(candles)<50:
                    diag("WARNING",f"Insufficient candles {asset}",
                         f"Got {len(candles)}, need 50+","Skipping")
                    continue

                # Candle dedup
                candle_ts=str(candles[-1].get("t",candles[-1].get("T","")))
                cur=float(candles[-1]["c"])
                hi =float(candles[-1]["h"])
                lo =float(candles[-1]["l"])

                if cur==0:
                    diag("WARNING",f"Zero price {asset}","Bad data","Skipping"); continue

                # Update asset health
                direction,signal_price=check_signal(candles)
                st["health"]["assets_ok"][asset]={
                    "ok":True,
                    "price":cur,
                    "last_candle":datetime.fromtimestamp(
                        int(candle_ts)/1000,tz=timezone.utc
                    ).strftime("%H:%M UTC") if candle_ts.isdigit() else candle_ts,
                    "signal":f"{direction} @ ${signal_price:,.2f}" if direction else "no signal",
                }

                # Skip if same candle
                if last_candle.get(asset)==candle_ts:
                    log(f"⏳ {asset}: same candle, waiting... @ ${cur:,.2f}")
                    continue
                last_candle[asset]=candle_ts

                # EXITS
                if asset in positions:
                    pos=positions[asset]
                    if pos["direction"]=="LONG" and hi>pos["trail_peak"]:
                        pos["trail_peak"]=hi; pos["trail_stop"]=round(hi*(1-TRAIL_PCT),2)
                        log(f"📈 {asset} trail → ${pos['trail_stop']:,.2f}")
                    elif pos["direction"]=="SHORT" and lo<pos["trail_peak"]:
                        pos["trail_peak"]=lo; pos["trail_stop"]=round(lo*(1+TRAIL_PCT),2)
                        log(f"📉 {asset} trail → ${pos['trail_stop']:,.2f}")

                    stop_hit =(pos["direction"]=="LONG"  and lo<=pos["stop"]) or \
                               (pos["direction"]=="SHORT" and hi>=pos["stop"])
                    trail_hit=(pos["direction"]=="LONG"  and lo<=pos["trail_stop"]) or \
                               (pos["direction"]=="SHORT" and hi>=pos["trail_stop"])
                    closes=[float(c["c"]) for c in candles]
                    ef=ema_calc(closes,EMA_FAST); em=ema_calc(closes,EMA_MID)
                    ema_x=(pos["direction"]=="LONG"  and ef[-1]<em[-1]) or \
                           (pos["direction"]=="SHORT" and ef[-1]>em[-1])

                    if stop_hit:    exit_trade(asset,pos["stop"],"stop")
                    elif trail_hit: exit_trade(asset,pos["trail_stop"],"trail")
                    elif ema_x:     exit_trade(asset,cur,"ema_cross")
                    else:
                        pnl=((cur-pos["entry"])*pos["size"] if pos["direction"]=="LONG"
                             else (pos["entry"]-cur)*pos["size"])
                        log(f"⏳ {asset} {pos['direction']} @ ${cur:,.2f} | "
                            f"trail=${pos['trail_stop']:,.2f} | P&L=${pnl:+.4f}")
                # ENTRIES
                else:
                    if direction:
                        log(f"🚨 SIGNAL: {asset} {direction} @ ${signal_price:,.2f}")
                        enter_trade(asset,direction,signal_price)
                    else:
                        log(f"⏳ {asset}: no signal @ ${cur:,.2f}")

                retry_count=0

            except Exception as e:
                retry_count+=1
                diag("ERROR",f"Error on {asset}",str(e),f"Retry {retry_count}/5")
                if retry_count>5:
                    diag("CRITICAL","Too many errors",f"{retry_count} consecutive",
                         "Pausing 5 minutes")
                    time.sleep(300); retry_count=0

            time.sleep(0.5)

        st["status"]    ="waiting"
        st["next_check"]=f"in {CHECK_INTERVAL}s"
        st["positions"] ={k:v for k,v in positions.items()}
        S.save(st)
        log(f"💤 Next check in {CHECK_INTERVAL}s")
        time.sleep(CHECK_INTERVAL)

if __name__=="__main__":
    try:
        run()
    except KeyboardInterrupt:
        st["status"]="stopped"; S.save(st)
        print(f"\n  Stopped. Trades: {st['tax']['total_trades']} | "
              f"Net P&L: ${st['tax']['total_net']:+.2f}\n")
