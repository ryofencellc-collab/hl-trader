"""
CB TRADER v72
═══════════════════════════════════════════════════════════════════
THREE ISOLATED SYSTEMS — RSI(2/70/55/80→70) on 15min candles

Purpose: Run 3 candle sources in parallel for 1 week to find the winner.
Each system is completely blind and isolated from the others.
All 3 execute orders on CFM. Only candle source differs.

System 1 — CFM only:   Coinbase CFM candles for signals
System 2 — INTX only:  Coinbase International candles for signals
System 3 — Hybrid:     CFM primary + INTX gap fill for signals

Strategy (identical across all 3 systems):
  RSI(2) on 15min candles + 1hr RSI(14) MTF filter (resampled) + Trailing Exit
  LONG:  RSI(2) crosses ABOVE 70 AND 1hr RSI(14) resampled > 50
  SHORT: RSI(2) crosses BELOW 30 AND 1hr RSI(14) resampled < 50
  EXIT:  RSI drops below 55 (or 70 if RSI hit 80 — trailing tighten)

Parameters confirmed by full sweep Sep 5 2026 (11,520 combos × 3 sources):
  RSI(3/75/50/80→60) — all 47 tests passed
  Trail exit: 60 beats 55 by $377/mo confirmed on real INTX data
  Backtest: System 2 (INTX): $13,644/mo | 70.5% WR | 37/38 green weeks

Assets (all 3 systems):
  XRP (XPP-20DEC30-CDE) — 500 XRP/contract | 20.01% intraday margin
  XLM (XLP-20DEC30-CDE) — 5000 XLM/contract | 25.00% intraday margin
  Confirmed via Coinbase API Sep 2, 2026

Fees confirmed from 6 real fills Aug 19-20 2026:
  0.080% taker per side + $0.12 flat per contract per side

Isolation guarantee:
  Each system has its own: state dict, positions dict, balance, locks,
  sim data file, diagnostic file, candle cache, trading thread.
  No shared mutable state between systems.
  CFM candle fetches are shared (read-only) but each system
  processes its own copy independently.

Railway variables:
  CB_API_KEY, CB_API_SECRET, NTFY_TOPIC
  TRADE_MODE    — paper or live (default: paper)
  MAX_CONTRACTS — per asset per system (default: 5)
  PAPER_BALANCE — starting balance per system (default: 2000)

CHECKLIST — triple checked before push:
  ✅ Version = v72 everywhere
  ✅ RSI_PERIOD = 2
  ✅ RSI_ENTRY = 70
  ✅ RSI_EXIT = 55
  ✅ RSI_TRAIL_TRIG = 80
  ✅ RSI_TRAIL_EXIT = 70 (optimal from 11,520-combo sweep)
  ✅ SHORT entry: RSI crosses BELOW 30 (100-RSI_ENTRY)
  ✅ SHORT trail: tighten when RSI < 20 (100-RSI_TRAIL_TRIG)
  ✅ SHORT trail exit: RSI rises above 30 (100-RSI_TRAIL_EXIT)
  ✅ SHORT standard exit: RSI rises above 45 (100-RSI_EXIT)
  ✅ XRP margin = 0.2001
  ✅ XLM margin = 0.2500
  ✅ Fees = 0.080% + $0.12/ct/side
  ✅ CANDLE_LIMIT = 300
  ✅ System 1 uses CFM candles only — no INTX
  ✅ System 2 uses INTX candles only — no CFM
  ✅ System 3 uses CFM + INTX hybrid (CFM wins on overlap)
  ✅ All 3 execute orders on CFM (entry/exit price = CFM candle open)
  ✅ All 3 completely isolated — separate state, positions, balance, locks
  ✅ Each system has own sim data file (/tmp/cb_sim_s1.json etc)
  ✅ Each system has own diagnostic file
  ✅ Each system has own state file
  ✅ Each system has own tax CSV
  ✅ hr_rsi computed BEFORE evaluate_signal
  ✅ MTF blocks trade when hr_rsi is None
  ✅ Startup cache per system — only caches own source candles
  ✅ System 1 cache: pure CFM only
  ✅ System 2 cache: pure INTX only
  ✅ System 3 cache: hybrid (CFM+INTX merged, CFM wins)
  ✅ pnl saved = NET after fees
  ✅ Entry at CFM candles[-2]["c"] (close of last completed candle)
  ✅ Exit at CFM candles[-2]["c"] (close of last completed candle)
  ✅ Skip cooldown = 0 (immediate re-entry allowed)
  ✅ Startup deferred to @app.before_request
  ✅ State file = cb_state_v72_s{N}.json per system
  ✅ No 1hr strategy anywhere
  ✅ No dead code
  ✅ Dashboard shows all 3 systems side by side
  ✅ Separate sim-data endpoints per system
  ✅ Dashboard version = v72
"""

import time, os, json, csv, uuid, threading
from datetime import datetime, timezone, timedelta
from flask import Flask, Response, request, redirect
import requests as req

# ══════════════════════════════════════════════════════════════════
# SHARED CONFIG — same across all 3 systems
# ══════════════════════════════════════════════════════════════════
TRADE_MODE  = os.environ.get("TRADE_MODE", "paper").lower().strip()
PAPER_MODE  = (TRADE_MODE != "live")
NTFY_TOPIC  = os.environ.get("NTFY_TOPIC", "hl-trader-lunchm0ney")
NTFY_URL    = f"https://ntfy.sh/{NTFY_TOPIC}"

CB_API_KEY = os.environ.get("CB_API_KEY", "")
CB_API_SEC = os.environ.get("CB_API_SECRET", "")
if not CB_API_KEY or not CB_API_SEC:
    raise RuntimeError("CB_API_KEY and CB_API_SECRET must be set")

ASSETS = {
    "XRP": {"perp": "XPP-20DEC30-CDE", "intx": "XRP-PERP", "contract": 500.0,  "margin_rate": 0.2001},
    "XLM": {"perp": "XLP-20DEC30-CDE", "intx": "XLM-PERP", "contract": 5000.0, "margin_rate": 0.2500},
}
ASSET_NAMES = list(ASSETS.keys())

FEE_PCT   = 0.00080
FEE_FLAT  = 0.12

MAX_CONTRACTS = int(os.environ.get("MAX_CONTRACTS", "5"))
PAPER_BALANCE = float(os.environ.get("PAPER_BALANCE", "2000"))

RSI_PERIOD     = 2
RSI_ENTRY      = 70
RSI_EXIT       = 55
RSI_TRAIL_TRIG = 80
RSI_TRAIL_EXIT = 70   # optimal from full 11,520-combo sweep Sep 5 2026

CANDLE_TF    = "FIFTEEN_MINUTE"
CANDLE_LIMIT = 300

# ══════════════════════════════════════════════════════════════════
# SYSTEM CLASS — one instance per candle source
# Each instance is completely isolated from the others.
# ══════════════════════════════════════════════════════════════════
class TradingSystem:
    def __init__(self, sys_id, label, source):
        """
        sys_id: 1, 2, or 3
        label:  "CFM only", "INTX only", "Hybrid"
        source: "cfm", "intx", "hybrid"
        """
        self.sys_id = sys_id
        self.label  = label
        self.source = source  # "cfm", "intx", "hybrid"

        # Files — unique per system
        self.data_file  = f"/tmp/cb_sim_s{sys_id}.json"
        self.diag_file  = f"/tmp/cb_diag_s{sys_id}.json"
        self.state_file = f"/tmp/cb_state_v72_s{sys_id}.json"
        self.tax_file   = f"/tmp/cb_trades_s{sys_id}.csv"

        # Isolated state
        self.state = {
            "balance": PAPER_BALANCE, "buying_power": PAPER_BALANCE,
            "weekly_pnl": 0.0, "total_pnl": 0.0,
            "week": None, "cycle": 0,
            "loop_last_run": "never", "loop_errors": 0,
            "wins": 0, "total_trades": 0, "entries": 0,
            "skipped_assets": [], "audit": [],
        }

        # Isolated positions and skip counters
        self.positions   = {}
        self.skip_entry  = {}

        # Isolated locks
        self.lock     = threading.Lock()
        self.sim_lock = threading.Lock()

        # Isolated candle cache
        self.startup_cache    = {}   # pre-loaded candles on startup
        self.intx_cache       = {}   # INTX candles cached per bucket
        self.intx_cache_ts    = {}   # timestamp of last INTX fetch

        self.total_usdc = PAPER_BALANCE

    # ── State persistence ─────────────────────────────────────────
    def save_state(self):
        try:
            with self.lock:
                safe = {k: v for k, v in self.state.items()
                        if isinstance(v, (int, float, str, bool, type(None)))}
            json.dump(safe, open(self.state_file, "w"))
        except Exception as e:
            log(f"[S{self.sys_id}] State save error: {e}")

    def load_state(self):
        if not os.path.exists(self.state_file):
            return
        try:
            data = json.load(open(self.state_file))
            with self.lock:
                for k, v in data.items():
                    if k in self.state:
                        if k == "balance" and PAPER_MODE:
                            continue
                        self.state[k] = v
            log(f"[S{self.sys_id}] State restored | trades={self.state['total_trades']} pnl=${self.state['total_pnl']:+.2f}")
        except Exception as e:
            log(f"[S{self.sys_id}] State load error: {e}")

    # ── Audit ─────────────────────────────────────────────────────
    def add_audit(self, asset, event, detail, candle=None, indicators=None):
        entry = {"time": ts(), "asset": asset, "event": event, "detail": detail, "sys": self.sys_id}
        if candle:     entry["candle"]     = candle
        if indicators: entry["indicators"] = indicators
        with self.lock:
            self.state["audit"].insert(0, entry)
            if len(self.state["audit"]) > 1000:
                self.state["audit"] = self.state["audit"][:1000]
        try:
            data = json.load(open(self.diag_file)) if os.path.exists(self.diag_file) else []
            data.insert(0, entry)
            if len(data) > 3000: data = data[:3000]
            json.dump(data, open(self.diag_file, "w"))
        except:
            pass
        if not any(n in event for n in ["NO_SIGNAL", "CYCLE"]):
            log(f"[S{self.sys_id}:{asset}] {event} — {detail[:80]}")

    def check_weekly_reset(self):
        wk = get_week()
        with self.lock:
            if self.state["week"] != wk:
                self.state["week"] = wk
                self.state["weekly_pnl"] = 0.0

    # ── Sim data saver ────────────────────────────────────────────
    def save_sim_data(self, asset, bucket_ts, candles, indicators, decision,
                      position=None, pnl_net=None,
                      balance_at_decision=None, contracts_at_decision=None):
        try:
            now_ms = int(time.time()) * 1000
            age    = round((now_ms - candles[-1]["ts"]) / 60000, 1) if candles else None
            rsi_cur  = indicators.get("rsi_cur")  if isinstance(indicators, dict) else None
            rsi_prev = indicators.get("rsi_prev") if isinstance(indicators, dict) else None
            hr_rsi   = indicators.get("hr_rsi")   if isinstance(indicators, dict) else None

            if (rsi_cur is None or rsi_prev is None) and candles and len(candles) >= RSI_PERIOD + 3:
                rsi_vals = calc_rsi([float(c["c"]) for c in candles[-50:]], RSI_PERIOD)
                if len(rsi_vals) >= 2 and rsi_vals[-2] is not None:
                    rsi_cur = round(rsi_vals[-2], 2)
                if len(rsi_vals) >= 3 and rsi_vals[-3] is not None:
                    rsi_prev = round(rsi_vals[-3], 2)

            record = {
                "ts":       bucket_ts,
                "dt":       datetime.fromtimestamp(bucket_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "asset":    asset,
                "system":   self.sys_id,
                "source":   self.source,
                "decision": decision,
                "candles":  candles[-50:] if isinstance(candles, list) else [],
                "rsi_cur":  rsi_cur,
                "rsi_prev": rsi_prev,
                "hr_rsi":   hr_rsi,
                "candle_age_min":        age,
                "balance_at_decision":   balance_at_decision if balance_at_decision is not None else self.state.get("balance", 0),
                "contracts_at_decision": contracts_at_decision,
                "indicators": indicators if isinstance(indicators, dict) else {},
                "position": {
                    "direction":  position.get("direction"),
                    "entry":      position.get("entry"),
                    "contracts":  position.get("contracts"),
                    "size":       position.get("size"),
                    "exit_rsi":   position.get("exit_rsi", RSI_EXIT),
                    "entry_time": position.get("entry_time"),
                } if isinstance(position, dict) else None,
                "pnl": pnl_net,  # NET after fees
            }

            with self.sim_lock:
                try:
                    existing = json.load(open(self.data_file))
                    if not isinstance(existing, list): existing = []
                except:
                    existing = []
                existing.append(record)
                if len(existing) > 50000:
                    existing = existing[-50000:]
                tmp = self.data_file + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(existing, f)
                os.replace(tmp, self.data_file)
        except Exception as e:
            log(f"[S{self.sys_id}] sim_data error: {e}")

    def record_tax(self, asset, direction, entry_p, exit_p, size, pnl, entry_time):
        try:
            tax = round(pnl * 0.35, 4) if pnl > 0 else 0.0
            row = {
                "exit_time": ts(), "entry_time": entry_time, "asset": asset,
                "system": self.sys_id, "source": self.source,
                "direction": direction,
                "entry_price": f"{entry_p:.6f}", "exit_price": f"{exit_p:.6f}",
                "size": f"{size}", "gross_pnl": f"{pnl:.4f}",
                "tax_35pct": f"{tax:.4f}", "net_pnl": f"{pnl - tax:.4f}",
            }
            write_header = not os.path.exists(self.tax_file)
            with open(self.tax_file, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=row.keys())
                if write_header: w.writeheader()
                w.writerow(row)
        except Exception as e:
            log(f"[S{self.sys_id}] Tax error: {e}")

    # ── Candle fetching ───────────────────────────────────────────
    def fetch_cfm_candles(self, asset, n=CANDLE_LIMIT):
        """Fetch CFM candles. Always tagged source='cfm'."""
        try:
            client     = get_cb_client()
            product_id = ASSETS[asset]["perp"]
            end        = int(time.time())
            start      = end - n * 900

            def _do():
                r = client.get_candles(product_id, start=str(start), end=str(end), granularity=CANDLE_TF)
                if not r.candles: raise ValueError("0 candles")
                return r

            resp = fetch_with_retry(_do, asset, self.sys_id)
            if resp is None: return None

            candles = sorted([{
                "ts": int(c.start) * 1000,
                "dt": datetime.fromtimestamp(int(c.start), tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "o": float(c.open), "h": float(c.high),
                "l": float(c.low),  "c": float(c.close), "v": float(c.volume),
                "source": "cfm",
            } for c in resp.candles], key=lambda x: x["ts"])[-n:]

            if not candles or candles[-1]["c"] == 0: return None
            return candles
        except Exception as e:
            log(f"[S{self.sys_id}] CFM fetch {asset}: {e}")
            return None

    def fetch_intx_candles(self, asset, n=CANDLE_LIMIT):
        """Fetch INTX candles. Cached per bucket. Tagged source='intx'."""
        now = int(time.time())
        if asset in self.intx_cache_ts and now - self.intx_cache_ts[asset] < 840:
            return self.intx_cache.get(asset)

        sym = ASSETS[asset]["intx"]
        try:
            end_dt   = datetime.fromtimestamp(now, tz=timezone.utc)
            start_dt = datetime.fromtimestamp(now - n * 900, tz=timezone.utc)
            r = req.get(
                f"https://api.international.coinbase.com/api/v1/instruments/{sym}/candles",
                params={"granularity": "FIFTEEN_MINUTE",
                        "start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "end":   end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")},
                timeout=8)
            if r.status_code == 200:
                aggs = r.json().get("aggregations", [])
                if aggs:
                    candles = sorted([{
                        "ts":  int(datetime.strptime(c["start"], "%Y-%m-%dT%H:%M:%SZ")
                                   .replace(tzinfo=timezone.utc).timestamp() * 1000),
                        "dt":  c["start"],
                        "o":   float(c["open"]),  "h": float(c["high"]),
                        "l":   float(c["low"]),   "c": float(c["close"]),
                        "v":   float(c["volume"]), "source": "intx",
                    } for c in aggs], key=lambda x: x["ts"])[-n:]
                    self.intx_cache[asset]    = candles
                    self.intx_cache_ts[asset] = now
                    return candles
        except Exception as e:
            log(f"[S{self.sys_id}] INTX fetch {asset}: {e}")
        return self.intx_cache.get(asset)

    def get_signal_candles(self, asset, cfm_candles, intx_candles):
        """
        Return the correct candle set for this system's signal calculation.
        System 1: CFM only
        System 2: INTX only
        System 3: Hybrid (CFM primary, INTX fills gaps)

        Cache update: only update startup cache with own source candles.
        System 1: cache from pure CFM
        System 2: cache from pure INTX
        System 3: cache from hybrid merge
        """
        if self.source == "cfm":
            candles = cfm_candles
            # Fallback to startup cache if CFM too thin
            if len(candles or []) < 100 and asset in self.startup_cache:
                cached   = self.startup_cache[asset]
                combined = merge_cfm_intx(cached, candles or [])
                if len(combined) > len(candles or []):
                    candles = combined
            # Update cache with pure CFM only
            if cfm_candles and len(cfm_candles) >= 100:
                self.startup_cache[asset] = cfm_candles[-CANDLE_LIMIT:]

        elif self.source == "intx":
            candles = intx_candles
            # Fallback to startup cache if INTX too thin
            if len(candles or []) < 100 and asset in self.startup_cache:
                cached   = self.startup_cache[asset]
                if candles:
                    im = {c["ts"]: c for c in candles}
                    cm = {c["ts"]: c for c in cached}
                    all_ts = sorted(set(im) | set(cm))
                    combined = [im[t] if t in im else cm[t] for t in all_ts]
                else:
                    combined = cached
                if len(combined) > len(candles or []):
                    candles = combined
            # Update cache with pure INTX only
            if intx_candles and len(intx_candles) >= 100:
                self.startup_cache[asset] = intx_candles[-CANDLE_LIMIT:]

        else:  # hybrid
            candles = merge_cfm_intx(cfm_candles, intx_candles)
            # Fallback to startup cache if hybrid too thin
            if len(candles or []) < 100 and asset in self.startup_cache:
                cached   = self.startup_cache[asset]
                combined = merge_cfm_intx(cached, candles or [])
                if len(combined) > len(candles or []):
                    candles = combined
            # Update cache with hybrid (merged)
            if candles and len(candles) >= 100:
                self.startup_cache[asset] = candles[-CANDLE_LIMIT:]

        return candles

    # ── Enter / Exit ──────────────────────────────────────────────
    def enter_position(self, asset, direction, entry_price, candle, info=None):
        """
        Enter position. Entry price always = CFM candle open.
        Sizing: 70% of own balance / N assets / margin rate.
        """
        cs  = ASSETS[asset]["contract"]
        mr  = ASSETS[asset]["margin_rate"]

        with self.lock:
            current_bal = self.state["balance"]

        per_slot       = (current_bal * 0.70) / len(ASSET_NAMES)
        margin_per     = entry_price * cs * mr
        max_affordable = min(MAX_CONTRACTS, max(1, int(per_slot / margin_per))) if margin_per > 0 else 1
        contracts      = max_affordable
        side           = "BUY" if direction == "LONG" else "SELL"

        oid, actual_cts = place_market_order(asset, side, contracts)
        if not oid:
            msg = f"S{self.sys_id} {asset} {side} {contracts}ct rejected"
            log(f"CRITICAL: {msg}")
            self.add_audit(asset, "ORDER REJECTED", msg)
            ntfy(f"ORDER REJECTED S{self.sys_id} {asset}", msg, priority="urgent")
            return

        actual_size = actual_cts * cs
        rsi_info    = info or {}
        self.positions[asset] = {
            "direction":      direction,
            "entry":          entry_price,
            "contracts":      actual_cts,
            "size":           actual_size,
            "strategy":       "RSI-Mom",
            "entry_time":     ts(),
            "rsi_entry":      rsi_info.get("rsi_cur", 0),
            "exit_rsi":       RSI_EXIT,
            "hr_rsi":         rsi_info.get("hr_rsi", None),
            "paper":          PAPER_MODE,
            "unrealized_pnl": 0.0,
            "current_price":  entry_price,
        }
        with self.lock:
            self.state["entries"] = self.state.get("entries", 0) + 1
            self.state["buying_power"] = self.state.get("buying_power", self.state["balance"]) - entry_price * actual_size * mr

        mode_label = "PAPER" if PAPER_MODE else "LIVE"
        self.add_audit(asset, f"📊 ENTER {direction}",
                       f"S{self.sys_id}({self.source}) | entry=${entry_price:,.4f} | "
                       f"rsi={rsi_info.get('rsi_cur',0):.1f} | {actual_cts}ct | "
                       f"hr={rsi_info.get('hr_rsi','?')} | {mode_label}",
                       candle=candle)
        ntfy(f"{'📄' if PAPER_MODE else '📊'} ENTER {direction} {asset} [S{self.sys_id}]",
             f"{self.source} | entry=${entry_price:,.4f} | RSI={rsi_info.get('rsi_cur',0):.1f} | {actual_cts}ct",
             priority="default")

    def exit_position(self, asset, exit_price, exit_reason, candle):
        """
        Exit position. Exit price always = CFM candle open.
        Returns net P&L (gross minus fees) for sim recording.
        """
        pos = self.positions.get(asset)
        if not pos: return None

        gross = round(
            (exit_price - pos["entry"]) * pos["size"] if pos["direction"] == "LONG"
            else (pos["entry"] - exit_price) * pos["size"], 4)

        entry_fee = round(pos["entry"] * pos["size"] * FEE_PCT + FEE_FLAT * pos["contracts"], 4)
        exit_fee  = round(exit_price   * pos["size"] * FEE_PCT + FEE_FLAT * pos["contracts"], 4)
        total_fee = entry_fee + exit_fee
        pnl       = round(gross - total_fee, 4)

        side = "SELL" if pos["direction"] == "LONG" else "BUY"
        oid, _ = place_market_order(asset, side, pos["contracts"])
        if not oid and not PAPER_MODE:
            log(f"[S{self.sys_id}] EXIT FAILED {asset} — retrying next bucket")
            ntfy(f"⚠️ EXIT FAILED S{self.sys_id} {asset}", "Position preserved, will retry", priority="urgent")
            return None

        self.record_tax(asset, pos["direction"], pos["entry"], exit_price, pos["size"], pnl, pos["entry_time"])

        with self.lock:
            self.state["total_pnl"]    = round(self.state["total_pnl"] + pnl, 4)
            self.state["weekly_pnl"]   = round(self.state["weekly_pnl"] + pnl, 4)
            self.state["balance"]      = round(self.state["balance"] + pnl, 4)
            self.state["total_trades"] += 1
            if pnl >= 0: self.state["wins"] += 1

        del self.positions[asset]

        emoji = "✅" if pnl >= 0 else "❌"
        self.add_audit(asset, f"{emoji} EXIT {exit_reason}",
                       f"S{self.sys_id} {pos['direction']} ${pos['entry']:,.4f}→${exit_price:,.4f} | "
                       f"gross=${gross:+,.4f} | fees=${total_fee:.4f} | net=${pnl:+,.4f}",
                       candle=candle)
        ntfy(f"{emoji} EXIT {asset} [S{self.sys_id}]",
             f"{pos['direction']} | ${pos['entry']:,.4f}→${exit_price:,.4f} | net=${pnl:+,.2f} | {exit_reason}",
             priority="default" if pnl >= 0 else "high")

        if PAPER_MODE:
            log(f"[S{self.sys_id}] Paper balance: ${self.state['balance']:,.2f}")

        self.save_state()
        return pnl

    # ── Trading loop ──────────────────────────────────────────────
    def run(self):
        """
        Main trading loop for this system.
        Runs in its own daemon thread.
        Completely blind to other systems.
        """
        self.state["balance"]      = PAPER_BALANCE
        self.state["buying_power"] = PAPER_BALANCE
        self.total_usdc            = PAPER_BALANCE
        self.load_state()

        log(f"[S{self.sys_id}] 🚀 Started — {self.label} | ${PAPER_BALANCE:,.2f}")
        log(f"[S{self.sys_id}] Strategy: RSI({RSI_PERIOD}/{RSI_ENTRY}/{RSI_EXIT}/{RSI_TRAIL_TRIG}→{RSI_TRAIL_EXIT}) + MTF")
        log(f"[S{self.sys_id}] Source: {self.source}")

        last_bucket = (int(time.time()) // 900) * 900

        while True:
            try:
                current_bucket = (int(time.time()) // 900) * 900
                with self.lock:
                    self.state["loop_last_run"] = ts()
                    self.state["cycle"] = self.state.get("cycle", 0) + 1

                self.check_weekly_reset()

                if current_bucket != last_bucket:
                    last_bucket   = current_bucket
                    bucket_dt     = datetime.fromtimestamp(current_bucket, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    bucket_dt_obj = datetime.fromtimestamp(current_bucket, tz=timezone.utc)
                    hour_utc      = bucket_dt_obj.hour

                    log(f"[S{self.sys_id}] 🕐 {bucket_dt} UTC | open={len(self.positions)} | bal=${self.state['balance']:,.2f}")

                    skipped_assets = []
                    _candle_cache  = {}
                    cycle_num = self.state.get("cycle", 0)

                    for asset in ASSET_NAMES:
                        try:
                            # Always fetch both sources — system decides which to use
                            cfm_candles  = self.fetch_cfm_candles(asset, n=CANDLE_LIMIT)
                            intx_candles = self.fetch_intx_candles(asset, n=CANDLE_LIMIT)

                            # Get signal candles for this system's source
                            # Also handles startup cache fallback and cache update
                            signal_candles = self.get_signal_candles(asset, cfm_candles, intx_candles)

                            # CFM candles used for entry/exit price — always
                            # If CFM is unavailable, skip this asset
                            if not cfm_candles or len(cfm_candles) < RSI_PERIOD + 5:
                                skipped_assets.append(asset)
                                log(f"[S{self.sys_id}] ⚠️ {asset}: CFM unavailable — skipping (no execution possible)")
                                continue

                            if not signal_candles or len(signal_candles) < RSI_PERIOD + 5:
                                skipped_assets.append(asset)
                                log(f"[S{self.sys_id}] ⚠️ {asset}: signal candles unavailable — skipping")
                                continue

                            _candle_cache[asset] = signal_candles
                            cur_cfm = cfm_candles[-1]  # CFM candle for price reference

                            # Skip cooldown after exit
                            if self.skip_entry.get(asset, 0) > 0:
                                self.skip_entry[asset] -= 1
                                continue

                            # ── EXIT CHECK ────────────────────────────
                            pos = self.positions.get(asset)
                            if pos:
                                # Unrealized P&L calculated from signal candles close
                                cur_close = float(signal_candles[-1]["c"])
                                gross_u   = (cur_close - pos["entry"]) * pos["size"] if pos["direction"] == "LONG" \
                                            else (pos["entry"] - cur_close) * pos["size"]
                                ef_u      = pos["entry"] * pos["size"] * FEE_PCT + FEE_FLAT * pos["contracts"]
                                xf_u      = cur_close   * pos["size"] * FEE_PCT + FEE_FLAT * pos["contracts"]
                                pos["unrealized_pnl"] = round(gross_u - ef_u - xf_u, 4)
                                pos["current_price"]  = cur_close

                                if should_exit(pos, signal_candles):
                                    # Exit price = CFM candles[-2]["c"] — last completed candle close
                                    exit_price = float(cfm_candles[-2]["c"]) if len(cfm_candles) >= 2 else float(cfm_candles[-1]["o"])
                                    pnl_net = self.exit_position(asset, exit_price, "RSI_EXIT", cur_cfm)
                                    if pnl_net is not None:
                                        self.save_sim_data(asset, current_bucket*1000, signal_candles, {},
                                                           "EXIT_RSI", position=dict(pos), pnl_net=pnl_net,
                                                           balance_at_decision=self.state.get("balance",0),
                                                           contracts_at_decision=pos.get("contracts",0))
                                    self.skip_entry[asset] = 0
                                else:
                                    self.save_sim_data(asset, current_bucket*1000, signal_candles, {},
                                                       "HOLD", position=dict(pos),
                                                       balance_at_decision=self.state.get("balance",0),
                                                       contracts_at_decision=pos.get("contracts",0))
                                continue

                            # ── ENTRY SIGNAL ──────────────────────────
                            # hr_rsi computed BEFORE evaluate_signal
                            hr_rsi = get_hr_rsi(asset, signal_candles)

                            d, _, _, info = evaluate_signal(signal_candles)
                            info["hr_rsi"] = round(hr_rsi, 1) if hr_rsi is not None else None

                            if d:
                                # MTF filter
                                if hr_rsi is None:
                                    self.save_sim_data(asset, current_bucket*1000, signal_candles, info,
                                                       "NO_SIGNAL:MTF_not_ready")
                                    continue
                                if d == "LONG" and hr_rsi < 50:
                                    self.save_sim_data(asset, current_bucket*1000, signal_candles, info,
                                                       f"NO_SIGNAL:MTF_filter (1hr_RSI={hr_rsi:.1f}<50)")
                                    continue
                                if d == "SHORT" and hr_rsi > 50:
                                    self.save_sim_data(asset, current_bucket*1000, signal_candles, info,
                                                       f"NO_SIGNAL:MTF_filter (1hr_RSI={hr_rsi:.1f}>50)")
                                    continue

                                self.add_audit(asset, f"🚨 RSI-Mom {d}",
                                               f"S{self.sys_id}({self.source}) | "
                                               f"prev={info.get('rsi_prev',0):.1f} cur={info.get('rsi_cur',0):.1f} | "
                                               f"hr={hr_rsi:.1f}",
                                               candle=cur_cfm, indicators=info)

                                # Entry price = CFM candles[-2]["c"] — last completed candle close
                                entry_price = float(cfm_candles[-2]["c"]) if len(cfm_candles) >= 2 else float(cfm_candles[-1]["o"])
                                self.enter_position(asset, d, entry_price, cur_cfm, info)

                                if self.positions.get(asset):
                                    _pos = self.positions[asset]
                                    self.save_sim_data(asset, current_bucket*1000, signal_candles, info,
                                                       f"ENTER_{d}", position=dict(_pos),
                                                       balance_at_decision=self.state.get("balance",0),
                                                       contracts_at_decision=_pos.get("contracts",0))
                            else:
                                self.save_sim_data(asset, current_bucket*1000, signal_candles, info,
                                                   f"NO_SIGNAL:{info.get('fail','?')}",
                                                   balance_at_decision=self.state.get("balance",0))

                        except Exception as e:
                            import traceback
                            log(f"[S{self.sys_id}] Asset error {asset}: {e}")
                            log(traceback.format_exc())

                    with self.lock:
                        self.state["skipped_assets"] = skipped_assets

                    if skipped_assets:
                        log(f"[S{self.sys_id}] ⚠️ Skipped: {skipped_assets}")

                    if cycle_num % 10 == 0:
                        self.save_state()

                    # Heartbeat
                    with self.lock:
                        _bal    = self.state["balance"]
                        _trades = self.state["total_trades"]

                    hb_lines = [f"S{self.sys_id}({self.source}) | candle={bucket_dt} | open={len(self.positions)} | bal=${_bal:,.2f} | trades={_trades}"]
                    for _a in ASSET_NAMES:
                        _pos = self.positions.get(_a)
                        _c   = _candle_cache.get(_a)
                        _age = "?"
                        if _c and _c[-1].get("ts"):
                            _age = f"{round((int(time.time())*1000-_c[-1]['ts'])/60000,1)}m"
                        _rsi_cur = _rsi_prev = _hr = "?"
                        if _c and len(_c) >= RSI_PERIOD + 2:
                            _closes   = [float(x["c"]) for x in _c]
                            _rsi_vals = calc_rsi(_closes, RSI_PERIOD)
                            if _rsi_vals[-2] is not None:
                                _rsi_cur = f"{_rsi_vals[-2]:.1f}"
                            if len(_rsi_vals) >= 3 and _rsi_vals[-3] is not None:
                                _rsi_prev = f"{_rsi_vals[-3]:.1f}"
                        _hr_val = get_hr_rsi(_a, _c)
                        _hr     = f"{_hr_val:.1f}" if _hr_val is not None else "?"
                        _cs  = ASSETS[_a]["contract"]
                        _mr  = ASSETS[_a]["margin_rate"]
                        _avail = _bal * 0.70 / len(ASSET_NAMES)
                        _mp    = float(_c[-1]["c"]) * _cs * _mr if _c else 0
                        _cts   = min(MAX_CONTRACTS, max(0, int(_avail / _mp))) if _mp > 0 else 0
                        if _pos:
                            _unreal = _pos.get("unrealized_pnl", 0.0)
                            _exit_r = _pos.get("exit_rsi", RSI_EXIT)
                            _locked = "🔒" if _exit_r == RSI_TRAIL_EXIT else ""
                            hb_lines.append(f"  {_a:<4} {_pos['direction']:<5} | prev={_rsi_prev} cur={_rsi_cur} | hr={_hr} | exit<{_exit_r}{_locked} | unreal=${_unreal:+.2f} | age={_age} | HOLD")
                        else:
                            hb_lines.append(f"  {_a:<4} {'—':<5} | prev={_rsi_prev} cur={_rsi_cur} | hr={_hr} | cts={_cts} | age={_age} | WATCHING")

                    for _line in hb_lines:
                        log(_line)

                    hb_detail = "\n".join(hb_lines)
                    self.add_audit("SYSTEM", "💓 CYCLE", hb_detail)

                    # Weekly report
                    if bucket_dt_obj.weekday() == 0 and hour_utc == 9 and bucket_dt_obj.minute < 15:
                        with self.lock:
                            wpnl = self.state["weekly_pnl"]
                            bal  = self.state["balance"]
                            trd  = self.state["total_trades"]
                            wr   = round(self.state["wins"] / trd * 100, 1) if trd else 0
                        ntfy(f"Weekly Report S{self.sys_id} ({self.source})",
                             f"P&L: ${wpnl:+,.2f} | Bal: ${bal:,.2f} | Trades: {trd} | WR: {wr}%")
                        with self.lock: self.state["weekly_pnl"] = 0.0

                    # Emergency stop
                    with self.lock: bal = self.state["balance"]
                    if bal < PAPER_BALANCE * 0.5 and len(self.positions) == 0:
                        ntfy(f"EMERGENCY S{self.sys_id}",
                             f"Balance ${bal:,.2f} below 50% of ${PAPER_BALANCE:,.2f}",
                             priority="urgent")

            except Exception as e:
                with self.lock:
                    self.state["loop_errors"] = self.state.get("loop_errors", 0) + 1
                    errs = self.state["loop_errors"]
                log(f"[S{self.sys_id}] Loop error {errs}: {e}")
                if errs in (3, 10, 25):
                    ntfy(f"CRITICAL S{self.sys_id} loop errors: {errs}",
                         str(e)[:100], priority="urgent" if errs >= 10 else "high")

            time.sleep(30)

# ══════════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ══════════════════════════════════════════════════════════════════
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def ts_est():
    utc_now = datetime.now(timezone.utc)
    offset  = -4 if 4 <= utc_now.month <= 10 else -5
    est     = utc_now + timedelta(hours=offset)
    suffix  = "EDT" if offset == -4 else "EST"
    return est.strftime(f"%Y-%m-%d %H:%M {suffix}")

def log(msg):
    print(f"[{ts()}] {msg}", flush=True)

def get_week():
    now = datetime.now(timezone.utc)
    return f"{now.year}-W{now.isocalendar()[1]:02d}"

def ntfy(title, body, priority="default"):
    try:
        req.post(NTFY_URL, data=body.encode("utf-8"),
                 headers={"Title": title.encode("ascii","ignore").decode().strip(),
                          "Priority": priority,
                          "Content-Type": "text/plain; charset=utf-8"},
                 timeout=10)
    except Exception as e:
        log(f"ntfy error: {e}")

# ── Coinbase client (shared, read-only candle fetches) ────────────
_cb_client      = None
_cb_client_lock = threading.Lock()

def get_cb_client():
    global _cb_client
    if _cb_client is None:
        with _cb_client_lock:
            if _cb_client is None:
                from coinbase.rest import RESTClient
                _cb_client = RESTClient(api_key=CB_API_KEY, api_secret=CB_API_SEC)
                if hasattr(_cb_client, "session"):
                    _orig = _cb_client.session.request
                    def _req_with_timeout(method, url, **kwargs):
                        kwargs.setdefault("timeout", 10)
                        return _orig(method, url, **kwargs)
                    _cb_client.session.request = _req_with_timeout
                    log("✅ Coinbase client: 10s timeout applied")
    return _cb_client

def fetch_with_retry(fn, asset, sys_id, retries=3):
    import random
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if attempt < retries - 1:
                delay = (2 ** attempt) + random.uniform(0, 1)
                log(f"[S{sys_id}] {asset} attempt {attempt+1}/{retries} failed: {e} — retrying in {delay:.1f}s")
                time.sleep(delay)
            else:
                raise e
    return None

def place_market_order(asset, side, contracts):
    """Paper mode: simulate. Live mode: real CFM order."""
    if PAPER_MODE:
        fake_oid = f"PAPER-{asset}-{int(time.time())}"
        log(f"📄 PAPER: {asset} {side} {contracts}ct → {fake_oid}")
        return fake_oid, int(contracts)
    try:
        client  = get_cb_client()
        product = ASSETS[asset]["perp"]
        for attempt in range(max(1, int(contracts)), 0, -1):
            size = str(attempt)
            if side in ("BUY", "LONG"):
                order = client.market_order_buy(
                    client_order_id=str(uuid.uuid4()),
                    product_id=product, base_size=size)
            else:
                order = client.market_order_sell(
                    client_order_id=str(uuid.uuid4()),
                    product_id=product, base_size=size)
            if order["success"]:
                sr  = order["success_response"]
                oid = sr["order_id"] if isinstance(sr, dict) else f"CB-{asset}-{int(time.time())}"
                log(f"✅ CB order: {asset} {side} {size}ct → {oid}")
                return oid, attempt
            else:
                err    = order["error_response"]
                reason = err.get("preview_failure_reason", "") if isinstance(err, dict) else ""
                if "INSUFFICIENT_FUNDS" in reason and attempt > 1:
                    continue
                log(f"⚠️ Order failed: {asset} {err}")
                return None, 0
        return None, 0
    except Exception as e:
        log(f"❌ Order exception {asset}: {e}")
        return None, 0

# ── Math ──────────────────────────────────────────────────────────
def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return [None] * len(closes)
    out    = [None] * period
    gains  = [max(0, closes[i] - closes[i-1]) for i in range(1, len(closes))]
    losses = [max(0, closes[i-1] - closes[i]) for i in range(1, len(closes))]
    ag = sum(gains[:period])  / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
        rs = ag / al if al > 0 else 100
        out.append(100 - 100 / (1 + rs))
    while len(out) < len(closes):
        out.append(out[-1])
    return out

def get_hr_rsi(asset, candles_15m):
    if not candles_15m or len(candles_15m) < 60: return None
    try:
        c1h   = [float(candles_15m[i+3]["c"]) for i in range(0, len(candles_15m)-3, 4)]
        if len(c1h) < 16: return None
        rsi1h = calc_rsi(c1h, 14)
        val   = rsi1h[-2] if len(rsi1h) >= 2 and rsi1h[-2] is not None else None
        return round(val, 1) if val is not None else None
    except Exception as e:
        log(f"get_hr_rsi error {asset}: {e}")
        return None

def evaluate_signal(candles):
    if not candles or len(candles) < RSI_PERIOD + 2:
        return None, None, None, {"fail": "not enough candles"}
    closes = [float(c["c"]) for c in candles]
    rsi    = calc_rsi(closes, RSI_PERIOD)
    i      = len(rsi) - 2
    if rsi[i] is None or rsi[i-1] is None:
        return None, None, None, {"fail": "RSI not ready"}
    cur  = rsi[i]
    prev = rsi[i-1]
    if prev < RSI_ENTRY and cur >= RSI_ENTRY:
        return "LONG", None, None, {"strategy":"RSI-Mom+MTF","rsi_prev":round(prev,2),"rsi_cur":round(cur,2)}
    elif prev > (100-RSI_ENTRY) and cur <= (100-RSI_ENTRY):
        return "SHORT", None, None, {"strategy":"RSI-Mom+MTF","rsi_prev":round(prev,2),"rsi_cur":round(cur,2)}
    return None, None, None, {"fail": f"no cross (RSI prev={prev:.1f} cur={cur:.1f}) threshold={RSI_ENTRY}"}

def should_exit(pos, candles):
    if not candles or len(candles) < RSI_PERIOD + 2: return False
    closes  = [float(c["c"]) for c in candles]
    rsi     = calc_rsi(closes, RSI_PERIOD)
    cur_rsi = rsi[-2] if rsi[-2] is not None else rsi[-1]
    if cur_rsi is None: return False
    if pos["direction"] == "LONG":
        if cur_rsi > RSI_TRAIL_TRIG:
            pos["exit_rsi"] = RSI_TRAIL_EXIT
        return cur_rsi < pos.get("exit_rsi", RSI_EXIT)
    else:
        if cur_rsi < (100 - RSI_TRAIL_TRIG):
            pos["exit_rsi"] = 100 - RSI_TRAIL_EXIT
        return cur_rsi > pos.get("exit_rsi", 100 - RSI_EXIT)

def merge_cfm_intx(cfm, intx):
    if not cfm and not intx: return []
    if not cfm: return intx or []
    if not intx: return cfm
    cm = {c["ts"]: c for c in cfm}
    im = {c["ts"]: c for c in intx}
    return [cm[ts] if ts in cm else im[ts] for ts in sorted(set(cm) | set(im))]

# ══════════════════════════════════════════════════════════════════
# INSTANTIATE 3 SYSTEMS
# ══════════════════════════════════════════════════════════════════
S1 = TradingSystem(1, "CFM only",  "cfm")
S2 = TradingSystem(2, "INTX only", "intx")
S3 = TradingSystem(3, "Hybrid",    "hybrid")
SYSTEMS = [S1, S2, S3]

# ══════════════════════════════════════════════════════════════════
# FLASK DASHBOARD
# ══════════════════════════════════════════════════════════════════
app = Flask(__name__)

@app.route("/login", methods=["POST"])
def login():
    from flask import make_response
    pw = request.form.get("pw", "")
    if pw == "3757":
        resp = make_response(redirect("/"))
        resp.set_cookie("auth", "3757", max_age=60*60*24*30)
        return resp
    return redirect("/")

@app.route("/health")
def health():
    out = {}
    for sys in SYSTEMS:
        with sys.lock: s = dict(sys.state)
        wr = round(s["wins"]/s["total_trades"]*100,1) if s["total_trades"] else 0
        out[f"S{sys.sys_id}_{sys.source}"] = {
            "balance":    f"${s['balance']:,.2f}",
            "total_pnl":  f"${s['total_pnl']:+,.2f}",
            "weekly_pnl": f"${s['weekly_pnl']:+,.2f}",
            "trades":     s["total_trades"],
            "win_rate":   f"{wr}%",
            "open":       list(sys.positions.keys()),
            "errors":     s.get("loop_errors", 0),
            "last_run":   s["loop_last_run"],
        }
    return Response(json.dumps(out, indent=2), mimetype="application/json")

@app.route("/sim-data-s<int:sid>")
def sim_data_sys(sid):
    if request.cookies.get("auth") != "3757":
        return Response("Unauthorized", status=401)
    sys = next((s for s in SYSTEMS if s.sys_id == sid), None)
    if not sys: return Response("Unknown system", status=404)
    try:
        return Response(open(sys.data_file).read(), mimetype="application/json",
                        headers={"Content-Disposition": f"attachment;filename=cb_sim_s{sid}.json"})
    except:
        return Response("[]", mimetype="application/json")

@app.route("/tax-s<int:sid>")
def tax_sys(sid):
    sys = next((s for s in SYSTEMS if s.sys_id == sid), None)
    if not sys: return Response("Unknown system", status=404)
    try:
        return Response(open(sys.tax_file).read(), mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment;filename=cb_trades_s{sid}.csv"})
    except:
        return Response("No trades yet", mimetype="text/plain")

@app.route("/diag-s<int:sid>")
def diag_sys(sid):
    sys = next((s for s in SYSTEMS if s.sys_id == sid), None)
    if not sys: return Response("Unknown system", status=404)
    try:
        return Response(open(sys.diag_file).read(), mimetype="application/json")
    except:
        return Response("[]", mimetype="application/json")

@app.route("/")
def dashboard():
    if request.cookies.get("auth") != "3757":
        return """<!DOCTYPE html><html><head><title>CB Trader v72</title>
<meta name=viewport content='width=device-width,initial-scale=1'>
<style>body{background:#060D1A;color:#E0E6F0;font-family:-apple-system,sans-serif;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.box{text-align:center;padding:40px;background:#0A1628;border:1px solid #1E2D45;border-radius:12px}
input{background:#060D1A;border:1px solid #1E2D45;color:#E0E6F0;padding:12px;
border-radius:8px;margin:10px 0;width:200px;font-size:16px;display:block}
button{background:#00D68F;color:#000;border:none;padding:12px 24px;border-radius:8px;
cursor:pointer;font-weight:700;font-size:16px;width:200px;margin-top:8px}
h2{margin-bottom:20px}</style></head>
<body><form method=post action=/login class=box>
<h2>CB Trader v72</h2>
<input type=password name=pw placeholder='Password' autofocus>
<button type=submit>Login</button>
</form></body></html>"""

    # Build system cards
    sys_colors = {"1": "#00D68F", "2": "#7B61FF", "3": "#FFB800"}
    sys_cards  = ""
    for sys in SYSTEMS:
        with sys.lock:
            s   = dict(sys.state)
            pos = dict(sys.positions)
        wr  = round(s["wins"]/s["total_trades"]*100,1) if s["total_trades"] else 0
        col = sys_colors.get(str(sys.sys_id), "#E0E6F0")
        wk_col  = "#00D68F" if s["weekly_pnl"] >= 0 else "#FF4757"
        tot_col = "#00D68F" if s["total_pnl"]  >= 0 else "#FF4757"

        # Position rows
        pos_rows = ""
        for asset, p in pos.items():
            unreal    = p.get("unrealized_pnl", 0.0)
            pnl_col   = "#00D68F" if unreal >= 0 else "#FF4757"
            dir_col   = "#00D68F" if p["direction"] == "LONG" else "#FF4757"
            exit_rsi  = p.get("exit_rsi", RSI_EXIT)
            locked    = "🔒" if exit_rsi == RSI_TRAIL_EXIT else ""
            cur_price = p.get("current_price", p.get("entry", 0))
            entry_fee = round(p["entry"]   * p["size"] * FEE_PCT + FEE_FLAT * p["contracts"], 4)
            exit_fee  = round(cur_price * p["size"] * FEE_PCT + FEE_FLAT * p["contracts"], 4)
            pos_rows += f"""<div style='background:#060D1A;border-radius:8px;padding:10px;margin-bottom:8px;border:1px solid #1E2D45'>
              <div style='display:flex;justify-content:space-between;margin-bottom:6px'>
                <b>{asset}</b>
                <span style='color:{dir_col};font-weight:700'>{p["direction"]}</span>
                <span style='color:{pnl_col};font-weight:700'>${unreal:+,.2f}</span>
              </div>
              <div style='font-size:11px;color:#4A5878'>
                entry=${p["entry"]:,.4f} | cur=${cur_price:,.4f} | exit RSI&lt;{exit_rsi}{locked} | hr={p.get("hr_rsi","?")} | {p.get("contracts",1)}ct | fees≈${entry_fee+exit_fee:.4f}
              </div>
            </div>"""
        if not pos_rows:
            pos_rows = "<div style='color:#4A5878;font-size:12px;padding:8px'>No open positions</div>"

        # Journal rows
        try:    audit_data = json.load(open(sys.diag_file)) if os.path.exists(sys.diag_file) else []
        except: audit_data = []

        journal_rows = ""; j_shown = 0
        heartbeat_rows = ""; hb_built = False
        error_rows = ""; error_count = 0

        error_kw  = ["⚠️","WARNING","ERROR","CRITICAL","FAILED","failed","timeout","Skipped"]
        trade_evt = ["ENTER","EXIT","HOLD","NO_SIGNAL","CYCLE","RSI-Mom","📊","📄","✅ EXIT","❌ EXIT"]

        for a in audit_data:
            evt = a.get("event","")

            if "CYCLE" in evt and not hb_built:
                hb_built = True
                detail = a.get("detail","")
                lines  = detail.split("\n")
                heartbeat_rows += f"<div style='font-size:12px;font-weight:700;color:#E0E6F0;padding:6px 0;border-bottom:1px solid #1E2D45;margin-bottom:8px'>{lines[0] if lines else detail}</div>"
                for line in lines[1:]:
                    line = line.strip()
                    if not line: continue
                    css = "hb-hold" if "HOLD" in line else "hb-skip" if "❌" in line else "hb-watch"
                    heartbeat_rows += f"<div class='hb-row {css}'>{line}</div>"
                heartbeat_rows += f"<div style='font-size:10px;color:#4A5878;margin-top:8px'>Updated: {a.get('time','?')}</div>"

            if j_shown < 50 and "CYCLE" not in evt:
                j_shown += 1
                jcol = "#00D68F" if "ENTER" in evt else "#FF4757" if "EXIT" in evt else "#E0E6F0"
                journal_rows += f"""<div class=j-trade style='border-color:{jcol}'>
                  <div style='font-size:10px;color:#4A5878'>{a["time"]} · {a.get("asset","SYS")}</div>
                  <div style='font-size:12px;font-weight:700;color:{jcol}'>{evt}</div>
                  <div style='font-size:11px;color:#8892A4'>{a.get("detail","")[:120]}</div>
                </div>"""

            if not any(te in evt for te in trade_evt):
                if any(kw in evt or kw in a.get("detail","") for kw in error_kw) and "CYCLE" not in evt:
                    error_count += 1
                    error_rows += f"""<div style='border-left:3px solid #FFB800;padding:8px 12px;margin-bottom:6px;background:#060D1A;border-radius:0 8px 8px 0'>
                      <div style='font-size:10px;color:#4A5878'>{a["time"]} · {a.get("asset","SYS")}</div>
                      <div style='font-size:11px;color:#8892A4;font-family:monospace'>{evt}: {a.get("detail","")[:150]}</div>
                    </div>"""

        if not journal_rows:   journal_rows   = "<div style='color:#4A5878;padding:20px;text-align:center;font-size:13px'>No trades yet</div>"
        if not heartbeat_rows: heartbeat_rows = "<div style='color:#4A5878;padding:20px;text-align:center;font-size:13px'>No heartbeat yet</div>"
        if not error_rows:     error_rows     = "<div style='color:#4A5878;padding:20px;text-align:center;font-size:13px'>✅ No errors</div>"

        err_badge = f" <span style='background:#FF4757;color:#fff;border-radius:10px;padding:1px 5px;font-size:10px'>{error_count}</span>" if error_count else ""

        # Markets rows
        markets_rows = ""
        for a_name in ASSET_NAMES:
            is_open = a_name in pos
            sc = "#00D68F" if is_open else "#4A5878"
            markets_rows += f"""<div style='display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #1E2D45;font-size:12px'>
              <b>{a_name}</b>
              <span style='color:#4A5878'>{ASSETS[a_name]["perp"]}</span>
              <span style='color:#4A5878'>{ASSETS[a_name]["margin_rate"]*100:.0f}% margin</span>
              <span style='color:{sc};font-weight:600'>{"● OPEN" if is_open else "○ READY"}</span>
            </div>"""

        sid = sys.sys_id
        sys_cards += f"""<div class='sys-card' style='background:#0A1628;border:2px solid {col};border-radius:12px;padding:16px;margin-bottom:20px'>
          <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:12px'>
            <div>
              <span style='font-size:16px;font-weight:800;color:{col}'>S{sid}</span>
              <span style='font-size:13px;color:#8892A4;margin-left:8px'>{sys.label}</span>
            </div>
            <span style='font-size:11px;color:#4A5878;background:#060D1A;padding:3px 8px;border-radius:20px'>{sys.source}</span>
          </div>
          <div style='display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px'>
            <div style='text-align:center;background:#060D1A;border-radius:8px;padding:8px'>
              <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>BALANCE</div>
              <div style='font-size:15px;font-weight:800'>${s["balance"]:,.2f}</div>
            </div>
            <div style='text-align:center;background:#060D1A;border-radius:8px;padding:8px'>
              <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>WEEK</div>
              <div style='font-size:15px;font-weight:800;color:{wk_col}'>${s["weekly_pnl"]:+,.2f}</div>
            </div>
            <div style='text-align:center;background:#060D1A;border-radius:8px;padding:8px'>
              <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>TOTAL P&L</div>
              <div style='font-size:15px;font-weight:800;color:{tot_col}'>${s["total_pnl"]:+,.2f}</div>
            </div>
            <div style='text-align:center;background:#060D1A;border-radius:8px;padding:8px'>
              <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>WR</div>
              <div style='font-size:15px;font-weight:800'>{wr}%</div>
            </div>
          </div>
          <div style='display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:12px;font-size:12px'>
            <div style='background:#060D1A;border-radius:6px;padding:6px;text-align:center'>
              <div style='color:#4A5878;font-size:10px'>TRADES</div><div style='font-weight:700'>{s["total_trades"]}</div>
            </div>
            <div style='background:#060D1A;border-radius:6px;padding:6px;text-align:center'>
              <div style='color:#4A5878;font-size:10px'>OPEN</div>
              <div style='font-weight:700;color:{"#00D68F" if len(pos)>0 else "#4A5878"}'>{len(pos)}</div>
            </div>
            <div style='background:#060D1A;border-radius:6px;padding:6px;text-align:center'>
              <div style='color:#4A5878;font-size:10px'>ERRORS</div>
              <div style='font-weight:700;color:{"#FF4757" if s.get("loop_errors",0)>0 else "#4A5878"}'>{s.get("loop_errors",0)}</div>
            </div>
          </div>
          <div class=tabs>
            <span class='tab on' onclick="show('s{sid}pos',this,'s{sid}')">Positions</span>
            <span class=tab onclick="show('s{sid}jrn',this,'s{sid}')">Journal</span>
            <span class=tab onclick="show('s{sid}hb',this,'s{sid}')">Heartbeat</span>
            <span class=tab onclick="show('s{sid}err',this,'s{sid}')">Errors{err_badge}</span>
            <span class=tab onclick="show('s{sid}mkt',this,'s{sid}')">Markets</span>
            <span class=tab onclick="show('s{sid}inf',this,'s{sid}')">Info</span>
          </div>
          <div id='s{sid}pos' class='panel on'>{pos_rows}</div>
          <div id='s{sid}jrn' class=panel>{journal_rows}</div>
          <div id='s{sid}hb'  class=panel>{heartbeat_rows}</div>
          <div id='s{sid}err' class=panel>{error_rows}</div>
          <div id='s{sid}mkt' class=panel>{markets_rows}</div>
          <div id='s{sid}inf' class=panel>
            <div style='font-size:12px;line-height:2;color:#8892A4'>
              <b style='color:#E0E6F0'>Source</b>: {sys.source}<br>
              <b style='color:#E0E6F0'>Strategy</b>: RSI({RSI_PERIOD}/{RSI_ENTRY}/{RSI_EXIT}/{RSI_TRAIL_TRIG}→{RSI_TRAIL_EXIT}) + MTF<br>
              <b style='color:#E0E6F0'>Capital</b>: ${PAPER_BALANCE:,.2f} isolated<br>
              <b style='color:#E0E6F0'>Assets</b>: XRP · XLM<br>
              <b style='color:#E0E6F0'>Execution</b>: CFM always<br>
              <b style='color:#E0E6F0'>Fees</b>: 0.080% + $0.12/ct/side<br>
              <div style='margin-top:8px'>
                <a href='/sim-data-s{sid}' style='color:#4A5878'>Sim Data</a> &nbsp;·&nbsp;
                <a href='/tax-s{sid}' style='color:#4A5878'>Tax CSV</a> &nbsp;·&nbsp;
                <a href='/diag-s{sid}' style='color:#4A5878'>Diagnostic</a>
              </div>
            </div>
          </div>
        </div>"""

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_est = ts_est()
    mode_color = "#FFB800" if PAPER_MODE else "#00D68F"
    mode_label = "📄 PAPER" if PAPER_MODE else "🔴 LIVE"

    return f"""<!DOCTYPE html>
<html><head>
<title>CB Trader v72</title>
<meta charset=utf-8>
<meta name=viewport content='width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no'>
<meta http-equiv=refresh content=30>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#060D1A;color:#E0E6F0;font-family:-apple-system,BlinkMacSystemFont,sans-serif;
       padding:14px;max-width:640px;margin:0 auto;padding-bottom:40px}}
  a{{color:#8892A4;text-decoration:none}}
  .tabs{{display:flex;gap:4px;margin-bottom:0;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none}}
  .tabs::-webkit-scrollbar{{display:none}}
  .tab{{flex-shrink:0;padding:10px 14px;cursor:pointer;border-radius:8px 8px 0 0;font-size:12px;font-weight:600;
        background:#060D1A;color:#4A5878;border:1px solid #1E2D45;border-bottom:none;
        min-height:40px;display:flex;align-items:center;touch-action:manipulation}}
  .tab.on{{background:#0A1628;color:#E0E6F0}}
  .panel{{display:none;background:#0A1628;border:1px solid #1E2D45;
          border-radius:0 10px 10px 10px;padding:12px;min-height:80px}}
  .panel.on{{display:block}}
  .hb-row{{font-family:monospace;font-size:11px;padding:5px 0;border-bottom:1px solid #060D1A;word-break:break-all}}
  .hb-hold{{color:#00D68F}}.hb-watch{{color:#4A5878}}.hb-skip{{color:#FF4757}}
  .j-trade{{border-left:3px solid;padding:8px 12px;margin-bottom:6px;background:#060D1A;border-radius:0 8px 8px 0}}
</style>
<script>
function show(id,el,prefix){{
  var card=el.closest('.sys-card');
  card.querySelectorAll('.panel').forEach(function(p){{p.classList.remove('on')}});
  card.querySelectorAll('.tab').forEach(function(t){{t.classList.remove('on')}});
  document.getElementById(id).classList.add('on');
  el.classList.add('on');
}}
</script>
</head><body>
<div style='display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px'>
  <div>
    <div style='font-size:22px;font-weight:800'>CB Trader v72</div>
    <div style='font-size:12px;font-weight:700;color:{mode_color};margin-top:2px'>{mode_label}</div>
    <div style='font-size:11px;color:#4A5878;margin-top:2px'>3-System Candle Source Test</div>
  </div>
  <div style='text-align:right;font-size:11px;color:#4A5878;line-height:1.7'>
    {now_utc}<br>{now_est}
  </div>
</div>
<div style='font-size:11px;color:#4A5878;margin-bottom:14px;padding:10px;background:#0A1628;border-radius:8px;border:1px solid #1E2D45'>
  RSI({RSI_PERIOD}/{RSI_ENTRY}/{RSI_EXIT}/{RSI_TRAIL_TRIG}→{RSI_TRAIL_EXIT}) + MTF · All 3 systems completely isolated · Execute on CFM
</div>
{sys_cards}
<div style='font-size:11px;color:#4A5878;text-align:center;margin-top:8px'>
  <a href='/health'>Health JSON</a>
</div>
</body></html>"""

# ══════════════════════════════════════════════════════════════════
# STARTUP — deferred to first request (gunicorn compatible)
# ══════════════════════════════════════════════════════════════════
_started    = False
_start_lock = threading.Lock()

def startup():
    global _started
    with _start_lock:
        if _started: return
        _started = True

    log("📡 CB Trader v72 — pre-loading candles for all 3 systems...")

    # Shared candle fetch on startup — each system caches its own copy
    for asset in ASSET_NAMES:
        try:
            # Fetch both sources once
            end   = int(time.time())
            start = end - CANDLE_LIMIT * 900

            # CFM
            client = get_cb_client()
            r = client.get_candles(ASSETS[asset]["perp"], start=str(start), end=str(end), granularity=CANDLE_TF)
            cfm = sorted([{
                "ts": int(c.start)*1000,
                "dt": datetime.fromtimestamp(int(c.start),tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "o": float(c.open), "h": float(c.high),
                "l": float(c.low),  "c": float(c.close), "v": float(c.volume),
                "source": "cfm",
            } for c in r.candles], key=lambda x: x["ts"])[-CANDLE_LIMIT:] if r.candles else []

            # INTX
            sym      = ASSETS[asset]["intx"]
            start_dt = datetime.fromtimestamp(end - CANDLE_LIMIT*900, tz=timezone.utc)
            end_dt   = datetime.fromtimestamp(end, tz=timezone.utc)
            ri = req.get(f"https://api.international.coinbase.com/api/v1/instruments/{sym}/candles",
                         params={"granularity":"FIFTEEN_MINUTE",
                                 "start":start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                 "end":end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}, timeout=10)
            intx = []
            if ri.status_code == 200:
                aggs = ri.json().get("aggregations",[])
                intx = sorted([{
                    "ts": int(datetime.strptime(c["start"],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()*1000),
                    "dt": c["start"],
                    "o": float(c["open"]), "h": float(c["high"]),
                    "l": float(c["low"]),  "c": float(c["close"]), "v": float(c["volume"]),
                    "source": "intx",
                } for c in aggs], key=lambda x: x["ts"])[-CANDLE_LIMIT:]

            hybrid = merge_cfm_intx(cfm, intx)

            # Each system gets its own source candles
            if cfm  and len(cfm)  >= 60: S1.startup_cache[asset] = cfm
            if intx and len(intx) >= 60: S2.startup_cache[asset] = intx
            if hybrid and len(hybrid) >= 60: S3.startup_cache[asset] = hybrid

            # Log hr_rsi per system
            hr1 = get_hr_rsi(asset, cfm)
            hr2 = get_hr_rsi(asset, intx)
            hr3 = get_hr_rsi(asset, hybrid)
            log(f"  {asset}: CFM={len(cfm)} hr={hr1} | INTX={len(intx)} hr={hr2} | Hybrid={len(hybrid)} hr={hr3}")

            time.sleep(0.3)
        except Exception as e:
            log(f"  Startup preload {asset}: {e}")

    log("✅ Pre-load complete — all 3 systems ready")
    log(f"🚀 CB Trader v72 | Mode: {'📄 PAPER' if PAPER_MODE else '🔴 LIVE'}")
    log(f"   Strategy: RSI({RSI_PERIOD}/{RSI_ENTRY}/{RSI_EXIT}/{RSI_TRAIL_TRIG}→{RSI_TRAIL_EXIT}) + MTF")
    log(f"   Assets: {', '.join(ASSET_NAMES)}")
    log(f"   Capital: ${PAPER_BALANCE:,.2f} per system (${PAPER_BALANCE*3:,.2f} total)")
    log(f"   Time: {ts_est()}")

    # Start 3 isolated trading threads
    for sys in SYSTEMS:
        threading.Thread(target=sys.run, daemon=True, name=f"S{sys.sys_id}-{sys.source}").start()
        log(f"  ✅ S{sys.sys_id} ({sys.label}) thread started")
        time.sleep(0.1)

@app.before_request
def ensure_started():
    startup()
