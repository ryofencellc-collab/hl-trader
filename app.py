"""
AP3X 2.0
═══════════════════════════════════════════════════════════════════
Grid Trading — XRP + SOL — Paper Mode
Two completely independent grid systems

S4 — XRP Grid  | XPP-20DEC30-CDE | $0.15 spacing | 50 levels
S5 — SOL Grid  | SLP-20DEC30-CDE | $7.00 spacing | 40 levels

Each system:
  - Completely isolated state, fills, candles, logs
  - Buys at grid level, sells at EXACT next level (no holding)
  - Recenters on price breakout (closes all positions at market)
  - Trend bias: 3x contracts (strong up), 2x (moderate), 1x (neutral/down)
  - 1hr EMA20/50/200 for trend detection
  - 300 hourly candles kept for EMA warmup

Contract specs (verified from Coinbase live API):
  XRP:  500 XRP/contract | 20.01% intraday margin | XPP-20DEC30-CDE
  SOL:  5 SOL/contract   | 20.00% intraday margin | SLP-20DEC30-CDE

Fees (confirmed from real Coinbase fills):
  0.080% taker per side + $0.12 flat per contract per side

Files saved per system (all in /tmp):
  grid_state_s{N}.json   — live state (balance, pnl, grid, open buys)
  grid_fills_s{N}.json   — every fill ever (permanent record for sim analysis)
  grid_candles_s{N}.json — 300 1hr close prices (EMA history)
  grid_log_s{N}.txt      — full text log of every event

Routes:
  /                       — dashboard (password protected)
  /health                 — health JSON
  /grid-state-s{4,5}     — live state JSON
  /grid-fills-s{4,5}     — all fills JSON
  /grid-candles-s{4,5}   — 1hr price history JSON
  /grid-log-s{4,5}       — log tail

Railway env vars:
  CB_API_KEY, CB_API_SECRET  — Coinbase API credentials
  PAPER_BALANCE              — starting capital per system (default: 2000)
  APP_PASSWORD               — dashboard password (default: 3757)
  NTFY_TOPIC                 — push notification topic

CHECKLIST — verified before push:
  ✅ S4 = XRP | XPP-20DEC30-CDE | 500 XRP | 20.01% margin | $0.15 spacing | 50 levels
  ✅ S5 = SOL | SLP-20DEC30-CDE | 5 SOL   | 20.00% margin | $7.00 spacing | 40 levels
  ✅ Fee = 0.080% per side + $0.12 flat per contract per side
  ✅ Exit at EXACT next grid level — not market price
  ✅ Breakout close at market — recenter grid
  ✅ Trend bias from EMA20/50/200 on 1hr candles
  ✅ 300 1hr candles kept — enough for EMA200 warmup
  ✅ Every fill appended to grid_fills_s{N}.json immediately
  ✅ State saved after every action
  ✅ Candles saved every hour
  ✅ Completely isolated — S4 never touches S5 files or state
  ✅ Separate threading locks per system
  ✅ Separate Coinbase price fetches per system
  ✅ All files unique paths — no shared /tmp files
  ✅ Dashboard shows both systems side by side
  ✅ Monthly P&L tracked and shown
  ✅ Weekly P&L tracked and shown
  ✅ Loop errors counted and shown
  ✅ ntfy alerts on every fill and error
  ✅ Health endpoint returns JSON status for Railway monitoring
  ✅ No RSI code — completely removed from v1
  ✅ No S1/S2/S3 — completely removed from v1
"""

import time, os, json, threading
from datetime import datetime, timezone, timedelta
from flask import Flask, Response, request, redirect
import requests as req

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
CB_API_KEY  = os.environ.get("CB_API_KEY", "")
CB_API_SEC  = os.environ.get("CB_API_SECRET", "")
if not CB_API_KEY or not CB_API_SEC:
    raise RuntimeError("CB_API_KEY and CB_API_SECRET must be set in Railway env vars")

NTFY_TOPIC  = os.environ.get("NTFY_TOPIC", "hl-trader-lunchm0ney")
NTFY_URL    = f"https://ntfy.sh/{NTFY_TOPIC}"
APP_PASSWORD = os.environ.get("APP_PASSWORD", "3757")
PAPER_BALANCE = float(os.environ.get("PAPER_BALANCE", "2000"))

FEE_PCT  = 0.00080   # 0.080% taker per side — confirmed from real Coinbase fills
FEE_FLAT = 0.12      # $0.12 flat per contract per side — confirmed from real Coinbase fills

# Grid system configurations — verified from live Coinbase API Sep 15 2026
GRID_CONFIGS = {
    4: {
        "label":      "XRP Grid",
        "product_id": "XPP-20DEC30-CDE",
        "contract":   500.0,    # 500 XRP per contract
        "margin":     0.2001,   # 20.01% intraday margin
        "spacing":    0.15,     # $0.15 between grid levels
        "n_grids":    50,       # 25 below + 25 above center
        "capital":    PAPER_BALANCE,
        "color":      "#00B4D8",
    },
    5: {
        "label":      "SOL Grid",
        "product_id": "SLP-20DEC30-CDE",
        "contract":   5.0,      # 5 SOL per contract
        "margin":     0.20,     # 20.00% intraday margin
        "spacing":    7.0,      # $7.00 between grid levels
        "n_grids":    40,       # 20 below + 20 above center
        "capital":    PAPER_BALANCE,
        "color":      "#9B5DE5",
    },
}

# ══════════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ══════════════════════════════════════════════════════════════════
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def ts_est():
    utc  = datetime.now(timezone.utc)
    off  = -4 if 4 <= utc.month <= 10 else -5
    est  = utc + timedelta(hours=off)
    sfx  = "EDT" if off == -4 else "EST"
    return est.strftime(f"%Y-%m-%d %H:%M {sfx}")

def log(msg):
    print(f"[{ts()}] {msg}", flush=True)

def get_week():
    n = datetime.now(timezone.utc)
    return f"{n.year}-W{n.isocalendar()[1]:02d}"

def ntfy(title, body, priority="default"):
    try:
        req.post(NTFY_URL, data=body.encode("utf-8"),
                 headers={"Title": title.encode("ascii","ignore").decode().strip(),
                          "Priority": priority,
                          "Content-Type": "text/plain; charset=utf-8"},
                 timeout=10)
    except Exception as e:
        log(f"ntfy error: {e}")

# Shared Coinbase client — lazy init, thread-safe
_cb_client      = None
_cb_client_lock = threading.Lock()

def get_cb_client():
    global _cb_client
    if _cb_client is None:
        with _cb_client_lock:
            if _cb_client is None:
                from coinbase.rest import RESTClient
                _cb_client = RESTClient(api_key=CB_API_KEY, api_secret=CB_API_SEC)
                # Apply 10s timeout to all requests
                if hasattr(_cb_client, "session"):
                    _orig = _cb_client.session.request
                    def _req_with_timeout(method, url, **kwargs):
                        kwargs.setdefault("timeout", 10)
                        return _orig(method, url, **kwargs)
                    _cb_client.session.request = _req_with_timeout
                log("✅ Coinbase client ready (10s timeout)")
    return _cb_client

# ══════════════════════════════════════════════════════════════════
# GRID SYSTEM CLASS
# One instance per grid bot. Completely self-contained.
# ══════════════════════════════════════════════════════════════════
class GridSystem:

    def __init__(self, sys_id):
        cfg = GRID_CONFIGS[sys_id]
        self.sys_id     = sys_id
        self.label      = cfg["label"]
        self.product_id = cfg["product_id"]
        self.cs         = cfg["contract"]   # units per contract
        self.mr         = cfg["margin"]     # intraday margin rate
        self.spacing    = cfg["spacing"]    # $ between grid levels
        self.n_grids    = cfg["n_grids"]    # total levels
        self.capital    = cfg["capital"]    # starting capital
        self.color      = cfg["color"]      # dashboard color

        # All files unique to this system — never shared
        self.state_file   = f"/tmp/grid_state_s{sys_id}.json"
        self.fills_file   = f"/tmp/grid_fills_s{sys_id}.json"
        self.candles_file = f"/tmp/grid_candles_s{sys_id}.json"
        self.log_file     = f"/tmp/grid_log_s{sys_id}.txt"

        self.lock = threading.Lock()

        # Load persisted state or start fresh
        self.state            = self._load_state()
        self.price_history_1h = self._load_candles()

    # ── Persistence ──────────────────────────────────────────────

    def _default_state(self):
        return {
            "balance":         self.capital,
            "total_pnl":       0.0,
            "total_fills":     0,
            "total_breakouts": 0,
            "grid_center":     None,
            "grid_levels":     [],
            "open_buys":       {},        # str(level) → entry_price
            "start_time":      datetime.now(timezone.utc).isoformat(),
            "last_price":      None,
            "last_update":     None,
            "monthly_pnl":     {},        # "YYYY-MM" → cumulative float
            "weekly_pnl":      0.0,
            "week":            None,
            "loop_errors":     0,
        }

    def _load_state(self):
        state = self._default_state()
        if os.path.exists(self.state_file):
            try:
                saved = json.load(open(self.state_file))
                state.update(saved)
                self._syslog(f"State loaded: balance=${state['balance']:.2f} "
                             f"pnl=${state['total_pnl']:+.2f} fills={state['total_fills']}")
            except Exception as e:
                self._syslog(f"State load error: {e} — starting fresh")
        return state

    def _save_state(self):
        try:
            self.state["last_update"] = datetime.now(timezone.utc).isoformat()
            tmp = self.state_file + ".tmp"
            json.dump(self.state, open(tmp, "w"), indent=2)
            os.replace(tmp, self.state_file)
        except Exception as e:
            self._syslog(f"State save error: {e}")

    def _load_candles(self):
        if os.path.exists(self.candles_file):
            try:
                data = json.load(open(self.candles_file))
                self._syslog(f"Candle history loaded: {len(data)} 1hr prices")
                return data
            except Exception as e:
                self._syslog(f"Candle load error: {e}")
        return []

    def _save_candles(self):
        try:
            # Keep exactly 300 — enough for EMA200 warmup
            to_save = self.price_history_1h[-300:]
            tmp = self.candles_file + ".tmp"
            json.dump(to_save, open(tmp, "w"))
            os.replace(tmp, self.candles_file)
        except Exception as e:
            self._syslog(f"Candle save error: {e}")

    def _append_fill(self, fill):
        """
        Append fill to fills_file permanently.
        This is the permanent record used for sim analysis.
        Fields: time, type, buy_level, sell_level, entry, exit, cts, bias, pnl, balance
        """
        try:
            fills = []
            if os.path.exists(self.fills_file):
                try:
                    fills = json.load(open(self.fills_file))
                except:
                    fills = []
            fills.append(fill)
            tmp = self.fills_file + ".tmp"
            json.dump(fills, open(tmp, "w"), indent=2)
            os.replace(tmp, self.fills_file)
        except Exception as e:
            self._syslog(f"Fill save error: {e}")

    def _syslog(self, msg):
        line = f"[{ts()}] [S{self.sys_id}/{self.label}] {msg}"
        log(line)
        try:
            with open(self.log_file, "a") as f:
                f.write(line + "\n")
        except:
            pass

    # ── Price feed ───────────────────────────────────────────────

    def get_bid_ask(self):
        """Fetch live bid/ask — 3 retries with backoff"""
        for attempt in range(3):
            try:
                client = get_cb_client()
                r = client.get_best_bid_ask(product_ids=[self.product_id])
                for p in r.pricebooks:
                    if p.product_id == self.product_id:
                        bid = float(p.bids[0].price) if p.bids else None
                        ask = float(p.asks[0].price) if p.asks else None
                        if bid and ask:
                            return bid, ask, (bid + ask) / 2.0
            except Exception as e:
                self._syslog(f"Price fetch attempt {attempt+1}/3 failed: {e}")
                time.sleep(3 * (attempt + 1))
        return None, None, None

    # ── EMA trend bias ───────────────────────────────────────────

    def _ema_val(self, prices, period):
        if len(prices) < period:
            return None
        k   = 2.0 / (period + 1)
        val = sum(prices[:period]) / period
        for p in prices[period:]:
            val = p * k + val * (1 - k)
        return val

    def trend_bias(self):
        """
        EMA20/50/200 on 1hr closes.
        Returns 3.0 (strong uptrend), 2.0 (moderate), 1.0 (neutral/down).
        Falls back to 1.0 while building 200-candle history (first ~8 days).
        """
        ph = self.price_history_1h
        if len(ph) < 200:
            return 1.0
        e20  = self._ema_val(ph, 20)
        e50  = self._ema_val(ph, 50)
        e200 = self._ema_val(ph, 200)
        cp   = ph[-1]
        if e20 and e50 and e200:
            if cp > e20 > e50 > e200:
                return 3.0
            if cp > e50:
                return 2.0
        return 1.0

    # ── Grid helpers ─────────────────────────────────────────────

    def _round_lvl(self, price):
        """Round to same precision as spacing"""
        if self.spacing < 1:
            return round(price, 4)
        return round(price, 2)

    def build_grid(self, center):
        return [self._round_lvl(center + (i - self.n_grids // 2) * self.spacing)
                for i in range(self.n_grids + 1)]

    def calc_pnl(self, entry, exit_p, contracts):
        """
        Net P&L after fees for one round-trip fill.
        Entry: long at ask. Exit: sell at exact next grid level (bid proxy).
        Fee: 0.080% × notional + $0.12 flat, each side.
        """
        sz         = contracts * self.cs
        gross      = (exit_p - entry) * sz
        fee_entry  = entry  * sz * FEE_PCT + FEE_FLAT * contracts
        fee_exit   = exit_p * sz * FEE_PCT + FEE_FLAT * contracts
        return round(gross - fee_entry - fee_exit, 4)

    def contracts_for(self, price, bias=1.0):
        """How many contracts to open at this grid level"""
        cpg        = self.state["balance"] / self.n_grids
        margin_ct  = price * self.cs * self.mr
        if margin_ct <= 0:
            return 1
        return max(1, min(10, int(cpg * bias / margin_ct)))

    def check_weekly_reset(self):
        wk = get_week()
        if self.state.get("week") != wk:
            if self.state.get("week"):   # not first run
                self._syslog(f"Weekly reset: week={self.state['week']} "
                             f"pnl=${self.state['weekly_pnl']:+.2f}")
                ntfy(f"S{self.sys_id} {self.label} Weekly",
                     f"Week {self.state['week']}: ${self.state['weekly_pnl']:+.2f}",
                     priority="default")
            self.state["weekly_pnl"] = 0.0
            self.state["week"] = wk

    # ── Main loop ────────────────────────────────────────────────

    def run(self):
        self._syslog(
            f"STARTED | product={self.product_id} "
            f"contract={self.cs} margin={self.mr*100:.2f}% "
            f"spacing=${self.spacing} levels={self.n_grids} "
            f"capital=${self.capital:.2f}")
        last_1h_ts = 0

        while True:
            try:
                bid, ask, mid = self.get_bid_ask()
                if not bid or not ask or not mid:
                    self._syslog("Price fetch failed — retrying in 15s")
                    time.sleep(15)
                    continue

                now = datetime.now(timezone.utc)
                self.state["last_price"] = round(mid, 6)
                self.check_weekly_reset()

                # ── 1hr candle update ─────────────────────────
                ts_now = int(time.time())
                if ts_now - last_1h_ts >= 3600:
                    self.price_history_1h.append(round(mid, 6))
                    if len(self.price_history_1h) > 300:
                        self.price_history_1h.pop(0)
                    self._save_candles()
                    last_1h_ts = ts_now
                    self._syslog(
                        f"1hr candle: price=${mid:.4f} "
                        f"history={len(self.price_history_1h)}/300 "
                        f"bias={self.trend_bias()}x")

                bias = self.trend_bias()

                # ── Initialize grid on first run ──────────────
                if not self.state["grid_center"] or not self.state["grid_levels"]:
                    self.state["grid_center"] = mid
                    self.state["grid_levels"] = self.build_grid(mid)
                    lo = self.state["grid_levels"][0]
                    hi = self.state["grid_levels"][-1]
                    self._syslog(
                        f"GRID INIT: center=${mid:.4f} "
                        f"range=${lo:.4f}-${hi:.4f} "
                        f"levels={self.n_grids}")
                    self._save_state()

                grid_lo = self.state["grid_levels"][0]
                grid_hi = self.state["grid_levels"][-1]

                # ── Breakout: close all open buys, recenter ───
                if mid < grid_lo or mid > grid_hi:
                    direction = "UP" if mid > grid_hi else "DOWN"
                    self._syslog(
                        f"BREAKOUT {direction}: price=${mid:.4f} "
                        f"grid=${grid_lo:.4f}-${grid_hi:.4f} "
                        f"open_buys={len(self.state['open_buys'])}")

                    for lvl_str, entry_p in list(self.state["open_buys"].items()):
                        cts = self.contracts_for(entry_p)
                        pnl = self.calc_pnl(entry_p, mid, cts)
                        mo  = now.strftime("%Y-%m")
                        self.state["total_pnl"]   += pnl
                        self.state["weekly_pnl"]  += pnl
                        self.state["balance"]     += pnl
                        self.state["monthly_pnl"][mo] = round(
                            self.state["monthly_pnl"].get(mo, 0.0) + pnl, 4)
                        fill = {
                            "time":      now.isoformat(),
                            "type":      "BREAKOUT_CLOSE",
                            "direction": direction,
                            "level":     float(lvl_str),
                            "entry":     entry_p,
                            "exit":      round(mid, 6),
                            "cts":       cts,
                            "bias":      bias,
                            "pnl":       pnl,
                            "balance":   round(self.state["balance"], 4),
                        }
                        self._append_fill(fill)
                        self._syslog(
                            f"  CLOSE level=${float(lvl_str):.4f} "
                            f"entry=${entry_p:.4f} exit=${mid:.4f} "
                            f"cts={cts} pnl=${pnl:+.4f}")

                    n_closed = len(self.state["open_buys"])
                    self.state["open_buys"] = {}
                    self.state["total_breakouts"] = \
                        self.state.get("total_breakouts", 0) + 1
                    self.state["grid_center"] = mid
                    self.state["grid_levels"] = self.build_grid(mid)
                    self._syslog(
                        f"RECENTERED: closed={n_closed} positions "
                        f"new_range=${self.state['grid_levels'][0]:.4f}"
                        f"-${self.state['grid_levels'][-1]:.4f} "
                        f"total_pnl=${self.state['total_pnl']:+.2f}")
                    if n_closed:
                        ntfy(f"S{self.sys_id} {self.label} Breakout {direction}",
                             f"Closed {n_closed} positions | "
                             f"Total: ${self.state['total_pnl']:+.2f}",
                             priority="default")
                    self._save_state()
                    time.sleep(5)
                    continue

                # ── Check every grid level ─────────────────────
                for lvl in self.state["grid_levels"]:
                    lvl_key  = str(self._round_lvl(lvl))
                    sell_lvl = self._round_lvl(lvl + self.spacing)

                    # BUY: ask has dropped to or below this level
                    if lvl_key not in self.state["open_buys"]:
                        if ask <= lvl:
                            cts = self.contracts_for(ask, bias)
                            self.state["open_buys"][lvl_key] = round(ask, 6)
                            self._syslog(
                                f"BUY  level=${lvl:.4f} ask=${ask:.4f} "
                                f"cts={cts} bias={bias}x "
                                f"open={len(self.state['open_buys'])}")
                            self._save_state()

                    # SELL: bid has risen to or above the sell level
                    # Exit at EXACT next grid level — not market price
                    if lvl_key in self.state["open_buys"]:
                        if bid >= sell_lvl:
                            entry_p = self.state["open_buys"][lvl_key]
                            cts     = self.contracts_for(entry_p, bias)
                            pnl     = self.calc_pnl(entry_p, sell_lvl, cts)
                            mo      = now.strftime("%Y-%m")

                            self.state["total_pnl"]   += pnl
                            self.state["weekly_pnl"]  += pnl
                            self.state["balance"]     += pnl
                            self.state["total_fills"] += 1
                            self.state["monthly_pnl"][mo] = round(
                                self.state["monthly_pnl"].get(mo, 0.0) + pnl, 4)

                            fill = {
                                "time":       now.isoformat(),
                                "type":       "GRID_FILL",
                                "buy_level":  lvl,
                                "sell_level": sell_lvl,
                                "entry":      entry_p,
                                "exit":       sell_lvl,  # exact grid level
                                "cts":        cts,
                                "bias":       bias,
                                "pnl":        pnl,
                                "balance":    round(self.state["balance"], 4),
                            }
                            self._append_fill(fill)
                            del self.state["open_buys"][lvl_key]

                            self._syslog(
                                f"FILL #{self.state['total_fills']} "
                                f"buy=${entry_p:.4f} sell=${sell_lvl:.4f} "
                                f"cts={cts} bias={bias}x "
                                f"pnl=${pnl:+.4f} "
                                f"total=${self.state['total_pnl']:+.2f} "
                                f"balance=${self.state['balance']:.2f}")

                            ntfy(f"S{self.sys_id} {self.label} Fill #{self.state['total_fills']}",
                                 f"${pnl:+.4f} | Total: ${self.state['total_pnl']:+.2f}",
                                 priority="default")
                            self._save_state()

                # ── Heartbeat ─────────────────────────────────
                self._syslog(
                    f"CYCLE bid=${bid:.4f} ask=${ask:.4f} "
                    f"grid=${grid_lo:.4f}-${grid_hi:.4f} "
                    f"open={len(self.state['open_buys'])} "
                    f"fills={self.state['total_fills']} "
                    f"pnl=${self.state['total_pnl']:+.2f} "
                    f"bal=${self.state['balance']:.2f} "
                    f"bias={bias}x "
                    f"1hr={len(self.price_history_1h)}/300")

                time.sleep(60)

            except Exception as e:
                self.state["loop_errors"] = self.state.get("loop_errors", 0) + 1
                self._syslog(f"LOOP ERROR #{self.state['loop_errors']}: {e}")
                ntfy(f"S{self.sys_id} {self.label} ERROR",
                     str(e), priority="urgent")
                try:
                    self._save_state()
                except:
                    pass
                time.sleep(30)

    # ── Dashboard helpers ─────────────────────────────────────────

    def get_fills(self, last_n=None):
        if os.path.exists(self.fills_file):
            try:
                fills = json.load(open(self.fills_file))
                return fills[-last_n:] if last_n else fills
            except:
                pass
        return []

    def get_log_tail(self, n=50):
        if os.path.exists(self.log_file):
            try:
                return open(self.log_file).readlines()[-n:]
            except:
                pass
        return []


# ══════════════════════════════════════════════════════════════════
# INSTANTIATE — two isolated grid systems
# ══════════════════════════════════════════════════════════════════
G4 = GridSystem(4)   # XRP
G5 = GridSystem(5)   # SOL
GRID_SYSTEMS = [G4, G5]


# ══════════════════════════════════════════════════════════════════
# FLASK APP
# ══════════════════════════════════════════════════════════════════
app = Flask(__name__)

def _auth():
    return request.cookies.get("auth") == APP_PASSWORD

@app.route("/login", methods=["POST"])
def login():
    from flask import make_response
    pw = request.form.get("pw", "")
    if pw == APP_PASSWORD:
        r = make_response(redirect("/"))
        r.set_cookie("auth", APP_PASSWORD, max_age=86400*30,
                     samesite="Lax", httponly=True)
        return r
    return redirect("/")

@app.route("/health")
def health():
    out = {"status": "ok", "time": ts(), "systems": {}}
    for g in GRID_SYSTEMS:
        with g.lock:
            s = g.state
        out["systems"][f"S{g.sys_id}"] = {
            "label":       g.label,
            "total_pnl":   s.get("total_pnl", 0),
            "total_fills": s.get("total_fills", 0),
            "balance":     s.get("balance", 0),
            "last_price":  s.get("last_price"),
            "last_update": s.get("last_update"),
            "loop_errors": s.get("loop_errors", 0),
            "open_buys":   len(s.get("open_buys", {})),
            "candles_1hr": len(g.price_history_1h),
        }
    return Response(json.dumps(out, indent=2), mimetype="application/json")

@app.route("/grid-state-s<int:sid>")
def grid_state(sid):
    if not _auth(): return Response("Unauthorized", status=401)
    g = next((g for g in GRID_SYSTEMS if g.sys_id == sid), None)
    if not g: return Response("Not found", status=404)
    return Response(json.dumps(g.state, indent=2), mimetype="application/json")

@app.route("/grid-fills-s<int:sid>")
def grid_fills(sid):
    if not _auth(): return Response("Unauthorized", status=401)
    g = next((g for g in GRID_SYSTEMS if g.sys_id == sid), None)
    if not g: return Response("Not found", status=404)
    return Response(json.dumps(g.get_fills(), indent=2), mimetype="application/json")

@app.route("/grid-candles-s<int:sid>")
def grid_candles(sid):
    if not _auth(): return Response("Unauthorized", status=401)
    g = next((g for g in GRID_SYSTEMS if g.sys_id == sid), None)
    if not g: return Response("Not found", status=404)
    return Response(json.dumps(g.price_history_1h), mimetype="application/json")

@app.route("/grid-log-s<int:sid>")
def grid_log(sid):
    if not _auth(): return Response("Unauthorized", status=401)
    g = next((g for g in GRID_SYSTEMS if g.sys_id == sid), None)
    if not g: return Response("Not found", status=404)
    return Response("".join(g.get_log_tail(100)), mimetype="text/plain")

# ── Dashboard ──────────────────────────────────────────────────────
LOGIN_PAGE = """<!DOCTYPE html><html><head><title>AP3X 2.0</title>
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
<h2>AP3X 2.0</h2>
<input type=password name=pw placeholder='Password' autofocus>
<button type=submit>Login</button>
</form></body></html>"""

def _sys_card(g):
    with g.lock:
        s = dict(g.state)

    pnl      = s.get("total_pnl", 0.0)
    bal      = s.get("balance", g.capital)
    fills    = s.get("total_fills", 0)
    open_pos = len(s.get("open_buys", {}))
    wk_pnl   = s.get("weekly_pnl", 0.0)
    errors   = s.get("loop_errors", 0)
    breakouts= s.get("total_breakouts", 0)
    candles  = len(g.price_history_1h)
    bias     = g.trend_bias()
    center   = s.get("grid_center") or "—"
    last_p   = s.get("last_price") or "—"

    # Days running + $/day
    try:
        start = datetime.fromisoformat(s["start_time"])
        days  = max((datetime.now(timezone.utc) - start).total_seconds() / 86400, 0.01)
        pd    = round(pnl / days, 2)
    except:
        days = 0.0; pd = 0.0

    pnl_col = "#00D68F" if pnl  >= 0 else "#FF4757"
    wk_col  = "#00D68F" if wk_pnl >= 0 else "#FF4757"

    # Monthly breakdown
    monthly_html = ""
    for mo in sorted(s.get("monthly_pnl", {}).keys()):
        mp  = s["monthly_pnl"][mo]
        mc  = "#00D68F" if mp >= 0 else "#FF4757"
        monthly_html += (
            f"<div style='display:flex;justify-content:space-between;"
            f"padding:5px 0;border-bottom:1px solid #1E2D45;font-size:12px'>"
            f"<span>{mo}</span>"
            f"<span style='color:{mc};font-weight:700'>${mp:+,.2f}</span>"
            f"</div>")
    if not monthly_html:
        monthly_html = "<div style='color:#4A5878;padding:8px;font-size:12px'>No fills yet — waiting for first grid level hit</div>"

    # Recent fills (last 10, newest first)
    fills_html = ""
    for f in g.get_fills(last_n=10)[::-1]:
        fc    = "#00D68F" if f.get("pnl", 0) >= 0 else "#FF4757"
        ftype = f.get("type", "?")
        fills_html += (
            f"<div style='border-left:3px solid {fc};padding:6px 10px;"
            f"margin-bottom:5px;background:#060D1A;border-radius:0 6px 6px 0'>"
            f"<div style='font-size:10px;color:#4A5878'>"
            f"{f.get('time','?')[5:16]} · {ftype}</div>"
            f"<div style='font-size:13px;font-weight:700;color:{fc}'>"
            f"${f.get('pnl',0):+,.4f}</div>"
            f"<div style='font-size:10px;color:#8892A4'>"
            f"entry=${f.get('entry',0):.4f} → "
            f"exit=${f.get('exit',0):.4f} | "
            f"{f.get('cts',1)}ct | bias={f.get('bias',1)}x"
            f"</div></div>")
    if not fills_html:
        fills_html = "<div style='color:#4A5878;padding:8px;font-size:12px'>No fills yet</div>"

    # Log tail
    log_lines = g.get_log_tail(25)
    log_html  = "".join(
        f"<div class=hb-row>{l.strip()}</div>" for l in log_lines
    ) or "<div style='color:#4A5878;padding:8px;font-size:12px'>No log entries yet</div>"

    sid = g.sys_id
    return f"""
<div class='sys-card' style='background:#0A1628;border:2px solid {g.color};
     border-radius:12px;padding:16px;margin-bottom:20px'>
  <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:12px'>
    <div>
      <span style='font-size:16px;font-weight:800;color:{g.color}'>S{sid}</span>
      <span style='font-size:13px;color:#8892A4;margin-left:8px'>{g.label}</span>
    </div>
    <span style='font-size:11px;color:#4A5878;background:#060D1A;
          padding:3px 8px;border-radius:20px'>PAPER · GRID</span>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px'>
    <div style='text-align:center;background:#060D1A;border-radius:8px;padding:8px'>
      <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>BALANCE</div>
      <div style='font-size:15px;font-weight:800'>${bal:,.2f}</div>
    </div>
    <div style='text-align:center;background:#060D1A;border-radius:8px;padding:8px'>
      <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>TOTAL P&amp;L</div>
      <div style='font-size:15px;font-weight:800;color:{pnl_col}'>${pnl:+,.2f}</div>
    </div>
    <div style='text-align:center;background:#060D1A;border-radius:8px;padding:8px'>
      <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>$/DAY</div>
      <div style='font-size:15px;font-weight:800;color:{pnl_col}'>${pd:+,.2f}</div>
    </div>
    <div style='text-align:center;background:#060D1A;border-radius:8px;padding:8px'>
      <div style='font-size:10px;color:#4A5878;margin-bottom:2px'>WEEK P&amp;L</div>
      <div style='font-size:15px;font-weight:800;color:{wk_col}'>${wk_pnl:+,.2f}</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:12px;font-size:11px'>
    <div style='background:#060D1A;border-radius:6px;padding:6px;text-align:center'>
      <div style='color:#4A5878;font-size:10px'>FILLS</div>
      <div style='font-weight:700'>{fills}</div>
    </div>
    <div style='background:#060D1A;border-radius:6px;padding:6px;text-align:center'>
      <div style='color:#4A5878;font-size:10px'>OPEN BUYS</div>
      <div style='font-weight:700;color:#00D68F'>{open_pos}</div>
    </div>
    <div style='background:#060D1A;border-radius:6px;padding:6px;text-align:center'>
      <div style='color:#4A5878;font-size:10px'>BIAS</div>
      <div style='font-weight:700;color:#FFB800'>{bias}x</div>
    </div>
    <div style='background:#060D1A;border-radius:6px;padding:6px;text-align:center'>
      <div style='color:#4A5878;font-size:10px'>ERRORS</div>
      <div style='font-weight:700;color:{"#FF4757" if errors>0 else "#4A5878"}'>{errors}</div>
    </div>
  </div>

  <div class=tabs>
    <span class='tab on' onclick="show('s{sid}mon',this,'s{sid}')">Monthly</span>
    <span class=tab onclick="show('s{sid}fil',this,'s{sid}')">Fills</span>
    <span class=tab onclick="show('s{sid}log',this,'s{sid}')">Log</span>
    <span class=tab onclick="show('s{sid}inf',this,'s{sid}')">Info</span>
  </div>
  <div id='s{sid}mon' class='panel on'>{monthly_html}</div>
  <div id='s{sid}fil' class=panel>{fills_html}</div>
  <div id='s{sid}log' class='panel' style='font-family:monospace;font-size:10px;word-break:break-all'>{log_html}</div>
  <div id='s{sid}inf' class=panel>
    <div style='font-size:12px;line-height:2.2;color:#8892A4'>
      <b style='color:#E0E6F0'>Product</b>: {g.product_id}<br>
      <b style='color:#E0E6F0'>Contract</b>: {g.cs} units · {g.mr*100:.2f}% intraday margin<br>
      <b style='color:#E0E6F0'>Spacing</b>: ${g.spacing}<br>
      <b style='color:#E0E6F0'>Levels</b>: {g.n_grids} ({g.n_grids//2} below + {g.n_grids//2} above)<br>
      <b style='color:#E0E6F0'>Capital</b>: ${g.capital:,.2f}<br>
      <b style='color:#E0E6F0'>Grid center</b>: {center}<br>
      <b style='color:#E0E6F0'>Last price</b>: {last_p}<br>
      <b style='color:#E0E6F0'>Breakouts</b>: {breakouts}<br>
      <b style='color:#E0E6F0'>Days running</b>: {days:.1f}<br>
      <b style='color:#E0E6F0'>1hr candles</b>: {candles}/300<br>
      <b style='color:#E0E6F0'>Fees</b>: 0.080% per side + $0.12/ct/side<br>
      <div style='margin-top:10px;display:flex;gap:12px;flex-wrap:wrap'>
        <a href='/grid-state-s{sid}' style='color:#4A5878;font-size:11px'>State JSON</a>
        <a href='/grid-fills-s{sid}' style='color:#4A5878;font-size:11px'>Fills JSON</a>
        <a href='/grid-candles-s{sid}' style='color:#4A5878;font-size:11px'>Candles JSON</a>
        <a href='/grid-log-s{sid}' style='color:#4A5878;font-size:11px'>Full Log</a>
      </div>
    </div>
  </div>
</div>"""

@app.route("/")
def dashboard():
    if not _auth():
        return LOGIN_PAGE

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Compare both systems
    pnls = []
    for g in GRID_SYSTEMS:
        with g.lock:
            pnls.append((g.label, g.state.get("total_pnl", 0.0)))
    winner_label = max(pnls, key=lambda x: x[1])[0] if pnls else "—"

    cards = "".join(_sys_card(g) for g in GRID_SYSTEMS)

    return f"""<!DOCTYPE html><html><head>
<title>AP3X 2.0</title>
<meta charset=utf-8>
<meta name=viewport content='width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no'>
<meta http-equiv=refresh content=30>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#060D1A;color:#E0E6F0;
       font-family:-apple-system,BlinkMacSystemFont,sans-serif;
       padding:14px;max-width:640px;margin:0 auto;padding-bottom:40px}}
  a{{color:#8892A4;text-decoration:none}}
  .tabs{{display:flex;gap:4px;margin-bottom:0;overflow-x:auto;
         -webkit-overflow-scrolling:touch;scrollbar-width:none}}
  .tabs::-webkit-scrollbar{{display:none}}
  .tab{{flex-shrink:0;padding:10px 14px;cursor:pointer;border-radius:8px 8px 0 0;
        font-size:12px;font-weight:600;background:#060D1A;color:#4A5878;
        border:1px solid #1E2D45;border-bottom:none;min-height:40px;
        display:flex;align-items:center;touch-action:manipulation}}
  .tab.on{{background:#0A1628;color:#E0E6F0}}
  .panel{{display:none;background:#0A1628;border:1px solid #1E2D45;
          border-radius:0 10px 10px 10px;padding:12px;min-height:80px}}
  .panel.on{{display:block}}
  .hb-row{{font-size:10px;padding:4px 0;border-bottom:1px solid #060D1A;
           word-break:break-all;color:#8892A4}}
  .sys-card{{}}
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
    <div style='font-size:22px;font-weight:800'>AP3X <span style='color:#4A5878'>2.0</span></div>
    <div style='font-size:12px;font-weight:700;color:#FFB800;margin-top:2px'>📄 PAPER TRADING</div>
    <div style='font-size:11px;color:#4A5878;margin-top:2px'>Grid Bots · XRP vs SOL</div>
  </div>
  <div style='text-align:right;font-size:11px;color:#4A5878;line-height:1.7'>
    {now_utc}<br>{ts_est()}
  </div>
</div>
<div style='font-size:11px;color:#4A5878;margin-bottom:16px;padding:10px;
     background:#0A1628;border-radius:8px;border:1px solid #1E2D45;
     display:flex;justify-content:space-between'>
  <span>S4 XRP $0.15 grid · S5 SOL $7.00 grid</span>
  <span style='color:#00D68F;font-weight:700'>🏆 {winner_label}</span>
</div>
{cards}
<div style='font-size:11px;color:#4A5878;text-align:center;margin-top:8px'>
  <a href='/health'>Health JSON</a>
  &nbsp;·&nbsp;
  <a href='/grid-fills-s4'>XRP Fills</a>
  &nbsp;·&nbsp;
  <a href='/grid-fills-s5'>SOL Fills</a>
</div>
</body></html>"""


# ══════════════════════════════════════════════════════════════════
# STARTUP — launch grid threads
# ══════════════════════════════════════════════════════════════════
def startup():
    log("🚀 AP3X 2.0 — Grid Trading | XRP + SOL | PAPER MODE")
    log(f"   S4 XRP: {G4.product_id} | ${G4.spacing} spacing | {G4.n_grids} levels | ${G4.capital:,.0f}")
    log(f"   S5 SOL: {G5.product_id} | ${G5.spacing} spacing | {G5.n_grids} levels | ${G5.capital:,.0f}")
    log(f"   Fees: {FEE_PCT*100:.3f}% per side + ${FEE_FLAT}/ct/side")

    # Verify Coinbase connection before starting
    try:
        client = get_cb_client()
        r = client.get_best_bid_ask(product_ids=["XPP-20DEC30-CDE", "SLP-20DEC30-CDE"])
        for pb in r.pricebooks:
            bid = float(pb.bids[0].price) if pb.bids else "?"
            ask = float(pb.asks[0].price) if pb.asks else "?"
            log(f"   ✅ {pb.product_id}: bid={bid} ask={ask}")
    except Exception as e:
        log(f"   ⚠️ Startup price check failed: {e}")
        ntfy("⚠️ AP3X 2.0 Startup Warning",
             f"Price check failed: {e}", priority="high")

    for g in GRID_SYSTEMS:
        t = threading.Thread(
            target=g.run,
            daemon=True,
            name=f"S{g.sys_id}-{g.label}")
        t.start()
        log(f"   ✅ S{g.sys_id} ({g.label}) thread started")
        time.sleep(0.5)

    log("✅ All grid systems running")
    ntfy("✅ AP3X 2.0 Started",
         f"S4 XRP ${G4.spacing} grid | S5 SOL ${G5.spacing} grid | PAPER MODE",
         priority="default")

# Start in background — Railway health check won't timeout
threading.Thread(target=startup, daemon=True, name="startup").start()
