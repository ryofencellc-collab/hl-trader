"""
Shared state between strategy.py and dashboard.py
Uses a simple JSON file — works on Railway with persistent volume
or falls back to in-memory for the dashboard to read via /api/state
"""
import json, os, time
from datetime import datetime, timezone

STATE_FILE = "/tmp/hl_state.json"  # /tmp is shared between processes on Railway

DEFAULT = {
    "status":         "starting",
    "last_check":     None,
    "next_check":     None,
    "cycle":          0,
    "dry_run":        True,
    "testnet":        True,
    "leverage":       3,
    "assets":         ["BTC", "ETH", "SOL", "BNB"],
    "balance":        998.93,
    "positions":      {},
    "trades":         [],
    "diagnostics":    [],
    "weekly_pnl":     {},
    "health": {
        "api_connected":    False,
        "last_ping":        None,
        "assets_ok":        {},
        "params": {
            "ema":          "5/13/34",
            "stop_pct":     "5%",
            "trail_pct":    "1%",
            "vol_filter":   "1.5x",
            "sep_filter":   "0.003",
            "brk_bars":     "12",
            "candle_tf":    "15m",
            "check_every":  "60s",
            "leverage":     "3x",
            "assets":       "BTC,ETH,SOL,BNB",
        }
    },
    "tax": {
        "total_pnl":      0.0,
        "total_tax":      0.0,
        "total_net":      0.0,
        "winning_trades": 0,
        "losing_trades":  0,
        "total_trades":   0,
    },
}

def load():
    for path in [STATE_FILE, "hl_state.json"]:
        if os.path.exists(path):
            try:
                with open(path) as f:
                    data = json.load(f)
                    # Merge with defaults to handle new fields
                    merged = dict(DEFAULT)
                    merged.update(data)
                    return merged
            except:
                pass
    return dict(DEFAULT)

def save(st):
    for path in [STATE_FILE, "hl_state.json"]:
        try:
            with open(path, "w") as f:
                json.dump(st, f, indent=2, default=str)
        except:
            pass

def add_diagnostic(st, level, event, cause, action):
    entry = {
        "time":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "level":  level,
        "event":  event,
        "cause":  cause,
        "action": action,
    }
    st["diagnostics"].insert(0, entry)
    st["diagnostics"] = st["diagnostics"][:200]
    save(st)
    return entry

def add_trade(st, asset, action, direction, entry, exit_p, size, lev, pnl, reason):
    trade = {
        "time":      datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "asset":     asset,
        "action":    action,
        "direction": direction,
        "entry":     entry,
        "exit":      exit_p,
        "size":      size,
        "leverage":  lev,
        "pnl":       round(pnl, 4) if pnl is not None else None,
        "reason":    reason,
    }
    st["trades"].insert(0, trade)
    st["trades"] = st["trades"][:500]
    if pnl is not None:
        wk = datetime.now(timezone.utc).strftime("%Y-W%W")
        st["weekly_pnl"][wk] = round(st["weekly_pnl"].get(wk, 0) + pnl, 4)
    save(st)
