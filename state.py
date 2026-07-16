"""
Shared state between strategy.py and dashboard.py
Persisted to hl_state.json so both processes share data
"""
import json, os
from datetime import datetime, timezone

STATE_FILE = "hl_state.json"

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
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except:
            pass
    return dict(DEFAULT)

def save(st):
    try:
        with open(STATE_FILE, "w") as f:
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
        st["weekly_pnl"][wk] = round(
            st["weekly_pnl"].get(wk, 0) + pnl, 4)
    save(st)
