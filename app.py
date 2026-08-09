"""
RAILWAY ENVIRONMENT TEST
Tests every dependency and API before the real app runs.
Push this as app.py first, check logs, then push the real app.
"""
import os, sys, time, json
from flask import Flask, Response
import requests as req

app = Flask(__name__)

@app.route("/")
def test():
    results = {}

    # 1. Python version
    results["python"] = sys.version

    # 2. Required packages
    packages = ["flask","requests","ccxt","websocket"]
    for pkg in packages:
        try:
            __import__(pkg)
            results[f"pkg_{pkg}"] = "✅ installed"
        except ImportError as e:
            results[f"pkg_{pkg}"] = f"❌ MISSING: {e}"

    # 3. HL candles
    try:
        end_ms = int(time.time()*1000)
        start_ms = end_ms - 200*5*60*1000
        r = req.post("https://api.hyperliquid.xyz/info",
            json={"type":"candleSnapshot","req":{
                "coin":"BTC","interval":"5m",
                "startTime":start_ms,"endTime":end_ms
            }}, timeout=15)
        candles = r.json() if r.status_code==200 else []
        results["hl_candles"] = f"✅ {len(candles)} candles" if candles else f"❌ status={r.status_code}"
    except Exception as e:
        results["hl_candles"] = f"❌ {e}"

    # 4. HL WebSocket reachable
    try:
        r = req.get("https://api.hyperliquid.xyz", timeout=10)
        results["hl_ws_host"] = f"✅ reachable status={r.status_code}"
    except Exception as e:
        results["hl_ws_host"] = f"❌ {e}"

    # 5. HL balance via CCXT
    try:
        import ccxt
        dex = ccxt.hyperliquid({
            "walletAddress": "0xa90566c8d886CA63c1194101a7dA2Fa129D26B58",
            "privateKey":    "0xdde0184ae92390a2b14c69d1e6b6f4b49d9f2d6bd2e800388aaa5381fb9a3b1f",
            "timeout":       30000,
        })
        bal = dex.fetch_balance()
        usdc = float(bal["USDC"]["total"] or 0)
        results["hl_balance"] = f"✅ USDC=${usdc:.4f}"
    except Exception as e:
        results["hl_balance"] = f"❌ {e}"

    # 6. Binance US candles (fallback)
    try:
        r = req.get("https://api.binance.us/api/v3/klines",
            params={"symbol":"BTCUSDT","interval":"5m","limit":5},
            timeout=10)
        data = r.json() if r.status_code==200 else []
        results["binance_us_candles"] = f"✅ {len(data)} candles close=${float(data[-1][4]):,.2f}" if data else f"❌ status={r.status_code}"
    except Exception as e:
        results["binance_us_candles"] = f"❌ {e}"

    # 7. ntfy
    try:
        r = req.get("https://ntfy.sh", timeout=5)
        results["ntfy"] = f"✅ reachable status={r.status_code}"
    except Exception as e:
        results["ntfy"] = f"❌ {e}"

    # Summary
    failed = [k for k,v in results.items() if "❌" in str(v)]
    results["SUMMARY"] = f"✅ ALL GOOD" if not failed else f"❌ FAILED: {', '.join(failed)}"

    return Response(json.dumps(results, indent=2), mimetype="application/json")

port = int(os.environ.get("PORT", 8080))
app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
