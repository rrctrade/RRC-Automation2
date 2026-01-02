# ============================================================
# RajanTradeAutomation – main.py
# STABLE VERSION + LOGS SHEET + SETTINGS READ
# (NO candle / WS logic change)
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# ENV (AS-IS)
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ Missing ENV variables")

# ------------------------------------------------------------
# LOG HELPER (Render + Google Sheet)
# ------------------------------------------------------------
def push_log(level, message):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {message}")
    try:
        requests.post(
            WEBAPP_URL,
            json={
                "action": "pushLog",
                "payload": {
                    "level": level,
                    "message": message
                }
            },
            timeout=3
        )
    except Exception:
        pass

push_log("SYSTEM", "main.py booted")

# ------------------------------------------------------------
# FLASK (DO NOT TOUCH)
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/callback")
def fyers_callback():
    return jsonify({"status": "callback_received"})

@app.route("/fyers-redirect")
def fyers_redirect():
    auth_code = request.args.get("auth_code") or request.args.get("code")
    return jsonify({"status": "redirect_received", "auth_code": auth_code})

# ------------------------------------------------------------
# SETTINGS READ (FIXED)
# ------------------------------------------------------------
def fetch_settings():
    try:
        r = requests.post(
            WEBAPP_URL,
            json={"action": "getSettings"},
            timeout=5
        )
        data = r.json()
        if not data.get("ok"):
            push_log("ERROR", f"Settings fetch failed: {data}")
            return {}
        return data.get("settings", {})
    except Exception as e:
        push_log("ERROR", f"Settings fetch exception: {e}")
        return {}

SETTINGS = fetch_settings()
push_log("SETTINGS", f"Raw SETTINGS={SETTINGS}")

BIAS_TIME = SETTINGS.get("BIAS_TIME")
push_log("SETTINGS", f"BIAS_TIME={BIAS_TIME}")

# ------------------------------------------------------------
# FYERS WS
# ------------------------------------------------------------
from fyers_apiv3.FyersWebsocket import data_ws

# ------------------------------------------------------------
# UNIVERSE (AS-IS)
# ------------------------------------------------------------
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted({s for lst in SECTOR_MAP.values() for s in lst})
push_log("SYSTEM", f"Universe loaded | Symbols={len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# CANDLE ENGINE (100% OLD STABLE – UNCHANGED)
# ------------------------------------------------------------
CANDLE_INTERVAL = 300
candles = {}
last_candle_vol = {}

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    prev = last_candle_vol.get(symbol, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_candle_vol[symbol] = c["cum_vol"]
    print(
        f"5m CANDLE | {symbol} | "
        f"O:{c['open']} H:{c['high']} "
        f"L:{c['low']} C:{c['close']} V:{vol}"
    )

def update_candle(msg):
    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or vol is None or ts is None:
        return

    start = candle_start(ts)
    c = candles.get(symbol)

    if c is None or c["start"] != start:
        if c:
            close_candle(symbol, c)
        candles[symbol] = {
            "start": start,
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "cum_vol": vol
        }
        return

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["cum_vol"] = vol

# ------------------------------------------------------------
# WS CALLBACKS (AS-IS)
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)

def on_error(msg):
    push_log("ERROR", f"WS ERROR {msg}")

def on_close(msg):
    push_log("WS", "WebSocket closed")

def on_connect():
    push_log("WS", "WebSocket connected")
    fyers_ws.subscribe(ALL_SYMBOLS, "SymbolUpdate")
    push_log("WS", f"Subscribed {len(ALL_SYMBOLS)} symbols")

# ------------------------------------------------------------
# START WS
# ------------------------------------------------------------
def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_connect=on_connect,
        reconnect=True
    )
    fyers_ws.connect()

threading.Thread(target=start_ws, daemon=True).start()

# ------------------------------------------------------------
# START FLASK
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
