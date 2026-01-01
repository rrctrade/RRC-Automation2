# ============================================================
# RajanTradeAutomation – main.py
# FINAL PRODUCTION VERSION (CANDLE-GUARDED BIAS + FULL LOGS)
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ Missing ENV variables")

# ------------------------------------------------------------
# LOG HELPERS (Render + Google Sheets)
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
# FYERS WS
# ------------------------------------------------------------
from fyers_apiv3.FyersWebsocket import data_ws

# ------------------------------------------------------------
# UNIVERSE
# ------------------------------------------------------------
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted({s for lst in SECTOR_MAP.values() for s in lst})
push_log("SYSTEM", f"Universe loaded | Symbols={len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# SETTINGS
# ------------------------------------------------------------
def fetch_settings():
    try:
        r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
        return r.json().get("settings", {})
    except Exception:
        return {}

SETTINGS = fetch_settings()
BIAS_TIME = SETTINGS.get("BIAS_TIME")  # HH:MM:SS

push_log("SETTINGS", f"BIAS_TIME={BIAS_TIME}")

# ------------------------------------------------------------
# CANDLE ENGINE (PERFECT 5-MIN ONLY)
# ------------------------------------------------------------
CANDLE_INTERVAL = 300

candles = {}
last_cum_vol = {}

ACTIVE_CANDLES = False
FIRST_CANDLE_TS = None

def is_5min_boundary(ts):
    dt = datetime.fromtimestamp(ts)
    return dt.minute % 5 == 0 and dt.second == 0

def update_candle(msg):
    global ACTIVE_CANDLES, FIRST_CANDLE_TS

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or vol is None or ts is None:
        return

    # Wait for first valid boundary
    if not ACTIVE_CANDLES:
        if is_5min_boundary(ts):
            ACTIVE_CANDLES = True
            FIRST_CANDLE_TS = ts
            push_log(
                "CANDLE",
                f"First valid candle boundary @ {datetime.fromtimestamp(ts)}"
            )
        else:
            return  # skip incomplete candle

    start = ts - (ts % CANDLE_INTERVAL)
    c = candles.get(symbol)

    if c is None or c["start"] != start:
        if c:
            prev = last_cum_vol.get(symbol, c["cum_vol"])
            vol5 = c["cum_vol"] - prev
            last_cum_vol[symbol] = c["cum_vol"]
            push_log("CANDLE", f"Closed | {symbol} | V={vol5}")

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
# SECTOR BIAS + UNSUBSCRIBE (CANDLE-GUARDED)
# ------------------------------------------------------------
from sector_engine import run_sector_bias

SELECTION_DONE = False
UNSUB_DONE = False
SELECTED_STOCKS = set()
LOCK = threading.Lock()

def try_run_bias():
    global SELECTION_DONE, SELECTED_STOCKS

    if SELECTION_DONE:
        return

    # 🔒 CRITICAL GUARD
    if not ACTIVE_CANDLES or FIRST_CANDLE_TS is None:
        return

    if not BIAS_TIME:
        return

    now = datetime.now().strftime("%H:%M:%S")
    if now >= BIAS_TIME:
        push_log("BIAS", "Bias check started")
        result = run_sector_bias()
        SELECTED_STOCKS = set(result.get("selected_stocks", []))
        SELECTION_DONE = True
        push_log(
            "BIAS",
            f"Completed | Selected stocks={len(SELECTED_STOCKS)}"
        )

def unsubscribe_others():
    global UNSUB_DONE

    if not SELECTION_DONE or UNSUB_DONE:
        return

    with LOCK:
        if UNSUB_DONE:
            return

        non_selected = set(candles.keys()) - SELECTED_STOCKS
        if non_selected:
            try:
                fyers_ws.unsubscribe(list(non_selected), "SymbolUpdate")
            except Exception:
                pass

            for s in non_selected:
                candles.pop(s, None)
                last_cum_vol.pop(s, None)

            push_log(
                "UNSUB",
                f"Unsubscribed {len(non_selected)} non-selected stocks"
            )

        push_log("LIVE", f"Active stocks={len(SELECTED_STOCKS)}")
        UNSUB_DONE = True

# ------------------------------------------------------------
# WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)
    try_run_bias()
    unsubscribe_others()

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
