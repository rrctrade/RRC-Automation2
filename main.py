# ============================================================
# RajanTradeAutomation – main.py
# STABLE CANDLE ENGINE + LOGS SHEET (SAFE VERSION)
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# BASIC LOG
# ------------------------------------------------------------
print("🚀 main.py STARTED")

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ ENV variables missing")

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
            timeout=2
        )
    except Exception:
        pass

push_log("SYSTEM", "main.py booted")

# ------------------------------------------------------------
# FLASK
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
# IMPORT FULL UNIVERSE
# ------------------------------------------------------------
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted(
    {symbol for stocks in SECTOR_MAP.values() for symbol in stocks}
)

push_log("SYSTEM", f"Universe loaded | Symbols={len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# CANDLE ENGINE (ORIGINAL – UNTOUCHED)
# ------------------------------------------------------------
CANDLE_INTERVAL = 300
candles = {}
last_candle_vol = {}

TOTAL_5MIN_CANDLES = 0
CANDLE_LOCK = threading.Lock()

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    global TOTAL_5MIN_CANDLES

    prev = last_candle_vol.get(symbol, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_candle_vol[symbol] = c["cum_vol"]

    # 🔒 ONLY SUMMARY LOG (NO PER-SYMBOL NOISE)
    with CANDLE_LOCK:
        TOTAL_5MIN_CANDLES += 1
        push_log(
            "CANDLE",
            f"5-min candle closed | total_closed={TOTAL_5MIN_CANDLES}"
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
# SELECTION + UNSUBSCRIBE STATE (UNCHANGED)
# ------------------------------------------------------------
SELECTION_DONE = False
UNSUBSCRIBE_DONE = False
SELECTED_STOCKS = set()
UNSUB_LOCK = threading.Lock()

def on_sector_selection_complete(result):
    global SELECTION_DONE, SELECTED_STOCKS
    SELECTED_STOCKS = set(result.get("selected_stocks", []))
    SELECTION_DONE = True
    push_log(
        "BIAS",
        f"Sector selection done | Selected={len(SELECTED_STOCKS)}"
    )

def atomic_unsubscribe_and_delete():
    global UNSUBSCRIBE_DONE

    if not SELECTION_DONE or UNSUBSCRIBE_DONE:
        return

    with UNSUB_LOCK:
        if not SELECTION_DONE or UNSUBSCRIBE_DONE:
            return

        non_selected = set(candles.keys()) - SELECTED_STOCKS

        if not non_selected:
            UNSUBSCRIBE_DONE = True
            return

        try:
            fyers_ws.unsubscribe(
                symbols=list(non_selected),
                data_type="SymbolUpdate"
            )
        except Exception:
            pass

        for s in non_selected:
            candles.pop(s, None)
            last_candle_vol.pop(s, None)

        UNSUBSCRIBE_DONE = True
        push_log(
            "UNSUB",
            f"Unsubscribed & deleted {len(non_selected)} stocks"
        )

# ------------------------------------------------------------
# WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)
    atomic_unsubscribe_and_delete()

def on_error(msg):
    push_log("ERROR", "WS ERROR")

def on_close(msg):
    push_log("WS", "WebSocket closed")

def on_connect():
    global fyers_ws
    push_log("WS", "WebSocket connected")
    fyers_ws.subscribe(
        symbols=ALL_SYMBOLS,
        data_type="SymbolUpdate"
    )
    push_log("WS", f"Subscribed {len(ALL_SYMBOLS)} symbols")

# ------------------------------------------------------------
# WS THREAD
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
# SECTOR ENGINE @ 09:25 (UNCHANGED)
# ------------------------------------------------------------
from sector_engine import run_sector_bias

def sector_engine_runner():
    while True:
        now = datetime.now().strftime("%H:%M:%S")
        if now >= "09:25:05" and not SELECTION_DONE:
            push_log("BIAS", "Sector bias check started")
            result = run_sector_bias()
            on_sector_selection_complete(result)
            break
        time.sleep(1)

threading.Thread(target=sector_engine_runner, daemon=True).start()

# ------------------------------------------------------------
# START FLASK
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
