# ============================================================
# RajanTradeAutomation – main.py (FINAL – ALIGNED CANDLE + SETTINGS BIAS)
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify, request

print("🚀 main.py STARTED")

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ Missing ENV variables")

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

ALL_SYMBOLS = sorted(
    {s for lst in SECTOR_MAP.values() for s in lst}
)

print(f"📦 Total symbols: {len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# SETTINGS (ONLY BIAS_TIME USED)
# ------------------------------------------------------------
def fetch_settings():
    try:
        r = requests.post(
            WEBAPP_URL,
            json={"action": "getSettings"},
            timeout=5
        )
        return r.json().get("settings", {})
    except Exception:
        return {}

SETTINGS = fetch_settings()
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")  # HH:MM:SS

print("⚙️ BIAS_TIME from Settings:", BIAS_TIME_STR)

# ------------------------------------------------------------
# CANDLE ENGINE (PERFECT 5-MIN ALIGNMENT)
# ------------------------------------------------------------
CANDLE_INTERVAL = 300
candles = {}
last_cum_vol = {}

ACTIVE_CANDLES = False
FIRST_CANDLE_START = None

def is_boundary(ts):
    dt = datetime.fromtimestamp(ts)
    return dt.minute % 5 == 0 and dt.second == 0

def update_candle(msg):
    global ACTIVE_CANDLES, FIRST_CANDLE_START

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or vol is None or ts is None:
        return

    if not ACTIVE_CANDLES:
        if is_boundary(ts):
            ACTIVE_CANDLES = True
            FIRST_CANDLE_START = ts
            print(f"🟢 First valid candle boundary @ {datetime.fromtimestamp(ts)}")
        else:
            return  # skip incomplete candle

    start = ts - (ts % CANDLE_INTERVAL)
    c = candles.get(symbol)

    if c is None or c["start"] != start:
        if c:
            prev = last_cum_vol.get(symbol, c["cum_vol"])
            vol5 = c["cum_vol"] - prev
            last_cum_vol[symbol] = c["cum_vol"]
            print(f"🟩 5m CANDLE | {symbol} V:{vol5}")

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
# SECTOR SELECTION + UNSUBSCRIBE
# ------------------------------------------------------------
from sector_engine import run_sector_bias

SELECTION_DONE = False
UNSUB_DONE = False
SELECTED_STOCKS = set()
LOCK = threading.Lock()

def run_bias_once():
    global SELECTION_DONE, SELECTED_STOCKS

    if SELECTION_DONE:
        return

    now = datetime.now().strftime("%H:%M:%S")
    if BIAS_TIME_STR and now >= BIAS_TIME_STR:
        print("🧠 Running sector bias…")
        res = run_sector_bias()
        SELECTED_STOCKS = set(res.get("selected_stocks", []))
        SELECTION_DONE = True
        print(f"✅ Bias done | Selected stocks: {len(SELECTED_STOCKS)}")

def unsubscribe_non_selected():
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

            print(f"✂️ Unsubscribed {len(non_selected)} stocks")

        UNSUB_DONE = True

# ------------------------------------------------------------
# WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)
    run_bias_once()
    unsubscribe_non_selected()

def on_error(msg):
    print("❌ WS ERROR", msg)

def on_close(msg):
    print("🔌 WS CLOSED")

def on_connect():
    global fyers_ws
    print("🔗 WS CONNECTED")
    fyers_ws.subscribe(ALL_SYMBOLS, "SymbolUpdate")

# ------------------------------------------------------------
# WS START
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
# FLASK START
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
