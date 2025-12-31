# ============================================================
# RajanTradeAutomation – main.py (SETTINGS DRIVEN + LOGGED)
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify, request

print("🚀 main.py STARTED")

# ---------------- ENV ----------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ ENV missing")

# ---------------- LOGGING ----------------
SETTINGS = {}
TICK_ENGINE_ACTIVE = False
WAIT_LOGGED = False

def post_log(msg, level="INFO"):
    try:
        requests.post(
            WEBAPP_URL,
            json={
                "action": "pushLog",
                "payload": {
                    "rows": [[
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        level,
                        msg
                    ]]
                }
            },
            timeout=3
        )
    except Exception:
        pass

def load_settings():
    global SETTINGS
    r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5).json()
    SETTINGS = r.get("settings", {})
    post_log("SETTINGS_LOADED")

# ---------------- FLASK ----------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/callback")
def callback():
    return jsonify({"status": "callback"})

@app.route("/fyers-redirect")
def redirect_uri():
    return jsonify({"status": "redirect"})

# ---------------- LOAD SETTINGS ----------------
load_settings()
post_log("SYSTEM_STARTED")

# ---------------- FYERS WS ----------------
from fyers_apiv3.FyersWebsocket import data_ws
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})
print("Subscribed symbols:", len(ALL_SYMBOLS))

CANDLE_INTERVAL = 300
candles = {}
last_candle_vol = {}

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    prev = last_candle_vol.get(symbol, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_candle_vol[symbol] = c["cum_vol"]
    post_log(f"5M_CANDLE_CLOSED | {symbol} | VOL={vol}")

def update_candle(msg):
    global TICK_ENGINE_ACTIVE, WAIT_LOGGED

    now = datetime.now().strftime("%H:%M:%S")
    tick_start = SETTINGS.get("TICK_START_TIME")

    if tick_start and now < tick_start:
        if not WAIT_LOGGED:
            post_log(f"WAITING_FOR_TICK_START = {tick_start}")
            WAIT_LOGGED = True
        return

    if not TICK_ENGINE_ACTIVE:
        post_log("TICK_ENGINE_ACTIVATED")
        TICK_ENGINE_ACTIVE = True

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

# ---------------- SECTOR ENGINE ----------------
from sector_engine import run_sector_bias

SELECTION_DONE = False
UNSUBSCRIBE_DONE = False
SELECTED_STOCKS = set()
LOCK = threading.Lock()

def sector_engine_runner():
    bias_time = SETTINGS.get("BIAS_TIME", "09:25:05")
    while True:
        now = datetime.now().strftime("%H:%M:%S")
        if now >= bias_time and not SELECTION_DONE:
            post_log(f"SECTOR_ENGINE_TRIGGERED @ {bias_time}")
            result = run_sector_bias()
            SELECTED_STOCKS.update(result.get("selected_stocks", []))
            post_log(f"SELECTED_STOCKS = {len(SELECTED_STOCKS)}")
            unsubscribe_non_selected()
            break
        time.sleep(1)

def unsubscribe_non_selected():
    global UNSUBSCRIBE_DONE
    with LOCK:
        if UNSUBSCRIBE_DONE:
            return
        non_selected = set(candles.keys()) - SELECTED_STOCKS
        fyers_ws.unsubscribe(list(non_selected), "SymbolUpdate")
        for s in non_selected:
            candles.pop(s, None)
            last_candle_vol.pop(s, None)
        UNSUBSCRIBE_DONE = True
        post_log(f"UNSUBSCRIBED {len(non_selected)} STOCKS")

# ---------------- WS CALLBACKS ----------------
def on_message(msg):
    update_candle(msg)

def on_connect():
    global fyers_ws
    post_log("WS_CONNECTED")
    fyers_ws.subscribe(ALL_SYMBOLS, "SymbolUpdate")

def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_message,
        on_connect=on_connect,
        reconnect=True
    )
    fyers_ws.connect()

threading.Thread(target=start_ws, daemon=True).start()
threading.Thread(target=sector_engine_runner, daemon=True).start()

# ---------------- FLASK RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
