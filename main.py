# ============================================================
# RajanTradeAutomation - main.py
# FINAL COMPLETE VERSION
# - FYERS Redirect URI included
# - Settings based Tick + Bias time
# - Proven 5-min Candle Engine
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
import pytz
from flask import Flask, request, jsonify
from fyers_apiv3.FyersWebsocket import data_ws

from sector_mapping import SECTOR_MAP
from sector_engine import run_sector_bias

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

IST = pytz.timezone("Asia/Kolkata")
CANDLE_INTERVAL = 300  # 5 minutes

# ------------------------------------------------------------
# FLASK APP
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

# REQUIRED FOR FYERS APP
@app.route("/callback")
def fyers_callback():
    return jsonify({"status": "callback_received"})

@app.route("/fyers-redirect")
def fyers_redirect():
    auth_code = request.args.get("auth_code") or request.args.get("code")
    return jsonify({
        "status": "redirect_received",
        "auth_code": auth_code
    })

# ------------------------------------------------------------
# SETTINGS
# ------------------------------------------------------------
def load_settings():
    try:
        r = requests.post(
            WEBAPP_URL,
            json={"action": "getSettings"},
            timeout=5
        )
        return r.json().get("settings", {})
    except Exception as e:
        print("SETTINGS_LOAD_ERROR", e)
        return {}

def time_to_sec(t):
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s

# ------------------------------------------------------------
# CANDLE ENGINE (PROVEN)
# ------------------------------------------------------------
candles = {}
last_cum_vol = {}

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    prev = last_cum_vol.get(symbol, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_cum_vol[symbol] = c["cum_vol"]

    print(
        f"5M_CANDLE | {symbol} | "
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
# FYERS WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)

def on_open():
    print("WS_CONNECTED")

def on_error(msg):
    print("WS_ERROR", msg)

def on_close(msg):
    print("WS_CLOSED")

# ------------------------------------------------------------
# START WEBSOCKET
# ------------------------------------------------------------
def start_ws():
    ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        reconnect=True
    )

    ws.on_connect = on_open
    ws.on_message = on_message
    ws.on_error = on_error
    ws.on_close = on_close

    symbols = set()
    for v in SECTOR_MAP.values():
        symbols.update(v)

    ws.subscribe(symbols=list(symbols), data_type="SymbolUpdate")
    ws.keep_running()

# ------------------------------------------------------------
# TICK CONTROLLER (SETTINGS BASED)
# ------------------------------------------------------------
def tick_controller(settings):
    tick_time = time_to_sec(settings.get("TICK_START_TIME", "09:15:00"))

    while True:
        now = datetime.now(IST)
        now_sec = now.hour * 3600 + now.minute * 60 + now.second

        if now_sec >= tick_time:
            print("TICK_ENGINE_STARTED")
            start_ws()
            break

        print("WAITING_FOR_TICK_START =", settings["TICK_START_TIME"])
        time.sleep(1)

# ------------------------------------------------------------
# BIAS CONTROLLER
# ------------------------------------------------------------
def bias_controller(settings):
    bias_time = time_to_sec(settings.get("BIAS_TIME", "09:25:05"))

    while True:
        now = datetime.now(IST)
        now_sec = now.hour * 3600 + now.minute * 60 + now.second

        if now_sec >= bias_time:
            print("BIAS_TIME_REACHED")
            run_sector_bias()
            break

        time.sleep(1)

# ------------------------------------------------------------
# BOOT
# ------------------------------------------------------------
if __name__ == "__main__":
    settings = load_settings()

    threading.Thread(
        target=tick_controller,
        args=(settings,),
        daemon=True
    ).start()

    threading.Thread(
        target=bias_controller,
        args=(settings,),
        daemon=True
    ).start()

    app.run(host="0.0.0.0", port=10000)
