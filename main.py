# ============================================================
# RajanTradeAutomation – main.py
# FULL UNIVERSE + SETTINGS DRIVEN (STABLE)
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime, time as dtime
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ ENV missing")

# ------------------------------------------------------------
# LOG HELPERS (Render + Google Sheet)
# ------------------------------------------------------------
def log(msg):
    print(msg)
    try:
        requests.post(
            WEBAPP_URL,
            json={
                "action": "pushLog",
                "payload": {
                    "level": "INFO",
                    "message": msg
                }
            },
            timeout=3
        )
    except Exception:
        pass

log("SYSTEM_STARTED")

# ------------------------------------------------------------
# LOAD SETTINGS FROM SHEET
# ------------------------------------------------------------
def load_settings():
    res = requests.post(
        WEBAPP_URL,
        json={"action": "getSettings"},
        timeout=10
    ).json()

    s = res.get("settings", {})

    tick_start = datetime.strptime(s["TICK_START_TIME"], "%H:%M:%S").time()
    bias_time = datetime.strptime(s["BIAS_TIME"], "%H:%M:%S").time()

    log(f"SETTINGS_LOADED | TICK_START={tick_start} | BIAS_TIME={bias_time}")
    return tick_start, bias_time

TICK_START_TIME, BIAS_TIME = load_settings()

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
# UNIVERSE
# ------------------------------------------------------------
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted(
    {sym for stocks in SECTOR_MAP.values() for sym in stocks}
)

log(f"UNIVERSE_READY | symbols={len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# CANDLE ENGINE (5 MIN)
# ------------------------------------------------------------
CANDLE_INTERVAL = 300
candles = {}
last_cum_vol = {}

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    prev = last_cum_vol.get(symbol, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_cum_vol[symbol] = c["cum_vol"]

    log(
        f"CANDLE_5M | {symbol} | "
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

    now = datetime.now().time()
    if now < TICK_START_TIME:
        return  # IGNORE ticks before tick start

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
# SECTOR SELECTION + UNSUBSCRIBE
# ------------------------------------------------------------
from sector_engine import run_sector_bias

SELECTION_DONE = False
UNSUB_DONE = False
SELECTED = set()
LOCK = threading.Lock()

def do_unsubscribe():
    global UNSUB_DONE
    if UNSUB_DONE or not SELECTION_DONE:
        return

    with LOCK:
        if UNSUB_DONE:
            return

        non_selected = set(candles.keys()) - SELECTED
        if non_selected:
            try:
                fyers_ws.unsubscribe(
                    symbols=list(non_selected),
                    data_type="SymbolUpdate"
                )
            except Exception:
                pass

            for s in non_selected:
                candles.pop(s, None)
                last_cum_vol.pop(s, None)

            log(f"UNSUBSCRIBED_STOCKS | count={len(non_selected)}")

        UNSUB_DONE = True

def sector_runner():
    global SELECTION_DONE, SELECTED

    while True:
        if datetime.now().time() >= BIAS_TIME and not SELECTION_DONE:
            log("BIAS_TIME_REACHED")
            result = run_sector_bias()
            SELECTED = set(result.get("selected_stocks", []))
            SELECTION_DONE = True
            log(f"SECTOR_SELECTED | stocks={len(SELECTED)}")
            break
        time.sleep(1)

threading.Thread(target=sector_runner, daemon=True).start()

# ------------------------------------------------------------
# WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)
    do_unsubscribe()

def on_connect():
    log("WS_CONNECTED")
    fyers_ws.subscribe(
        symbols=ALL_SYMBOLS,
        data_type="SymbolUpdate"
    )
    log("TICK_ENGINE_STARTED")

def on_error(msg):
    log("WS_ERROR")

def on_close(msg):
    log("WS_CLOSED")

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
