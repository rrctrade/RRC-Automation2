# ============================================================
# RajanTradeAutomation – main.py
# FINAL : HISTORY + TARGETED WS VERSION
# ============================================================

import os
import time
import threading
from datetime import datetime, timedelta

from flask import Flask, jsonify, request
import requests

from fyers_apiv3.FyersWebsocket import data_ws
from fyers_apiv3 import fyersModel

from sector_engine import run_sector_bias

# ============================================================
# ENV
# ============================================================
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ Missing ENV variables")

# ============================================================
# LOGGING → Google Sheets
# ============================================================
def log(level, message):
    try:
        requests.post(WEBAPP_URL, json={
            "action": "pushLog",
            "level": level,
            "message": message
        }, timeout=5)
    except Exception:
        pass
    print(f"[{level}] {message}")

# ============================================================
# FLASK
# ============================================================
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    return jsonify({"status": "redirect_ok"})

# ============================================================
# SETTINGS (FROM SHEET)
# ============================================================
def read_settings():
    res = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=10)
    data = res.json()
    return data

SETTINGS = read_settings()
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")

log("SETTINGS", f"BIAS_TIME={BIAS_TIME_STR}")

BIAS_TIME = datetime.strptime(BIAS_TIME_STR, "%H:%M:%S").time()

# ============================================================
# HISTORY API
# ============================================================
fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

def fetch_history(symbol, start_ts, end_ts):
    data = {
        "symbol": symbol,
        "resolution": "5",
        "date_format": "1",
        "range_from": str(start_ts),
        "range_to": str(end_ts),
        "cont_flag": "1"
    }
    r = fyers.history(data)
    return r.get("candles", [])

# ============================================================
# TIME HELPERS
# ============================================================
def floor_5min(dt):
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)

# ============================================================
# WS STATE
# ============================================================
SELECTED_STOCKS = []
WS_CONNECTED = False

# ============================================================
# WS CALLBACKS
# ============================================================
def on_message(msg):
    pass  # live candle engine will handle later

def on_connect():
    global WS_CONNECTED
    WS_CONNECTED = True
    log("WS", "WebSocket connected")

def on_error(msg):
    log("WS", f"Error: {msg}")

def on_close(msg):
    log("WS", "WebSocket closed")

# ============================================================
# WS INIT (NO AUTO SUBSCRIBE)
# ============================================================
fyers_ws = data_ws.FyersDataSocket(
    access_token=FYERS_ACCESS_TOKEN,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_connect=on_connect,
    reconnect=True
)

threading.Thread(target=fyers_ws.connect, daemon=True).start()

# ============================================================
# MAIN CONTROLLER
# ============================================================
def controller():
    global SELECTED_STOCKS

    log("SYSTEM", "Waiting for BIAS time")

    while True:
        now = datetime.now()
        if now.time() >= BIAS_TIME:
            break
        time.sleep(1)

    # ---------------- BIAS ----------------
    log("BIAS", "Running sector bias engine")
    bias_result = run_sector_bias()

    SELECTED_STOCKS = bias_result["selected_stocks"]

    log("BIAS", f"Sectors={len(bias_result['strong_sectors'])}")
    log("BIAS", f"Selected stocks={len(SELECTED_STOCKS)}")

    if not SELECTED_STOCKS:
        log("BIAS", "No stocks selected. Exit.")
        return

    # ---------------- HISTORY ----------------
    bias_dt = datetime.now()
    base = floor_5min(bias_dt)

    c1_start = base - timedelta(minutes=10)
    c2_start = base - timedelta(minutes=5)
    c3_start = base

    log("HISTORY", f"Candle-1 {c1_start.time()}-{(c1_start+timedelta(minutes=5)).time()}")
    log("HISTORY", f"Candle-2 {c2_start.time()}-{(c2_start+timedelta(minutes=5)).time()}")
    log("HISTORY", f"Candle-3 {c3_start.time()}-{(c3_start+timedelta(minutes=5)).time()}")

    for sym in SELECTED_STOCKS:
        fetch_history(sym, int(c1_start.timestamp()), int((c1_start+timedelta(minutes=5)).timestamp()))
        fetch_history(sym, int(c2_start.timestamp()), int((c2_start+timedelta(minutes=5)).timestamp()))
        fetch_history(sym, int(c3_start.timestamp()), int((c3_start+timedelta(minutes=5)).timestamp()))

    log("HISTORY", "3 candles loaded from history")

    # ---------------- SUBSCRIBE ----------------
    subscribe_time = c3_start + timedelta(minutes=5) - timedelta(seconds=3)
    wait = max(0, (subscribe_time - datetime.now()).total_seconds())
    time.sleep(wait)

    fyers_ws.subscribe(
        symbols=SELECTED_STOCKS,
        data_type="SymbolUpdate"
    )

    log("WS", f"Subscribed {len(SELECTED_STOCKS)} stocks (LIVE candles start)")

# ============================================================
# START
# ============================================================
log("SYSTEM", "main.py booted")
threading.Thread(target=controller, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
