# ============================================================
# RajanTradeAutomation – main.py
# HISTORY + TARGETED WS VERSION (FINAL LOCKED)
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime, timedelta
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
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
# FLASK (LOCKED)
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok", "mode": "history_targeted"})

@app.route("/callback")
def fyers_callback():
    return jsonify({"status": "callback_received"})

@app.route("/fyers-redirect")
def fyers_redirect():
    auth_code = request.args.get("auth_code") or request.args.get("code")
    return jsonify({"status": "redirect_received", "auth_code": auth_code})

# ------------------------------------------------------------
# SETTINGS (FROM GOOGLE SHEETS VIA WEBAPP)
# ------------------------------------------------------------
def fetch_settings():
    try:
        r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
        return r.json().get("settings", {})
    except Exception:
        return {}

SETTINGS = fetch_settings()
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")  # HH:MM:SS

push_log("SETTINGS", f"Raw SETTINGS={SETTINGS}")
push_log("SETTINGS", f"BIAS_TIME={BIAS_TIME_STR}")

if not BIAS_TIME_STR:
    push_log("ERROR", "BIAS_TIME missing in Settings sheet")
    raise Exception("BIAS_TIME missing")

# ------------------------------------------------------------
# TIME HELPERS
# ------------------------------------------------------------
def parse_today_time(hms: str) -> datetime:
    h, m, s = map(int, hms.split(":"))
    now = datetime.now()
    return now.replace(hour=h, minute=m, second=s, microsecond=0)

def floor_5min(dt: datetime) -> datetime:
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)

# ------------------------------------------------------------
# SECTOR + STOCK SELECTION (NSE → sector_mapping)
# ------------------------------------------------------------
from sector_engine import run_sector_bias
from sector_mapping import SECTOR_MAP

SELECTED_SECTORS = []
SELECTED_STOCKS = []

def run_bias_and_select_stocks():
    global SELECTED_SECTORS, SELECTED_STOCKS

    push_log("BIAS", "Bias check started (NSE)")
    result = run_sector_bias()

    SELECTED_SECTORS = result.get("selected_sectors", [])
    push_log("SECTOR", f"Selected sectors={SELECTED_SECTORS}")

    stocks = set()
    for sec in SELECTED_SECTORS:
        stocks.update(SECTOR_MAP.get(sec, []))

    SELECTED_STOCKS = sorted(stocks)
    push_log("STOCKS", f"Selected stocks={len(SELECTED_STOCKS)}")

# ------------------------------------------------------------
# FYERS HISTORY
# ------------------------------------------------------------
from fyers_apiv3 import fyersModel

def fetch_history(symbol, start_ts, end_ts):
    fy = fyersModel.FyersModel(
        client_id=FYERS_CLIENT_ID,
        token=FYERS_ACCESS_TOKEN,
        log_path=""
    )

    data = {
        "symbol": symbol,
        "resolution": "5",
        "date_format": "1",
        "range_from": str(int(start_ts.timestamp())),
        "range_to": str(int(end_ts.timestamp())),
        "cont_flag": "1"
    }

    res = fy.history(data)
    if res.get("s") == "ok":
        return res.get("candles", [])
    return []

# ------------------------------------------------------------
# WS (TARGETED ONLY)
# ------------------------------------------------------------
from fyers_apiv3.FyersWebsocket import data_ws

WS_CONNECTED = False
WS_SUBSCRIBED = False

def on_message(msg):
    pass  # live candles handled later

def on_error(msg):
    push_log("ERROR", f"WS ERROR {msg}")

def on_close(msg):
    push_log("WS", "WebSocket closed")

def on_connect():
    global WS_CONNECTED
    WS_CONNECTED = True
    push_log("WS", "WebSocket connected")

fyers_ws = data_ws.FyersDataSocket(
    access_token=FYERS_ACCESS_TOKEN,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_connect=on_connect,
    reconnect=True
)

# ------------------------------------------------------------
# MAIN CONTROLLER
# ------------------------------------------------------------
def main_controller():
    global WS_SUBSCRIBED

    bias_time = parse_today_time(BIAS_TIME_STR)
    push_log("SYSTEM", f"Waiting for BIAS_TIME={bias_time.time()}")

    while datetime.now() < bias_time:
        time.sleep(1)

    # 1️⃣ Bias + sector + stocks
    run_bias_and_select_stocks()

    # 2️⃣ History candles (3)
    boundary = floor_5min(bias_time)
    c1_start = boundary - timedelta(minutes=10)
    c2_start = boundary - timedelta(minutes=5)
    c3_start = boundary

    push_log(
        "HISTORY",
        f"Fetching history candles @ {c1_start.time()}, {c2_start.time()}, {c3_start.time()}"
    )

    total_hist = 0
    for sym in SELECTED_STOCKS:
        total_hist += len(fetch_history(sym, c1_start, c1_start + timedelta(minutes=5)))
        total_hist += len(fetch_history(sym, c2_start, c2_start + timedelta(minutes=5)))
        total_hist += len(fetch_history(sym, c3_start, c3_start + timedelta(minutes=5)))

    push_log("HISTORY", f"History candles fetched count={total_hist}")

    # 3️⃣ Subscribe just before live candle
    subscribe_time = c3_start + timedelta(minutes=5) - timedelta(seconds=3)
    push_log("SYSTEM", f"Waiting to subscribe @ {subscribe_time.time()}")

    while datetime.now() < subscribe_time:
        time.sleep(0.5)

    if WS_CONNECTED and not WS_SUBSCRIBED:
        fyers_ws.subscribe(SELECTED_STOCKS, "SymbolUpdate")
        WS_SUBSCRIBED = True
        push_log("WS", f"Subscribed selected stocks={len(SELECTED_STOCKS)}")

# ------------------------------------------------------------
# START THREADS
# ------------------------------------------------------------
threading.Thread(target=lambda: fyers_ws.connect(), daemon=True).start()
threading.Thread(target=main_controller, daemon=True).start()

# ------------------------------------------------------------
# START FLASK
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
