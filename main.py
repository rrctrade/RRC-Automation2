# ============================================================
# RajanTradeAutomation – main.py
# HISTORY + TARGETED WS (STABLE CANDLE ENGINE UNCHANGED)
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
# LOG PUSH (Render + Google Sheet)
# ------------------------------------------------------------
def push_log(level, message):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {message}")
    try:
        requests.post(
            WEBAPP_URL,
            json={
                "action": "pushLog",
                "payload": {"level": level, "message": message}
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
# UNIVERSE (USED ONLY FOR SECTOR LOGIC)
# ------------------------------------------------------------
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted({s for lst in SECTOR_MAP.values() for s in lst})
push_log("SYSTEM", f"Universe loaded | Symbols={len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# SETTINGS (ONLY BIAS_TIME)
# ------------------------------------------------------------
def fetch_settings():
    try:
        r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
        return r.json().get("settings", {})
    except Exception:
        return {}

SETTINGS = fetch_settings()
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")  # HH:MM:SS

push_log("SETTINGS", f"BIAS_TIME={BIAS_TIME_STR}")

# ------------------------------------------------------------
# STABLE CANDLE ENGINE (UNCHANGED)
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

    # ❗ NO NOISE IN LOGS – ONLY COUNT
    CandleCounter.increment()

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
# CANDLE COUNTER (LOG FRIENDLY)
# ------------------------------------------------------------
class CandleCounter:
    total_closed = 0
    last_log_ts = None

    @classmethod
    def increment(cls):
        cls.total_closed += 1
        now = time.time()
        if not cls.last_log_ts or now - cls.last_log_ts > 2:
            push_log(
                "CANDLE",
                f"5-min candle closed | total={cls.total_closed}"
            )
            cls.last_log_ts = now

# ------------------------------------------------------------
# SECTOR BIAS
# ------------------------------------------------------------
from sector_engine import run_sector_bias

SELECTED_STOCKS = set()
BIAS_DONE = False
SUBSCRIBED = False
LOCK = threading.Lock()

def run_bias_and_prepare():
    global SELECTED_STOCKS, BIAS_DONE

    if not BIAS_TIME_STR:
        return

    bias_time = datetime.strptime(BIAS_TIME_STR, "%H:%M:%S").time()

    while True:
        now = datetime.now().time()
        if now >= bias_time and not BIAS_DONE:
            push_log("BIAS", "Bias check started")

            result = run_sector_bias()
            SELECTED_STOCKS = set(result.get("selected_stocks", []))
            BIAS_DONE = True

            push_log(
                "BIAS",
                f"Completed | Selected stocks={len(SELECTED_STOCKS)}"
            )

            prepare_history_and_subscribe()
            break

        time.sleep(1)

# ------------------------------------------------------------
# HISTORY + TARGETED SUBSCRIBE
# ------------------------------------------------------------
from fyers_apiv3 import fyersModel

def prepare_history_and_subscribe():
    global SUBSCRIBED

    if not SELECTED_STOCKS:
        return

    # -------- determine perfect boundary --------
    now = datetime.now()
    boundary = now.replace(second=0, microsecond=0)
    boundary -= timedelta(minutes=boundary.minute % 5)

    # -------- history candles --------
    push_log("HISTORY", "Fetching last 3 candles")

    fyers = fyersModel.FyersModel(
        client_id=FYERS_CLIENT_ID,
        token=FYERS_ACCESS_TOKEN
    )

    for sym in SELECTED_STOCKS:
        payload = {
            "symbol": sym,
            "resolution": "5",
            "date_format": "1",
            "range_from": int((boundary - timedelta(minutes=15)).timestamp()),
            "range_to": int(boundary.timestamp()),
            "cont_flag": "1"
        }
        try:
            fyers.history(payload)
        except Exception:
            pass

    push_log("HISTORY", "History ready")

    # -------- subscribe just before next boundary --------
    target_sub_time = boundary + timedelta(minutes=5) - timedelta(seconds=2)

    while datetime.now() < target_sub_time:
        time.sleep(0.5)

    with LOCK:
        if not SUBSCRIBED:
            fyers_ws.subscribe(
                symbols=list(SELECTED_STOCKS),
                data_type="SymbolUpdate"
            )
            SUBSCRIBED = True
            push_log("WS", f"Subscribed selected stocks={len(SELECTED_STOCKS)}")
            push_log("LIVE", "Live candles started")

# ------------------------------------------------------------
# WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    if msg.get("symbol") not in SELECTED_STOCKS:
        return
    update_candle(msg)

def on_error(msg):
    push_log("ERROR", f"WS ERROR {msg}")

def on_close(msg):
    push_log("WS", "WebSocket closed")

def on_connect():
    push_log("WS", "WebSocket connected")

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
threading.Thread(target=run_bias_and_prepare, daemon=True).start()

# ------------------------------------------------------------
# START FLASK
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
