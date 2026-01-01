# ============================================================
# RajanTradeAutomation – main.py (FINAL STABLE – SETTINGS SAFE)
# ============================================================

import os
import time
import json
import threading
import requests
from datetime import datetime, date
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# BASIC
# ------------------------------------------------------------
print("🚀 main.py STARTED")

IST_TZ = "Asia/Kolkata"

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")  # GAS WebApp URL

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ Missing ENV variables")

# ------------------------------------------------------------
# FLASK
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    return jsonify({
        "status": "redirect_received",
        "auth_code": request.args.get("auth_code") or request.args.get("code")
    })

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def post_to_webapp(action, payload):
    try:
        requests.post(
            WEBAPP_URL,
            json={"action": action, "payload": payload},
            timeout=5
        )
    except Exception:
        pass

def log(msg):
    print(msg)
    post_to_webapp("pushLog", {
        "rows": [[datetime.now(), "INFO", msg]]
    })

# ------------------------------------------------------------
# SETTINGS (FROM SHEET)
# ------------------------------------------------------------
def load_settings():
    res = requests.post(
        WEBAPP_URL,
        json={"action": "getSettings"},
        timeout=10
    ).json()
    return res.get("settings", {})

SETTINGS = load_settings()

TICK_START_STR = SETTINGS.get("TICK_START_TIME", "09:15:00")
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME", "09:25:05")

today = date.today().strftime("%Y-%m-%d")

TICK_START_EPOCH = int(
    datetime.strptime(
        f"{today} {TICK_START_STR}", "%Y-%m-%d %H:%M:%S"
    ).timestamp()
)

BIAS_TIME_EPOCH = int(
    datetime.strptime(
        f"{today} {BIAS_TIME_STR}", "%Y-%m-%d %H:%M:%S"
    ).timestamp()
)

log(f"SETTINGS_LOADED | TICK_START={TICK_START_STR} | BIAS_TIME={BIAS_TIME_STR}")

# ------------------------------------------------------------
# FYERS WS
# ------------------------------------------------------------
from fyers_apiv3.FyersWebsocket import data_ws
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted(
    {s for v in SECTOR_MAP.values() for s in v}
)

log(f"UNIVERSE_READY | symbols={len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# CANDLE ENGINE (LOCKED – SAME AS OLD)
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
        f"5M_CANDLE | {symbol} | "
        f"O:{c['open']} H:{c['high']} "
        f"L:{c['low']} C:{c['close']} V:{vol}"
    )

def update_candle(msg):
    ts = msg.get("exch_feed_time")

    # 🔒 ONLY GATING POINT (FIXED)
    if ts is None or ts < TICK_START_EPOCH:
        return

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")

    if not symbol or ltp is None or vol is None:
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
# SECTOR SELECTION + UNSUBSCRIBE
# ------------------------------------------------------------
from sector_engine import run_sector_bias

SELECTION_DONE = False
UNSUBSCRIBE_DONE = False
SELECTED = set()
LOCK = threading.Lock()

def apply_selection(result):
    global SELECTION_DONE, SELECTED
    SELECTED = set(result.get("selected_stocks", []))
    SELECTION_DONE = True
    log(f"SECTOR_SELECTED | stocks={len(SELECTED)}")

def unsubscribe_rest():
    global UNSUBSCRIBE_DONE

    if not SELECTION_DONE or UNSUBSCRIBE_DONE:
        return

    with LOCK:
        if UNSUBSCRIBE_DONE:
            return

        drop = set(candles.keys()) - SELECTED

        if drop:
            try:
                fyers_ws.unsubscribe(list(drop), "SymbolUpdate")
            except Exception:
                pass

            for s in drop:
                candles.pop(s, None)
                last_cum_vol.pop(s, None)

            log(f"UNSUBSCRIBED | removed={len(drop)}")

        UNSUBSCRIBE_DONE = True

# ------------------------------------------------------------
# WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)
    unsubscribe_rest()

def on_connect():
    log("WS_CONNECTED")
    fyers_ws.subscribe(ALL_SYMBOLS, "SymbolUpdate")

def on_error(msg):
    log("WS_ERROR")

def on_close(msg):
    log("WS_CLOSED")

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
# BIAS TIMER
# ------------------------------------------------------------
def bias_timer():
    while True:
        now = int(time.time())
        if now >= BIAS_TIME_EPOCH and not SELECTION_DONE:
            log("BIAS_TIME_REACHED")
            result = run_sector_bias()
            apply_selection(result)
            break
        time.sleep(1)

threading.Thread(target=bias_timer, daemon=True).start()

# ------------------------------------------------------------
# START FLASK
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
