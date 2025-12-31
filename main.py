# ============================================================
# RajanTradeAutomation – main.py (FINAL SETTINGS-DRIVEN VERSION)
# ============================================================

import os
import time
import json
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify, request

# ============================================================
# ENV
# ============================================================

FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")  # GAS WebApp URL

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ Missing ENV variables")

# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/ping")
def ping():
    return jsonify({"ok": True})

# FYERS Redirect URI (MANDATORY)
@app.route("/fyers-redirect")
def fyers_redirect():
    return jsonify({
        "status": "redirect_received",
        "code": request.args.get("code")
    })

# ============================================================
# GOOGLE SHEETS COMM
# ============================================================

def call_webapp(action, payload=None):
    try:
        requests.post(
            WEBAPP_URL,
            json={"action": action, "payload": payload or {}},
            timeout=5
        )
    except Exception:
        pass

def log(msg):
    print(msg)
    call_webapp("pushLog", {
        "rows": [[
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "INFO",
            msg
        ]]
    })

# ============================================================
# LOAD SETTINGS
# ============================================================

def load_settings():
    r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
    data = r.json()
    return data.get("settings", {})

SETTINGS = load_settings()

TICK_START_TIME = SETTINGS.get("TICK_START_TIME", "09:15:00")
BIAS_TIME = SETTINGS.get("BIAS_TIME", "09:25:05")

log(f"SETTINGS_LOADED | TICK_START={TICK_START_TIME} | BIAS_TIME={BIAS_TIME}")

# ============================================================
# FYERS WS
# ============================================================

from fyers_apiv3.FyersWebsocket import data_ws

# ============================================================
# UNIVERSE
# ============================================================

from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted(
    {s for stocks in SECTOR_MAP.values() for s in stocks}
)

log(f"UNIVERSE_READY | symbols={len(ALL_SYMBOLS)}")

# ============================================================
# CANDLE ENGINE (LOCKED)
# ============================================================

CANDLE_INTERVAL = 300
candles = {}
last_cum_vol = {}

TICK_COUNTING_ENABLED = False

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(sym, c):
    prev = last_cum_vol.get(sym, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_cum_vol[sym] = c["cum_vol"]

    log(f"CANDLE_CLOSE | {sym} | O:{c['open']} H:{c['high']} "
        f"L:{c['low']} C:{c['close']} V:{vol}")

def update_candle(msg):
    if not TICK_COUNTING_ENABLED:
        return

    sym = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not sym or ltp is None or vol is None or ts is None:
        return

    start = candle_start(ts)
    c = candles.get(sym)

    if c is None or c["start"] != start:
        if c:
            close_candle(sym, c)
        candles[sym] = {
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

# ============================================================
# SELECTION STATE
# ============================================================

SELECTION_DONE = False
UNSUB_DONE = False
SELECTED = set()
LOCK = threading.Lock()

from sector_engine import run_sector_bias

def apply_selection(result):
    global SELECTION_DONE, SELECTED
    SELECTED = set(result.get("selected_stocks", []))
    SELECTION_DONE = True
    log(f"SECTOR_SELECTED | stocks={len(SELECTED)}")

def try_unsubscribe():
    global UNSUB_DONE
    if not SELECTION_DONE or UNSUB_DONE:
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

        UNSUB_DONE = True
        log(f"UNSUBSCRIBE_DONE | removed={len(non_selected)}")

# ============================================================
# WS CALLBACKS
# ============================================================

def on_message(msg):
    update_candle(msg)
    try_unsubscribe()

def on_connect():
    log("WS_CONNECTED")
    fyers_ws.subscribe(
        symbols=ALL_SYMBOLS,
        data_type="SymbolUpdate"
    )

def on_error(msg):
    log("WS_ERROR")

def on_close(msg):
    log("WS_CLOSED")

# ============================================================
# WS START
# ============================================================

def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_message,
        on_connect=on_connect,
        on_error=on_error,
        on_close=on_close,
        reconnect=True
    )
    fyers_ws.connect()

threading.Thread(target=start_ws, daemon=True).start()

# ============================================================
# TICK START WATCHER
# ============================================================

def tick_start_watcher():
    global TICK_COUNTING_ENABLED
    while True:
        if datetime.now().strftime("%H:%M:%S") >= TICK_START_TIME:
            TICK_COUNTING_ENABLED = True
            log("TICK_COUNTING_STARTED")
            break
        time.sleep(1)

threading.Thread(target=tick_start_watcher, daemon=True).start()

# ============================================================
# BIAS WATCHER
# ============================================================

def bias_watcher():
    while True:
        if datetime.now().strftime("%H:%M:%S") >= BIAS_TIME and not SELECTION_DONE:
            log("BIAS_TIME_REACHED")
            result = run_sector_bias()
            apply_selection(result)
            break
        time.sleep(1)

threading.Thread(target=bias_watcher, daemon=True).start()

# ============================================================
# START FLASK
# ============================================================

if __name__ == "__main__":
    log("SYSTEM_STARTED")
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
