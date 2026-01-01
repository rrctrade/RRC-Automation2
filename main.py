# ============================================================
# RajanTradeAutomation – main.py (VISIBILITY + SETTINGS FIXED)
# ============================================================

import os, time, threading, requests
from datetime import datetime
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")  # GAS WebApp URL

if not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("ENV missing")

# ------------------------------------------------------------
# LOG PUSHER (RENDER → GOOGLE SHEETS)
# ------------------------------------------------------------
def push_log(level, msg):
    try:
        payload = {
            "action": "pushLog",
            "payload": {
                "rows": [[
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    level,
                    msg
                ]]
            }
        }
        requests.post(WEBAPP_URL, json=payload, timeout=3)
    except Exception:
        pass

def log(msg, level="INFO"):
    print(msg)
    push_log(level, msg)

log("SYSTEM_STARTED")

# ------------------------------------------------------------
# LOAD SETTINGS
# ------------------------------------------------------------
def load_settings():
    r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=10)
    s = r.json()["settings"]
    return s

SETTINGS = load_settings()
TICK_START_TIME = SETTINGS.get("TICK_START_TIME", "09:15:00")
BIAS_TIME = SETTINGS.get("BIAS_TIME", "09:25:05")

log(f"SETTINGS_LOADED | TICK_START={TICK_START_TIME} | BIAS_TIME={BIAS_TIME}")

# ------------------------------------------------------------
# FLASK
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"ok": True})

@app.route("/fyers-redirect")
def fyers_redirect():
    return jsonify(dict(request.args))

# ------------------------------------------------------------
# UNIVERSE
# ------------------------------------------------------------
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})
log(f"UNIVERSE_READY | symbols={len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# CANDLE ENGINE
# ------------------------------------------------------------
CANDLE_INTERVAL = 300
candles = {}
last_cum_vol = {}
ALLOW_TICKS = False

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(sym, c):
    prev = last_cum_vol.get(sym, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_cum_vol[sym] = c["cum_vol"]

    log(
        f"5m CANDLE | {sym} | "
        f"O:{c['open']} H:{c['high']} "
        f"L:{c['low']} C:{c['close']} V:{vol}"
    )

def update_candle(msg):
    global ALLOW_TICKS

    now = datetime.now().strftime("%H:%M:%S")
    if now < TICK_START_TIME:
        return

    if not ALLOW_TICKS:
        ALLOW_TICKS = True
        log("TICK_ENGINE_STARTED")

    sym = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if None in (sym, ltp, vol, ts):
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

# ------------------------------------------------------------
# FYERS WS
# ------------------------------------------------------------
from fyers_apiv3.FyersWebsocket import data_ws

SELECTION_DONE = False
UNSUB_DONE = False
SELECTED_STOCKS = set()
LOCK = threading.Lock()

def on_message(msg):
    update_candle(msg)
    unsubscribe_if_needed()

def on_connect():
    log("WS_CONNECTED")
    fyers_ws.subscribe(symbols=ALL_SYMBOLS, data_type="SymbolUpdate")

def on_error(e):
    log(f"WS_ERROR | {e}", "ERROR")

def on_close(e):
    log("WS_CLOSED", "ERROR")

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

# ------------------------------------------------------------
# SECTOR ENGINE
# ------------------------------------------------------------
from sector_engine import run_sector_bias

def sector_runner():
    global SELECTION_DONE, SELECTED_STOCKS
    while True:
        if datetime.now().strftime("%H:%M:%S") >= BIAS_TIME and not SELECTION_DONE:
            log("BIAS_TIME_REACHED")
            res = run_sector_bias()

            log(f"SECTOR_RESULT | {res['strong_sectors']}")
            SELECTED_STOCKS = set(res["selected_stocks"])
            log(f"SELECTED_STOCKS | count={len(SELECTED_STOCKS)}")

            SELECTION_DONE = True
            break
        time.sleep(1)

threading.Thread(target=sector_runner, daemon=True).start()

# ------------------------------------------------------------
# UNSUBSCRIBE AFTER BIAS
# ------------------------------------------------------------
def unsubscribe_if_needed():
    global UNSUB_DONE

    if not SELECTION_DONE or UNSUB_DONE:
        return

    with LOCK:
        if UNSUB_DONE:
            return

        remove = set(candles.keys()) - SELECTED_STOCKS
        if not remove:
            UNSUB_DONE = True
            return

        fyers_ws.unsubscribe(symbols=list(remove), data_type="SymbolUpdate")
        for s in remove:
            candles.pop(s, None)
            last_cum_vol.pop(s, None)

        UNSUB_DONE = True
        log(f"UNSUBSCRIBE_DONE | removed={len(remove)} | remaining={len(candles)}")

# ------------------------------------------------------------
# FLASK START
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
