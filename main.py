# ============================================================
# RajanTradeAutomation – main.py (FINAL SETTINGS-DRIVEN STABLE)
# ============================================================

import os, time, threading, requests
from datetime import datetime
from flask import Flask, jsonify, request

print("🚀 main.py STARTED")

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ ENV missing")

# ------------------------------------------------------------
# LOG PUSHER (RENDER → GOOGLE SHEETS)
# ------------------------------------------------------------
def push_log(level, msg):
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

def log(msg, level="INFO"):
    print(msg)
    push_log(level, msg)

log("SYSTEM_STARTED")

# ------------------------------------------------------------
# LOAD SETTINGS
# ------------------------------------------------------------
def load_settings():
    try:
        r = requests.post(
            WEBAPP_URL,
            json={"action": "getSettings"},
            timeout=10
        )
        return r.json().get("settings", {})
    except Exception:
        return {}

SETTINGS = load_settings()

TICK_START_TIME = SETTINGS.get("TICK_START_TIME", "09:15:00")
BIAS_TIME       = SETTINGS.get("BIAS_TIME", "09:25:05")

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
log(f"UNIVERSE_READY | TOTAL={len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# CANDLE ENGINE (LOCKED & PROVEN)
# ------------------------------------------------------------
CANDLE_INTERVAL = 300
candles = {}
last_cum_vol = {}
ALLOW_TICKS = False

def normalize_ts(ts):
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, str):
        try:
            return int(ts)
        except:
            return int(time.time())
    return int(time.time())

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(sym, c):
    prev = last_cum_vol.get(sym, c["cum_vol"])
    vol = max(0, c["cum_vol"] - prev)
    last_cum_vol[sym] = c["cum_vol"]

    log(
        f"5M_CANDLE | {sym} | "
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
    ts  = normalize_ts(msg.get("exch_feed_time"))

    if not sym or ltp is None or vol is None:
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
    c["low"]  = min(c["low"], ltp)
    c["close"] = ltp
    c["cum_vol"] = vol

# ------------------------------------------------------------
# SELECTION / UNSUBSCRIBE STATE
# ------------------------------------------------------------
SELECTION_DONE = False
UNSUB_DONE = False
SELECTED_STOCKS = set()
LOCK = threading.Lock()

def unsubscribe_non_selected():
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

        fyers_ws.unsubscribe(
            symbols=list(remove),
            data_type="SymbolUpdate"
        )

        for s in remove:
            candles.pop(s, None)
            last_cum_vol.pop(s, None)

        UNSUB_DONE = True
        log(f"UNSUBSCRIBE_DONE | removed={len(remove)} | remaining={len(candles)}")

# ------------------------------------------------------------
# FYERS WS
# ------------------------------------------------------------
from fyers_apiv3.FyersWebsocket import data_ws

def on_message(msg):
    update_candle(msg)
    unsubscribe_non_selected()

def on_connect():
    log("WS_CONNECTED")
    fyers_ws.subscribe(
        symbols=ALL_SYMBOLS,
        data_type="SymbolUpdate"
    )

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
        if datetime.now().strftime("%H:%M:%S") >= BIAS_TIME:
            log("BIAS_TIME_REACHED")
            res = run_sector_bias()

            SELECTED_STOCKS = set(res.get("selected_stocks", []))
            SELECTION_DONE = True

            log(f"SECTOR_SELECTED | count={len(SELECTED_STOCKS)}")
            break
        time.sleep(1)

threading.Thread(target=sector_runner, daemon=True).start()

# ------------------------------------------------------------
# START FLASK
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
