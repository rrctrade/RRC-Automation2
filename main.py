# ============================================================
# RajanTradeAutomation – FINAL ENGINE (LOCAL BIAS MODE)
# FULL FLOW HEADER + FULL LOGGING (HISTORY 3 + TRUE LIVE4 BASE)
# ============================================================

"""
============================================================
FULL SYSTEM FLOW (FINAL – LOCAL BIAS ARCHITECTURE)
============================================================

PHASE 1 – DEPLOY
1) ENV load
2) Logs cleared
3) Settings fetch
4) WebSocket connect (ALL symbols)

PHASE 2 – LOCAL BIAS WAIT
5) Local system waits till BIAS_TIME
6) Local pushes strong_sectors + selected_stocks
7) Render receives bias
8) Create STOCK_BIAS_MAP
9) Filter ACTIVE_SYMBOLS
10) Unsubscribe non-active symbols

PHASE 3 – HISTORY LOAD
11) Load 3 history candles
12) LIVE4 first tick becomes BASE
13) System LIVE

PHASE 4 – SIGNAL LOGIC
(Currently Disabled – VOLCHK FROM LIVE4)

PHASE 5 – ORDER LIFECYCLE
(Currently Disabled)

============================================================
"""

# ============================================================
# IMPORTS
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
import pytz
from flask import Flask, jsonify, request

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

from sector_mapping import SECTOR_MAP
from sector_engine import SECTOR_LIST

# ============================================================
# TIME
# ============================================================

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300

# ============================================================
# ENV
# ============================================================

FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise RuntimeError("Missing ENV variables")

# ============================================================
# APP
# ============================================================

app = Flask(__name__)

fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

# ============================================================
# LOGGING
# ============================================================

def log(level, msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {msg}", flush=True)
    try:
        requests.post(
            WEBAPP_URL,
            json={"action": "pushLog", "payload": {"level": level, "message": msg}},
            timeout=3
        )
    except Exception:
        pass

def clear_logs():
    try:
        requests.post(WEBAPP_URL, json={"action": "clearLogs"}, timeout=5)
    except Exception:
        pass

def fmt_ist(ts):
    return datetime.fromtimestamp(int(ts), UTC).astimezone(IST).strftime("%H:%M:%S")

clear_logs()
log("SYSTEM", "FINAL ENGINE START – LOCAL BIAS MODE (HISTORY 3 LIVE4 BASE)")

# ============================================================
# SETTINGS
# ============================================================

def get_settings():
    for _ in range(3):
        try:
            r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
            if r.ok:
                return r.json().get("settings", {})
        except Exception:
            time.sleep(1)
    raise RuntimeError("Unable to fetch Settings")

SETTINGS = get_settings()

BUY_SECTOR_COUNT = int(SETTINGS.get("BUY_SECTOR_COUNT", 0))
SELL_SECTOR_COUNT = int(SETTINGS.get("SELL_SECTOR_COUNT", 0))

# ============================================================
# STATE
# ============================================================

ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})

ACTIVE_SYMBOLS = set()
BIAS_DONE = False

candles = {}
last_base_vol = {}
last_ws_base_before_bias = {}

volume_history = {}

BT_FLOOR_TS = None
STOCK_BIAS_MAP = {}

# ============================================================
# HISTORY FETCH (3 CANDLES)
# ============================================================

def fetch_three_history_candles(symbol, end_ts):
    res = fyers.history({
        "symbol": symbol,
        "resolution": "5",
        "date_format": "0",
        "range_from": int(end_ts - 900),
        "range_to": int(end_ts - 1),
        "cont_flag": "1"
    })
    return res.get("candles", []) if res.get("s") == "ok" else []

# ============================================================
# CLOSE LIVE CANDLE (VOLCHK FROM LIVE4)
# ============================================================

def close_live_candle(symbol, c):

    if symbol not in last_base_vol:
        return  # LIVE4 base not set yet

    candle_vol = c["base_vol"] - last_base_vol[symbol]
    last_base_vol[symbol] = c["base_vol"]

    prev_min = min(volume_history[symbol]) if volume_history.get(symbol) else None
    is_lowest = prev_min is not None and candle_vol < prev_min

    volume_history.setdefault(symbol, []).append(candle_vol)

    color = "RED" if c["open"] > c["close"] else \
            "GREEN" if c["open"] < c["close"] else "DOJI"

    bias = STOCK_BIAS_MAP.get(symbol, "")

    offset = (c["start"] - BT_FLOOR_TS) // CANDLE_INTERVAL
    label = f"LIVE{offset + 4}"

    log(
        "VOLCHK",
        f"{symbol} | {label} | vol={round(candle_vol,2)} | "
        f"is_lowest={is_lowest} | {color} {bias}"
    )

# ============================================================
# UPDATE CANDLE
# ============================================================

def update_candle(msg):

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    base_vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if ltp is None or base_vol is None or ts is None:
        return

    if not BIAS_DONE:
        last_ws_base_before_bias[symbol] = base_vol
        return

    if symbol not in ACTIVE_SYMBOLS:
        return

    start = ts - (ts % CANDLE_INTERVAL)
    c = candles.get(symbol)

    if c is None or c["start"] != start:

        if c:
            close_live_candle(symbol, c)

        candles[symbol] = {
            "start": start,
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "base_vol": base_vol
        }

        # 🎯 FIRST TICK OF LIVE4 BECOMES BASE
        if symbol not in last_base_vol:
            last_base_vol[symbol] = base_vol
            log("SYSTEM", f"{symbol} | LIVE4 BASE CAPTURED | base={base_vol}")

        return

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["base_vol"] = base_vol

# ============================================================
# WEBSOCKET
# ============================================================

def on_message(msg):
    update_candle(msg)

def on_connect():
    log("SYSTEM", "WS CONNECTED")
    fyers_ws.subscribe(symbols=ALL_SYMBOLS, data_type="SymbolUpdate")

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

# ============================================================
# LOCAL BIAS RECEIVE
# ============================================================

@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():

    global BT_FLOOR_TS, STOCK_BIAS_MAP, ACTIVE_SYMBOLS, BIAS_DONE

    data = request.get_json(force=True)
    strong = data.get("strong_sectors", [])
    selected = data.get("selected_stocks", [])

    bias_ts = int(datetime.now(UTC).timestamp())
    BT_FLOOR_TS = bias_ts - (bias_ts % CANDLE_INTERVAL)

    log("BIAS", "Bias received from LOCAL")

    filtered = (
        [x for x in strong if x["bias"] == "BUY"][:BUY_SECTOR_COUNT] +
        [x for x in strong if x["bias"] == "SELL"][:SELL_SECTOR_COUNT]
    )

    for s in filtered:
        log(
            "BIAS",
            f"{s['bias']} - {s['sector']} - "
            f"ADVANCES {s['up_pct']}% DECLINES {s['down_pct']}%"
        )

    STOCK_BIAS_MAP.clear()
    ACTIVE_SYMBOLS.clear()

    for s in filtered:
        key = SECTOR_LIST.get(s["sector"])
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = "B" if s["bias"] == "BUY" else "S"

    ACTIVE_SYMBOLS = set(selected) & set(STOCK_BIAS_MAP.keys())
    BIAS_DONE = True

    fyers_ws.unsubscribe(
        symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
        data_type="SymbolUpdate"
    )

    log("SYSTEM", f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

    for s in ACTIVE_SYMBOLS:
        volume_history.setdefault(s, [])
        history = fetch_three_history_candles(s, BT_FLOOR_TS)

        for ts, o, h, l, c, v in history[:3]:
            volume_history[s].append(v)
            log("HISTORY", f"{s} | {fmt_ist(ts)} | V={v}")

    log("SYSTEM", "History loaded – waiting for LIVE4")

    return jsonify({"status": "bias_received"})

# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    log("SYSTEM", "FYERS redirect hit")
    return jsonify({"status": "ok"})

# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
