# ============================================================
# RajanTradeAutomation – DEBUG ENGINE (LOCAL BIAS MODE)
# VOLCHK ONLY – HISTORY BASELINE + FULL ROUTES RESTORED
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
log("SYSTEM", "DEBUG ENGINE START – HISTORY BASELINE MODE")

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
BT_FLOOR_TS = None
STOCK_BIAS_MAP = {}

# ============================================================
# HISTORY
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

def fetch_boundary_cumulative(symbol, boundary_ts):
    res = fyers.history({
        "symbol": symbol,
        "resolution": "1",
        "date_format": "0",
        "range_from": boundary_ts,
        "range_to": boundary_ts,
        "cont_flag": "1"
    })
    if res.get("s") != "ok":
        return None
    candles = res.get("candles", [])
    if not candles:
        return None
    return candles[0][5]

# ============================================================
# CLOSE LIVE CANDLE
# ============================================================

def close_live_candle(symbol, c):

    prev_base = last_base_vol.get(symbol)
    if prev_base is None:
        return

    candle_vol = c["base_vol"] - prev_base
    last_base_vol[symbol] = c["base_vol"]

    offset = (c["start"] - BT_FLOOR_TS) // CANDLE_INTERVAL
    label = f"LIVE{offset + 4}"

    log("VOLCHK", f"{symbol} | {label} | vol={round(candle_vol,2)}")

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
# RECEIVE LOCAL BIAS
# ============================================================

@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():

    global BT_FLOOR_TS, STOCK_BIAS_MAP, ACTIVE_SYMBOLS, BIAS_DONE

    data = request.get_json(force=True)
    strong = data.get("strong_sectors", [])
    selected = data.get("selected_stocks", [])

    bias_ts = int(datetime.now(UTC).timestamp())
    BT_FLOOR_TS = bias_ts - (bias_ts % CANDLE_INTERVAL)

    log("BIAS", f"Bias received at {fmt_ist(bias_ts)}")

    filtered = (
        [x for x in strong if x["bias"] == "BUY"][:BUY_SECTOR_COUNT] +
        [x for x in strong if x["bias"] == "SELL"][:SELL_SECTOR_COUNT]
    )

    STOCK_BIAS_MAP.clear()

    for s in filtered:
        key = SECTOR_LIST.get(s["sector"])
        bias_char = "B" if s["bias"] == "BUY" else "S"
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = bias_char

    ACTIVE_SYMBOLS = set(selected) & set(STOCK_BIAS_MAP.keys())
    BIAS_DONE = True

    fyers_ws.unsubscribe(
        symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
        data_type="SymbolUpdate"
    )

    log("SYSTEM", f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

    for s in ACTIVE_SYMBOLS:

        history = fetch_three_history_candles(s, BT_FLOOR_TS)
        for ts, o, h, l, c, v in history[:3]:
            log("HISTORY", f"{s} | {fmt_ist(ts)} | V={v}")

        boundary_base = fetch_boundary_cumulative(s, BT_FLOOR_TS)

        if boundary_base is not None:
            last_base_vol[s] = boundary_base
            log("SYSTEM", f"{s} | HISTORY BASELINE USED")
        else:
            last_base_vol[s] = 0
            log("SYSTEM", f"{s} | BASELINE FALLBACK=0")

    log("SYSTEM", "History loaded – system LIVE")

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
