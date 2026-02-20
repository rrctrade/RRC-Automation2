# ============================================================
# RajanTradeAutomation – FINAL ENGINE (STABLE + FIXED)
# ARCHITECTURE PRESERVED
# 4TH CANDLE RULE STRICTLY ENFORCED
# FULL LOGGING ENABLED
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime, time as dt_time
import pytz
from flask import Flask, jsonify, request

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

from sector_mapping import SECTOR_MAP
from sector_engine import SECTOR_LIST
from signal_candle_order import (
    handle_signal_event,
    handle_ltp_event,
    ORDER_STATE
)

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
log("SYSTEM", "FINAL ENGINE START – STABLE FIXED VERSION")

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
PER_TRADE_RISK = float(SETTINGS.get("PER_TRADE_RISK", 0))
MODE = SETTINGS.get("MODE", "PAPER")

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
signal_counter = {}

BT_FLOOR_TS = None
STOCK_BIAS_MAP = {}

WS_CONNECT_TIME = None
MARKET_OPEN_TIME = dt_time(9, 15, 0)
HISTORY_MODE = True

# ============================================================
# HISTORY FETCH
# ============================================================

def fetch_two_history_candles(symbol, end_ts):
    res = fyers.history({
        "symbol": symbol,
        "resolution": "5",
        "date_format": "0",
        "range_from": int(end_ts - 600),
        "range_to": int(end_ts - 1),
        "cont_flag": "1"
    })
    return res.get("candles", []) if res.get("s") == "ok" else []

# ============================================================
# CLOSE LIVE CANDLE
# ============================================================

def close_live_candle(symbol, c):

    prev_base = last_base_vol.get(symbol)
    if prev_base is None:
        last_base_vol[symbol] = c["base_vol"]
        return

    candle_vol = c["base_vol"] - prev_base
    last_base_vol[symbol] = c["base_vol"]

    volume_history.setdefault(symbol, [])

    candle_no = len(volume_history[symbol]) + 1

    log("CLOSE",
        f"{symbol} | CANDLE#{candle_no} | "
        f"O={c['open']} H={c['high']} L={c['low']} C={c['close']} | "
        f"V={round(candle_vol,2)}")

    # FIRST 3 CANDLES → REFERENCE ONLY
    if candle_no <= 3:
        volume_history[symbol].append(candle_vol)
        log("REF", f"{symbol} | CANDLE#{candle_no} | REF VOL ADDED")
        return

    prev_min = min(volume_history[symbol])
    is_lowest = candle_vol < prev_min

    log("VOLCHK",
        f"{symbol} | CANDLE#{candle_no} | "
        f"VOL={round(candle_vol,2)} | PREV_MIN={round(prev_min,2)} | "
        f"LOWEST={is_lowest}")

    volume_history[symbol].append(candle_vol)

    if not is_lowest:
        return

    log("SYSTEM", f"{symbol} | NEW LOWEST VOLUME FOUND")

    color = (
        "RED" if c["open"] > c["close"]
        else "GREEN" if c["open"] < c["close"]
        else "DOJI"
    )

    bias = STOCK_BIAS_MAP.get(symbol, "")

    if (bias == "B" and color == "RED") or \
       (bias == "S" and color == "GREEN"):

        sc = signal_counter.get(symbol, 0) + 1
        signal_counter[symbol] = sc
        side = "BUY" if bias == "B" else "SELL"

        log("SYSTEM", f"{symbol} | SIGNAL CONFIRMED | CANDLE#{candle_no}")

        handle_signal_event(
            fyers=fyers,
            symbol=symbol,
            side=side,
            high=c["high"],
            low=c["low"],
            per_trade_risk=PER_TRADE_RISK,
            mode=MODE,
            signal_no=sc,
            log_fn=lambda m: log("ORDER", m)
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

    handle_ltp_event(
        fyers=fyers,
        symbol=symbol,
        ltp=ltp,
        mode=MODE,
        log_fn=lambda m: log("ORDER", m)
    )

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
    global WS_CONNECT_TIME, HISTORY_MODE

    WS_CONNECT_TIME = datetime.now(IST).time()

    if WS_CONNECT_TIME < MARKET_OPEN_TIME:
        HISTORY_MODE = False
        log("SYSTEM", f"WS BEFORE 09:15 → PURE LIVE MODE")
    else:
        HISTORY_MODE = True
        log("SYSTEM", f"WS AFTER 09:15 → HISTORY MODE")

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
# RECEIVE BIAS
# ============================================================

@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():

    global BT_FLOOR_TS, STOCK_BIAS_MAP, ACTIVE_SYMBOLS, BIAS_DONE

    data = request.get_json(force=True)
    strong = data.get("strong_sectors", [])
    selected = data.get("selected_stocks", [])

    bias_ts = int(datetime.now(UTC).timestamp())
    BT_FLOOR_TS = bias_ts - (bias_ts % CANDLE_INTERVAL)

    log("BIAS", f"Strong sectors received = {len(strong)}")

    STOCK_BIAS_MAP.clear()
    ACTIVE_SYMBOLS.clear()

    for s in [x for x in strong if x["bias"] == "BUY"][:BUY_SECTOR_COUNT]:
        key = SECTOR_LIST.get(s["sector"])
        log("SECTOR", f"{s['sector']} | BUY | UP={s['up_pct']}%")
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = "B"

    for s in [x for x in strong if x["bias"] == "SELL"][:SELL_SECTOR_COUNT]:
        key = SECTOR_LIST.get(s["sector"])
        log("SECTOR", f"{s['sector']} | SELL | DOWN={s['down_pct']}%")
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = "S"

    ACTIVE_SYMBOLS = set(selected) & set(STOCK_BIAS_MAP.keys())

    log("SYSTEM", f"ACTIVE_SYMBOLS = {len(ACTIVE_SYMBOLS)}")

    BIAS_DONE = True

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
