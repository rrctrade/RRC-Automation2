# ============================================================
# RajanTradeAutomation – FINAL ENGINE (LOCAL BIAS MODE)
# STABLE VERSION + MAX_TRADES_PER_DAY CONTROL
# ============================================================

"""
FULL SYSTEM FLOW (ORIGINAL STABLE)
+ MAX_TRADES_PER_DAY EXECUTION LIMIT
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
log("SYSTEM", "FINAL ENGINE START – LOCAL BIAS MODE")

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
MAX_TRADES_PER_DAY = int(SETTINGS.get("MAX_TRADES_PER_DAY", 999))

# ============================================================
# TRADE LIMIT STATE
# ============================================================

TRADE_COUNT = 0
TRADE_LIMIT_REACHED = False

def cancel_all_pending():
    for symbol, state in list(ORDER_STATE.items()):
        if state.get("status") == "PENDING":
            handle_signal_event(
                fyers=fyers,
                symbol=symbol,
                side=None,
                high=None,
                low=None,
                per_trade_risk=PER_TRADE_RISK,
                mode=MODE,
                signal_no=None,
                log_fn=lambda m: log("ORDER", m)
            )
    log("SYSTEM", "ALL PENDING ORDERS CANCELLED – TRADE LIMIT REACHED")

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

    if TRADE_LIMIT_REACHED:
        return

    prev_base = last_base_vol.get(symbol)
    if prev_base is None:
        return

    candle_vol = c["base_vol"] - prev_base
    last_base_vol[symbol] = c["base_vol"]

    prev_min = min(volume_history[symbol]) if volume_history.get(symbol) else None
    is_lowest = prev_min is not None and candle_vol < prev_min
    volume_history.setdefault(symbol, []).append(candle_vol)

    color = "RED" if c["open"] > c["close"] else "GREEN" if c["open"] < c["close"] else "DOJI"
    bias = STOCK_BIAS_MAP.get(symbol, "")

    if not is_lowest:
        return

    state = ORDER_STATE.get(symbol)
    status = state.get("status") if state else None

    if status == "PENDING":
        handle_signal_event(
            fyers=fyers,
            symbol=symbol,
            side=None,
            high=None,
            low=None,
            per_trade_risk=PER_TRADE_RISK,
            mode=MODE,
            signal_no=None,
            log_fn=lambda m: log("ORDER", m)
        )

    if TRADE_LIMIT_REACHED:
        return

    if (bias == "B" and color == "RED") or (bias == "S" and color == "GREEN"):

        sc = signal_counter.get(symbol, 0) + 1
        signal_counter[symbol] = sc
        side = "BUY" if bias == "B" else "SELL"

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

    global TRADE_COUNT, TRADE_LIMIT_REACHED

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

    prev_state = ORDER_STATE.get(symbol)
    prev_status = prev_state.get("status") if prev_state else None

    handle_ltp_event(
        fyers=fyers,
        symbol=symbol,
        ltp=ltp,
        mode=MODE,
        log_fn=lambda m: log("ORDER", m)
    )

    new_state = ORDER_STATE.get(symbol)
    new_status = new_state.get("status") if new_state else None

    # Detect fresh execution
    if prev_status == "PENDING" and new_status == "SL_PLACED":

        TRADE_COUNT += 1
        log("SYSTEM", f"TRADE EXECUTED | COUNT={TRADE_COUNT}/{MAX_TRADES_PER_DAY}")

        if TRADE_COUNT >= MAX_TRADES_PER_DAY and not TRADE_LIMIT_REACHED:
            TRADE_LIMIT_REACHED = True
            cancel_all_pending()

    # Candle aggregation
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

    STOCK_BIAS_MAP.clear()
    ACTIVE_SYMBOLS.clear()

    for s in [x for x in strong if x["bias"] == "BUY"][:BUY_SECTOR_COUNT]:
        key = SECTOR_LIST.get(s["sector"])
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = "B"

    for s in [x for x in strong if x["bias"] == "SELL"][:SELL_SECTOR_COUNT]:
        key = SECTOR_LIST.get(s["sector"])
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = "S"

    ACTIVE_SYMBOLS = set(selected) & set(STOCK_BIAS_MAP.keys())
    BIAS_DONE = True

    fyers_ws.unsubscribe(
        symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
        data_type="SymbolUpdate"
    )

    log("SYSTEM", f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

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
