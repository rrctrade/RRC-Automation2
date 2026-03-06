# ============================================================
# RajanTradeAutomation
# MAIN ENGINE – PRODUCTION VERSION
# WS + Candle Engine + Bias Receiver
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
import pytz
from queue import Queue

from flask import Flask, jsonify, request

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

from sector_mapping import SECTOR_MAP

# ================= TIME =================

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc

CANDLE_INTERVAL = 300

# ================= ENV =================

FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise RuntimeError("Missing ENV variables")

# ================= APP =================

app = Flask(__name__)

# ================= FYERS =================

fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

# ================= SETTINGS =================

def get_settings():

    for _ in range(3):

        try:

            r = requests.post(
                WEBAPP_URL,
                json={"action": "getSettings"},
                timeout=5
            )

            if r.ok:

                return r.json().get("settings", {})

        except Exception:

            time.sleep(1)

    return {}

SETTINGS = get_settings()

MODE = SETTINGS.get("MODE", "PAPER")
BUY_SECTOR_COUNT = int(SETTINGS.get("BUY_SECTOR_COUNT", 0))
SELL_SECTOR_COUNT = int(SETTINGS.get("SELL_SECTOR_COUNT", 0))
PER_TRADE_RISK = float(SETTINGS.get("PER_TRADE_RISK", 0))
MAX_TRADES_PER_DAY = int(SETTINGS.get("MAX_TRADES_PER_DAY", 0))

# ================= STATE =================

ALL_SYMBOLS = sorted(
    set(s for sector in SECTOR_MAP.values() for s in sector)
)

ACTIVE_SYMBOLS = set()

BIAS_DONE = False

candles = {}

last_base_vol = {}

last_ws_base_before_bias = {}

BIAS_FLOOR_TS = None

STOCK_BIAS_MAP = {}

# ================= QUEUE =================

tick_queue = Queue(maxsize=5000)

# ================= LOG =================

def log(level, msg):

    ts = datetime.now(IST).strftime("%H:%M:%S")

    print(f"[{ts}] {level} | {msg}", flush=True)

    try:

        requests.post(
            WEBAPP_URL,
            json={
                "action": "pushLog",
                "payload": {
                    "level": level,
                    "message": msg
                }
            },
            timeout=3
        )

    except:
        pass

# ================= COLOR =================

def candle_color(o, c):

    if o > c:
        return "R"

    if o < c:
        return "G"

    return "D"

# ================= LOG CANDLE =================

def log_candle(symbol, label, o, h, l, c, v):

    bias = STOCK_BIAS_MAP.get(symbol)

    bias_char = "B" if bias == "B" else "S"

    col = candle_color(o, c)

    log(
        "CANDLE",
        f"{symbol} | {label} | "
        f"O={o} H={h} L={l} C={c} | "
        f"V={v} | {col} | {bias_char}"
    )

# ================= HISTORY =================

def fetch_three_history(symbol):

    start = BIAS_FLOOR_TS - (3 * CANDLE_INTERVAL)

    end = BIAS_FLOOR_TS - 1

    res = fyers.history({

        "symbol": symbol,
        "resolution": "5",
        "date_format": "0",
        "range_from": start,
        "range_to": end,
        "cont_flag": "1"

    })

    if res.get("s") != "ok":
        return []

    return res.get("candles", [])[-3:]

# ================= CLOSE CANDLE =================

def close_candle(symbol, c):

    prev = last_base_vol.get(symbol)

    if prev is None:

        last_base_vol[symbol] = c["base_vol"]
        return

    vol = c["base_vol"] - prev

    last_base_vol[symbol] = c["base_vol"]

    offset = int((c["start"] - BIAS_FLOOR_TS) / CANDLE_INTERVAL)

    label = f"LIVE{offset+3}"

    log_candle(
        symbol,
        label,
        c["open"],
        c["high"],
        c["low"],
        c["close"],
        vol
    )

# ================= UPDATE CANDLE =================

def update_candle(msg):

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if ltp is None or vol is None or ts is None:
        return

    if not BIAS_DONE:

        last_ws_base_before_bias[symbol] = vol
        return

    if symbol not in ACTIVE_SYMBOLS:
        return

    start = ts - (ts % CANDLE_INTERVAL)

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
            "base_vol": vol
        }

        return

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["base_vol"] = vol

# ================= QUEUE WORKER =================

def tick_worker():

    while True:

        msg = tick_queue.get()

        update_candle(msg)

threading.Thread(
    target=tick_worker,
    daemon=True
).start()

# ================= WS =================

def on_message(msg):

    try:
        tick_queue.put_nowait(msg)
    except:
        pass

def on_connect():

    log("SYSTEM", "WS CONNECTED")

    fyers_ws.subscribe(
        symbols=ALL_SYMBOLS,
        data_type="SymbolUpdate"
    )

def start_ws():

    global fyers_ws

    fyers_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_message,
        on_connect=on_connect,
        reconnect=True
    )

    fyers_ws.connect()

threading.Thread(
    target=start_ws,
    daemon=True
).start()

# ================= RECEIVE BIAS =================

@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():

    global ACTIVE_SYMBOLS
    global BIAS_DONE
    global BIAS_FLOOR_TS

    data = request.get_json(force=True)

    selected = data.get("selected_stocks", [])

    log("BIAS", "Bias received from LOCAL")

    bias_ts = int(datetime.now(UTC).timestamp())

    BIAS_FLOOR_TS = bias_ts - (bias_ts % CANDLE_INTERVAL)

    ACTIVE_SYMBOLS = set(selected)

    log("SYSTEM", f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

    BIAS_DONE = True

    try:

        fyers_ws.unsubscribe(
            symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
            data_type="SymbolUpdate"
        )

    except Exception as e:

        log("SYSTEM", f"UNSUBSCRIBE_FAIL | {e}")

    # ================= HISTORY =================

    for s in ACTIVE_SYMBOLS:

        history = fetch_three_history(s)

        for i, c in enumerate(history):

            ts, o, h, l, cl, v = c

            label = f"C{i+1}"

            log_candle(s, label, o, h, l, cl, v)

        if s in last_ws_base_before_bias:

            last_base_vol[s] = last_ws_base_before_bias[s]

    log("SYSTEM", "History loaded – system LIVE")

    return jsonify({"status": "bias_received"})

# ================= ROUTES =================

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/ping")
def ping():
    return jsonify({"status": "alive"})

@app.route("/fyers-redirect")
def fyers_redirect():

    log("SYSTEM", "FYERS redirect hit")

    return jsonify({"status": "ok"})

# ================= START =================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
