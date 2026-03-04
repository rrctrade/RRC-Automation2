# ============================================================
# RajanTradeAutomation – ENGINE
# WS + Perfect 5m Candle Engine
# Local Bias Compatible
# Boundary Mismatch FIXED
# ============================================================

import os
import threading
import requests
from datetime import datetime
import pytz

from flask import Flask, jsonify, request
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws


# ================= TIME =================

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300

def fmt_ist(ts):
    return datetime.fromtimestamp(ts, UTC).astimezone(IST).strftime("%H:%M")

# ================= ENV =================

FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise RuntimeError("Missing ENV variables")

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
        except:
            pass

    raise RuntimeError("Unable to fetch settings")

SETTINGS = get_settings()

NIFTY_ADV_THRESHOLD = float(
    SETTINGS.get("NIFTY_ADVANCE_THRESHOLD", 60)
)

NIFTY_DEC_THRESHOLD = float(
    SETTINGS.get("NIFTY_DECLINE_THRESHOLD", 60)
)

# ================= APP =================

app = Flask(__name__)

# ================= FYERS =================

fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

# ================= SYMBOLS =================

ALL_SYMBOLS = sorted({
    s for v in SECTOR_MAP.values() for s in v
})

# ================= STATE =================

candles = {}
last_base_vol = {}

ACTIVE_SYMBOLS = set()
BIAS_DONE = False
BIAS_ANCHOR = None

# ================= LOG =================

def log(msg):

    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ================= CLOSE CANDLE =================

def close_candle(symbol, c):

    prev_base = last_base_vol.get(symbol)

    if prev_base is None:
        last_base_vol[symbol] = c["base_vol"]
        return

    vol = c["base_vol"] - prev_base
    last_base_vol[symbol] = c["base_vol"]

    log(
        f"CANDLE | {symbol} | {fmt_ist(c['start'])} | "
        f"O={c['open']} H={c['high']} "
        f"L={c['low']} C={c['close']} V={vol}"
    )

# ================= UPDATE CANDLE =================

def update_candle(msg):

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or vol is None or ts is None:
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

# ================= WEBSOCKET =================

def on_message(msg):

    update_candle(msg)

def on_connect():

    log("WS CONNECTED")

    fyers_ws.subscribe(
        symbols=ALL_SYMBOLS,
        data_type="SymbolUpdate"
    )

    log(f"SUBSCRIBED_SYMBOLS={len(ALL_SYMBOLS)}")

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

# ================= BIAS RECEIVE =================

@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():

    global ACTIVE_SYMBOLS
    global BIAS_DONE
    global BIAS_ANCHOR

    data = request.get_json(force=True)

    bias = data.get("bias")
    symbols = data.get("active_symbols", [])

    log(f"BIAS RECEIVED = {bias}")

    ACTIVE_SYMBOLS = set(symbols)

    # -------- BOUNDARY FIX --------
    now_ts = int(datetime.now(UTC).timestamp())
    BIAS_ANCHOR = now_ts - (now_ts % CANDLE_INTERVAL)

    log(f"BIAS_ANCHOR = {fmt_ist(BIAS_ANCHOR)}")

    unsubscribe = list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS)

    if unsubscribe:

        try:

            fyers_ws.unsubscribe(
                symbols=unsubscribe,
                data_type="SymbolUpdate"
            )

            log(f"UNSUBSCRIBED_SYMBOLS={len(unsubscribe)}")

        except Exception as e:

            log(f"UNSUBSCRIBE_FAIL | {e}")

    log(f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

    BIAS_DONE = True

    return jsonify({"status": "bias_received"})

# ================= ROUTES =================

@app.route("/")
def health():

    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():

    log("FYERS REDIRECT HIT")

    return jsonify({"status": "ok"})

# ================= START =================

if __name__ == "__main__":

    log("ENGINE START")

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
