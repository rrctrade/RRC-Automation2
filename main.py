# ============================================================
# RajanTradeAutomation STEP-1
# WebSocket + Perfect 5m Candle Engine
# FYERS Redirect Enabled
# CLEAR LOGS ON DEPLOY
# ============================================================

import os
import threading
from datetime import datetime
import pytz
import requests

from flask import Flask, jsonify
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

from sector_mapping import SECTOR_MAP

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

# ================= APP =================

app = Flask(__name__)

# ================= FYERS =================

fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

# ================= SYMBOL LIST =================

ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})

print("TOTAL SYMBOLS =", len(ALL_SYMBOLS))

# ================= STATE =================

candles = {}
last_base_vol = {}

# ================= CLEAR LOGS =================

def clear_logs():
    try:
        requests.post(
            WEBAPP_URL,
            json={"action": "clearLogs"},
            timeout=5
        )
        print("OLD LOGS CLEARED")
    except Exception:
        print("LOG CLEAR FAILED")

clear_logs()

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

    candle_vol = c["base_vol"] - prev_base
    last_base_vol[symbol] = c["base_vol"]

    log(
        f"CANDLE | {symbol} | {fmt_ist(c['start'])} | "
        f"O={c['open']} H={c['high']} L={c['low']} C={c['close']} "
        f"V={candle_vol}"
    )

# ================= UPDATE CANDLE =================

def update_candle(msg):

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    base_vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or base_vol is None or ts is None:
        return

    start = ts - (ts % CANDLE_INTERVAL)

    c = candles.get(symbol)

    # -------- NEW CANDLE --------

    if c is None or c["start"] != start:

        if c:
            close_candle(symbol, c)

        candles[symbol] = {
            "start": start,
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "base_vol": base_vol
        }

        return

    # -------- UPDATE CURRENT --------

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["base_vol"] = base_vol

# ================= WS =================

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

threading.Thread(target=start_ws, daemon=True).start()

# ================= ROUTES =================

@app.route("/")
def health():
    return jsonify({"status": "ok"})


@app.route("/fyers-redirect")
def fyers_redirect():
    log("FYERS REDIRECT HIT")
    return jsonify({"status": "redirect_ok"})


# ================= START =================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
