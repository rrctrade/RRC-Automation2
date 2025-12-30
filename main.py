# ============================================================
# RajanTradeAutomation – main.py (FINAL ASSEMBLED VERSION)
# Phase-0 + Phase-1 + Phase-2 (LOCKED)
# ============================================================

import os
import time
import threading
from datetime import datetime
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# BASIC LOG
# ------------------------------------------------------------
print("🚀 main.py STARTED")

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN:
    raise Exception("❌ FYERS ENV variables missing")

# ------------------------------------------------------------
# FLASK
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/callback")
def fyers_callback():
    return jsonify({"status": "callback_received"})

# ------------------------------------------------------------
# FYERS WS
# ------------------------------------------------------------
from fyers_apiv3.FyersWebsocket import data_ws

# ------------------------------------------------------------
# CANDLE ENGINE (LOCKED)
# ------------------------------------------------------------
CANDLE_INTERVAL = 300

candles = {}
last_candle_vol = {}

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    prev = last_candle_vol.get(symbol, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_candle_vol[symbol] = c["cum_vol"]

    print(
        f"\n🟩 5m CANDLE | {symbol} | "
        f"O:{c['open']} H:{c['high']} "
        f"L:{c['low']} C:{c['close']} V:{vol}"
    )

def update_candle(msg):
    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or vol is None or ts is None:
        return

    start = candle_start(ts)
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
            "cum_vol": vol
        }
        return

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["cum_vol"] = vol

# ------------------------------------------------------------
# STEP-B : SELECTION + UNSUBSCRIBE STATE
# ------------------------------------------------------------
SELECTION_DONE = False
UNSUBSCRIBE_DONE = False
SELECTED_STOCKS = set()
UNSUB_LOCK = threading.Lock()

def on_sector_selection_complete(result):
    global SELECTION_DONE, SELECTED_STOCKS
    SELECTED_STOCKS = set(result.get("selected_stocks", []))
    SELECTION_DONE = True
    print("✅ Sector selection complete")

def atomic_unsubscribe_and_delete():
    global UNSUBSCRIBE_DONE

    if not SELECTION_DONE or UNSUBSCRIBE_DONE:
        return

    with UNSUB_LOCK:
        if not SELECTION_DONE or UNSUBSCRIBE_DONE:
            return

        all_syms = set(candles.keys())
        non_selected = all_syms - SELECTED_STOCKS

        if not non_selected:
            UNSUBSCRIBE_DONE = True
            return

        try:
            fyers_ws.unsubscribe(
                symbols=list(non_selected),
                data_type="SymbolUpdate"
            )
        except Exception:
            pass

        for s in non_selected:
            candles.pop(s, None)
            last_candle_vol.pop(s, None)

        UNSUBSCRIBE_DONE = True
        print("✂️ Unsubscribed + Deleted non-selected stocks")

# ------------------------------------------------------------
# WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)
    atomic_unsubscribe_and_delete()

def on_error(msg):
    print("❌ WS ERROR")

def on_close(msg):
    print("🔌 WS CLOSED")

def on_connect():
    global fyers_ws
    print("🔗 WS CONNECTED")

    # TEMP – 5 STOCKS (later expand to 200)
    symbols = [
        "NSE:SBIN-EQ",
        "NSE:RELIANCE-EQ",
        "NSE:VEDL-EQ",
        "NSE:AXISBANK-EQ",
        "NSE:KOTAKBANK-EQ",
    ]

    fyers_ws.subscribe(
        symbols=symbols,
        data_type="SymbolUpdate"
    )

# ------------------------------------------------------------
# WS THREAD
# ------------------------------------------------------------
def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_connect=on_connect,
        reconnect=True
    )
    fyers_ws.connect()

threading.Thread(target=start_ws, daemon=True).start()

# ------------------------------------------------------------
# STEP-C : SECTOR ENGINE @ 09:25
# ------------------------------------------------------------
from sector_engine import run_sector_bias

def sector_engine_runner():
    while True:
        now = datetime.now().strftime("%H:%M:%S")
        if now >= "09:25:05" and not SELECTION_DONE:
            result = run_sector_bias()
            on_sector_selection_complete(result)
            break
        time.sleep(1)

threading.Thread(target=sector_engine_runner, daemon=True).start()

# ------------------------------------------------------------
# START FLASK
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
