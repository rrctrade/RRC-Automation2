# ============================================================
# RajanTradeAutomation – main.py (FINAL STABLE)
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

@app.route("/fyers-redirect")
def fyers_redirect():
    auth_code = request.args.get("auth_code") or request.args.get("code")
    return jsonify({"status": "redirect_received", "auth_code": auth_code})

# ------------------------------------------------------------
# FYERS WS
# ------------------------------------------------------------
from fyers_apiv3.FyersWebsocket import data_ws

# ------------------------------------------------------------
# UNIVERSE
# ------------------------------------------------------------
from sector_mapping import SECTOR_MAP

ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})
print(f"📦 Total symbols to subscribe: {len(ALL_SYMBOLS)}")

# ------------------------------------------------------------
# CANDLE ENGINE
# ------------------------------------------------------------
CANDLE_INTERVAL = 300
candles = {}
last_cum_vol = {}

LIVE_COUNT = 0  # 🔥 LIVE C / LIVE3 counter

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    global LIVE_COUNT

    prev = last_cum_vol.get(symbol, c["cum_vol"])
    vol = c["cum_vol"] - prev
    last_cum_vol[symbol] = c["cum_vol"]

    if LIVE_COUNT == 0:
        label = "LIVE C"
    elif LIVE_COUNT == 1:
        label = "LIVE3"
    else:
        label = "LIVE"

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] {label} | {symbol} | "
        f"{time.strftime('%H:%M:%S', time.localtime(c['start']))} | "
        f"O={c['open']} H={c['high']} L={c['low']} C={c['close']} V={vol}"
    )

    LIVE_COUNT += 1

def update_candle(msg):
    # 🛡️ GUARD – ignore non-tick packets
    if not isinstance(msg, dict):
        return
    if "symbol" not in msg or "ltp" not in msg:
        return

    symbol = msg["symbol"]
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if ltp is None or vol is None or ts is None:
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
# SELECTION STATE
# ------------------------------------------------------------
SELECTION_DONE = False
UNSUB_DONE = False
SELECTED_STOCKS = set()
LOCK = threading.Lock()

def on_sector_selection(result):
    global SELECTION_DONE, SELECTED_STOCKS
    SELECTED_STOCKS = set(result.get("selected_stocks", []))
    SELECTION_DONE = True
    print(f"✅ STOCKS | Selected={len(SELECTED_STOCKS)}")

def unsubscribe_non_selected():
    global UNSUB_DONE

    if not SELECTION_DONE or UNSUB_DONE:
        return

    with LOCK:
        if UNSUB_DONE:
            return

        remove = set(candles.keys()) - SELECTED_STOCKS
        if remove:
            try:
                fyers_ws.unsubscribe(list(remove), data_type="SymbolUpdate")
            except Exception:
                pass

            for s in remove:
                candles.pop(s, None)
                last_cum_vol.pop(s, None)

            print(f"✂️ SYSTEM | Unsubscribed non-selected stocks = {len(remove)}")

        UNSUB_DONE = True

# ------------------------------------------------------------
# WS CALLBACKS
# ------------------------------------------------------------
def on_message(msg):
    update_candle(msg)
    unsubscribe_non_selected()

def on_connect():
    print("🔗 WS CONNECTED")
    fyers_ws.subscribe(symbols=ALL_SYMBOLS, data_type="SymbolUpdate")
    print(f"📡 Subscribed ALL stocks ({len(ALL_SYMBOLS)})")

def on_error(msg):
    print("❌ WS ERROR", msg)

def on_close(msg):
    print("🔌 WS CLOSED")

# ------------------------------------------------------------
# START WS
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
# SECTOR ENGINE
# ------------------------------------------------------------
from sector_engine import run_sector_bias

def sector_runner():
    while True:
        now = datetime.now().strftime("%H:%M:%S")
        if now >= "09:25:05" and not SELECTION_DONE:
            print("⏱️ BIAS | Sector bias check started")
            result = run_sector_bias()
            on_sector_selection(result)
            break
        time.sleep(1)

threading.Thread(target=sector_runner, daemon=True).start()

# ------------------------------------------------------------
# START FLASK
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
