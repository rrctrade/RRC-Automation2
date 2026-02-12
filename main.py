# ============================================================
# RajanTradeAutomation – HYBRID FINAL main.py
# LOCAL BIAS → RENDER EXECUTION ENGINE
# FYERS REDIRECT SAFE VERSION
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

clear_logs()
log("SYSTEM", "HYBRID FINAL DEPLOY START")

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
STOCK_BIAS_MAP = {}
BIAS_DONE = False

candles = {}
last_base_vol = {}
volume_history = {}
signal_counter = {}

# ============================================================
# HISTORY
# ============================================================
def fetch_two_history_candles(symbol):
    now = int(datetime.now(UTC).timestamp())
    res = fyers.history({
        "symbol": symbol,
        "resolution": "5",
        "date_format": "0",
        "range_from": now - 600,
        "range_to": now - 1,
        "cont_flag": "1"
    })
    return res.get("candles", []) if res.get("s") == "ok" else []

# ============================================================
# CLOSE LIVE CANDLE
# ============================================================
def close_live_candle(symbol, c):
    prev = last_base_vol.get(symbol)
    if prev is None:
        return

    candle_vol = c["base_vol"] - prev
    last_base_vol[symbol] = c["base_vol"]

    prev_min = min(volume_history[symbol]) if volume_history.get(symbol) else None
    is_lowest = prev_min is not None and candle_vol < prev_min
    volume_history.setdefault(symbol, []).append(candle_vol)

    color = "RED" if c["open"] > c["close"] else "GREEN" if c["open"] < c["close"] else "DOJI"
    bias = STOCK_BIAS_MAP.get(symbol, "")

    log("VOLCHK", f"{symbol} | vol={round(candle_vol,2)} | lowest={is_lowest} | {color} {bias}")

    if is_lowest:
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
    symbol = msg.get("symbol")
    if BIAS_DONE and symbol not in ACTIVE_SYMBOLS:
        return

    ltp = msg.get("ltp")
    base_vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")
    if ltp is None or base_vol is None or ts is None:
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
        last_base_vol.setdefault(symbol, base_vol)
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
# HYBRID BIAS RECEIVE
# ============================================================
@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():

    global ACTIVE_SYMBOLS, STOCK_BIAS_MAP, BIAS_DONE

    data = request.get_json(force=True)

    strong = data.get("strong_sectors", [])
    selected = data.get("selected_stocks", [])

    STOCK_BIAS_MAP.clear()
    ACTIVE_SYMBOLS.clear()

    for s in strong[:BUY_SECTOR_COUNT]:
        if s["bias"] == "BUY":
            for sym in selected:
                STOCK_BIAS_MAP[sym] = "B"

    for s in strong[:SELL_SECTOR_COUNT]:
        if s["bias"] == "SELL":
            for sym in selected:
                STOCK_BIAS_MAP[sym] = "S"

    ACTIVE_SYMBOLS = set(selected)
    BIAS_DONE = True

    fyers_ws.unsubscribe(
        symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
        data_type="SymbolUpdate"
    )

    log("SYSTEM", f"BIAS RECEIVED | ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

    for s in ACTIVE_SYMBOLS:
        volume_history.setdefault(s, [])
        history = fetch_two_history_candles(s)
        for ts, o, h, l, c, v in history[:2]:
            volume_history[s].append(v)

    log("SYSTEM", "History loaded – LIVE started")

    return jsonify({"status": "bias_received"})

# ============================================================
# HEALTH + FYERS ROUTES
# ============================================================
@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/ping")
def ping():
    return jsonify({"status": "alive"})

@app.route("/fyers-redirect")
def fyers_redirect():
    log("SYSTEM", "FYERS redirect hit")
    return jsonify({"status": "redirect_received"})

# ============================================================
# START
# ============================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
