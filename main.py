# ============================================================
# RajanTradeAutomation – FINAL main.py (INTEGRATED)
# ============================================================

import os, time, threading, requests
from datetime import datetime
import pytz
from flask import Flask, jsonify, request

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

from sector_mapping import SECTOR_MAP
from sector_engine import run_sector_bias, SECTOR_LIST
from signal_candle import init_symbols, on_candle_close
import trade

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300

FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

app = Flask(__name__)

fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

def log(msg):
    print(msg, flush=True)

def get_settings():
    r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
    return r.json()["settings"]

SETTINGS = get_settings()
BIAS_TIME = SETTINGS["BIAS_TIME"]
PER_TRADE_RISK = int(SETTINGS["PER_TRADE_RISK"])
MODE = SETTINGS.get("MODE", "LIVE")

ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})
candles = {}
last_cum_vol = {}
volume_history = {}
STOCK_BIAS_MAP = {}
BT_FLOOR_TS = None

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def update_candle(msg):
    symbol = msg["symbol"]
    ltp = msg["ltp"]
    vol = msg["vol_traded_today"]
    ts = msg["exch_feed_time"]

    start = candle_start(ts)
    c = candles.get(symbol)

    if c is None or c["start"] != start:
        if c:
            close_candle(symbol, c)
        candles[symbol] = {
            "start": start, "open": ltp, "high": ltp,
            "low": ltp, "close": ltp, "cum": vol
        }
        last_cum_vol.setdefault(symbol, vol)
        return

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["cum"] = vol

def close_candle(symbol, c):
    prev = last_cum_vol.get(symbol)
    vol = c["cum"] - prev
    last_cum_vol[symbol] = c["cum"]

    prev_min = min(volume_history[symbol]) if volume_history.get(symbol) else None
    is_lowest = prev_min is not None and vol < prev_min

    volume_history.setdefault(symbol, []).append(vol)

    color = "RED" if c["open"] > c["close"] else "GREEN" if c["open"] < c["close"] else "DOJI"
    bias = STOCK_BIAS_MAP.get(symbol, "")

    on_candle_close(
        symbol, "LIVE",
        c["open"], c["high"], c["low"], c["close"],
        vol, is_lowest, color, bias
    )

def on_message(msg):
    update_candle(msg)

def on_connect():
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

def controller():
    global STOCK_BIAS_MAP

    time.sleep(2)
    res = run_sector_bias()
    selected = res["selected_stocks"]

    for s in res["strong_sectors"]:
        key = SECTOR_LIST[s["sector"]]
        tag = "B" if s["bias"] == "BUY" else "S"
        for sym in SECTOR_MAP[key]:
            STOCK_BIAS_MAP[sym] = tag

    init_symbols(
        selected, fyers, PER_TRADE_RISK, MODE,
        trade.on_order_placed, trade.on_order_failed
    )

threading.Thread(target=controller, daemon=True).start()

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    code = request.args.get("code") or request.args.get("auth_code")
    log(f"FYERS redirect | code={code}")
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
