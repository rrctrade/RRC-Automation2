# ============================================================
# RajanTradeAutomation – FINAL main.py
# STEP-3C + STEP-4A (LIVE ORDER WS ADDED)
# ============================================================

import os
import time
import threading
import requests
import json
from datetime import datetime
import pytz
from flask import Flask, jsonify

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws, order_ws

from sector_mapping import SECTOR_MAP
from sector_engine import run_sector_bias, SECTOR_LIST
from signal_candle_order import (
    handle_signal_event,
    handle_lowest_event,
    handle_ltp_event
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
            json={"action": "pushLog",
                  "payload": {"level": level, "message": msg}},
            timeout=3
        )
    except Exception:
        pass

def clear_logs():
    try:
        requests.post(WEBAPP_URL, json={"action": "clearLogs"}, timeout=5)
    except Exception:
        pass

# ============================================================
# CLEAR LOGS ON DEPLOY
# ============================================================
clear_logs()
log("SYSTEM", "main.py START (MARKET WS + ORDER WS)")

# ============================================================
# SETTINGS
# ============================================================
def get_settings():
    r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
    return r.json().get("settings", {})

SETTINGS = get_settings()

BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")
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

# ============================================================
# ================= MARKET DATA WEBSOCKET ====================
# ============================================================
def on_market_message(msg):
    symbol = msg.get("symbol")
    if not symbol:
        return

    ltp = msg.get("ltp")
    if ltp is not None:
        # PAPER execution logic (unchanged)
        handle_ltp_event(
            symbol=symbol,
            ltp=ltp,
            log_fn=lambda m: log("ORDER", m)
        )

def on_market_connect():
    log("SYSTEM", "MARKET WS CONNECTED")
    if not BIAS_DONE:
        market_ws.subscribe(symbols=ALL_SYMBOLS, data_type="SymbolUpdate")
    else:
        market_ws.subscribe(symbols=list(ACTIVE_SYMBOLS), data_type="SymbolUpdate")

def start_market_ws():
    global market_ws
    market_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_market_message,
        on_connect=on_market_connect,
        reconnect=True
    )
    market_ws.connect()

threading.Thread(target=start_market_ws, daemon=True).start()

# ============================================================
# ================= ORDER / TRADE WEBSOCKET ==================
# ============================================================
def on_order_message(msg):
    """
    LIVE ORDER / TRADE UPDATE
    """
    log("FYERS_ORDER_RAW", json.dumps(msg, default=str))

    status = msg.get("orderStatus") or msg.get("status")
    symbol = msg.get("symbol")
    filled_qty = msg.get("filledQty") or msg.get("tradedQty")
    avg_price = msg.get("avgPrice") or msg.get("tradePrice")

    if status in ("EXECUTED", "FILLED", "COMPLETE"):
        log(
            "ORDER_EXECUTED_LIVE",
            f"{symbol} | qty={filled_qty} | price={avg_price}"
        )

def on_order_connect():
    log("SYSTEM", "ORDER WS CONNECTED")

def start_order_ws():
    global order_ws_client
    order_ws_client = order_ws.FyersOrderSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_order_message,
        on_connect=on_order_connect,
        reconnect=True
    )
    order_ws_client.connect()

threading.Thread(target=start_order_ws, daemon=True).start()

# ============================================================
# FLASK ROUTES
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
