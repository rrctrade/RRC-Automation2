# ============================================================
# RajanTradeAutomation – FINAL main.py
# STEP-3C : PAPER Execution Detection via REST POLLING
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
import pytz
from flask import Flask, jsonify

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

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
log("SYSTEM", "main.py STEP-3C REST POLLING DEPLOY START")

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
STOCK_BIAS_MAP = {}
BIAS_DONE = False

executed_orders = set()   # ⭐ execution freeze memory

# ============================================================
# 🟢 ORDER REST POLLING THREAD
# ============================================================
def order_polling_loop():
    log("SYSTEM", "Order REST polling started")

    while True:
        try:
            ob = fyers.orderbook()
            if ob.get("s") != "ok":
                time.sleep(2)
                continue

            for o in ob.get("orderBook", []):
                oid = o.get("id")
                status = o.get("status")
                symbol = o.get("symbol")
                filled = o.get("filledQty", 0)

                if status == "TRADED" and oid not in executed_orders:
                    executed_orders.add(oid)

                    log(
                        "EXECUTION",
                        f"{symbol} | ORDER EXECUTED | qty={filled} | id={oid}"
                    )

        except Exception as e:
            log("ERROR", f"Order polling error: {e}")

        time.sleep(2)   # ⏱ polling interval

threading.Thread(target=order_polling_loop, daemon=True).start()

# ============================================================
# MARKET DATA WEBSOCKET (UNCHANGED)
# ============================================================
def on_message(msg):
    handle_ltp_event(
        symbol=msg.get("symbol"),
        ltp=msg.get("ltp"),
        log_fn=lambda m: log("ORDER", m)
    )

def on_connect():
    log("SYSTEM", "Market WS CONNECTED")
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
# CONTROLLER (unchanged logic)
# ============================================================
def controller():
    global BIAS_DONE, ACTIVE_SYMBOLS, STOCK_BIAS_MAP

    bias_dt = datetime.strptime(BIAS_TIME_STR, "%H:%M:%S").time()
    ist_dt = IST.localize(datetime.combine(datetime.now(IST).date(), bias_dt))
    bias_utc = ist_dt.astimezone(UTC)

    log("SYSTEM", f"Waiting for BIAS_TIME={BIAS_TIME_STR}")

    while datetime.now(UTC) < bias_utc:
        time.sleep(1)

    log("BIAS", "Bias calculation started")

    res = run_sector_bias()
    strong = res.get("strong_sectors", [])
    selected = res.get("selected_stocks", [])

    for s in strong:
        key = SECTOR_LIST.get(s["sector"])
        bias = "B" if s["bias"] == "BUY" else "S"
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = bias

    ACTIVE_SYMBOLS = set(selected) & set(STOCK_BIAS_MAP.keys())
    BIAS_DONE = True

    log("SYSTEM", f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

threading.Thread(target=controller, daemon=True).start()

# ============================================================
# FLASK
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
