# ============================================================
# RajanTradeAutomation – FINAL main.py
# MODE: LOCAL SECTOR PUSH + CLEAN EXECUTION
# SINGLE ORDER_EXECUTED GUARANTEED
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
log("SYSTEM", "main.py DEPLOYED | CLEAN HYBRID MODE")

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

BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")
PER_TRADE_RISK = float(SETTINGS.get("PER_TRADE_RISK", 0))
MODE = SETTINGS.get("MODE", "PAPER")

log(
    "SYSTEM",
    f"SETTINGS | BIAS_TIME={BIAS_TIME_STR} | "
    f"PER_TRADE_RISK={PER_TRADE_RISK} | MODE={MODE}"
)

# ============================================================
# STATE
# ============================================================
ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})

ACTIVE_SYMBOLS = set()
STOCK_BIAS_MAP = {}
BIAS_DONE = False

# ============================================================
# LOCAL PUSH RECEIVER (🔥 FIXED 🔥)
# ============================================================
@app.route("/push-sector-bias", methods=["POST"])
def push_sector_bias():
    global ACTIVE_SYMBOLS, STOCK_BIAS_MAP, BIAS_DONE

    data = request.get_json(force=True)

    strong_sectors = data.get("strong_sectors", [])
    selected_stocks = data.get("selected_stocks", [])

    ACTIVE_SYMBOLS = set(selected_stocks)

    STOCK_BIAS_MAP.clear()
    for sec in strong_sectors:
        sector_name = sec.get("sector")
        bias = sec.get("bias")
        STOCK_BIAS_MAP[sector_name] = bias

    BIAS_DONE = True

    log("SYSTEM", f"SECTOR PUSH RECEIVED | Active Stocks={len(ACTIVE_SYMBOLS)}")

    return jsonify({"ok": True})


# ============================================================
# UPDATE CANDLE
# ============================================================
def update_candle(msg):
    symbol = msg.get("symbol")

    if BIAS_DONE and symbol not in ACTIVE_SYMBOLS:
        return

    ltp = msg.get("ltp")
    ts = msg.get("exch_feed_time")
    if ltp is None or ts is None:
        return

    # ---------------- LTP Handling ----------------
    handle_ltp_event(
        fyers=fyers,
        symbol=symbol,
        ltp=ltp,
        mode=MODE,
        log_fn=lambda m: log("ORDER", m)
    )

    # ---------------- SINGLE EXECUTION LOG ----------------
    state = ORDER_STATE.get(symbol)
    if (
        state
        and state.get("status") == "SL_PLACED"
        and state.get("entry_price")
        and not state.get("entry_sl_logged")
    ):
        log(
            "ORDER",
            f"ORDER_EXECUTED | {symbol} | "
            f"entry={state['entry_price']} | "
            f"SL={round(state['sl_price'],2)} | MODE={MODE}"
        )
        state["entry_sl_logged"] = True


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
# BASIC ROUTES
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
