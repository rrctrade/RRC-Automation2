# ============================================================
# RajanTradeAutomation – FINAL main.py
# MODE: LOCAL SECTOR PUSH + STABLE SECTOR LOGIC
# LOG: ACTIVE SECTORS ONLY (NO STOCK-LEVEL NOISE)
# CLEAN EXECUTION – SINGLE ORDER_EXECUTED
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


def fmt_ist(ts):
    return datetime.fromtimestamp(int(ts), UTC).astimezone(IST).strftime("%H:%M:%S")


# ============================================================
# CLEAR LOGS ON DEPLOY
# ============================================================
clear_logs()
log("SYSTEM", "main.py DEPLOYED | CLEAN EXECUTION MODE")

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
BUY_SECTOR_COUNT = int(SETTINGS.get("BUY_SECTOR_COUNT", 0))
SELL_SECTOR_COUNT = int(SETTINGS.get("SELL_SECTOR_COUNT", 0))
PER_TRADE_RISK = float(SETTINGS.get("PER_TRADE_RISK", 0))
MODE = SETTINGS.get("MODE", "PAPER")

log(
    "SYSTEM",
    f"SETTINGS | BIAS_TIME={BIAS_TIME_STR} | "
    f"BUY_SECTOR_COUNT={BUY_SECTOR_COUNT} | "
    f"SELL_SECTOR_COUNT={SELL_SECTOR_COUNT} | "
    f"PER_TRADE_RISK={PER_TRADE_RISK}"
)

# ============================================================
# SECTOR NAME MAP (UNCHANGED)
# ============================================================
SECTOR_NAME_TO_KEY = {
    "NIFTY AUTO": "AUTO",
    "NIFTY FINANCIAL SERVICES": "FINANCIAL_SERVICES",
    "NIFTY FIN SERVICE EX BANK": "FIN_SERVICES_EX_BANK",
    "NIFTY FMCG": "FMCG",
    "NIFTY IT": "IT",
    "NIFTY MEDIA": "MEDIA",
    "NIFTY METAL": "METAL",
    "NIFTY PHARMA": "PHARMA",
    "NIFTY PSU BANK": "PSU_BANK",
    "NIFTY PRIVATE BANK": "PRIVATE_BANK",
    "NIFTY REALTY": "REALTY",
    "NIFTY CONSUMER DURABLES": "CONSUMER_DURABLES",
    "NIFTY OIL & GAS": "OIL_GAS",
    "NIFTY CHEMICALS": "CHEMICALS",
    "NIFTY BANK": "BANK",
    "NIFTY 50": "NIFTY50",
}

# ============================================================
# TIME HELPERS
# ============================================================
def parse_bias_time_utc(tstr):
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    ist_dt = IST.localize(datetime.combine(datetime.now(IST).date(), t))
    return ist_dt.astimezone(UTC)


def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)


def floor_5min(ts):
    return ts - (ts % CANDLE_INTERVAL)


# ============================================================
# HISTORY
# ============================================================
def fetch_two_history_candles(symbol, end_ts):
    res = fyers.history({
        "symbol": symbol,
        "resolution": "5",
        "date_format": "0",
        "range_from": int(end_ts - 600),
        "range_to": int(end_ts - 1),
        "cont_flag": "1"
    })
    return res.get("candles", []) if res.get("s") == "ok" else []


# ============================================================
# STATE
# ============================================================
ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})

ACTIVE_SYMBOLS = set()
STOCK_BIAS_MAP = {}
BIAS_DONE = False
WAITING_FOR_LOCAL_PUSH = False

candles = {}
last_base_vol = {}
last_ws_base_before_bias = {}

volume_history = {}
signal_counter = {}

BT_FLOOR_TS = None
bias_ts = None


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

    if not BIAS_DONE and bias_ts and ts < bias_ts:
        last_ws_base_before_bias[symbol] = base_vol

    handle_ltp_event(
        fyers=fyers,
        symbol=symbol,
        ltp=ltp,
        mode=MODE,
        log_fn=lambda m: log("ORDER", m)
    )

    # ✅ SINGLE COMBINED EXECUTION LOG
    state = ORDER_STATE.get(symbol)
    if (
        state
        and state.get("status") == "SL_PLACED"
        and state.get("entry_price")
        and not state.get("entry_sl_logged")
    ):
        log(
            "ORDER",
            f"ORDER_EXECUTED | {symbol} | entry={state['entry_price']} | "
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
