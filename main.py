# ============================================================
# RajanTradeAutomation – FINAL main.py
# SAFE BOOT + WS FIX + ENTRY+SL COMBINED + FYERS REDIRECT
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
from signal_candle_order import handle_ltp_event, ORDER_STATE

# ================= TIME =================
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300

# ================= ENV =================
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise RuntimeError("Missing ENV variables")

# ================= APP =================
app = Flask(__name__)

fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

# ================= LOGGING =================
def log(level, msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {msg}", flush=True)
    try:
        requests.post(
            WEBAPP_URL,
            json={"action": "pushLog", "payload": {"level": level, "message": msg}},
            timeout=2
        )
    except Exception:
        pass

def clear_logs():
    try:
        requests.post(WEBAPP_URL, json={"action": "clearLogs"}, timeout=2)
    except Exception:
        pass

# ================= SETTINGS (SAFE LOAD) =================
def get_settings_safe():
    try:
        r = requests.post(
            WEBAPP_URL,
            json={"action": "getSettings"},
            timeout=2
        )
        if r.ok:
            return r.json().get("settings", {})
    except Exception:
        pass
    return {}

SETTINGS = get_settings_safe()

BIAS_TIME_STR = SETTINGS.get("BIAS_TIME", "09:20:00")
BUY_SECTOR_COUNT = int(SETTINGS.get("BUY_SECTOR_COUNT", 0))
SELL_SECTOR_COUNT = int(SETTINGS.get("SELL_SECTOR_COUNT", 0))
PER_TRADE_RISK = float(SETTINGS.get("PER_TRADE_RISK", 500))
MODE = SETTINGS.get("MODE", "PAPER")

# ================= DEPLOY =================
clear_logs()
log("SYSTEM", "main.py DEPLOYED (FINAL SAFE VERSION)")

# ================= TIME HELPERS =================
def parse_bias_time_utc(tstr):
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    ist_dt = IST.localize(datetime.combine(datetime.now(IST).date(), t))
    return ist_dt.astimezone(UTC)

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def floor_5min(ts):
    return ts - (ts % CANDLE_INTERVAL)

# ================= STATE =================
ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})

ACTIVE_SYMBOLS = set()
BIAS_DONE = False

candles = {}
last_base_vol = {}
last_ws_base_before_bias = {}

BT_FLOOR_TS = None
bias_ts = None

# ================= CLOSE CANDLE =================
def close_live_candle(symbol, c):
    prev = last_base_vol.get(symbol)
    if prev is None:
        return

    vol = c["base_vol"] - prev
    last_base_vol[symbol] = c["base_vol"]

    log("VOLCHK", f"{symbol} | vol={round(vol,2)}")

    state = ORDER_STATE.get(symbol)
    if state and state.get("status") == "SL_PLACED" and state.get("entry_price"):
        entry = state["entry_price"]
        qty = state["qty"]
        side = state["side"]
        close = c["close"]

        pl = (close - entry) * qty if side == "BUY" else (entry - close) * qty
        log("ORDER", f"OPEN_TRADE_PL | {symbol} | PL={round(pl,2)}")

# ================= UPDATE CANDLE =================
def update_candle(msg):
    symbol = msg.get("symbol")

    if BIAS_DONE and symbol not in ACTIVE_SYMBOLS:
        return

    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if ltp is None or vol is None or ts is None:
        return

    if not BIAS_DONE and bias_ts and ts < bias_ts:
        last_ws_base_before_bias[symbol] = vol

    # LTP → order engine
    handle_ltp_event(
        fyers=fyers,
        symbol=symbol,
        ltp=ltp,
        mode=MODE,
        log_fn=lambda m: log("ORDER", m)
    )

    # Combined ENTRY + SL log (once)
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

    start = candle_start(ts)
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

# ================= CONTROLLER =================
def controller():
    global BT_FLOOR_TS, ACTIVE_SYMBOLS, BIAS_DONE, bias_ts

    bias_ts = int(parse_bias_time_utc(BIAS_TIME_STR).timestamp())
    log("SYSTEM", f"Waiting for BIAS_TIME={BIAS_TIME_STR}")

    while datetime.now(UTC).timestamp() < bias_ts:
        time.sleep(1)

    BT_FLOOR_TS = floor_5min(bias_ts)
    log("BIAS", "Bias calculation started")

    res = run_sector_bias()
    ACTIVE_SYMBOLS.update(res.get("selected_stocks", []))
    BIAS_DONE = True

    # 🔑 CRITICAL: unsubscribe non-active symbols
    fyers_ws.unsubscribe(
        symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
        data_type="SymbolUpdate"
    )

    log("SYSTEM", f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

threading.Thread(target=controller, daemon=True).start()

# ================= FLASK ROUTES =================
@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    log("SYSTEM", "FYERS redirect hit")
    return jsonify({"status": "ok"})

# ================= START =================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
