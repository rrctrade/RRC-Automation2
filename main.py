# ============================================================
# RajanTradeAutomation – FINAL ENGINE (AUTO LIVE MODE)
# STAGE 1 + STAGE 2 HARD LOCK
# HISTORY MODE + PURE LIVE MODE (AUTO DETECT)
# PRODUCTION SAFE
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
from sector_engine import SECTOR_LIST
from signal_candle_order import (
    handle_signal_event,
    handle_ltp_event,
    ORDER_STATE
)

# ================= TIME =================

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300
MARKET_START = datetime.strptime("09:15:00", "%H:%M:%S").time()

def fmt_ist(ts):
    return datetime.fromtimestamp(int(ts), UTC).astimezone(IST).strftime("%H:%M:%S")

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

# ================= MODE DETECT =================

WS_CONNECT_TIME = None
PURE_LIVE_MODE = False

# ================= COUNTERS =================

ORDER_EXECUTION_COUNT = 0
DAILY_EXECUTED_COUNT = 0
TRADING_LOCKED = False

# ================= SETTINGS =================

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
MAX_TRADES_PER_DAY = int(SETTINGS.get("MAX_TRADES_PER_DAY", 0))

# ================= STATE =================

ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})
ACTIVE_SYMBOLS = set()
BIAS_DONE = False

candles = {}
volume_history = {}
signal_counter = {}

BT_FLOOR_TS = None
STOCK_BIAS_MAP = {}

# ================= LOGGING =================

def log(level, msg):
    global ORDER_EXECUTION_COUNT, DAILY_EXECUTED_COUNT, TRADING_LOCKED

    if level == "ORDER" and msg.startswith("ORDER_EXECUTED"):
        ORDER_EXECUTION_COUNT += 1
        DAILY_EXECUTED_COUNT += 1
        msg = msg.replace("ORDER_EXECUTED",
                          f"ORDER_EXECUTED {ORDER_EXECUTION_COUNT}", 1)

        if (
            MAX_TRADES_PER_DAY > 0
            and DAILY_EXECUTED_COUNT >= MAX_TRADES_PER_DAY
            and not TRADING_LOCKED
        ):
            TRADING_LOCKED = True
            log("SYSTEM", "MAX_TRADES_REACHED – HARD LOCK ACTIVATED")
            hard_lock_cleanup()

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

# ================= HARD LOCK =================

def hard_lock_cleanup():
    global ACTIVE_SYMBOLS

    active_trade_symbols = {
        sym for sym, st in ORDER_STATE.items()
        if st.get("status") in ("EXECUTED", "SL_PLACED")
    }

    unsubscribe_symbols = list(set(ACTIVE_SYMBOLS) - active_trade_symbols)

    if unsubscribe_symbols:
        try:
            fyers_ws.unsubscribe(
                symbols=unsubscribe_symbols,
                data_type="SymbolUpdate"
            )
            log("SYSTEM", f"UNSUBSCRIBED_SYMBOLS={len(unsubscribe_symbols)}")
        except Exception as e:
            log("SYSTEM", f"UNSUBSCRIBE_FAIL | {e}")

    ACTIVE_SYMBOLS = active_trade_symbols

# ================= CLOSE LIVE CANDLE =================

def close_live_candle(symbol, c):

    if TRADING_LOCKED:
        return

    # PURE LIVE MODE volume calculation
    prev = volume_history.get(symbol, [])
    if PURE_LIVE_MODE:
        if not prev:
            candle_vol = c["base_vol"]
        else:
            candle_vol = c["base_vol"] - prev[-1]["base"]
    else:
        if not prev:
            return
        candle_vol = c["base_vol"] - prev[-1]["base"]

    volume_history.setdefault(symbol, []).append({
        "vol": candle_vol,
        "base": c["base_vol"]
    })

    prev_vols = [x["vol"] for x in volume_history[symbol][:-1]]
    prev_min = min(prev_vols) if prev_vols else None
    is_lowest = prev_min is not None and candle_vol < prev_min

    color = "RED" if c["open"] > c["close"] else "GREEN" if c["open"] < c["close"] else "DOJI"
    bias = STOCK_BIAS_MAP.get(symbol, "")

    offset = (c["start"] - BT_FLOOR_TS) // CANDLE_INTERVAL
    label = f"LIVE{offset + 1}"

    if BIAS_DONE:
        log("VOLCHK", f"{symbol} | {label} | vol={round(candle_vol,2)} | is_lowest={is_lowest} | {color} {bias}")

    if offset < 3:
        return

    if not is_lowest:
        return

    side = "BUY" if bias == "B" else "SELL" if bias == "S" else None
    if not side:
        return

    sc = signal_counter.get(symbol, 0) + 1
    signal_counter[symbol] = sc

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

# ================= UPDATE CANDLE =================

def update_candle(msg):

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    base_vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if ltp is None or base_vol is None or ts is None:
        return

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
        return

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["base_vol"] = base_vol

# ================= WEBSOCKET =================

def on_message(msg):
    update_candle(msg)

def on_connect():
    global WS_CONNECT_TIME, PURE_LIVE_MODE

    WS_CONNECT_TIME = datetime.now(IST)
    PURE_LIVE_MODE = WS_CONNECT_TIME.time() < MARKET_START

    mode_text = "PURE LIVE MODE" if PURE_LIVE_MODE else "HISTORY MODE"
    log("SYSTEM", f"WS CONNECTED – {mode_text}")

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

# ================= LOCAL BIAS RECEIVE =================

@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():

    global BT_FLOOR_TS, STOCK_BIAS_MAP, ACTIVE_SYMBOLS, BIAS_DONE

    data = request.get_json(force=True)
    strong = data.get("strong_sectors", [])
    selected = data.get("selected_stocks", [])

    bias_ts = int(datetime.now(UTC).timestamp())
    BT_FLOOR_TS = bias_ts - (bias_ts % CANDLE_INTERVAL)

    log("BIAS", "Bias received from LOCAL")

    STOCK_BIAS_MAP.clear()
    ACTIVE_SYMBOLS.clear()

    for s in [x for x in strong if x["bias"] == "BUY"][:BUY_SECTOR_COUNT]:
        key = SECTOR_LIST.get(s["sector"])
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = "B"

    for s in [x for x in strong if x["bias"] == "SELL"][:SELL_SECTOR_COUNT]:
        key = SECTOR_LIST.get(s["sector"])
        for sym in SECTOR_MAP.get(key, []):
            STOCK_BIAS_MAP[sym] = "S"

    ACTIVE_SYMBOLS = set(selected) & set(STOCK_BIAS_MAP.keys())
    BIAS_DONE = True

    fyers_ws.unsubscribe(
        symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
        data_type="SymbolUpdate"
    )

    log("SYSTEM", f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

    return jsonify({"status": "bias_received"})

# ================= ROUTES =================

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
