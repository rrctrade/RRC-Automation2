# ============================================================
# RajanTradeAutomation – FINAL main.py (MERGED)
# HISTORY + LIVE + SIGNAL + SL + PL_CYCLE_UPDATE
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
            json={
                "action": "pushLog",
                "payload": {"level": level, "message": msg}
            },
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
log("SYSTEM", "DEPLOY START – LOGS CLEARED")

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
PER_TRADE_RISK = float(SETTINGS.get("PER_TRADE_RISK", 500))
MODE = SETTINGS.get("MODE", "PAPER")

# ============================================================
# TIME HELPERS
# ============================================================
def parse_bias_time_utc(tstr):
    if not tstr:
        raise ValueError("BIAS_TIME missing")
    if len(tstr.split(":")) == 2:
        tstr += ":00"
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    ist_dt = IST.localize(datetime.combine(datetime.now(IST).date(), t))
    return ist_dt.astimezone(UTC)

def floor_5min(ts):
    return ts - (ts % CANDLE_INTERVAL)

def candle_start(ts):
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
BIAS_DONE = False

candles = {}
last_base_vol = {}
last_ws_base_before_bias = {}

volume_history = {}
lowest_counter = {}
signal_counter = {}

BT_FLOOR_TS = None
bias_ts = None
STOCK_BIAS_MAP = {}

# ===== PL STATE =====
CYCLE_PL_BUFFER = {}
LAST_PL_CYCLE_TS = None
DAY_REALISED_PL = 0.0

# ============================================================
# CLOSE LIVE CANDLE
# ============================================================
def close_live_candle(symbol, c):
    prev_base = last_base_vol.get(symbol)
    if prev_base is None:
        return

    candle_vol = c["base_vol"] - prev_base
    last_base_vol[symbol] = c["base_vol"]

    prev_min = min(volume_history[symbol]) if volume_history.get(symbol) else None
    is_lowest = prev_min is not None and candle_vol < prev_min
    volume_history.setdefault(symbol, []).append(candle_vol)

    color = (
        "RED" if c["open"] > c["close"]
        else "GREEN" if c["open"] < c["close"]
        else "DOJI"
    )

    bias = STOCK_BIAS_MAP.get(symbol, "")
    offset = (c["start"] - BT_FLOOR_TS) // CANDLE_INTERVAL
    label = f"LIVE{offset + 3}"

    log(
        "VOLCHK",
        f"{symbol} | {label} | vol={round(candle_vol,2)} | "
        f"is_lowest={is_lowest} | {color} {bias}"
    )

    # ===== UNREALISED PL BUFFER =====
    state = ORDER_STATE.get(symbol)
    if state and state.get("status") == "SL_PLACED" and state.get("entry_price"):
        entry = state["entry_price"]
        qty = state["qty"]
        side = state["side"]
        close_price = c["close"]

        pl = (
            (close_price - entry) * qty
            if side == "BUY"
            else (entry - close_price) * qty
        )

        CYCLE_PL_BUFFER[symbol] = round(pl, 2)

    # ===== SIGNAL GENERATION =====
    if is_lowest:
        lc = lowest_counter.get(symbol, 0) + 1
        lowest_counter[symbol] = lc

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
# UPDATE CANDLE (TICK LEVEL)
# ============================================================
def update_candle(msg):
    global LAST_PL_CYCLE_TS, DAY_REALISED_PL

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

    def _log_and_capture(m):
        global DAY_REALISED_PL
        log("ORDER", m)
        if m.startswith("SL_EXECUTED") and "LOSS" in m:
            DAY_REALISED_PL -= PER_TRADE_RISK

    handle_ltp_event(
        fyers=fyers,
        symbol=symbol,
        ltp=ltp,
        mode=MODE,
        log_fn=_log_and_capture
    )

    start = candle_start(ts)
    c = candles.get(symbol)

    if c is None or c["start"] != start:
        if c:
            close_live_candle(symbol, c)

            if LAST_PL_CYCLE_TS != c["start"]:
                LAST_PL_CYCLE_TS = c["start"]

                unrealised = round(sum(CYCLE_PL_BUFFER.values()), 2)
                total = round(DAY_REALISED_PL + unrealised, 2)

                parts = [f"{s}={p}" for s, p in CYCLE_PL_BUFFER.items()]

                log(
                    "ORDER",
                    "PL_CYCLE_UPDATE | "
                    + " | ".join(parts)
                    + f" | REALISED={round(DAY_REALISED_PL,2)}"
                    + f" | UNREALISED={unrealised}"
                    + f" | TOTAL={total}"
                )

                CYCLE_PL_BUFFER.clear()

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
# CONTROLLER
# ============================================================
def controller():
    global BT_FLOOR_TS, STOCK_BIAS_MAP, ACTIVE_SYMBOLS, BIAS_DONE, bias_ts

    bias_ts = int(parse_bias_time_utc(BIAS_TIME_STR).timestamp())
    log("SYSTEM", f"Waiting for BIAS_TIME={BIAS_TIME_STR}")

    while datetime.now(UTC).timestamp() < bias_ts:
        time.sleep(1)

    BT_FLOOR_TS = floor_5min(bias_ts)
    log("BIAS", "Bias calculation started")

    res = run_sector_bias()
    strong = res.get("strong_sectors", [])
    all_selected = res.get("selected_stocks", [])

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

    ACTIVE_SYMBOLS = set(all_selected) & set(STOCK_BIAS_MAP.keys())
    BIAS_DONE = True

    fyers_ws.unsubscribe(
        symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
        data_type="SymbolUpdate"
    )

    log("SYSTEM", f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

    for s in ACTIVE_SYMBOLS:
        volume_history.setdefault(s, [])

        history = fetch_two_history_candles(s, BT_FLOOR_TS)
        for ts, o, h, l, c, v in history[:2]:
            volume_history[s].append(v)
            log("HISTORY", f"{s} | {fmt_ist(ts)} | V={v}")

        if s in last_ws_base_before_bias:
            last_base_vol[s] = last_ws_base_before_bias[s]
            log("SYSTEM", f"{s} | LIVE3 BASE SET | base={last_base_vol[s]}")

    log("SYSTEM", "History loaded – system LIVE")

threading.Thread(target=controller, daemon=True).start()

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
