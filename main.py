# ============================================================
# RajanTradeAutomation – FINAL main.py
# MODE: LOCAL SECTOR PUSH (NO NSE CALLS ON RENDER)
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
log("SYSTEM", "main.py DEPLOYED | MODE=LOCAL_SECTOR_PUSH")

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

    color = "RED" if c["open"] > c["close"] else "GREEN" if c["open"] < c["close"] else "DOJI"
    bias = STOCK_BIAS_MAP.get(symbol, "")
    offset = (c["start"] - BT_FLOOR_TS) // CANDLE_INTERVAL
    label = f"LIVE{offset + 3}"

    log("VOLCHK", f"{symbol} | {label} | vol={round(candle_vol,2)} | is_lowest={is_lowest} | {color} {bias}")

    # -------- OPEN TRADE PL --------
    state = ORDER_STATE.get(symbol)
    if state and state.get("status") == "SL_PLACED" and state.get("entry_price"):
        entry = state["entry_price"]
        qty = state["qty"]
        side = state["side"]
        close_price = c["close"]
        pl = (close_price - entry) * qty if side == "BUY" else (entry - close_price) * qty
        log("ORDER", f"OPEN_TRADE_PL | {symbol} | PL={round(pl,2)}")

    # -------- SIGNAL GENERATION --------
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

    if not BIAS_DONE and bias_ts and ts < bias_ts:
        last_ws_base_before_bias[symbol] = base_vol

    handle_ltp_event(
        fyers=fyers,
        symbol=symbol,
        ltp=ltp,
        mode=MODE,
        log_fn=lambda m: log("ORDER", m)
    )

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
# CONTROLLER (WAIT ONLY)
# ============================================================
def controller():
    global bias_ts, BT_FLOOR_TS, WAITING_FOR_LOCAL_PUSH

    bias_dt = parse_bias_time_utc(BIAS_TIME_STR)
    bias_ts = int(bias_dt.timestamp())

    log("SYSTEM", f"Waiting for BIAS_TIME={BIAS_TIME_STR}")

    while datetime.now(UTC).timestamp() < bias_ts:
        time.sleep(1)

    BT_FLOOR_TS = floor_5min(bias_ts)
    WAITING_FOR_LOCAL_PUSH = True
    log("SYSTEM", "Bias time reached – waiting for LOCAL sector push")

threading.Thread(target=controller, daemon=True).start()

# ============================================================
# LOCAL PUSH ENDPOINT
# ============================================================
@app.route("/push-sector-bias", methods=["POST"])
def push_sector_bias():
    global ACTIVE_SYMBOLS, STOCK_BIAS_MAP, BIAS_DONE, WAITING_FOR_LOCAL_PUSH

    if not WAITING_FOR_LOCAL_PUSH:
        return jsonify({"ok": False, "error": "Not ready"}), 400

    data = request.json or {}
    strong = data.get("strong_sectors", [])
    selected = set(data.get("selected_stocks", []))

    STOCK_BIAS_MAP.clear()
    ACTIVE_SYMBOLS.clear()

    for s in strong:
        bias = s.get("bias")
        sector = s.get("sector")
        if bias not in ("BUY", "SELL"):
            continue

        for sym in selected:
            STOCK_BIAS_MAP[sym] = "B" if bias == "BUY" else "S"

    ACTIVE_SYMBOLS = set(STOCK_BIAS_MAP.keys())
    BIAS_DONE = True
    WAITING_FOR_LOCAL_PUSH = False

    fyers_ws.unsubscribe(
        symbols=list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS),
        data_type="SymbolUpdate"
    )

    log("SYSTEM", f"SECTOR_BIAS_PUSHED | ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

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
    return jsonify({"ok": True})

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
