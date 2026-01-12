# ============================================================
# RajanTradeAutomation – FINAL main.py
# HISTORY (C1,C2) + EARLY WS + LIVE (C3+)
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
from sector_engine import run_sector_bias

# ============================================================
# TIMEZONES
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

def log_render(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def fmt_ist(ts):
    return datetime.fromtimestamp(int(ts), UTC).astimezone(IST).strftime("%H:%M:%S")

# ============================================================
# CLEAR LOGS ON DEPLOY
# ============================================================
try:
    requests.post(WEBAPP_URL, json={"action": "clearLogs"}, timeout=5)
except Exception:
    pass

log("SYSTEM", "main.py FINAL (EARLY WS + HISTORY + LIVE3 BASELINE FIXED)")

# ============================================================
# SETTINGS
# ============================================================
def get_settings():
    r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
    return r.json().get("settings", {})

SETTINGS = get_settings()
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")
log("SETTINGS", f"BIAS_TIME={BIAS_TIME_STR}")

# ============================================================
# HELPERS
# ============================================================
def parse_bias_time_utc(tstr):
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
    start_ts = end_ts - 600
    res = fyers.history({
        "symbol": symbol,
        "resolution": "5",
        "date_format": "0",
        "range_from": int(start_ts),
        "range_to": int(end_ts - 1),
        "cont_flag": "1"
    })
    return res.get("candles", []) if res.get("s") == "ok" else []

# ============================================================
# LIVE ENGINE (CORE)
# ============================================================
ALL_SYMBOLS = sorted({s for v in SECTOR_MAP.values() for s in v})

candles = {}
last_cum_vol = {}
BT_FLOOR_TS = None

def close_live_candle(symbol, c):
    # Log ONLY candles >= BT_floor
    if BT_FLOOR_TS is None or c["start"] < BT_FLOOR_TS:
        return

    prev = last_cum_vol.get(symbol)
    if prev is None:
        return  # safety guard

    vol = c["cum_vol"] - prev
    last_cum_vol[symbol] = c["cum_vol"]

    offset = (c["start"] - BT_FLOOR_TS) // CANDLE_INTERVAL
    label = f"LIVE{offset + 3}"

    log_render(
        f"{label} | {symbol} | {fmt_ist(c['start'])} | "
        f"O={c['open']} H={c['high']} L={c['low']} "
        f"C={c['close']} V={vol}"
    )

def update_candle(msg):
    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or vol is None or ts is None:
        return

    start = candle_start(ts)

    # 🔒 CRITICAL FIX:
    # Set baseline EXACTLY at BT_floor on first LIVE3 tick
    if BT_FLOOR_TS is not None and start == BT_FLOOR_TS and symbol not in last_cum_vol:
        last_cum_vol[symbol] = vol

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
            "cum_vol": vol
        }
        return

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["cum_vol"] = vol

# ============================================================
# WS CALLBACKS
# ============================================================
def on_message(msg):
    update_candle(msg)

def on_connect():
    print("🔗 WS CONNECTED", flush=True)
    fyers_ws.subscribe(symbols=ALL_SYMBOLS, data_type="SymbolUpdate")
    print(f"📦 Subscribed ALL stocks ({len(ALL_SYMBOLS)})", flush=True)

# ============================================================
# START WS
# ============================================================
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
    global BT_FLOOR_TS

    bias_dt = parse_bias_time_utc(BIAS_TIME_STR)
    log("SYSTEM", f"Waiting for BIAS_TIME={BIAS_TIME_STR} IST")

    while datetime.now(UTC) < bias_dt:
        time.sleep(1)

    BT_FLOOR_TS = floor_5min(int(bias_dt.timestamp()))

    log("BIAS", "Sector bias check started")
    res = run_sector_bias()
    selected = res.get("selected_stocks", [])
    log("STOCKS", f"Selected={len(selected)}")

    non_selected = set(ALL_SYMBOLS) - set(selected)
    try:
        fyers_ws.unsubscribe(symbols=list(non_selected), data_type="SymbolUpdate")
    except Exception:
        pass

    for s in non_selected:
        candles.pop(s, None)
        last_cum_vol.pop(s, None)

    log(
        "SYSTEM",
        f"History window = {fmt_ist(BT_FLOOR_TS-600)}→{fmt_ist(BT_FLOOR_TS)} IST"
    )

    for s in selected:
        for i, (ts,o,h,l,c,v) in enumerate(fetch_two_history_candles(s, BT_FLOOR_TS)):
            if i < 2:
                log_render(
                    f"HISTORY | {s} | {fmt_ist(ts)} | "
                    f"O={o} H={h} L={l} C={c} V={v}"
                )

    log("SYSTEM", "History COMPLETE (C1, C2 only)")

threading.Thread(target=controller, daemon=True).start()

# ============================================================
# FLASK
# ============================================================
@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    code = request.args.get("code") or request.args.get("auth_code")
    log("SYSTEM", f"FYERS redirect | code={code}")
    return jsonify({"status": "ok"})

# ============================================================
# START
# ============================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
