# ============================================================
# RajanTradeAutomation – main.py
# FINAL : HISTORY (C1,C2) + LIVE (LIVE3+) with FIXED VOLUME
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

# ============================================================
# TIME
# ============================================================
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300

def now_ist():
    return datetime.now(IST)

def floor_5min(ts):
    return ts - (ts % CANDLE_INTERVAL)

def fmt_ist(ts):
    return datetime.fromtimestamp(int(ts), UTC).astimezone(IST).strftime("%H:%M:%S")

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

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    code = request.args.get("code") or request.args.get("auth_code")
    log("SYSTEM", f"FYERS redirect | code=200")
    return jsonify({"status": "ok"})

# ============================================================
# FYERS REST
# ============================================================
fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

# ============================================================
# LOGGING (LOCKED FORMAT)
# ============================================================
def log(level, msg):
    ts = now_ist().strftime("%H:%M:%S")
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
    ts = now_ist().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ============================================================
# CLEAR LOGS ON DEPLOY
# ============================================================
try:
    requests.post(WEBAPP_URL, json={"action": "clearLogs"}, timeout=5)
except Exception:
    pass

log("SYSTEM", "main.py FINAL (HISTORY + LIVE3 FIXED)")

# ============================================================
# SETTINGS
# ============================================================
def get_settings():
    try:
        r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
        return r.json().get("settings", {})
    except Exception:
        return {}

SETTINGS = get_settings()
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")
log("SETTINGS", f"BIAS_TIME={BIAS_TIME_STR}")

def parse_bias_time_utc(tstr):
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    ist_dt = IST.localize(datetime.combine(now_ist().date(), t))
    return ist_dt.astimezone(UTC)

# ============================================================
# HISTORY FETCH (C1,C2)
# ============================================================
def fetch_two_history(symbol, end_ts):
    start_ts = end_ts - 600
    res = fyers.history({
        "symbol": symbol,
        "resolution": "5",
        "date_format": "0",
        "range_from": int(start_ts),
        "range_to": int(end_ts - 1),
        "cont_flag": "1"
    })
    if res.get("s") == "ok":
        return res.get("candles", [])
    return []

# ============================================================
# SECTOR BIAS
# ============================================================
from sector_engine import run_sector_bias
from sector_mapping import SECTOR_MAP

# ============================================================
# GLOBAL STATE
# ============================================================
candles = {}
history_c2_cumvol = {}   # 🔒 FIX: baseline from HISTORY C2
last_candle_vol = {}

LIVE_COUNTER = {}        # per symbol live index
SELECTION_DONE = False
SELECTED = set()

# ============================================================
# CANDLE ENGINE (FIXED)
# ============================================================
def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    idx = LIVE_COUNTER.get(symbol, 0)
    tag = "LIVE C" if idx == 0 else f"LIVE{idx+2}"
    vol = c["cum_vol"] - last_candle_vol.get(symbol, c["cum_vol"])
    last_candle_vol[symbol] = c["cum_vol"]

    log_render(
        f"{tag} | {symbol} | {fmt_ist(c['start'])} | "
        f"O={c['open']} H={c['high']} L={c['low']} C={c['close']} V={vol}"
    )

    LIVE_COUNTER[symbol] = idx + 1

def update_candle(msg):
    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ts is None:
        return

    start = candle_start(ts)
    c = candles.get(symbol)

    if c is None or c["start"] != start:
        if c:
            close_candle(symbol, c)

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
    from sector_mapping import SECTOR_MAP
    all_symbols = sorted({s for v in SECTOR_MAP.values() for s in v})
    fyers_ws.subscribe(symbols=all_symbols, data_type="SymbolUpdate")
    print("🟢 WS CONNECTED | Subscribed ALL stocks", flush=True)

def on_error(msg): pass
def on_close(msg): pass

# ============================================================
# WS START
# ============================================================
def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_message,
        on_connect=on_connect,
        on_error=on_error,
        on_close=on_close,
        reconnect=True
    )
    fyers_ws.connect()

threading.Thread(target=start_ws, daemon=True).start()

# ============================================================
# CONTROLLER
# ============================================================
def controller():
    bias_dt = parse_bias_time_utc(BIAS_TIME_STR)
    log("SYSTEM", f"Waiting for BIAS_TIME={BIAS_TIME_STR} IST")

    while datetime.now(UTC) < bias_dt:
        time.sleep(1)

    log("BIAS", "Sector bias check started")
    result = run_sector_bias()
    selected = result.get("selected_stocks", [])
    log("STOCKS", f"Selected={len(selected)}")

    bias_ts = int(bias_dt.timestamp())
    ref_end = floor_5min(bias_ts)
    log("SYSTEM", f"History window = {fmt_ist(ref_end-600)}→{fmt_ist(ref_end)} IST")

    for symbol in selected:
        candles_h = fetch_two_history(symbol, ref_end)
        if len(candles_h) >= 2:
            c2 = candles_h[-1]
            history_c2_cumvol[symbol] = c2[5]  # 🔒 STORE C2 CUM VOL
            last_candle_vol[symbol] = c2[5]   # 🔒 FIX BASELINE

            for ts, o, h, l, c, v in candles_h[:2]:
                log_render(
                    f"HISTORY | {symbol} | {fmt_ist(ts)} | "
                    f"O={o} H={h} L={l} C={c} V={v}"
                )

    log("SYSTEM", "History COMPLETE (C1, C2 only)")

threading.Thread(target=controller, daemon=True).start()

# ============================================================
# FLASK START
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
