# ============================================================
# RajanTradeAutomation – main.py (FINAL FIXED VERSION)
# HISTORY + TARGETED WS + EARLY SUBSCRIBE
# + UTC–IST SAFE BIAS TIME
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime, timedelta
import pytz
from flask import Flask, jsonify, request
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ============================================================
# TIMEZONES
# ============================================================
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc

# ============================================================
# ENV
# ============================================================
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise Exception("❌ Missing ENV variables")

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
    print(f"[{ts}] {level} | {msg}")
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

log("SYSTEM", "main.py booted")

# ============================================================
# SETTINGS
# ============================================================
def get_settings():
    try:
        r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
        return r.json().get("settings", {})
    except Exception as e:
        log("ERROR", f"Settings read failed: {e}")
        return {}

SETTINGS = get_settings()
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")

log("SETTINGS", f"Loaded settings keys={list(SETTINGS.keys())}")
log("SETTINGS", f"BIAS_TIME={BIAS_TIME_STR}")

# ============================================================
# GLOBAL STATE
# ============================================================
CANDLE_INTERVAL = 300

SELECTED_STOCKS = set()
CANDLES = {}

BIAS_DONE = False
SUBSCRIBED = False
C3_REPLACED = False

# ============================================================
# TIME HELPERS (UTC SAFE)
# ============================================================
def ist_bias_datetime_utc(tstr: str) -> datetime:
    """
    Converts IST bias time from Sheets into UTC datetime
    """
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    ist_now = datetime.now(IST)
    ist_dt = IST.localize(datetime.combine(ist_now.date(), t))
    return ist_dt.astimezone(UTC)

def candle_start(ts: int) -> int:
    return ts - (ts % CANDLE_INTERVAL)

# ============================================================
# HISTORY FETCH
# ============================================================
def fetch_history(symbol, start_ts, end_ts):
    try:
        res = fyers.history({
            "symbol": symbol,
            "resolution": "5",
            "date_format": "1",
            "range_from": int(start_ts),
            "range_to": int(end_ts),
            "cont_flag": "1"
        })
        if res.get("s") == "ok":
            return res.get("candles", [])
    except Exception as e:
        log("ERROR", f"History error {symbol}: {e}")
    return []

# ============================================================
# LIVE CANDLE ENGINE
# ============================================================
def update_candle(msg):
    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or vol is None or ts is None:
        return

    if symbol not in SELECTED_STOCKS:
        return

    start = candle_start(ts)
    bucket = CANDLES.setdefault(symbol, {})

    c = bucket.get(start)
    if not c:
        bucket[start] = {
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
# SECTOR BIAS
# ============================================================
def run_bias():
    from sector_engine import run_sector_bias

    log("BIAS", "Bias check started")
    result = run_sector_bias()

    sectors = result.get("strong_sectors", [])
    raw_stocks = result.get("selected_stocks", [])

    for s in sectors:
        log("SECTOR", f"{s['sector']} | {s['bias']} | up={s['up_pct']} down={s['down_pct']}")

    unique_stocks = set(raw_stocks)

    log(
        "STOCKS",
        f"Raw={len(raw_stocks)} | Unique={len(unique_stocks)} | DuplicatesRemoved={len(raw_stocks)-len(unique_stocks)}"
    )

    return unique_stocks

# ============================================================
# MAIN CONTROLLER
# ============================================================
def controller():
    global SELECTED_STOCKS, BIAS_DONE, SUBSCRIBED, C3_REPLACED

    if not BIAS_TIME_STR:
        log("ERROR", "BIAS_TIME missing")
        return

    bias_dt_utc = ist_bias_datetime_utc(BIAS_TIME_STR)

    log(
        "SYSTEM",
        f"Waiting for Bias Time IST={BIAS_TIME_STR} | UTC={bias_dt_utc.strftime('%H:%M:%S')}"
    )

    while True:
        now_utc = datetime.now(UTC)

        # ---------- BIAS ----------
        if now_utc >= bias_dt_utc and not BIAS_DONE:
            SELECTED_STOCKS = run_bias()
            BIAS_DONE = True

            if not SELECTED_STOCKS:
                log("ERROR", "No stocks selected")
                return

            # -------- HISTORY C1 & C2 --------
            ref = candle_start(int(bias_dt_utc.timestamp()))
            c2_start = ref - 300
            c1_start = ref - 600

            for s in SELECTED_STOCKS:
                h = fetch_history(s, c1_start, c2_start + 300)
                for ts, o, h_, l, c, v in h:
                    CANDLES.setdefault(s, {})[int(ts)] = {
                        "open": o, "high": h_, "low": l, "close": c, "cum_vol": v
                    }

            log("HISTORY", "C1 & C2 inserted")

            # -------- EARLY SUBSCRIBE --------
            fyers_ws.subscribe(list(SELECTED_STOCKS), "SymbolUpdate")
            SUBSCRIBED = True
            log("WS", f"Subscribed early {len(SELECTED_STOCKS)} stocks")

        # ---------- REPLACE C3 ----------
        if BIAS_DONE and SUBSCRIBED and not C3_REPLACED:
            ref = candle_start(int(bias_dt_utc.timestamp()))
            if time.time() >= ref + 330:
                for s in SELECTED_STOCKS:
                    h = fetch_history(s, ref, ref + 300)
                    if h:
                        ts, o, h_, l, c, v = h[-1]
                        CANDLES.setdefault(s, {})[int(ts)] = {
                            "open": o, "high": h_, "low": l, "close": c, "cum_vol": v
                        }
                C3_REPLACED = True
                log("HISTORY", "C3 replaced from history")

        time.sleep(1)

# ============================================================
# WS CALLBACKS
# ============================================================
def on_message(msg): update_candle(msg)
def on_error(msg): log("ERROR", f"WS error {msg}")
def on_close(msg): log("WS", "WS closed")
def on_connect(): log("WS", "WS connected")

# ============================================================
# START WS
# ============================================================
fyers_ws = data_ws.FyersDataSocket(
    access_token=FYERS_ACCESS_TOKEN,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_connect=on_connect,
    reconnect=True
)

threading.Thread(target=fyers_ws.connect, daemon=True).start()
threading.Thread(target=controller, daemon=True).start()

# ============================================================
# FLASK ROUTES
# ============================================================
@app.route("/")
def health():
    return jsonify({
        "status": "ok",
        "bias_done": BIAS_DONE,
        "subscribed": SUBSCRIBED
    })

@app.route("/fyers-redirect")
def fyers_redirect():
    auth_code = request.args.get("auth_code") or request.args.get("code")
    log("SYSTEM", f"FYERS redirect received | auth_code={auth_code}")
    return jsonify({"status": "redirect_received", "auth_code": auth_code})

# ============================================================
# START FLASK
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
