# ============================================================
# RajanTradeAutomation – main.py (FINAL FLOW-CORRECT VERSION)
# HISTORY → SUBSCRIBE → LIVE
# Candle logs → Render ONLY
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
    """Render + Google Sheets"""
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {msg}")
    if level == "CANDLE":
        return
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

def log_render(msg):
    """Render ONLY"""
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] CANDLE | {msg}")

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

log("SETTINGS", f"BIAS_TIME={BIAS_TIME_STR}")

# ============================================================
# GLOBAL STATE
# ============================================================
CANDLE_INTERVAL = 300
SELECTED_STOCKS = set()
CANDLES = {}
LAST_CANDLE_TS = {}

BIAS_DONE = False
SUBSCRIBED = False
C3_REPLACED = False

# ============================================================
# TIME HELPERS
# ============================================================
def ist_bias_datetime_utc(tstr):
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    ist_now = datetime.now(IST)
    ist_dt = IST.localize(datetime.combine(ist_now.date(), t))
    return ist_dt.astimezone(UTC)

def candle_start(ts):
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
# LIVE CANDLE ENGINE (RENDER LOG ONLY)
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

    last_ts = LAST_CANDLE_TS.get(symbol)
    if last_ts and start > last_ts:
        prev = bucket.get(last_ts)
        if prev:
            log_render(
                f"LIVE | {symbol} | {datetime.fromtimestamp(last_ts).strftime('%H:%M')} | "
                f"O={prev['open']} H={prev['high']} L={prev['low']} "
                f"C={prev['close']} V={prev['cum_vol']}"
            )

    LAST_CANDLE_TS[symbol] = start

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
    stocks = set(result.get("selected_stocks", []))
    log("STOCKS", f"Selected={len(stocks)}")
    return stocks

# ============================================================
# MAIN CONTROLLER (FLOW CORRECT)
# ============================================================
def controller():
    global SELECTED_STOCKS, BIAS_DONE, SUBSCRIBED, C3_REPLACED

    bias_dt = ist_bias_datetime_utc(BIAS_TIME_STR)
    log("SYSTEM", f"Waiting for Bias Time IST={BIAS_TIME_STR}")

    while True:
        now = datetime.now(UTC)

        if now >= bias_dt and not BIAS_DONE:
            SELECTED_STOCKS = run_bias()
            BIAS_DONE = True

            ref = candle_start(int(bias_dt.timestamp()))
            c2 = ref - 300
            c1 = ref - 600

            # ---------- HISTORY C1 & C2 ----------
            for s in SELECTED_STOCKS:
                h = fetch_history(s, c1, ref)
                for ts, o, h_, l, c, v in h:
                    candle = {"open": o, "high": h_, "low": l, "close": c, "cum_vol": v}
                    CANDLES.setdefault(s, {})[int(ts)] = candle
                    LAST_CANDLE_TS[s] = int(ts)
                    log_render(
                        f"HISTORY | {s} | {datetime.fromtimestamp(ts).strftime('%H:%M')} | "
                        f"O={o} H={h_} L={l} C={c} V={v}"
                    )

            # ---------- SUBSCRIBE AFTER HISTORY ----------
            fyers_ws.subscribe(list(SELECTED_STOCKS), "SymbolUpdate")
            SUBSCRIBED = True
            log("WS", f"Subscribed early {len(SELECTED_STOCKS)} stocks")

        time.sleep(1)

# ============================================================
# WS CALLBACKS
# ============================================================
def on_message(msg): update_candle(msg)
def on_error(msg): log("ERROR", f"WS error {msg}")
d
