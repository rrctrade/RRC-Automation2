# ============================================================
# RajanTradeAutomation – main.py (FINAL – VERIFIED)
# History → Subscribe → Live
# Candle close logs → Render ONLY
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

CURRENT_CANDLE_START = {}
LAST_LOGGED_CANDLE_START = {}

BIAS_DONE = False
SUBSCRIBED = False
C3_REPLACED = False

# ============================================================
# TIME HELPERS
# ============================================================
def safe_bias_time_utc(tstr):
    try:
        t = datetime.strptime(tstr, "%H:%M:%S").time()
        ist_now = datetime.now(IST)
        ist_dt = IST.localize(datetime.combine(ist_now.date(), t))
        return ist_dt.astimezone(UTC)
    except Exception as e:
        log("ERROR", f"Invalid BIAS_TIME: {tstr} | {e}")
        return None

def candle_start(ts):
    # FYERS timestamp normalization
    if ts > 10**12:  # milliseconds → seconds
        ts = ts // 1000
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
# LIVE CANDLE ENGINE (RENDER ONLY)
# ============================================================
def update_candle(msg):
    try:
        symbol = msg.get("symbol")
        ltp = msg.get("ltp")
        vol = msg.get("vol_traded_today")
        ts = msg.get("exch_feed_time")

        if not symbol or ltp is None or vol is None or ts is None:
            return
        if symbol not in SELECTED_STOCKS:
            return

        if ts > 10**12:
            ts = ts // 1000

        start = candle_start(ts)
        bucket = CANDLES.setdefault(symbol, {})

        prev_start = CURRENT_CANDLE_START.get(symbol)
        if prev_start is not None and start != prev_start:
            if LAST_LOGGED_CANDLE_START.get(symbol) != prev_start:
                prev = bucket.get(prev_start)
                if prev:
                    log_render(
                        f"LIVE | {symbol} | "
                        f"{datetime.fromtimestamp(prev_start).strftime('%H:%M')} | "
                        f"O={prev['open']} H={prev['high']} "
                        f"L={prev['low']} C={prev['close']} "
                        f"V={prev['cum_vol']}"
                    )
                    LAST_LOGGED_CANDLE_START[symbol] = prev_start

        CURRENT_CANDLE_START[symbol] = start

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

    except Exception as e:
        log("ERROR", f"Candle error: {e}")

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
# MAIN CONTROLLER (FLOW-CORRECT)
# ============================================================
def controller():
    global SELECTED_STOCKS, BIAS_DONE, SUBSCRIBED, C3_REPLACED

    try:
        bias_dt = safe_bias_time_utc(BIAS_TIME_STR)
        if not bias_dt:
            return

        log("SYSTEM", f"Waiting for Bias Time IST={BIAS_TIME_STR}")

        while True:
            now = datetime.now(UTC)

            # -------- BIAS + HISTORY --------
            if now >= bias_dt and not BIAS_DONE:
                SELECTED_STOCKS = run_bias()
                BIAS_DONE = True

                ref = candle_start(int(bias_dt.timestamp()))
                c1 = ref - 600
                c2 = ref - 300

                # ---------- HISTORY C1 & C2 ----------
                for s in SELECTED_STOCKS:
                    h = fetch_history(s, c1, ref)
                    for ts, o, h_, l, c, v in h:
                        ts = int(ts)
                        CANDLES.setdefault(s, {})[ts] = {
                            "open": o, "high": h_, "low": l,
                            "close": c, "cum_vol": v
                        }
                        CURRENT_CANDLE_START[s] = ts
                        LAST_LOGGED_CANDLE_START[s] = ts
                        log_render(
                            f"HISTORY | {s} | "
                            f"{datetime.fromtimestamp(ts).strftime('%H:%M')} | "
                            f"O={o} H={h_} L={l} C={c} V={v}"
                        )

                # ---------- SUBSCRIBE ----------
                fyers_ws.subscribe(list(SELECTED_STOCKS), "SymbolUpdate")
                SUBSCRIBED = True
                log("WS", f"Subscribed early {len(SELECTED_STOCKS)} stocks")

            # -------- C3 HISTORY REPLACE --------
            if BIAS_DONE and SUBSCRIBED and not C3_REPLACED:
                ref = candle_start(int(bias_dt.timestamp()))
                if int(time.time()) >= ref + 60:
                    for s in SELECTED_STOCKS:
                        h = fetch_history(s, ref, ref + 300)
                        if h:
                            ts, o, h_, l, c, v = h[-1]
                            ts = int(ts)
                            CANDLES.setdefault(s, {})[ts] = {
                                "open": o, "high": h_, "low": l,
                                "close": c, "cum_vol": v
                            }
                            log_render(
                                f"REPLACE | {s} | "
                                f"{datetime.fromtimestamp(ts).strftime('%H:%M')} | "
                                f"O={o} H={h_} L={l} C={c} V={v}"
                            )
                    C3_REPLACED = True

            time.sleep(1)

    except Exception as e:
        log("ERROR", f"Controller crashed: {e}")

# ============================================================
# WS CALLBACKS
# ============================================================
def on_message(msg): update_candle(msg)
def on_error(msg): log("ERROR", f"WS error {msg}")
def on_close(msg): log("WS", "WS closed")
def on_connect(): log("WS", "WS connected")

# ============================================================
# START WS + CONTROLLER
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
    return jsonify({"status": "ok"})

# ============================================================
# START FLASK
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
