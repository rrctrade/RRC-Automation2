# ============================================================
# RajanTradeAutomation – main.py
# HISTORY ONLY (Live disabled, Framework intact)
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime
import pytz
from flask import Flask, jsonify, request
from fyers_apiv3 import fyersModel

# ============================================================
# TIMEZONES
# ============================================================
IST = pytz.timezone("Asia/Kolkata")

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
    print(f"[{ts}] CANDLE | {msg}")

log("SYSTEM", "main.py booted (HISTORY ONLY)")

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
# TIME HELPERS
# ============================================================
CANDLE_INTERVAL = 300

def floor_5min(ts: int) -> int:
    return ts - (ts % CANDLE_INTERVAL)

def bias_time_epoch(tstr: str) -> int:
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    now_ist = datetime.now(IST)
    bias_dt = IST.localize(datetime.combine(now_ist.date(), t))
    return int(bias_dt.timestamp())

# ============================================================
# HISTORY FETCH
# ============================================================
def fetch_history(symbol, start_ts, end_ts):
    try:
        res = fyers.history({
            "symbol": symbol,
            "resolution": "5",
            "date_format": "0",   # epoch
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
# CONTROLLER (Bias → 2 History Candles)
# ============================================================
def controller():
    try:
        from sector_engine import run_sector_bias

        log("BIAS", "Bias check started")
        result = run_sector_bias()
        stocks = result.get("selected_stocks", [])

        log("STOCKS", f"Selected={len(stocks)}")
        if not stocks:
            return

        bias_ts = bias_time_epoch(BIAS_TIME_STR)
        ref = floor_5min(bias_ts)
        c1 = ref - 600
        c2 = ref - 300

        log(
            "SYSTEM",
            f"History window "
            f"{datetime.fromtimestamp(c1, IST).strftime('%H:%M')} → "
            f"{datetime.fromtimestamp(ref, IST).strftime('%H:%M')}"
        )

        for s in stocks:
            candles = fetch_history(s, c1, ref)
            if not candles:
                log("WARN", f"No history for {s}")
                continue

            for ts, o, h, l, c, v in candles:
                log_render(
                    f"HISTORY | {s} | "
                    f"{datetime.fromtimestamp(int(ts), IST).strftime('%H:%M')} | "
                    f"O={o} H={h} L={l} C={c} V={v}"
                )

        log("SYSTEM", "HISTORY PHASE COMPLETE")

    except Exception as e:
        log("ERROR", f"Controller crashed: {e}")

# ============================================================
# START CONTROLLER
# ============================================================
threading.Thread(target=controller, daemon=True).start()

# ============================================================
# FLASK ROUTES (INTACT)
# ============================================================
@app.route("/")
def health():
    return jsonify({"status": "ok", "mode": "HISTORY_ONLY"})

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
