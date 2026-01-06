# ============================================================
# RajanTradeAutomation – main.py
# FINAL: HISTORY + CORRECT SECTOR BIAS INTEGRATION
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

def fmt_ist(ts):
    return datetime.fromtimestamp(int(ts), UTC).astimezone(IST).strftime("%H:%M")

# ============================================================
# CLEAR LOGS ON DEPLOY
# ============================================================
try:
    requests.post(WEBAPP_URL, json={"action": "clearLogs"}, timeout=5)
except Exception:
    pass

log("SYSTEM", "main.py FINAL HISTORY+BIAS booted (logs cleared)")

# ============================================================
# SETTINGS
# ============================================================
def get_settings():
    try:
        r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
        return r.json().get("settings", {})
    except Exception as e:
        log("ERROR", f"Settings fetch failed: {e}")
        return {}

SETTINGS = get_settings()
BIAS_TIME_STR = SETTINGS.get("BIAS_TIME")
log("SETTINGS", f"BIAS_TIME={BIAS_TIME_STR}")

# ============================================================
# HELPERS
# ============================================================
CANDLE_INTERVAL = 300

def parse_bias_time_utc(tstr):
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    ist_dt = IST.localize(datetime.combine(datetime.now(IST).date(), t))
    return ist_dt.astimezone(UTC)

def floor_5min(ts):
    return ts - (ts % CANDLE_INTERVAL)

# ============================================================
# HISTORY FETCH
# ============================================================
def fetch_history(symbol, start_ts, end_ts):
    log("HISTORY_FETCH", f"{symbol} | {fmt_ist(start_ts)}→{fmt_ist(end_ts)} IST")
    try:
        res = fyers.history({
            "symbol": symbol,
            "resolution": "5",
            "date_format": "0",
            "range_from": int(start_ts),
            "range_to": int(end_ts),
            "cont_flag": "1"
        })
        if res.get("s") == "ok":
            candles = res.get("candles", [])
            log("HISTORY_RESULT", f"{symbol} | candles_count={len(candles)}")
            return candles
        else:
            log("HISTORY_ERROR", f"{symbol} | response={res}")
    except Exception as e:
        log("ERROR", f"History exception {symbol}: {e}")
    return []

# ============================================================
# SECTOR BIAS (CORRECT INTEGRATION)
# ============================================================
def run_bias():
    from sector_engine import run_sector_bias

    log("BIAS", "Sector bias check started")
    result = run_sector_bias()

    strong_sectors = result.get("strong_sectors", [])
    selected = result.get("selected_stocks", [])

    if not strong_sectors:
        log("SECTOR_BIAS", "No strong sector found – NO TRADE DAY")
        return []

    for s in strong_sectors:
        log(
            "SECTOR_BIAS",
            f"{s['sector']} | {s['bias']} | "
            f"Advance={s['up_pct']}% | Decline={s['down_pct']}%"
        )

    log("STOCKS", f"Selected={len(selected)}")
    return selected

# ============================================================
# CONTROLLER
# ============================================================
def controller():
    try:
        if not BIAS_TIME_STR:
            log("ERROR", "BIAS_TIME missing")
            return

        bias_dt = parse_bias_time_utc(BIAS_TIME_STR)
        log("SYSTEM", f"Waiting for BIAS_TIME={BIAS_TIME_STR} IST")

        while datetime.now(UTC) < bias_dt:
            time.sleep(1)

        selected = run_bias()
        if not selected:
            log("SYSTEM", "No stocks selected – stopping")
            return

        bias_ts = int(bias_dt.timestamp())
        ref_end = floor_5min(bias_ts)

        c2_start, c2_end = ref_end - 300, ref_end
        c1_start, c1_end = ref_end - 600, ref_end - 300

        log(
            "SYSTEM",
            f"C1={fmt_ist(c1_start)}→{fmt_ist(c1_end)} | "
            f"C2={fmt_ist(c2_start)}→{fmt_ist(c2_end)} IST"
        )

        for symbol in selected:
            for ts, o, h, l, c, v in fetch_history(symbol, c1_start, c1_end):
                log_render(f"HISTORY | {symbol} | {fmt_ist(ts)} | O={o} H={h} L={l} C={c} V={v}")

            for ts, o, h, l, c, v in fetch_history(symbol, c2_start, c2_end):
                log_render(f"HISTORY | {symbol} | {fmt_ist(ts)} | O={o} H={h} L={l} C={c} V={v}")

        log("SYSTEM", "HISTORY FETCH COMPLETE")

    except Exception as e:
        log("ERROR", f"Controller crashed: {e}")

# ============================================================
# FLASK
# ============================================================
@app.route("/")
def health():
    return jsonify({"status": "ok", "mode": "HISTORY+BIAS"})

@app.route("/fyers-redirect")
def fyers_redirect():
    code = request.args.get("code") or request.args.get("auth_code")
    log("SYSTEM", f"FYERS redirect | code={code}")
    return jsonify({"status": "ok"})

# ============================================================
# START
# ============================================================
threading.Thread(target=controller, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
