# ============================================================
# RajanTradeAutomation – main.py
# HISTORY ONLY DEBUG VERSION
# Scope:
#   - Read BIAS_TIME from Settings
#   - Run Bias at BIAS_TIME
#   - Select stocks
#   - Fetch EXACT two historical candles (C1, C2)
#   - Log EVERYTHING (success + empty + errors)
# NO WS / NO LIVE / NO SUBSCRIBE
# ============================================================

import os
import time
import requests
from datetime import datetime, timedelta
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
# LOGGING (Render + Sheets)
# ============================================================
def log(level, msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {msg}")
    try:
        requests.post(
            WEBAPP_URL,
            json={
                "action": "pushLog",
                "payload": {
                    "level": level,
                    "message": msg
                }
            },
            timeout=3
        )
    except Exception as e:
        print(f"[{ts}] LOG_FAIL | {e}")

def log_render(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] CANDLE | {msg}")

log("SYSTEM", "main.py HISTORY-ONLY booted")

# ============================================================
# SETTINGS
# ============================================================
def get_settings():
    try:
        r = requests.post(
            WEBAPP_URL,
            json={"action": "getSettings"},
            timeout=5
        )
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
CANDLE_INTERVAL = 300  # 5 min

def parse_bias_time_utc(tstr):
    t = datetime.strptime(tstr, "%H:%M:%S").time()
    ist_now = datetime.now(IST)
    ist_dt = IST.localize(datetime.combine(ist_now.date(), t))
    return ist_dt.astimezone(UTC)

def floor_5min(ts):
    return ts - (ts % CANDLE_INTERVAL)

# ============================================================
# HISTORY FETCH (STRICT)
# ============================================================
def fetch_history(symbol, start_ts, end_ts):
    log("HISTORY_FETCH",
        f"{symbol} | from={start_ts} ({datetime.fromtimestamp(start_ts).strftime('%H:%M:%S')}) "
        f"to={end_ts} ({datetime.fromtimestamp(end_ts).strftime('%H:%M:%S')})"
    )
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
            candles = res.get("candles", [])
            log("HISTORY_RESULT", f"{symbol} | candles_count={len(candles)}")
            return candles
        else:
            log("HISTORY_ERROR", f"{symbol} | response={res}")
    except Exception as e:
        log("ERROR", f"History exception {symbol}: {e}")
    return []

# ============================================================
# SECTOR BIAS
# ============================================================
def run_bias():
    from sector_engine import run_sector_bias
    log("BIAS", "Bias check started")
    result = run_sector_bias()
    stocks = result.get("selected_stocks", [])
    log("STOCKS", f"Selected={len(stocks)}")
    return stocks

# ============================================================
# CONTROLLER (HISTORY ONLY)
# ============================================================
def controller():
    try:
        if not BIAS_TIME_STR:
            log("ERROR", "BIAS_TIME missing")
            return

        bias_dt = parse_bias_time_utc(BIAS_TIME_STR)
        log("SYSTEM", f"Waiting for BIAS_TIME={BIAS_TIME_STR} IST")

        # -------- WAIT FOR BIAS TIME --------
        while datetime.now(UTC) < bias_dt:
            time.sleep(1)

        # -------- RUN BIAS --------
        selected = run_bias()
        if not selected:
            log("ERROR", "No stocks selected")
            return

        # -------- CALCULATE EXACT C1 / C2 --------
        bias_ts = int(bias_dt.timestamp())
        ref_end = floor_5min(bias_ts)   # completed candle end

        c2_start = ref_end - 300
        c2_end   = ref_end

        c1_start = ref_end - 600
        c1_end   = ref_end - 300

        log("SYSTEM",
            f"C1={datetime.fromtimestamp(c1_start).strftime('%H:%M')}→{datetime.fromtimestamp(c1_end).strftime('%H:%M')} | "
            f"C2={datetime.fromtimestamp(c2_start).strftime('%H:%M')}→{datetime.fromtimestamp(c2_end).strftime('%H:%M')}"
        )

        # -------- FETCH HISTORY --------
        for symbol in selected:

            # --- C1 ---
            h1 = fetch_history(symbol, c1_start, c1_end)
            if not h1:
                log("HISTORY_EMPTY", f"{symbol} | C1 EMPTY")
            for ts, o, h, l, c, v in h1:
                log_render(
                    f"HISTORY | {symbol} | "
                    f"{datetime.fromtimestamp(int(ts)).strftime('%H:%M')} | "
                    f"O={o} H={h} L={l} C={c} V={v}"
                )

            # --- C2 ---
            h2 = fetch_history(symbol, c2_start, c2_end)
            if not h2:
                log("HISTORY_EMPTY", f"{symbol} | C2 EMPTY")
            for ts, o, h, l, c, v in h2:
                log_render(
                    f"HISTORY | {symbol} | "
                    f"{datetime.fromtimestamp(int(ts)).strftime('%H:%M')} | "
                    f"O={o} H={h} L={l} C={c} V={v}"
                )

        log("SYSTEM", "HISTORY FETCH COMPLETE")

    except Exception as e:
        log("ERROR", f"Controller crashed: {e}")

# ============================================================
# FLASK ROUTES
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
# START
# ============================================================
import threading
threading.Thread(target=controller, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
