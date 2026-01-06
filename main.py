# ============================================================
# RajanTradeAutomation – main.py
# PHASE-2 : History + Bias + LIVE WS (C3 onward)
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
    print(f"[{ts}] LIVE | {msg}")

def fmt_ist(ts):
    return datetime.fromtimestamp(int(ts), UTC).astimezone(IST).strftime("%H:%M:%S")

# ============================================================
# CLEAR LOGS ON DEPLOY
# ============================================================
try:
    requests.post(WEBAPP_URL, json={"action": "clearLogs"}, timeout=5)
except Exception:
    pass

log("SYSTEM", "main.py PHASE-2 (History + Bias + LIVE WS) booted")

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
# HISTORY FETCH (UNCHANGED)
# ============================================================
def fetch_two_history_candles(symbol, end_ts):
    start_ts = end_ts - 600

    log(
        "HISTORY_FETCH",
        f"{symbol} | {fmt_ist(start_ts)}→{fmt_ist(end_ts)} IST"
    )

    try:
        res = fyers.history({
            "symbol": symbol,
            "resolution": "5",
            "date_format": "0",
            "range_from": int(start_ts),
            "range_to": int(end_ts - 1),
            "cont_flag": "1"
        })

        if res.get("s") == "ok":
            candles = res.get("candles", [])
            log("HISTORY_RESULT", f"{symbol} | candles_count={len(candles)}")
            return candles

    except Exception as e:
        log("ERROR", f"History exception {symbol}: {e}")

    return []

# ============================================================
# SECTOR BIAS
# ============================================================
def run_bias():
    from sector_engine import run_sector_bias

    log("BIAS", "Sector bias check started")
    result = run_sector_bias()

    strong_sectors = result.get("strong_sectors", [])
    selected = result.get("selected_stocks", [])

    if not strong_sectors:
        log("SECTOR_BIAS", "No strong sector – NO TRADE DAY")
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
# LIVE WS CANDLE ENGINE (SIMPLE)
# ============================================================
live_candles = {}

def on_ticks(message):
    try:
        symbol = message["symbol"]
        ltp = message["ltp"]
        ts = int(message["timestamp"])

        bucket = floor_5min(ts)

        c = live_candles.get(symbol)
        if not c or c["bucket"] != bucket:
            c = {
                "bucket": bucket,
                "o": ltp,
                "h": ltp,
                "l": ltp,
                "c": ltp,
                "v": 0
            }
            live_candles[symbol] = c
        else:
            c["h"] = max(c["h"], ltp)
            c["l"] = min(c["l"], ltp)
            c["c"] = ltp

        log_render(
            f"CANDLE | {symbol} | {fmt_ist(bucket)} | "
            f"O={c['o']} H={c['h']} L={c['l']} C={c['c']}"
        )

    except Exception as e:
        log("ERROR", f"Tick error: {e}")

def start_ws(symbols):
    ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        log_path="",
        litemode=True,
        reconnect=True
    )

    ws.on_ticks = on_ticks
    ws.on_connect = lambda: ws.subscribe(symbols=symbols, data_type="symbolData")

    log("WS", f"Connecting WS for {len(symbols)} symbols")
    ws.connect()

# ============================================================
# CONTROLLER
# ============================================================
def controller():
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

    log(
        "SYSTEM",
        f"History window = {fmt_ist(ref_end-600)}→{fmt_ist(ref_end)} IST"
    )

    for symbol in selected:
        candles = fetch_two_history_candles(symbol, ref_end)
        for ts, o, h, l, c, v in candles:
            log_render(
                f"HISTORY | {symbol} | {fmt_ist(ts)} | "
                f"O={o} H={h} L={l} C={c} V={v}"
            )

    log("SYSTEM", "HISTORY COMPLETE → STARTING LIVE WS")
    start_ws(selected)

# ============================================================
# FLASK ROUTES
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
threading.Thread(target=controller, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
