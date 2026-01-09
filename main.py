# ============================================================
# main.py
# FINAL LOCKED VERSION
# History anchored to BIAS TIME, Live numbered deterministically
# ============================================================

import os, time, threading, requests
from datetime import datetime
import pytz
from flask import Flask, jsonify
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ---------------- TIME ----------------
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
INTERVAL = 300

def now_ist():
    return datetime.now(IST)

def floor_5m(ts):
    return ts - (ts % INTERVAL)

def ist_str(ts):
    return datetime.fromtimestamp(ts, UTC).astimezone(IST).strftime("%H:%M:%S")

# ---------------- ENV ----------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

# ---------------- APP ----------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    print("SYSTEM | FYERS redirect | code=200", flush=True)
    return jsonify({"status": "ok"})

# ---------------- LOG ----------------
def log(msg):
    print(f"[{now_ist().strftime('%H:%M:%S')}] {msg}", flush=True)
    try:
        requests.post(WEBAPP_URL, json={
            "action": "pushLog",
            "payload": {"level": "INFO", "message": msg}
        }, timeout=2)
    except:
        pass

# ---------------- SETTINGS ----------------
def get_settings():
    try:
        r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
        return r.json().get("settings", {})
    except:
        return {}

SETTINGS = get_settings()
BIAS_TIME = SETTINGS.get("BIAS_TIME")

bias_ist = IST.localize(
    datetime.combine(now_ist().date(),
    datetime.strptime(BIAS_TIME, "%H:%M:%S").time())
)
BIAS_TS = int(bias_ist.astimezone(UTC).timestamp())
BIAS_FLOOR = floor_5m(BIAS_TS)

log(f"SETTINGS | BIAS_TIME={BIAS_TIME}")

# ---------------- FYERS REST ----------------
fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

# ---------------- STATE ----------------
candles = {}
last_cum_vol = {}
history_c2_vol = {}
live_index = {}

# ---------------- HISTORY ----------------
def fetch_history(symbol):
    res = fyers.history({
        "symbol": symbol,
        "resolution": "5",
        "date_format": "0",
        "range_from": BIAS_FLOOR - 600,
        "range_to": BIAS_FLOOR - 1,
        "cont_flag": "1"
    })
    return res.get("candles", []) if res.get("s") == "ok" else []

# ---------------- CANDLE ENGINE ----------------
def close_candle(symbol, c):
    start = c["start"]

    if start < BIAS_FLOOR:
        return  # ignore all pre-bias live candles

    if symbol not in live_index:
        live_index[symbol] = 3

    idx = live_index[symbol]
    vol = c["cum_vol"] - last_cum_vol.get(symbol, c["cum_vol"])
    last_cum_vol[symbol] = c["cum_vol"]

    log(
        f"LIVE{idx} | {symbol} | {ist_str(start)} | "
        f"O={c['o']} H={c['h']} L={c['l']} C={c['c']} V={vol}"
    )

    live_index[symbol] += 1

def on_tick(msg):
    symbol = msg["symbol"]
    ts = msg["exch_feed_time"]
    ltp = msg["ltp"]
    cum = msg["vol_traded_today"]

    start = floor_5m(ts)
    c = candles.get(symbol)

    if not c or c["start"] != start:
        if c:
            close_candle(symbol, c)
        candles[symbol] = {
            "start": start,
            "o": ltp, "h": ltp, "l": ltp, "c": ltp,
            "cum_vol": cum
        }
        return

    c["h"] = max(c["h"], ltp)
    c["l"] = min(c["l"], ltp)
    c["c"] = ltp
    c["cum_vol"] = cum

# ---------------- WS ----------------
from sector_mapping import SECTOR_MAP

def on_connect():
    symbols = sorted({s for v in SECTOR_MAP.values() for s in v})
    fyers_ws.subscribe(symbols=symbols, data_type="SymbolUpdate")
    log(f"Subscribed ALL stocks ({len(symbols)})")

fyers_ws = data_ws.FyersDataSocket(
    access_token=FYERS_ACCESS_TOKEN,
    on_connect=on_connect,
    on_message=on_tick,
    reconnect=True
)

threading.Thread(target=fyers_ws.connect, daemon=True).start()

# ---------------- CONTROLLER ----------------
from sector_engine import run_sector_bias

def controller():
    log(f"Waiting for BIAS TIME {BIAS_TIME}")
    while int(time.time()) < BIAS_TS:
        time.sleep(1)

    result = run_sector_bias()
    selected = result["selected_stocks"]
    log(f"STOCKS | Selected={len(selected)}")

    for s in selected:
        hist = fetch_history(s)
        if len(hist) >= 2:
            c2 = hist[-1]
            history_c2_vol[s] = c2[5]
            last_cum_vol[s] = c2[5]
            for ts,o,h,l,c,v in hist[:2]:
                log(
                    f"HISTORY | {s} | {ist_str(ts)} | "
                    f"O={o} H={h} L={l} C={c} V={v}"
                )

    log("SYSTEM | History COMPLETE (C1, C2 only)")

threading.Thread(target=controller, daemon=True).start()

# ---------------- START ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
