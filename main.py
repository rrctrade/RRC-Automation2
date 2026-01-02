# ============================================================
# RajanTradeAutomation – main.py (FINAL)
# HISTORY + TARGETED WS VERSION
# ============================================================

import os, time, json, threading, requests
from datetime import datetime, timedelta
from flask import Flask, jsonify, request

from fyers_apiv3.FyersWebsocket import data_ws
from fyers_apiv3 import fyersModel

from sector_engine import run_sector_bias

# ============================================================
# ENV
# ============================================================

FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not all([FYERS_ACCESS_TOKEN, FYERS_CLIENT_ID, WEBAPP_URL]):
    raise Exception("Missing ENV variables")

# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    return jsonify({"ok": True, "code": request.args.get("code")})

# ============================================================
# GOOGLE SHEETS HELPERS
# ============================================================

def push_log(level, msg):
    try:
        requests.post(WEBAPP_URL, json={
            "action": "pushLog",
            "payload": {"level": level, "message": msg}
        }, timeout=5)
    except:
        pass

def get_settings():
    try:
        r = requests.post(WEBAPP_URL, json={"action": "getSettings"}, timeout=5)
        return r.json().get("settings", {})
    except:
        return {}

def get_bias_time_safe(settings):
    t = settings.get("BIAS_TIME")
    if not t:
        return None
    try:
        return datetime.strptime(t.strip(), "%H:%M:%S").time()
    except:
        return None

# ============================================================
# TIME HELPERS
# ============================================================

def floor_5min(dt):
    return dt.replace(second=0, microsecond=0) - timedelta(
        minutes=dt.minute % 5
    )

# ============================================================
# HISTORY
# ============================================================

def fetch_history(symbol, start, end):
    session = fyersModel.SessionModel()
    session.set_token(FYERS_ACCESS_TOKEN)

    payload = {
        "symbol": symbol,
        "resolution": "5",
        "date_format": "1",
        "range_from": str(int(start.timestamp())),
        "range_to": str(int(end.timestamp())),
        "cont_flag": "1"
    }
    try:
        r = session.history(payload)
        if r.get("s") == "ok" and r.get("candles"):
            return r["candles"][0]
    except:
        pass
    return None

# ============================================================
# WS CANDLE ENGINE (SILENT)
# ============================================================

CANDLES = {}
LAST_VOL = {}

def on_message(msg):
    pass  # live candle logic later

def on_connect():
    push_log("WS", "WebSocket connected")

def on_error(e):
    push_log("ERROR", f"WS error {e}")

def on_close(e):
    push_log("WS", "WebSocket closed")

fyers_ws = data_ws.FyersDataSocket(
    access_token=FYERS_ACCESS_TOKEN,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_connect=on_connect,
    reconnect=True
)

# ============================================================
# MAIN CONTROLLER
# ============================================================

def main_controller():
    push_log("SYSTEM", "main.py booted")

    bias_time = None
    settings = {}

    # ---- WAIT FOR SETTINGS ----
    while not bias_time:
        settings = get_settings()
        bias_time = get_bias_time_safe(settings)
        if not bias_time:
            push_log("SYSTEM", "Waiting for valid BIAS_TIME")
            time.sleep(3)

    push_log("SETTINGS", f"BIAS_TIME={bias_time}")

    # ---- WAIT FOR BIAS TIME ----
    while datetime.now().time() < bias_time:
        time.sleep(1)

    push_log("SYSTEM", "BIAS TIME reached – running sector engine")

    # ---- SECTOR ENGINE ----
    result = run_sector_bias()
    selected = result["selected_stocks"]

    push_log("SYSTEM", f"Selected stocks count={len(selected)}")

    if not selected:
        push_log("SYSTEM", "No stocks selected – exit")
        return

    # ---- HISTORY CANDLES ----
    bt = datetime.combine(datetime.today(), bias_time)
    c3_end = floor_5min(bt)
    c3_start = c3_end - timedelta(minutes=5)
    c2_start = c3_start - timedelta(minutes=5)
    c1_start = c2_start - timedelta(minutes=5)

    push_log("SYSTEM", "Fetching history candles")

    for sym in selected:
        for s, e, tag in [
            (c1_start, c2_start, "C1"),
            (c2_start, c3_start, "C2"),
            (c3_start, c3_end, "C3"),
        ]:
            c = fetch_history(sym, s, e)
            if c:
                push_log("HISTORY", f"{sym} {tag} OK")

    # ---- SUBSCRIBE BEFORE LIVE ----
    sub_time = c3_end - timedelta(seconds=3)
    while datetime.now() < sub_time:
        time.sleep(0.5)

    fyers_ws.subscribe(symbols=selected, data_type="SymbolUpdate")
    push_log("WS", f"Subscribed {len(selected)} stocks")

# ============================================================
# THREADS
# ============================================================

threading.Thread(target=fyers_ws.connect, daemon=True).start()
threading.Thread(target=main_controller, daemon=True).start()

# ============================================================
# START FLASK
# ============================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
