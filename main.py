# ============================================================
# RajanTradeAutomation – main.py (FINAL / RENDER SAFE)
# FYERS OAUTH + LIVE WS + 5 MIN CANDLE
# WS keep_running() FIX APPLIED
# ============================================================

import os
import time
import threading
from flask import Flask, jsonify, request

print("🚀 main.py STARTED")

# ------------------------------------------------------------
# ENV CHECK (SAFE FOR OAUTH PHASE)
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_SECRET_KEY = os.getenv("FYERS_SECRET_KEY")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

print("🔍 ENV CHECK")
print("FYERS_CLIENT_ID =", FYERS_CLIENT_ID[:10] + "..." if FYERS_CLIENT_ID else "❌ MISSING")
print("FYERS_SECRET_KEY =", "✅ SET" if FYERS_SECRET_KEY else "❌ MISSING")
print(
    "FYERS_ACCESS_TOKEN prefix =",
    FYERS_ACCESS_TOKEN[:20] + "..." if FYERS_ACCESS_TOKEN else "❌ MISSING"
)

if not FYERS_CLIENT_ID or not FYERS_SECRET_KEY:
    raise Exception("❌ FYERS CLIENT ID / SECRET KEY missing")

if not FYERS_ACCESS_TOKEN:
    print("⚠️ FYERS_ACCESS_TOKEN missing – OAuth activation required")

# ------------------------------------------------------------
# FYERS IMPORTS (v3 CORRECT)
# ------------------------------------------------------------
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ------------------------------------------------------------
# FLASK APP
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "RajanTradeAutomation",
        "ws_active": bool(FYERS_ACCESS_TOKEN)
    })

# ------------------------------------------------------------
# ACTIVATE → FYERS LOGIN
# ------------------------------------------------------------
@app.route("/activate")
def activate():
    session = fyersModel.SessionModel(
        client_id=FYERS_CLIENT_ID,
        secret_key=FYERS_SECRET_KEY,
        redirect_uri="https://rrc-automation2.onrender.com/fyers-redirect",
        response_type="code",
        grant_type="authorization_code"
    )

    auth_url = session.generate_authcode()
    print("🔑 ACTIVATE URL:", auth_url)

    return jsonify({"url": auth_url})

# ------------------------------------------------------------
# FYERS REDIRECT → TOKEN
# ------------------------------------------------------------
@app.route("/fyers-redirect")
def fyers_redirect():
    auth_code = request.args.get("auth_code") or request.args.get("code")
    print("🔁 FYERS REDIRECT HIT | AUTH CODE =", auth_code)

    if not auth_code:
        return jsonify({"error": "auth_code_missing"})

    try:
        session = fyersModel.SessionModel(
            client_id=FYERS_CLIENT_ID,
            secret_key=FYERS_SECRET_KEY,
            redirect_uri="https://rrc-automation2.onrender.com/fyers-redirect",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        token = response.get("access_token")

        print("✅ FYERS ACCESS TOKEN GENERATED")

        return jsonify({
            "status": "activated",
            "next": "Save token as FYERS_ACCESS_TOKEN in Render ENV and redeploy"
        })

    except Exception as e:
        print("❌ TOKEN ERROR:", e)
        return jsonify({"error": str(e)}), 500

# ------------------------------------------------------------
# 5 MIN CANDLE ENGINE
# ------------------------------------------------------------
CANDLE_INTERVAL = 300
candles = {}
last_candle_vol = {}

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    prev_vol = last_candle_vol.get(symbol, c["cum_vol"])
    candle_vol = c["cum_vol"] - prev_vol
    last_candle_vol[symbol] = c["cum_vol"]

    print(
        f"\n🟩 5M CANDLE CLOSED | {symbol}"
        f"\nO:{c['open']} H:{c['high']} L:{c['low']} "
        f"C:{c['close']} V:{candle_vol}"
        f"\n-------------------------------"
    )

def update_candle_from_tick(msg):
    if not isinstance(msg, dict):
        return

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not symbol or ltp is None or vol is None or ts is None:
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

# ------------------------------------------------------------
# FYERS WEBSOCKET CALLBACKS
# ------------------------------------------------------------
def on_message(message):
    print("📩 TICK:", message)
    update_candle_from_tick(message)

def on_error(message):
    print("❌ WS ERROR:", message)

def on_close(message):
    print("🔌 WS CLOSED:", message)

def on_connect():
    print("🔗 WS CONNECTED")

    symbols = [
        "NSE:SBIN-EQ",
        "NSE:RELIANCE-EQ",
        "NSE:VEDL-EQ",
        "NSE:AXISBANK-EQ",
        "NSE:KOTAKBANK-EQ"
    ]

    print("📡 Subscribing:", symbols)
    fyers_ws.subscribe(symbols=symbols, data_type="SymbolUpdate")

# ------------------------------------------------------------
# START WEBSOCKET (KEEP_RUNNING FIX)
# ------------------------------------------------------------
def start_ws():
    global fyers_ws
    try:
        print("🧵 WS THREAD STARTED")

        fyers_ws = data_ws.FyersDataSocket(
            access_token=FYERS_ACCESS_TOKEN,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_connect=on_connect,
            reconnect=True
        )

        print("📡 Calling WS connect()")
        fyers_ws.connect()

        print("🔁 WS keep_running()")
        fyers_ws.keep_running()   # 🔥 CRITICAL FIX

    except Exception as e:
        print("🔥 WS CRASH:", e)

if FYERS_ACCESS_TOKEN:
    threading.Thread(target=start_ws, daemon=True).start()
else:
    print("⏳ WS WAITING – OPEN /activate")

# ------------------------------------------------------------
# START FLASK SERVER
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Flask starting on port {port}")
    app.run(host="0.0.0.0", port=port)
