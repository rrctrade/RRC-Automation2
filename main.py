# ============================================================
# RajanTradeAutomation – main.py (FINAL / SAFE)
# FYERS OAUTH ACTIVATION + LIVE WS + 5 MIN CAN# ============================================================
# RajanTradeAutomation – main.py
# Phase-0 : FYERS LIVE TICK BY TICK + 5 MIN CANDLE
# WS FLOW LOCKED | ONLY CANDLE LOGIC ADDED
# ============================================================

import os
import time
import threading
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# Basic Logs
# ------------------------------------------------------------
print("🚀 main.py STARTED")

# ------------------------------------------------------------
# ENV CHECK
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

print("🔍 ENV CHECK")
print("FYERS_CLIENT_ID =", FYERS_CLIENT_ID)
print(
    "FYERS_ACCESS_TOKEN prefix =",
    FYERS_ACCESS_TOKEN[:20] if FYERS_ACCESS_TOKEN else "❌ MISSING"
)

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN:
    raise Exception("❌ FYERS ENV variables missing")

# ------------------------------------------------------------
# Flask App (Ping + Redirects)
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok", "service": "RajanTradeAutomation"})

@app.route("/callback")
def fyers_callback():
    auth_code = request.args.get("auth_code")
    print("🔑 FYERS CALLBACK HIT | AUTH CODE =", auth_code)
    return jsonify({"status": "callback_received", "auth_code": auth_code})

@app.route("/fyers-redirect")
def fyers_redirect():
    auth_code = request.args.get("auth_code") or request.args.get("code")
    state = request.args.get("state")

    print("🔑 FYERS REDIRECT HIT")
    print("AUTH CODE =", auth_code)
    print("STATE =", state)

    return jsonify({
        "status": "redirect_received",
        "auth_code": auth_code,
        "state": state
    })

# ------------------------------------------------------------
# FYERS WebSocket
# ------------------------------------------------------------
print("📦 Importing fyers_apiv3 WebSocket")
from fyers_apiv3.FyersWebsocket import data_ws
print("✅ data_ws IMPORT SUCCESS")

# ------------------------------------------------------------
# 🔒 5-MIN CANDLE ENGINE (LOCAL, PROVEN)
# ------------------------------------------------------------
CANDLE_INTERVAL = 300  # 5 minutes

candles = {}          # symbol -> running candle
last_candle_vol = {}  # symbol -> last candle cumulative volume

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    prev_vol = last_candle_vol.get(symbol, c["cum_vol"])
    candle_vol = c["cum_vol"] - prev_vol
    last_candle_vol[symbol] = c["cum_vol"]

    print(
        f"\n🟩 5m CANDLE CLOSED | {symbol}"
        f"\nTime : {time.strftime('%H:%M:%S', time.localtime(c['start']))}"
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

    # NEW CANDLE
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

    # UPDATE RUNNING CANDLE
    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["cum_vol"] = vol

# ------------------------------------------------------------
# WebSocket Callbacks (WS FLOW UNCHANGED)
# ------------------------------------------------------------
def on_message(message):
    print("📩 WS MESSAGE:", message)
    update_candle_from_tick(message)   # ✅ ONLY ADDITION

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

    print("📡 Subscribing symbols:", symbols)

    fyers_ws.subscribe(
        symbols=symbols,
        data_type="SymbolUpdate"
    )

# ------------------------------------------------------------
# Start WebSocket (NON-BLOCKING) – 🔒 UNCHANGED
# ------------------------------------------------------------
def start_ws():
    try:
        print("🧵 WS THREAD STARTED")

        global fyers_ws
        fyers_ws = data_ws.FyersDataSocket(
            access_token=FYERS_ACCESS_TOKEN,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_connect=on_connect,
            reconnect=True
        )

        print("✅ FyersDataSocket CREATED")
        fyers_ws.connect()
        print("📶 WS CONNECT CALLED")

    except Exception as e:
        print("🔥 WS THREAD CRASHED:", e)

ws_thread = threading.Thread(target=start_ws, daemon=True)
ws_thread.start()

# ------------------------------------------------------------
# Start Flask (MAIN THREAD)
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Starting Flask on port {port}")
    app.run(host="0.0.0.0", port=port)
DLES
# ============================================================

import os
import time
import threading
from flask import Flask, jsonify, request

print("🚀 main.py STARTED")

# ------------------------------------------------------------
# ENV (ASSUMED SET ON RENDER)
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_SECRET_KEY = os.getenv("FYERS_SECRET_KEY")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

print("🔍 ENV CHECK")
print("FYERS_CLIENT_ID =", FYERS_CLIENT_ID[:10] + "..." if FYERS_CLIENT_ID else "❌ MISSING")
print("FYERS_SECRET_KEY =", "✅ SET" if FYERS_SECRET_KEY else "❌ MISSING")
print("FYERS_ACCESS_TOKEN =", FYERS_ACCESS_TOKEN[:20] + "..." if FYERS_ACCESS_TOKEN else "❌ MISSING")

if not FYERS_CLIENT_ID or not FYERS_SECRET_KEY:
    raise Exception("FYERS CLIENT ID / SECRET KEY missing")

# ------------------------------------------------------------
# FYERS IMPORTS
# ------------------------------------------------------------
from fyers_apiv3 import fyers_api
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
        "fyers_ready": bool(FYERS_ACCESS_TOKEN)
    })

# ------------------------------------------------------------
# ACTIVATE → PERMISSION PAGE
# ------------------------------------------------------------
@app.route("/activate")
def activate():
    session = fyers_api.SessionModel(
        client_id=FYERS_CLIENT_ID,
        secret_key=FYERS_SECRET_KEY,
        redirect_uri="https://rrc-automation2.onrender.com/fyers-redirect",
        response_type="code",
        grant_type="authorization_code"
    )

    auth_url = session.generate_authcode()
    print("ACTIVATION URL:", auth_url)

    return jsonify({
        "status": "activation_ready",
        "url": auth_url
    })

# ------------------------------------------------------------
# REDIRECT → TOKEN GENERATION
# ------------------------------------------------------------
@app.route("/fyers-redirect")
def fyers_redirect():
    auth_code = request.args.get("auth_code") or request.args.get("code")

    print("FYERS REDIRECT HIT")
    print("AUTH CODE =", auth_code)

    if not auth_code:
        return jsonify({"status": "auth_code_missing"})

    try:
        session = fyers_api.SessionModel(
            client_id=FYERS_CLIENT_ID,
            secret_key=FYERS_SECRET_KEY,
            redirect_uri="https://rrc-automation2.onrender.com/fyers-redirect",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()

        token = response.get("access_token")
        print("FYERS ACTIVATED, TOKEN PREFIX:", token[:20])

        return jsonify({
            "status": "activated",
            "token_preview": token[:20],
            "next": "Save token in Render ENV as FYERS_ACCESS_TOKEN and redeploy"
        })

    except Exception as e:
        print("TOKEN ERROR:", e)
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

    print("5M CANDLE CLOSED",
          symbol,
          "O", c["open"],
          "H", c["high"],
          "L", c["low"],
          "C", c["close"],
          "V", candle_vol)

def update_candle_from_tick(msg):
    if not isinstance(msg, dict):
        return

    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if symbol is None or ltp is None or vol is None or ts is None:
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
# FYERS WEBSOCKET
# ------------------------------------------------------------
def on_message(message):
    update_candle_from_tick(message)

def on_error(message):
    print("WS ERROR:", message)

def on_close(message):
    print("WS CLOSED")

def on_connect():
    print("WS CONNECTED")
    symbols = [
        "NSE:SBIN-EQ",
        "NSE:RELIANCE-EQ",
        "NSE:VEDL-EQ",
        "NSE:AXISBANK-EQ",
        "NSE:KOTAKBANK-EQ"
    ]
    fyers_ws.subscribe(symbols=symbols, data_type="SymbolUpdate")

def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_connect=on_connect,
        reconnect=True
    )
    fyers_ws.connect()

if FYERS_ACCESS_TOKEN:
    threading.Thread(target=start_ws, daemon=True).start()
else:
    print("WS WAITING FOR TOKEN – OPEN /activate")

# ------------------------------------------------------------
# START SERVER
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print("SERVER STARTING ON PORT", port)
    print("OPEN /activate TO ACTIVATE FYERS APP")
    app.run(host="0.0.0.0", port=port)
