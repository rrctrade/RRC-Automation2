# ============================================================
# RajanTradeAutomation – main.py (ACTIVATION READY!)
# Phase-0 : FYERS LIVE TICK BY TICK + 5 MIN CANDLE + AUTO ACTIVATION
# ============================================================

import os
import time
import threading
from flask import Flask, jsonify, request

# ------------------------------------------------------------
# FYERS ACTIVATION IMPORT (TOP)
# ------------------------------------------------------------
print("🚀 main.py STARTED - ACTIVATION READY")

try:
    from fyers_apiv3 import fyers_api
    print("✅ fyers_apiv3 IMPORTED")
except ImportError:
    print("⚠️ fyers_apiv3 not installed - install: pip install fyers-apiv3")

# ------------------------------------------------------------
# ENV CHECK (राजनट्रेड details)
# ------------------------------------------------------------
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID") or "NEJM5X-XXXXXXXX"  # तुझा ID
FYERS_SECRET_KEY = os.getenv("FYERS_SECRET_KEY") or "RAIWEKTXXXXXXXX"  # तुझा Secret
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

print("🔍 ENV CHECK - राजनट्रेड")
print("FYERS_CLIENT_ID =", FYERS_CLIENT_ID[:10] + "..." if FYERS_CLIENT_ID else "❌ MISSING")
print("FYERS_SECRET_KEY =", "✅ SET" if FYERS_SECRET_KEY else "❌ MISSING")
print("FYERS_ACCESS_TOKEN =", FYERS_ACCESS_TOKEN[:20] if FYERS_ACCESS_TOKEN else "❌ MISSING")

# ------------------------------------------------------------
# Flask App (ACTIVATION + ORIGINAL)
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({
        "status": "ok", 
        "service": "RajanTradeAutomation ✅",
        "fyers_ready": bool(FYERS_ACCESS_TOKEN),
        "client_id": FYERS_CLIENT_ID[:10] + "..."
    })

@app.route("/activate")
def activate():
    """🚀 1-CLICK ACTIVATION URL"""
    if not FYERS_SECRET_KEY:
        return jsonify({"error": "FYERS_SECRET_KEY missing in ENV"}), 400
    
    session = fyers_api.SessionModel(
        client_id=FYERS_CLIENT_ID,
        secret_key=FYERS_SECRET_KEY,
        redirect_uri="https://rrc-automation2.onrender.com/fyers-redirect",
        response_type="code",
        grant_type="authorization_code"
    )
    
    auth_url = session.generate_authcode()
    print("🔗 ACTIVATION URL GENERATED:", auth_url)
    
    return jsonify({
        "status": "activation_ready",
        "message": "राजनट्रेड activate करा!",
        "url": auth_url,
        "next": "Login → Permission → Auto Redirect → TOKEN READY"
    })

@app.route("/fyers-redirect")
def fyers_redirect():
    """🔑 AUTO TOKEN GENERATION"""
    auth_code = request.args.get("auth_code") or request.args.get("code")
    state = request.args.get("state")
    
    print("🔑 FYERS REDIRECT HIT")
    print("AUTH CODE =", auth_code)
    print("STATE =", state)
    
    if auth_code and FYERS_SECRET_KEY:
        try:
            session = fyers_api.SessionModel(
                client_id=FYERS_CLIENT_ID,
                secret_key=FYERS_SECRET_KEY,
                redirect_uri="https://rrc-automation2.onrender.com/fyers-redirect"
            )
            session.set_token(auth_code)
            response = session.generate_token()
            
            token = response["access_token"]
            print(f"🎉 राजनट्रेड ACTIVATED! TOKEN: {token[:20]}...")
            
            return jsonify({
                "status": "✅ ACTIVATED!",
                "message": "राजनट्रेड app active झाला!",
                "token_preview": token[:20] + "...",
                "expires": response["expires_at"],
                "next_step": "1. Copy full token 2. Render ENV → FYERS_ACCESS_TOKEN 3. Redeploy"
            })
        except Exception as e:
            print("❌ TOKEN ERROR:", e)
            return jsonify({"error": str(e)}), 500
    
    return jsonify({
        "status": "redirect_received",
        "auth_code": auth_code or "missing",
        "next": "https://rrc-automation2.onrender.com/activate"
    })

# तुझा /callback (backup)
@app.route("/callback")
def fyers_callback():
    return fyers_redirect()  # Same logic

# ------------------------------------------------------------
# तुझा ORIGINAL CANDLE + WS CODE (UNTOUCHED)
# ------------------------------------------------------------
print("📦 Importing fyers_apiv3 WebSocket")
from fyers_apiv3.FyersWebsocket import data_ws
print("✅ data_ws IMPORT SUCCESS")

CANDLE_INTERVAL = 300
candles = {}
last_candle_vol = {}

def candle_start(ts):
    return ts - (ts % CANDLE_INTERVAL)

def close_candle(symbol, c):
    prev_vol = last_candle_vol.get(symbol, c["cum_vol"])
    candle_vol = c["cum_vol"] - prev_vol
    last_candle_vol[symbol] = c["cum_vol"]
    print(f"
🟩 5m CANDLE CLOSED | {symbol} | O:{c['open']} H:{c['high']} L:{c['low']} C:{c['close']} V:{candle_vol}")

def update_candle_from_tick(msg):
    if not isinstance(msg, dict): return
    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")
    if not all([symbol, ltp, vol, ts]): return
    
    start = candle_start(ts)
    c = candles.get(symbol)
    if c is None or c["start"] != start:
        if c: close_candle(symbol, c)
        candles[symbol] = {"start": start, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "cum_vol": vol}
        return
    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["cum_vol"] = vol

def on_message(message): update_candle_from_tick(message)
def on_error(message): print("❌ WS ERROR:", message)
def on_close(message): print("🔌 WS CLOSED:", message)
def on_connect():
    print("🔗 WS CONNECTED ✅")
    symbols = ["NSE:SBIN-EQ", "NSE:RELIANCE-EQ", "NSE:VEDL-EQ", "NSE:AXISBANK-EQ", "NSE:KOTAKBANK-EQ"]
    print("📡 Subscribing:", symbols)
    fyers_ws.subscribe(symbols=symbols, data_type="SymbolUpdate")

# WS START (ONLY IF TOKEN)
def start_ws():
    if not FYERS_ACCESS_TOKEN:
        print("⏳ WS WAITING FOR TOKEN...")
        return
    try:
        print("🧵 WS THREAD STARTED")
        global fyers_ws
        fyers_ws = data_ws.FyersDataSocket(
            access_token=FYERS_ACCESS_TOKEN,
            on_message=on_message, on_error=on_error, on_close=on_close, on_connect=on_connect,
            reconnect=True
        )
        fyers_ws.connect()
    except Exception as e:
        print("🔥 WS CRASHED:", e)

# AUTO WS START
if FYERS_ACCESS_TOKEN:
    threading.Thread(target=start_ws, daemon=True).start()
else:
    print("🔄 Token missing - Go to /activate")

# ------------------------------------------------------------
# START SERVER
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 राजनट्रेड starting on port {port}")
    print("🚀 1. https://rrc-automation2.onrender.com/activate")
    print("🚀 2. Login → Permission → TOKEN GET!")
    app.run(host="0.0.0.0", port=port)
