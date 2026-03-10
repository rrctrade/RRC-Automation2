import os
import time
import threading
import requests
from datetime import datetime
import pytz
from queue import Queue
from flask import Flask, jsonify, request

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
from sector_mapping import SECTOR_MAP

# ================= TIME & CONFIG =================
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300

FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

if not FYERS_CLIENT_ID or not FYERS_ACCESS_TOKEN or not WEBAPP_URL:
    raise RuntimeError("Missing ENV variables")

app = Flask(__name__)

# ================= FYERS INITIALIZATION =================
fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)

# ================= STATE MANAGEMENT =================
ALL_SYMBOLS = sorted(set(s for sector in SECTOR_MAP.values() for s in sector))
ACTIVE_SYMBOLS = set()
BIAS_DONE = False
BIAS_FLOOR_TS = None

candles = {}
last_base_vol = {}
last_ws_base_before_bias = {}
tick_queue = Queue(maxsize=15000)

# ================= LOGGING =================
def log(level, msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {msg}", flush=True)
    try:
        requests.post(
            WEBAPP_URL, 
            json={"action": "pushLog", "payload": {"level": level, "message": msg}}, 
            timeout=3
        )
    except:
        pass

# ================= CANDLE ENGINE =================
def candle_color(o, c):
    return "G" if c > o else "R" if c < o else "D"

def update_candle(msg):
    global BIAS_DONE
    symbol = msg.get("symbol")
    ltp = msg.get("ltp")
    vol = msg.get("vol_traded_today")
    ts = msg.get("exch_feed_time")

    if not all([symbol, ltp, vol, ts]): return

    if not BIAS_DONE:
        last_ws_base_before_bias[symbol] = vol
        return

    if symbol not in ACTIVE_SYMBOLS: return

    start = ts - (ts % CANDLE_INTERVAL)
    c = candles.get(symbol)

    if c is None or c["start"] != start:
        if c: # Previous candle closing logic
            offset = int((c["start"] - BIAS_FLOOR_TS) / CANDLE_INTERVAL)
            label = f"LIVE{offset+3}"
            v_diff = c["base_vol"] - last_base_vol.get(symbol, c["base_vol"])
            last_base_vol[symbol] = c["base_vol"]
            
            color = candle_color(c["open"], c["close"])
            bias_char = "B" if symbol in ACTIVE_SYMBOLS else "S" # Simplified for log
            
            log("CANDLE", f"{symbol} | {label} | O:{c['open']} H:{c['high']} L:{c['low']} C:{c['close']} | V:{v_diff} | {color} | {bias_char}")
        
        candles[symbol] = {"start": start, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "base_vol": vol}
        return
    
    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    c["base_vol"] = vol

def tick_worker():
    while True:
        msg = tick_queue.get()
        update_candle(msg)
        tick_queue.task_done()

threading.Thread(target=tick_worker, daemon=True).start()

# ================= WS (Throttled for Cloudflare Fix) =================
def on_message(msg):
    try: tick_queue.put_nowait(msg)
    except: pass

def on_connect():
    log("SYSTEM", f"WS CONNECTED - Starting Throttled Sub for {len(ALL_SYMBOLS)} stocks")
    # तुकड्या-तुकड्यांत सबस्क्रिप्शन (Cloudflare सुरक्षित ठेवण्यासाठी)
    batch_size = 10
    for i in range(0, len(ALL_SYMBOLS), batch_size):
        batch = ALL_SYMBOLS[i : i + batch_size]
        try:
            fyers_ws.subscribe(symbols=batch, data_type="SymbolUpdate")
            time.sleep(0.2) # स्मूथ सबस्क्रिप्शनसाठी छोटा गॅप
        except Exception as e:
            print(f"Sub Error: {e}")
    log("SYSTEM", "Throttled Subscription Complete.")

def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN, 
        on_message=on_message, 
        on_connect=on_connect, 
        reconnect=True
    )
    fyers_ws.connect()

threading.Thread(target=start_ws, daemon=True).start()

# ================= ROUTES (Stable Routes Restored) =================
@app.route("/")
def health():
    return jsonify({"status": "ok", "active_count": len(ACTIVE_SYMBOLS)})

@app.route("/ping")
def ping():
    return jsonify({"status": "alive"})

@app.route("/fyers-redirect")
def fyers_redirect():
    log("SYSTEM", "FYERS redirect hit")
    return jsonify({"status": "ok"})

# ================= RECEIVE BIAS (Batch Handling) =================
@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():
    global ACTIVE_SYMBOLS, BIAS_DONE, BIAS_FLOOR_TS

    data = request.get_json(force=True)
    selected = data.get("selected_stocks", [])
    is_first = data.get("is_first_batch", False)
    is_last = data.get("is_last_batch", False)

    if is_first:
        log("BIAS", "Receiving stocks from LOCAL...")
        ACTIVE_SYMBOLS.clear()
        bias_ts = int(datetime.now(UTC).timestamp())
        BIAS_FLOOR_TS = bias_ts - (bias_ts % CANDLE_INTERVAL)

    for s in selected:
        ACTIVE_SYMBOLS.add(s)
        if s in last_ws_base_before_bias:
            last_base_vol[s] = last_ws_base_before_bias[s]

    if is_last:
        BIAS_DONE = True
        log("SYSTEM", f"Bias Process Complete. Active: {len(ACTIVE_SYMBOLS)}")
        
        # अनावश्यक स्टॉक्स अनसबस्क्राईब करणे
        to_unsub = list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS)
        def throttled_unsub():
            for i in range(0, len(to_unsub), 20):
                fyers_ws.unsubscribe(symbols=to_unsub[i:i+20])
                time.sleep(0.1)
        threading.Thread(target=throttled_unsub).start()

    return jsonify({"status": "batch_processed", "current_count": len(ACTIVE_SYMBOLS)})

# ================= START =================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
