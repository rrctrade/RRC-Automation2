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

# ================= CONFIG =================
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300

FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")

app = Flask(__name__)
fyers = fyersModel.FyersModel(client_id=FYERS_CLIENT_ID, token=FYERS_ACCESS_TOKEN, log_path="")

# ================= STATE =================
ALL_SYMBOLS = sorted(set(s for sector in SECTOR_MAP.values() for s in sector))
ACTIVE_SYMBOLS = set()
BIAS_DONE = False
BIAS_FLOOR_TS = None
candles = {}
last_base_vol = {}
last_ws_base_before_bias = {}
tick_queue = Queue(maxsize=10000)

# ================= LOGGING =================
def log(level, msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {msg}", flush=True)
    try:
        requests.post(WEBAPP_URL, json={"action": "pushLog", "payload": {"level": level, "message": msg}}, timeout=2)
    except: pass

# ================= CANDLE ENGINE =================
def update_candle(msg):
    global BIAS_DONE
    symbol, ltp, vol, ts = msg.get("symbol"), msg.get("ltp"), msg.get("vol_traded_today"), msg.get("exch_feed_time")
    if not all([symbol, ltp, vol, ts]): return

    if not BIAS_DONE:
        last_ws_base_before_bias[symbol] = vol
        return

    if symbol not in ACTIVE_SYMBOLS: return

    start = ts - (ts % CANDLE_INTERVAL)
    c = candles.get(symbol)
    if c is None or c["start"] != start:
        if c: # Close previous candle
            offset = int((c["start"] - BIAS_FLOOR_TS) / CANDLE_INTERVAL)
            v_diff = c["base_vol"] - last_base_vol.get(symbol, c["base_vol"])
            last_base_vol[symbol] = c["base_vol"]
            log("CANDLE", f"{symbol} | LIVE{offset+3} | O:{c['open']} C:{c['close']} V:{v_diff}")
        
        candles[symbol] = {"start": start, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "base_vol": vol}
        return
    
    c["high"], c["low"], c["close"], c["base_vol"] = max(c["high"], ltp), min(c["low"], ltp), ltp, vol

def tick_worker():
    while True:
        update_candle(tick_queue.get())

threading.Thread(target=tick_worker, daemon=True).start()

# ================= WEBSOCKET =================
def on_message(msg):
    try: tick_queue.put_nowait(msg)
    except: pass

def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(access_token=FYERS_ACCESS_TOKEN, on_message=on_message, 
                                      on_connect=lambda: fyers_ws.subscribe(symbols=ALL_SYMBOLS, data_type="SymbolUpdate"), 
                                      reconnect=True)
    fyers_ws.connect()

threading.Thread(target=start_ws, daemon=True).start()

# ================= RECEIVE BIAS (Batch Logic) =================
@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():
    global ACTIVE_SYMBOLS, BIAS_DONE, BIAS_FLOOR_TS

    data = request.get_json(force=True)
    selected = data.get("selected_stocks", [])
    is_first = data.get("is_first_batch", False)
    is_last = data.get("is_last_batch", False)

    if is_first:
        log("BIAS", "Started receiving new stock batches...")
        ACTIVE_SYMBOLS.clear()
        bias_ts = int(datetime.now(UTC).timestamp())
        BIAS_FLOOR_TS = bias_ts - (bias_ts % CANDLE_INTERVAL)

    # बॅचमधील स्टॉक्स ॲड करणे
    for s in selected:
        ACTIVE_SYMBOLS.add(s)
        # History fetch logic can be added here if needed

    if is_last:
        BIAS_DONE = True
        log("SYSTEM", f"Bias Complete. Active Stocks: {len(ACTIVE_SYMBOLS)}")
        # Unsubscribe non-active
        to_unsub = list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS)
        threading.Thread(target=lambda: fyers_ws.unsubscribe(symbols=to_unsub)).start()

    return jsonify({"status": "batch_received"})

@app.route("/")
def health(): return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
