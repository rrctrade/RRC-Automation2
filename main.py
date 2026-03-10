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
from sector_engine import SECTOR_LIST
from signal_candle_order import handle_signal_event, handle_ltp_event, ORDER_STATE

# ================= TIME & CONFIG =================
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
BT_FLOOR_TS = None
STOCK_BIAS_MAP = {}

candles = {}
last_base_vol = {}
last_ws_base_before_bias = {}
volume_history = {}
signal_counter = {}

tick_queue = Queue(maxsize=15000)

# ================= LOGGING (Debug Enabled) =================
def log(level, msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {level} | {msg}", flush=True)
    try:
        requests.post(WEBAPP_URL, json={"action": "pushLog", "payload": {"level": level, "message": msg}}, timeout=2)
    except: pass

# ================= CANDLE ENGINE (Stable Logic) =================
def close_live_candle(symbol, c):
    prev_base = last_base_vol.get(symbol)
    if prev_base is None: return

    candle_vol = c["base_vol"] - prev_base
    last_base_vol[symbol] = c["base_vol"]

    volume_history.setdefault(symbol, []).append(candle_vol)
    prev_min = min(volume_history[symbol][:-1]) if len(volume_history[symbol]) > 1 else None
    is_lowest = prev_min is not None and candle_vol < prev_min

    color = "RED" if c["open"] > c["close"] else "GREEN" if c["open"] < c["close"] else "DOJI"
    bias = STOCK_BIAS_MAP.get(symbol, "")
    
    offset = (c["start"] - BT_FLOOR_TS) // CANDLE_INTERVAL
    label = f"LIVE{offset + 3}"

    log("VOLCHK", f"{symbol} | {label} | V={round(candle_vol,1)} | lowest={is_lowest} | {color} {bias}")

    # SIGNAL TRIGGER LOGIC
    if is_lowest:
        state = ORDER_STATE.get(symbol)
        if state and state.get("status") == "PENDING":
            handle_signal_event(fyers=fyers, symbol=symbol, side=None, log_fn=lambda m: log("ORDER", m))

        if (bias == "B" and color == "RED") or (bias == "S" and color == "GREEN"):
            sc = signal_counter.get(symbol, 0) + 1
            signal_counter[symbol] = sc
            side = "BUY" if bias == "B" else "SELL"
            handle_signal_event(fyers=fyers, symbol=symbol, side=side, high=c["high"], low=c["low"], 
                                per_trade_risk=float(os.getenv("PER_TRADE_RISK", 500)), mode="PAPER", signal_no=sc, log_fn=lambda m: log("ORDER", m))

def update_candle(msg):
    symbol, ltp, base_vol, ts = msg.get("symbol"), msg.get("ltp"), msg.get("vol_traded_today"), msg.get("exch_feed_time")
    if not all([symbol, ltp, base_vol, ts]): return

    if not BIAS_DONE:
        last_ws_base_before_bias[symbol] = base_vol
        return

    if symbol not in ACTIVE_SYMBOLS: return

    # LTP Event for Order Tracking
    handle_ltp_event(fyers=fyers, symbol=symbol, ltp=ltp, mode="PAPER", log_fn=lambda m: log("ORDER", m))

    start = ts - (ts % CANDLE_INTERVAL)
    c = candles.get(symbol)

    if c is None or c["start"] != start:
        if c: close_live_candle(symbol, c)
        candles[symbol] = {"start": start, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "base_vol": base_vol}
        return

    c["high"], c["low"], c["close"], c["base_vol"] = max(c["high"], ltp), min(c["low"], ltp), ltp, base_vol

def tick_worker():
    while True:
        update_candle(tick_queue.get())

threading.Thread(target=tick_worker, daemon=True).start()

# ================= WS (Cloudflare & 403 Debug) =================
def on_message(msg):
    try: tick_queue.put_nowait(msg)
    except: pass

def on_connect():
    log("SYSTEM", f"DEBUG: WS CONNECTED. Attempting Throttled Sub for {len(ALL_SYMBOLS)} stocks.")
    batch_size = 5
    for i in range(0, len(ALL_SYMBOLS), batch_size):
        batch = ALL_SYMBOLS[i : i + batch_size]
        try:
            fyers_ws.subscribe(symbols=batch, data_type="SymbolUpdate")
            time.sleep(0.7) # Extra safe delay
        except Exception as e:
            log("DEBUG_ERR", f"Subscription Batch {i} Failed: {e}")
    log("SYSTEM", "DEBUG: All Initial Subscriptions Attempted.")

def start_ws():
    global fyers_ws
    fyers_ws = data_ws.FyersDataSocket(access_token=FYERS_ACCESS_TOKEN, on_message=on_message, on_connect=on_connect, reconnect=True)
    fyers_ws.connect()

threading.Thread(target=start_ws, daemon=True).start()

# ================= RECEIVE BIAS (Batch Support) =================
@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():
    global BT_FLOOR_TS, STOCK_BIAS_MAP, ACTIVE_SYMBOLS, BIAS_DONE
    data = request.get_json(force=True)
    
    selected = data.get("selected_stocks", [])
    strong = data.get("strong_sectors", [])
    is_first = data.get("is_first_batch", False)
    is_last = data.get("is_last_batch", False)

    if is_first:
        log("BIAS", "DEBUG: Receiving first batch from LOCAL.")
        ACTIVE_SYMBOLS.clear()
        STOCK_BIAS_MAP.clear()
        bias_ts = int(datetime.now(UTC).timestamp())
        BT_FLOOR_TS = bias_ts - (bias_ts % CANDLE_INTERVAL)

    # Map Creation
    for s in strong:
        key = SECTOR_LIST.get(s["sector"])
        if key in SECTOR_MAP:
            for sym in SECTOR_MAP[key]:
                STOCK_BIAS_MAP[sym] = "B" if s["bias"] == "BUY" else "S"

    for s in selected:
        ACTIVE_SYMBOLS.add(s)
        if s in last_ws_base_before_bias:
            last_base_vol[s] = last_ws_base_before_bias[s]

    if is_last:
        BIAS_DONE = True
        log("SYSTEM", f"DEBUG: Bias Sync Complete. Active Stocks: {len(ACTIVE_SYMBOLS)}")
        
        # History Fetch for C1, C2, C3
        for s in ACTIVE_SYMBOLS:
            res = fyers.history({"symbol": s, "resolution": "5", "date_format": "0", "range_from": BT_FLOOR_TS-900, "range_to": BT_FLOOR_TS-1, "cont_flag": "1"})
            if res.get("s") == "ok":
                for i, c in enumerate(res.get("candles", [])[-3:]):
                    volume_history.setdefault(s, []).append(c[5])
                    log("HISTORY", f"{s} | C{i+1} | V={c[5]}")

        # Unsubscribe others
        to_unsub = list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS)
        threading.Thread(target=lambda: [fyers_ws.unsubscribe(symbols=to_unsub[i:i+20]) or time.sleep(0.1) for i in range(0, len(to_unsub), 20)]).start()

    return jsonify({"status": "received"})

# ================= ROUTES =================
@app.route("/")
def health(): return jsonify({"status": "ok"})

@app.route("/fyers-redirect")
def fyers_redirect():
    log("SYSTEM", "FYERS redirect hit")
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
