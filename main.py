# ============================================================
# RajanTradeAutomation – main.py
# FINAL FIXED VERSION
# Tick + Candle engine with Settings-based timing
# ============================================================

import os
import time
import threading
import requests
from datetime import datetime, time as dtime
import pytz

from flask import Flask

from fyers_apiv3.FyersWebsocket import data_ws

from sector_engine import run_sector_bias
from sector_mapping import SECTOR_MAP

# ============================================================
# BASIC
# ============================================================

IST = pytz.timezone("Asia/Kolkata")

WEBAPP_URL = os.getenv("WEBAPP_URL")

app = Flask(__name__)

# ============================================================
# LOGGING → GOOGLE SHEETS (Logs sheet)
# ============================================================

def log_info(msg):
    print(msg)
    try:
        requests.post(
            WEBAPP_URL,
            json={
                "action": "pushState",
                "payload": {
                    "items": [{
                        "key": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
                        "value": msg
                    }]
                }
            },
            timeout=3
        )
    except:
        pass

# ============================================================
# SETTINGS
# ============================================================

def load_settings():
    res = requests.post(
        WEBAPP_URL,
        json={"action": "getSettings"},
        timeout=5
    ).json()

    settings = res.get("settings", {})
    log_info("SETTINGS_LOADED")
    return settings

def parse_time(tstr):
    h, m, s = map(int, tstr.split(":"))
    return dtime(h, m, s)

# ============================================================
# GLOBAL STATE
# ============================================================

settings = {}
tick_start_time = None
bias_time = None

tick_engine_started = False
bias_done = False

# ============================================================
# FYERS WS CALLBACKS
# ============================================================

def on_message(message):
    # ticks silently consumed
    pass

def on_error(message):
    print("WS_ERROR", message)

def on_close(message):
    print("WS_CLOSED", message)

def on_open():
    global tick_engine_started
    log_info("WS_CONNECTED")

# ============================================================
# FYERS WS INIT
# ============================================================

def start_ws():
    ws = data_ws.FyersDataSocket(
        access_token=os.getenv("FYERS_ACCESS_TOKEN"),
        log_path="",
        litemode=True,
        write_to_file=False,
        reconnect=True
    )

    ws.on_connect = on_open
    ws.on_message = on_message
    ws.on_error = on_error
    ws.on_close = on_close

    symbols = []
    for lst in SECTOR_MAP.values():
        symbols.extend(lst)

    symbols = list(set(symbols))

    ws.subscribe(symbols=symbols, data_type="SymbolUpdate")
    ws.keep_running()

# ============================================================
# TICK ENGINE CONTROL
# ============================================================

def tick_engine_controller():
    global tick_engine_started

    now = datetime.now(IST)

    if now.time() < tick_start_time:
        log_info(f"WAITING_FOR_TICK_START = {tick_start_time.strftime('%H:%M:%S')}")
        while datetime.now(IST).time() < tick_start_time:
            time.sleep(1)
    else:
        log_info("TICK_START_TIME already passed, starting immediately")

    tick_engine_started = True
    log_info("TICK_ENGINE_ACTIVATED")

# ============================================================
# BIAS CONTROLLER
# ============================================================

def bias_controller():
    global bias_done

    while True:
        now = datetime.now(IST)

        if not bias_done and now.time() >= bias_time:
            log_info("BIAS_TIME_REACHED")

            result = run_sector_bias()

            # unsubscribe logic handled inside engine
            log_info(f"BIAS_DONE | Selected stocks: {len(result['selected_stocks'])}")

            bias_done = True
            break

        time.sleep(1)

# ============================================================
# MAIN RUNTIME
# ============================================================

def runtime():
    global settings, tick_start_time, bias_time

    log_info("SYSTEM_STARTED")

    settings = load_settings()

    tick_start_time = parse_time(settings.get("TICK_START_TIME", "09:15:00"))
    bias_time = parse_time(settings.get("BIAS_TIME", "09:25:05"))

    log_info("RUNTIME_INIT_DONE")

    # Threads
    threading.Thread(target=start_ws, daemon=True).start()
    threading.Thread(target=tick_engine_controller, daemon=True).start()
    threading.Thread(target=bias_controller, daemon=True).start()

# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def health():
    return {"status": "ok"}

# ============================================================
# BOOT
# ============================================================

if __name__ == "__main__":
    runtime()
    app.run(host="0.0.0.0", port=10000)
