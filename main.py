# ============================================================
# RajanTradeAutomation – FINAL ENGINE
# WS + Candle Engine + Local Bias Compatible
# ============================================================

import os
import threading
import requests
from datetime import datetime
import pytz

from flask import Flask, jsonify, request
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

from sector_mapping import SECTOR_MAP
from sector_engine import SECTOR_LIST


IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
CANDLE_INTERVAL = 300


FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL")


def get_settings():

    for _ in range(3):
        try:
            r = requests.post(
                WEBAPP_URL,
                json={"action": "getSettings"},
                timeout=5
            )

            if r.ok:
                return r.json().get("settings", {})

        except:
            pass

    raise RuntimeError("Settings fetch failed")


SETTINGS = get_settings()

MODE = SETTINGS.get("MODE", "PAPER")
BIAS_TIME = SETTINGS.get("BIAS_TIME", "09:30:10")
BUY_SECTOR_COUNT = int(SETTINGS.get("BUY_SECTOR_COUNT", 0))
SELL_SECTOR_COUNT = int(SETTINGS.get("SELL_SECTOR_COUNT", 0))
PER_TRADE_RISK = float(SETTINGS.get("PER_TRADE_RISK", 500))


app = Flask(__name__)


fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=""
)


ALL_SYMBOLS = sorted({
    s for v in SECTOR_MAP.values() for s in v
})


candles = {}
last_base_vol = {}

ACTIVE_SYMBOLS = set()
BIAS_DONE = False


def log(msg):

    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def on_message(msg):

    symbol = msg.get("symbol")

    if symbol not in ACTIVE_SYMBOLS:
        return


def on_connect():

    log("WS CONNECTED")

    fyers_ws.subscribe(
        symbols=ALL_SYMBOLS,
        data_type="SymbolUpdate"
    )

    log(f"SUBSCRIBED_SYMBOLS={len(ALL_SYMBOLS)}")


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


@app.route("/push-sector-bias", methods=["POST"])
def receive_bias():

    global ACTIVE_SYMBOLS
    global BIAS_DONE

    data = request.get_json(force=True)

    strong = data.get("strong_sectors", [])
    selected = data.get("selected_stocks", [])

    log("BIAS RECEIVED")

    ACTIVE_SYMBOLS = set(selected)

    unsubscribe = list(set(ALL_SYMBOLS) - ACTIVE_SYMBOLS)

    if unsubscribe:

        fyers_ws.unsubscribe(
            symbols=unsubscribe,
            data_type="SymbolUpdate"
        )

    log(f"ACTIVE_SYMBOLS={len(ACTIVE_SYMBOLS)}")

    BIAS_DONE = True

    return jsonify({"status": "bias_received"})


@app.route("/")
def health():
    return jsonify({"status": "ok"})


@app.route("/fyers-redirect")
def fyers_redirect():
    log("FYERS REDIRECT HIT")
    return jsonify({"status": "ok"})


if __name__ == "__main__":

    log("ENGINE START")

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
