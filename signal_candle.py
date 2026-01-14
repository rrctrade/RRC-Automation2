# ============================================================
# signal_candle.py
# REAL BUY Trigger Order on Signal Candle
# ============================================================

from datetime import datetime
import math

WAITING = "WAITING"
ORDER_PENDING = "ORDER_PENDING"

signal_state = {}

fyers = None
PER_TRADE_RISK = 0
MODE = "LIVE"

trade_on_order_placed = None
trade_on_order_failed = None


def init_symbols(symbols, fyers_obj, risk, mode,
                 on_order_placed, on_order_failed):
    global fyers, PER_TRADE_RISK, MODE
    global trade_on_order_placed, trade_on_order_failed

    fyers = fyers_obj
    PER_TRADE_RISK = risk
    MODE = mode

    trade_on_order_placed = on_order_placed
    trade_on_order_failed = on_order_failed

    signal_state.clear()
    for s in symbols:
        signal_state[s] = {"state": WAITING}


def on_candle_close(symbol, candle_label,
                    open_, high, low, close,
                    volume, is_lowest, color, bias):

    st = signal_state.get(symbol)
    if not st or st["state"] != WAITING:
        return None

    if not (bias == "B" and is_lowest and color == "RED"):
        return None

    risk_per_share = high - low
    if risk_per_share <= 0:
        return None

    qty = math.floor(PER_TRADE_RISK / risk_per_share)
    if qty <= 0:
        return None

    st["state"] = ORDER_PENDING

    if MODE == "PAPER":
        trade_on_order_placed(
            symbol, "PAPER_ORDER", "Paper order", qty, high, low
        )
        return

    payload = {
        "symbol": symbol,
        "qty": qty,
        "type": 4,              # STOP
        "side": 1,              # BUY
        "productType": "INTRADAY",
        "limitPrice": 0,
        "stopPrice": round(high, 2),
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False
    }

    try:
        res = fyers.place_order(payload)
        if res.get("s") == "ok":
            trade_on_order_placed(
                symbol, res.get("id"),
                res.get("message"), qty, high, low
            )
        else:
            raise Exception(res.get("message"))
    except Exception as e:
        st["state"] = WAITING
        trade_on_order_failed(symbol, str(e))
