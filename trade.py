# ============================================================
# trade.py
# Order Lifecycle Tracker
# ============================================================

from datetime import datetime

TRADES = {}

def now():
    return datetime.now().strftime("%H:%M:%S")


def on_order_placed(symbol, order_id, message, qty, entry, sl):
    TRADES[order_id] = {
        "symbol": symbol,
        "status": "PENDING",
        "qty": qty,
        "entry": entry,
        "sl": sl,
        "message": message,
        "time": now()
    }
    print(f"[TRADE] {symbol} | PENDING | {message}", flush=True)


def on_order_failed(symbol, message):
    print(f"[TRADE] {symbol} | FAILED | {message}", flush=True)
