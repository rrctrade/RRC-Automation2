# ============================================================
# signal_candle_order.py
# RR 1.5 ONLY → TRAILING SL (FINAL COMBINED LOG)
# ============================================================

from math import floor

ORDER_STATE = {}

RR_PROFIT = 750
LOCK_PROFIT = 200

def round_price(price):
    if price >= 500: unit = 1.0
    elif price >= 100: unit = 0.1
    else: unit = 0.05
    return floor(price / unit) * unit

def calc_qty(high, low, risk):
    rng = abs(high - low)
    return floor(risk / rng) if rng > 0 else 0

def place_signal_order(
    *, fyers, symbol, side, high, low,
    per_trade_risk, mode, signal_no, log_fn
):
    qty = calc_qty(high, low, per_trade_risk)
    if qty <= 0:
        log_fn(f"ORDER_SKIP | {symbol} | qty=0")
        return

    trigger = high if side == "BUY" else low
    txn = 1 if side == "BUY" else -1

    log_fn(
        f"ORDER_SIGNAL | {symbol} | {side} | "
        f"trigger={trigger} qty={qty} | SIGNAL#{signal_no}"
    )

    ORDER_STATE[symbol] = {
        "status": "PENDING",
        "side": side,
        "trigger": trigger,
        "qty": qty,
        "signal_high": high,
        "signal_low": low,
        "entry_price": None,
        "sl_price": None,
        "trail_done": False,
    }

def handle_ltp_event(*, fyers, symbol, ltp, mode, log_fn):
    state = ORDER_STATE.get(symbol)
    if not state: return

    side, qty = state["side"], state["qty"]

    # ENTRY
    if state["status"] == "PENDING":
        if (side == "BUY" and ltp >= state["trigger"]) or \
           (side == "SELL" and ltp <= state["trigger"]):

            entry = round_price(ltp)
            state["entry_price"] = entry

            init_sl = state["signal_low"] if side == "BUY" else state["signal_high"]
            state["sl_price"] = init_sl
            state["status"] = "SL_PLACED"

            log_fn(
                f"ORDER_EXECUTED | {symbol} | entry={entry}\n"
                f"SL_PLACED | {symbol} | SL={round(init_sl,2)} | MODE={mode}"
            )
        return
