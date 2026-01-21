# ============================================================
# signal_candle_order.py
# STEP-3B-1 : Pending Order Cancel / Replace Logic
# ============================================================

from math import floor, ceil

# ------------------------------------------------------------
# ORDER STATE (authoritative, in-memory)
# ------------------------------------------------------------
ORDER_STATE = {}
# symbol -> {
#   status: NONE / PENDING / EXECUTED
#   side: BUY / SELL
#   trigger: float
#   signal_no: int
# }

# ------------------------------------------------------------
# QUANTITY CALCULATION
# ------------------------------------------------------------
def calculate_quantity(high, low, per_trade_risk):
    candle_range = abs(high - low)
    if candle_range <= 0:
        return 0, candle_range

    qty = floor(per_trade_risk / candle_range)
    return qty, candle_range

# ------------------------------------------------------------
# CANCEL PENDING ORDER
# ------------------------------------------------------------
def cancel_pending_order(
    *,
    fyers,
    symbol,
    mode,
    reason,
    log_fn
):
    state = ORDER_STATE.get(symbol)

    if not state or state.get("status") != "PENDING":
        return

    log_fn(
        f"ORDER_CANCEL | {symbol} | reason={reason} | MODE={mode}"
    )

    if mode == "LIVE":
        try:
            # NOTE: real order id handling will come later
            fyers.cancel_order({"symbol": symbol})
        except Exception as e:
            log_fn(f"LIVE_CANCEL_ERROR | {symbol} | {e}")

    ORDER_STATE[symbol]["status"] = "NONE"

# ------------------------------------------------------------
# PLACE SIGNAL ORDER (SL-M)
# ------------------------------------------------------------
def place_signal_order(
    *,
    fyers,
    symbol,
    side,
    high,
    low,
    per_trade_risk,
    mode,
    signal_no,
    log_fn
):
    qty, candle_range = calculate_quantity(high, low, per_trade_risk)

    if qty <= 0:
        log_fn(
            f"ORDER_SKIP | {symbol} | qty=0 | range={round(candle_range,4)}"
        )
        return

    if side == "BUY":
        trigger_price = ceil(high * 1.0005)
        txn_type = 1
    else:
        trigger_price = floor(low * 0.9995)
        txn_type = -1

    order_payload = {
        "symbol": symbol,
        "qty": qty,
        "type": 3,  # STOP-MARKET
        "side": txn_type,
        "productType": "INTRADAY",
        "stopPrice": trigger_price,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
    }

    log_fn(
        f"ORDER_SIGNAL | {symbol} | {side} | "
        f"trigger={trigger_price} qty={qty} "
        f"range={round(candle_range,4)} | SIGNAL#{signal_no} | MODE={mode}"
    )

    if mode != "LIVE":
        log_fn(
            f"PAPER_TRIGGER_ORDER_PLACED | {symbol} | trigger={trigger_price}"
        )
    else:
        try:
            fyers.place_order(order_payload)
            log_fn(f"LIVE_TRIGGER_ORDER_PLACED | {symbol}")
        except Exception as e:
            log_fn(f"LIVE_ORDER_ERROR | {symbol} | {e}")
            return

    ORDER_STATE[symbol] = {
        "status": "PENDING",
        "side": side,
        "trigger": trigger_price,
        "signal_no": signal_no,
    }

# ------------------------------------------------------------
# HANDLE NEW SIGNAL (STEP-3B-1 ENTRY POINT)
# ------------------------------------------------------------
def handle_signal_event(
    *,
    fyers,
    symbol,
    side,
    high,
    low,
    per_trade_risk,
    mode,
    signal_no,
    log_fn
):
    state = ORDER_STATE.get(symbol)

    # SIGNAL#1 → simple place
    if signal_no == 1:
        place_signal_order(
            fyers=fyers,
            symbol=symbol,
            side=side,
            high=high,
            low=low,
            per_trade_risk=per_trade_risk,
            mode=mode,
            signal_no=signal_no,
            log_fn=log_fn
        )
        return

    # SIGNAL#2+ → cancel + replace
    if state and state.get("status") == "PENDING":
        cancel_pending_order(
            fyers=fyers,
            symbol=symbol,
            mode=mode,
            reason="CANCEL_SIGNAL_UPDATE",
            log_fn=log_fn
        )

    place_signal_order(
        fyers=fyers,
        symbol=symbol,
        side=side,
        high=high,
        low=low,
        per_trade_risk=per_trade_risk,
        mode=mode,
        signal_no=signal_no,
        log_fn=log_fn
    )

# ------------------------------------------------------------
# HANDLE LOWEST UPDATE (NO SIGNAL)
# ------------------------------------------------------------
def handle_lowest_event(
    *,
    fyers,
    symbol,
    mode,
    log_fn
):
    state = ORDER_STATE.get(symbol)
    if state and state.get("status") == "PENDING":
        cancel_pending_order(
            fyers=fyers,
            symbol=symbol,
            mode=mode,
            reason="CANCEL_LOWEST_UPDATE",
            log_fn=log_fn
        )

# ------------------------------------------------------------
# EXPORTS
# ------------------------------------------------------------
__all__ = [
    "handle_signal_event",
    "handle_lowest_event",
]
