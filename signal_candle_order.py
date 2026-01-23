# ============================================================
# signal_candle_order.py
# STEP-4C : ENTRY → SL PLACE → SL EXECUTION (FINAL)
# ============================================================

from math import floor, ceil

# ------------------------------------------------------------
# ORDER STATE (authoritative, in-memory)
# ------------------------------------------------------------
# symbol -> {
#   status: NONE / PENDING / EXECUTED / SL_PLACED / SL_HIT
#   side: BUY / SELL
#   trigger: float
#   signal_no: int
#   qty: int
#   signal_high: float
#   signal_low: float
#   sl_price: float
#   sl_side: BUY / SELL
# }
ORDER_STATE = {}

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def is_frozen(symbol):
    state = ORDER_STATE.get(symbol)
    return state and state.get("status") == "SL_HIT"

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
# CANCEL PENDING ENTRY ORDER
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

    log_fn(f"ORDER_CANCEL | {symbol} | reason={reason} | MODE={mode}")

    if mode == "LIVE":
        try:
            fyers.cancel_order({"symbol": symbol})
        except Exception as e:
            log_fn(f"LIVE_CANCEL_ERROR | {symbol} | {e}")

    ORDER_STATE[symbol]["status"] = "NONE"

# ------------------------------------------------------------
# PLACE ENTRY SIGNAL ORDER (SL-M)
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
        log_fn(f"ORDER_SKIP | {symbol} | qty=0 | range={round(candle_range,4)}")
        return

    if side == "BUY":
        trigger_price = ceil(high * 1.0005)
        txn_type = 1
    else:
        trigger_price = floor(low * 0.9995)
        txn_type = -1

    log_fn(
        f"ORDER_SIGNAL | {symbol} | {side} | "
        f"trigger={trigger_price} qty={qty} "
        f"range={round(candle_range,4)} | SIGNAL#{signal_no} | MODE={mode}"
    )

    if mode != "LIVE":
        log_fn(f"PAPER_TRIGGER_ORDER_PLACED | {symbol} | trigger={trigger_price}")
    else:
        fyers.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 3,
            "side": txn_type,
            "productType": "INTRADAY",
            "stopPrice": trigger_price,
            "validity": "DAY",
            "offlineOrder": False,
        })
        log_fn(f"LIVE_TRIGGER_ORDER_PLACED | {symbol}")

    ORDER_STATE[symbol] = {
        "status": "PENDING",
        "side": side,
        "trigger": trigger_price,
        "signal_no": signal_no,
        "qty": qty,
        "signal_high": high,
        "signal_low": low,
    }

# ------------------------------------------------------------
# HANDLE SIGNAL EVENT
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
    if is_frozen(symbol):
        return

    state = ORDER_STATE.get(symbol)

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
# HANDLE LOWEST EVENT
# ------------------------------------------------------------
def handle_lowest_event(
    *,
    fyers,
    symbol,
    mode,
    log_fn
):
    if is_frozen(symbol):
        return

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
# PLACE STOPLOSS ORDER
# ------------------------------------------------------------
def place_stoploss_order(
    *,
    fyers,
    symbol,
    mode,
    log_fn
):
    state = ORDER_STATE.get(symbol)
    if not state:
        return

    side = state["side"]
    qty = state["qty"]

    # BUY entry → SELL SL at LOW
    if side == "BUY":
        sl_price = state["signal_low"]
        sl_side = "SELL"
    # SELL entry → BUY SL at HIGH
    else:
        sl_price = state["signal_high"]
        sl_side = "BUY"

    state["sl_price"] = sl_price
    state["sl_side"] = sl_side
    state["status"] = "SL_PLACED"

    log_fn(
        f"SL_ORDER_SIGNAL | {symbol} | entry_side={side} | SL={sl_price} | MODE={mode}"
    )

    if mode != "LIVE":
        log_fn(f"PAPER_SL_ORDER_PLACED | {symbol} | SL={sl_price}")
        return

    fyers.place_order({
        "symbol": symbol,
        "qty": qty,
        "type": 3,
        "side": -1 if sl_side == "SELL" else 1,
        "productType": "INTRADAY",
        "stopPrice": sl_price,
        "validity": "DAY",
        "offlineOrder": False,
    })

    log_fn(f"LIVE_SL_ORDER_PLACED | {symbol} | SL={sl_price}")

# ------------------------------------------------------------
# HANDLE LTP EVENT (ENTRY + SL EXECUTION)
# ------------------------------------------------------------
def handle_ltp_event(
    *,
    fyers,
    symbol,
    ltp,
    mode,
    log_fn
):
    state = ORDER_STATE.get(symbol)
    if not state:
        return

    # ---------------- ENTRY EXECUTION ----------------
    if state.get("status") == "PENDING":
        side = state["side"]
        trigger = state["trigger"]

        executed = (
            (side == "BUY" and ltp >= trigger) or
            (side == "SELL" and ltp <= trigger)
        )

        if executed:
            state["status"] = "EXECUTED"

            log_fn(
                f"ORDER_EXECUTED | {symbol} | side={side} | "
                f"trigger={trigger} | ltp={ltp} | MODE={mode}"
            )

            place_stoploss_order(
                fyers=fyers,
                symbol=symbol,
                mode=mode,
                log_fn=log_fn
            )
        return

    # ---------------- SL EXECUTION ----------------
    if state.get("status") == "SL_PLACED":
        sl_price = state["sl_price"]
        sl_side = state["sl_side"]

        sl_hit = (
            (sl_side == "SELL" and ltp <= sl_price) or
            (sl_side == "BUY" and ltp >= sl_price)
        )

        if sl_hit:
            state["status"] = "SL_HIT"

            log_fn(
                f"SL_EXECUTED | {symbol} | side={sl_side} | "
                f"SL={sl_price} | ltp={ltp} | MODE={mode}"
            )

# ------------------------------------------------------------
# EXPORTS
# ------------------------------------------------------------
__all__ = [
    "handle_signal_event",
    "handle_lowest_event",
    "handle_ltp_event",
]
