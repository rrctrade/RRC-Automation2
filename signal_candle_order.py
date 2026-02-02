# ============================================================
# signal_candle_order.py
# STEP-4E : RR-BASED TRAILING (1.5R → +200 LOCK)
# ============================================================

from math import floor, ceil

# ------------------------------------------------------------
# ORDER STATE (authoritative, in-memory)
# ------------------------------------------------------------
ORDER_STATE = {}

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def is_frozen(symbol):
    state = ORDER_STATE.get(symbol)
    return state and state.get("status") == "SL_HIT"


def round_max(price):
    if price >= 500:
        unit = 1.0
    elif price >= 100:
        unit = 0.1
    else:
        unit = 0.05
    return floor(price / unit) * unit


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
        trigger_price = high
        txn_type = 1
    else:
        trigger_price = low
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
        "entry_price": trigger_price,
        "signal_no": signal_no,
        "qty": qty,
        "signal_high": high,
        "signal_low": low,
        "trail_done": False,
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

    if side == "BUY":
        sl_price = state["signal_low"]
        sl_side = "SELL"
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
# HANDLE LTP EVENT (ENTRY + RR TRAILING + SL EXECUTION)
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

    side = state["side"]
    qty = state["qty"]

    # ---------------- ENTRY EXECUTION ----------------
    if state["status"] == "PENDING":
        trigger = state["trigger"]

        executed = (
            (side == "BUY" and ltp >= trigger) or
            (side == "SELL" and ltp <= trigger)
        )

        if executed:
            state["status"] = "EXECUTED"

            exec_price = ltp
            if mode != "LIVE":
                buffer = trigger * 0.001
                if side == "SELL":
                    exec_price = round_max(trigger - buffer)
                else:
                    exec_price = round_max(trigger + buffer)

            state["entry_price"] = exec_price

            log_fn(
                f"ORDER_EXECUTED | {symbol} | side={side} | "
                f"trigger={trigger} | exec={exec_price} | MODE={mode}"
            )

            place_stoploss_order(
                fyers=fyers,
                symbol=symbol,
                mode=mode,
                log_fn=log_fn
            )
        return

    # ---------------- RR-BASED TRAILING ----------------
    if state["status"] == "SL_PLACED" and not state["trail_done"]:
        entry = state["entry_price"]

        if side == "SELL":
            profit = (entry - ltp) * qty
        else:
            profit = (ltp - entry) * qty

        if profit >= 750:  # 1.5R
            if side == "SELL":
                new_sl = entry - (200 / qty)
            else:
                new_sl = entry + (200 / qty)

            state["sl_price"] = new_sl
            state["trail_done"] = True

            log_fn(
                f"RR_TRAIL | {symbol} | RR=1.5 | SL moved to {round(new_sl,2)} | LOCK=200"
            )

    # ---------------- SL EXECUTION ----------------
    if state["status"] == "SL_PLACED":
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
    "handle_ltp_event",
]
