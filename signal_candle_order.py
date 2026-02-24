# ============================================================
# signal_candle_order.py
# RR 2.5 (DYNAMIC) → TRAILING SL (LOCK 200)
# PRODUCTION VERSION – CLEAN CLOSE STATE
# LIVE + PAPER COMPATIBLE
# ============================================================

from math import floor

# ------------------------------------------------------------
# ORDER STATE
# ------------------------------------------------------------
ORDER_STATE = {}

RR_MULTIPLIER = 2.5
LOCK_PROFIT = 200


# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def round_price(price):
    if price >= 500:
        unit = 1.0
    elif price >= 100:
        unit = 0.1
    else:
        unit = 0.05
    return floor(price / unit) * unit


def calc_qty(high, low, risk):
    rng = abs(high - low)
    if rng <= 0:
        return 0
    return floor(risk / rng)


# ------------------------------------------------------------
# PLACE ENTRY ORDER
# ------------------------------------------------------------
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
    init_sl = low if side == "BUY" else high

    log_fn(
        f"ORDER_SIGNAL | {symbol} | {side} | "
        f"trigger={trigger} SL={round(init_sl,2)} qty={qty} | SIGNAL#{signal_no}"
    )

    signal_order_id = None

    if mode == "LIVE":
        resp = fyers.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 3,
            "side": txn,
            "productType": "INTRADAY",
            "stopPrice": trigger,
            "validity": "DAY",
            "offlineOrder": False,
        })
        signal_order_id = resp.get("id")

    ORDER_STATE[symbol] = {
        "status": "PENDING",
        "side": side,
        "trigger": trigger,
        "qty": qty,
        "signal_high": high,
        "signal_low": low,
        "entry_price": None,
        "sl_price": None,
        "sl_order_id": None,
        "signal_order_id": signal_order_id,
        "trail_done": False,
        "risk": per_trade_risk,
    }


# ------------------------------------------------------------
# HANDLE SIGNAL EVENT
# ------------------------------------------------------------
def handle_signal_event(**kwargs):

    symbol = kwargs["symbol"]
    fyers = kwargs["fyers"]
    mode = kwargs["mode"]
    log_fn = kwargs["log_fn"]
    side = kwargs.get("side")

    state = ORDER_STATE.get(symbol)

    # CANCEL ONLY MODE
    if side is None:
        if state and state.get("status") == "PENDING":
            if mode == "LIVE" and state.get("signal_order_id"):
                try:
                    fyers.cancel_order({"id": state["signal_order_id"]})
                    log_fn(f"ORDER_CANCEL | {symbol} | SIGNAL")
                except Exception as e:
                    log_fn(f"SIGNAL_CANCEL_FAIL | {symbol} | {e}")
                    return
            else:
                log_fn(f"PAPER_ORDER_CANCEL | {symbol} | SIGNAL")

            ORDER_STATE.pop(symbol, None)
        return

    # If trade already active or closed → ignore
    if state and state.get("status") in ("EXECUTED", "SL_PLACED"):
        return

    # If previously closed → remove stale state
    if state and state.get("status") in ("SL_HIT",):
        ORDER_STATE.pop(symbol, None)

    # Cancel old pending
    if state and state.get("status") == "PENDING":
        if mode == "LIVE" and state.get("signal_order_id"):
            try:
                fyers.cancel_order({"id": state["signal_order_id"]})
                log_fn(f"ORDER_CANCEL | {symbol} | SIGNAL")
            except Exception as e:
                log_fn(f"SIGNAL_CANCEL_FAIL | {symbol} | {e}")
                return
        else:
            log_fn(f"PAPER_ORDER_CANCEL | {symbol} | SIGNAL")

        ORDER_STATE.pop(symbol, None)

    place_signal_order(**kwargs)


# ------------------------------------------------------------
# PLACE SL
# ------------------------------------------------------------
def place_sl(fyers, state, symbol, sl_price, mode):

    side = state["side"]
    qty = state["qty"]
    sl_side = -1 if side == "BUY" else 1

    if mode == "LIVE":
        resp = fyers.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 3,
            "side": sl_side,
            "productType": "INTRADAY",
            "stopPrice": round_price(sl_price),
            "validity": "DAY",
            "offlineOrder": False,
        })
        state["sl_order_id"] = resp.get("id")

    state["sl_price"] = sl_price
    state["status"] = "SL_PLACED"


def cancel_sl(fyers, state, symbol, mode, log_fn):
    if mode == "LIVE" and state.get("sl_order_id"):
        try:
            fyers.cancel_order({"id": state["sl_order_id"]})
            log_fn(f"ORDER_CANCEL | {symbol} | SL")
        except Exception as e:
            log_fn(f"SL_CANCEL_FAIL | {symbol} | {e}")
            return False
    state["sl_order_id"] = None
    return True


# ------------------------------------------------------------
# HANDLE LTP EVENT
# ------------------------------------------------------------
def handle_ltp_event(*, fyers, symbol, ltp, mode, log_fn):

    state = ORDER_STATE.get(symbol)
    if not state:
        return

    status = state["status"]
    side = state["side"]
    qty = state["qty"]

    # --------------------------------------------------------
    # ENTRY EXECUTION
    # --------------------------------------------------------
    if status == "PENDING":

        if (side == "BUY" and ltp >= state["trigger"]) or \
           (side == "SELL" and ltp <= state["trigger"]):

            entry = ltp

            if mode != "LIVE":
                buf = state["trigger"] * 0.001
                entry = round_price(
                    state["trigger"] + buf if side == "BUY"
                    else state["trigger"] - buf
                )

            state["entry_price"] = entry
            state["status"] = "EXECUTED"

            log_fn(
                f"ORDER_EXECUTED | {symbol} | "
                f"ENTRY={round(entry,2)} | QTY={qty} | MODE={mode}"
            )

            init_sl = (
                state["signal_low"] if side == "BUY"
                else state["signal_high"]
            )

            place_sl(fyers, state, symbol, init_sl, mode)

        return

    # --------------------------------------------------------
    # IGNORE IF TRADE CLOSED
    # --------------------------------------------------------
    if status == "SL_HIT":
        return

    # --------------------------------------------------------
    # PROFIT CALCULATION
    # --------------------------------------------------------
    entry = state["entry_price"]

    profit = (
        (ltp - entry) * qty if side == "BUY"
        else (entry - ltp) * qty
    )

    rr_profit = state["risk"] * RR_MULTIPLIER

    # --------------------------------------------------------
    # TRAILING
    # --------------------------------------------------------
    if status == "SL_PLACED" and profit >= rr_profit and not state["trail_done"]:

        new_sl = (
            entry + (LOCK_PROFIT / qty)
            if side == "BUY"
            else entry - (LOCK_PROFIT / qty)
        )

        if cancel_sl(fyers, state, symbol, mode, log_fn):
            place_sl(fyers, state, symbol, new_sl, mode)
            state["trail_done"] = True

            log_fn(
                f"MODIFIED_SL | {symbol} | SL={round(new_sl,2)} | RR=2.5 | LOCK=200"
            )

    # --------------------------------------------------------
    # SL HIT
    # --------------------------------------------------------
    if status == "SL_PLACED":

        if (side == "BUY" and ltp <= state["sl_price"]) or \
           (side == "SELL" and ltp >= state["sl_price"]):

            log_fn(
                f"SL_EXECUTED | {symbol} | SL={round(state['sl_price'],2)}"
            )

            state["status"] = "SL_HIT"

            # 🔥 PRODUCTION CLEANUP
            ORDER_STATE.pop(symbol, None)
