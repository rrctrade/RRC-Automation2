# ============================================================
# signal_engine.py
# STEP-2B : BUY Signal Candle + Pending Lifecycle
# ============================================================

from datetime import datetime

# ------------------------------------------------------------
# STATES
# ------------------------------------------------------------
WAITING = "WAITING"
SIGNAL_FOUND = "SIGNAL_FOUND"
ORDER_PENDING = "ORDER_PENDING"

# ------------------------------------------------------------
# INTERNAL STATE STORE
# symbol -> state dict
# ------------------------------------------------------------
signal_state = {}


# ------------------------------------------------------------
# INIT AFTER BIAS
# main.py bias finalize झाल्यावर हे call करायचं
# ------------------------------------------------------------
def init_symbols(symbol_list):
    """
    Initialize independent state for all selected symbols
    """
    signal_state.clear()
    for s in symbol_list:
        signal_state[s] = {
            "state": WAITING,
            "signal_candle": None,
            "signal_high": None,
            "signal_low": None,
            "signal_volume": None,
            "signal_color": None,
            "created_at": None
        }


# ------------------------------------------------------------
# CANDLE CLOSE EVENT
# ------------------------------------------------------------
def on_candle_close(
    symbol,
    candle_label,
    open_,
    high,
    low,
    close,
    volume,
    is_lowest,
    color,
    bias
):
    """
    Called by main.py on every completed candle
    Returns: instruction dict or None
    """

    st = signal_state.get(symbol)
    if not st:
        return None

    # ----------------------------
    # STATE : WAITING
    # ----------------------------
    if st["state"] == WAITING:

        # BUY Signal condition
        if bias == "B" and is_lowest and color == "RED":
            st.update({
                "state": SIGNAL_FOUND,
                "signal_candle": candle_label,
                "signal_high": high,
                "signal_low": low,
                "signal_volume": volume,
                "signal_color": color,
                "created_at": datetime.now().strftime("%H:%M:%S")
            })

            return {
                "action": "PLACE_BUY_ORDER",
                "symbol": symbol,
                "entry": high,
                "sl": low
            }

        return None

    # ----------------------------
    # STATE : ORDER_PENDING
    # ----------------------------
    if st["state"] == ORDER_PENDING:

        # 🔥 NEW LOWER VOLUME INVALIDATES SIGNAL
        if is_lowest and volume < st["signal_volume"]:
            # reset
            signal_state[symbol] = {
                "state": WAITING,
                "signal_candle": None,
                "signal_high": None,
                "signal_low": None,
                "signal_volume": None,
                "signal_color": None,
                "created_at": None
            }

            return {
                "action": "CANCEL_ORDER",
                "symbol": symbol,
                "reason": "NEW_LOWER_VOLUME"
            }

    return None


# ------------------------------------------------------------
# ORDER ENGINE ACKS
# ------------------------------------------------------------
def on_order_placed(symbol):
    """
    order_engine confirms order placed
    """
    st = signal_state.get(symbol)
    if not st:
        return

    if st["state"] == SIGNAL_FOUND:
        st["state"] = ORDER_PENDING


def on_order_executed(symbol):
    """
    order_engine confirms execution
    """
    if symbol in signal_state:
        signal_state[symbol] = {
            "state": WAITING,
            "signal_candle": None,
            "signal_high": None,
            "signal_low": None,
            "signal_volume": None,
            "signal_color": None,
            "created_at": None
        }


def on_order_cancelled(symbol):
    """
    order_engine confirms cancel
    """
    if symbol in signal_state:
        signal_state[symbol] = {
            "state": WAITING,
            "signal_candle": None,
            "signal_high": None,
            "signal_low": None,
            "signal_volume": None,
            "signal_color": None,
            "created_at": None
        }


# ------------------------------------------------------------
# LTP EVENT (EXECUTION CHECK)
# ------------------------------------------------------------
def on_ltp_update(symbol, ltp):
    """
    Called on every tick or price update
    Returns instruction or None
    """
    st = signal_state.get(symbol)
    if not st:
        return None

    if st["state"] == ORDER_PENDING:
        if ltp >= st["signal_high"]:
            return {
                "action": "EXECUTE_ORDER",
                "symbol": symbol
            }

    return None
