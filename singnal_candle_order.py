# ============================================================
# signal_candle_order.py
# STEP-1 : Signal Candle → Trigger Order Placement
# BUY / SELL + SL + Quantity
# ============================================================

from math import floor

# in-memory guard → one pending order per stock
PENDING_ORDERS = set()


def calculate_quantity(high, low, per_trade_risk):
    """
    Range rounding + quantity calc
    """
    candle_range = abs(high - low)
    candle_range = int(candle_range)  # round down (8.33 → 8)

    if candle_range <= 0:
        return 0, candle_range

    qty = floor(per_trade_risk / candle_range)
    return qty, candle_range


def place_signal_order(
    *,
    fyers,
    symbol,
    side,          # BUY / SELL
    high,
    low,
    per_trade_risk,
    mode,          # PAPER / LIVE
    log_fn
):
    """
    Main public function
    """

    # 🔒 one pending order per stock
    if symbol in PENDING_ORDERS:
        log_fn(f"ORDER_SKIP | {symbol} | already pending")
        return

    qty, candle_range = calculate_quantity(high, low, per_trade_risk)

    if qty <= 0:
        log_fn(f"ORDER_SKIP | {symbol} | qty=0")
        return

    if side == "BUY":
        price = round(high, 2)
        trigger_price = round(high + 1, 2)
        stoploss = round(low, 2)
        txn_type = 1  # BUY
    else:
        price = round(low, 2)
        trigger_price = round(low - 1, 2)
        stoploss = round(high, 2)
        txn_type = -1  # SELL

    order_payload = {
        "symbol": symbol,
        "qty": qty,
        "type": 4,              # STOP-LIMIT
        "side": txn_type,
        "productType": "INTRADAY",
        "limitPrice": price,
        "stopPrice": trigger_price,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
        "stopLoss": stoploss,
    }

    log_fn(
        f"ORDER_SIGNAL | {symbol} | {side} | "
        f"price={price} trigger={trigger_price} "
        f"sl={stoploss} qty={qty} range={candle_range} | MODE={mode}"
    )

    if mode == "PAPER":
        log_fn(f"PAPER_ORDER_PLACED | {symbol}")
        PENDING_ORDERS.add(symbol)
        return

    # LIVE MODE
    try:
        res = fyers.place_order(order_payload)
        log_fn(f"LIVE_ORDER_RESPONSE | {symbol} | {res}")
        PENDING_ORDERS.add(symbol)
    except Exception as e:
        log_fn(f"LIVE_ORDER_ERROR | {symbol} | {e}")
