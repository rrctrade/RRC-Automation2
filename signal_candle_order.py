# ============================================================
# signal_candle_order.py
# STEP-2A : Signal Candle → Immediate SL-M Trigger Order
# BUY only (SELL ready, not used yet)
# ============================================================

from math import floor

# one pending order per stock (in-memory guard)
PENDING_ORDERS = set()


def calculate_quantity(high, low, per_trade_risk):
    """
    Qty = risk / (high - low)
    Range rounded down (int)
    """
    candle_range = abs(high - low)
    candle_range = int(candle_range)

    if candle_range <= 0:
        return 0, candle_range

    qty = floor(per_trade_risk / candle_range)
    return qty, candle_range


def place_signal_order(
    *,
    fyers,
    symbol,
    side,          # "BUY" / "SELL"
    high,
    low,
    per_trade_risk,
    mode,          # "PAPER" / "LIVE"
    log_fn
):
    """
    Places SL-M (Stop-Market) trigger order.
    Order remains PENDING until trigger price is crossed.
    """

    # guard: only one pending order per symbol
    if symbol in PENDING_ORDERS:
        log_fn(f"ORDER_SKIP | {symbol} | already pending")
        return

    qty, candle_range = calculate_quantity(high, low, per_trade_risk)

    if qty <= 0:
        log_fn(f"ORDER_SKIP | {symbol} | qty=0 | range={candle_range}")
        return

    # BUY / SELL mapping
    if side == "BUY":
        trigger_price = round(high, 2)
        txn_type = 1   # BUY
    else:
        trigger_price = round(low, 2)
        txn_type = -1  # SELL

    # SL-M payload (STOP-MARKET)
    order_payload = {
        "symbol": symbol,
        "qty": qty,
        "type": 3,                 # 3 = STOP-MARKET (SL-M)
        "side": txn_type,
        "productType": "INTRADAY",
        "stopPrice": trigger_price,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
    }

    log_fn(
        f"ORDER_SIGNAL | {symbol} | {side} | "
        f"trigger={trigger_price} qty={qty} range={candle_range} | MODE={mode}"
    )

    # PAPER MODE
    if mode == "PAPER":
        log_fn(f"PAPER_TRIGGER_ORDER_PLACED | {symbol} | trigger={trigger_price}")
        PENDING_ORDERS.add(symbol)
        return

    # LIVE MODE
    try:
        res = fyers.place_order(order_payload)
        log_fn(f"LIVE_TRIGGER_ORDER_PLACED | {symbol} | {res}")
        PENDING_ORDERS.add(symbol)
    except Exception as e:
        log_fn(f"LIVE_ORDER_ERROR | {symbol} | {e}")
