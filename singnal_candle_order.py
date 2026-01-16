# ============================================================
# signal_candle_order.py
# STEP-2 PART-A : BUY Trigger Order Placement
# ============================================================

import math

# ------------------------------------------------------------
# STATE
# ------------------------------------------------------------
PENDING_BUY_ORDERS = set()   # one pending BUY per stock


# ------------------------------------------------------------
# BUY TRIGGER ORDER
# ------------------------------------------------------------
def place_buy_trigger_order(
    *,
    fyers,
    settings,
    log,
    symbol,
    high,
    low
):
    """
    Places ONE pending BUY trigger order per symbol.
    """

    # 1️⃣ Risk per share (floor)
    risk_per_share = int(high - low)
    if risk_per_share <= 0:
        log("ORDER_SKIP", f"{symbol} | Invalid risk {risk_per_share}")
        return

    # 2️⃣ Quantity
    per_trade_risk = int(settings.get("PER_TRADE_RISK", 0))
    qty = per_trade_risk // risk_per_share
    if qty <= 0:
        log("ORDER_SKIP", f"{symbol} | Qty zero")
        return

    # 3️⃣ Trigger price = High + 0.05% (ceil)
    trigger_price = math.ceil(high * 1.0005)
    price = int(high)

    if trigger_price <= price:
        log("ORDER_SKIP", f"{symbol} | Trigger <= Price")
        return

    # 4️⃣ Single pending order rule
    if symbol in PENDING_BUY_ORDERS:
        log("ORDER_SKIP", f"{symbol} | Pending BUY already exists")
        return

    mode = settings.get("MODE", "PAPER")

    # 5️⃣ PAPER MODE
    if mode != "LIVE":
        log(
            "PAPER_ORDER",
            f"{symbol} | BUY TRIGGER | Qty={qty} | "
            f"Price={price} | Trigger={trigger_price} | SL={low}"
        )
        PENDING_BUY_ORDERS.add(symbol)
        return

    # 6️⃣ LIVE MODE (FYERS)
    try:
        res = fyers.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 4,                 # STOP / TRIGGER
            "side": 1,                 # BUY
            "productType": "INTRADAY",
            "limitPrice": price,
            "stopPrice": trigger_price,
            "stopLoss": low,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False
        })

        if res.get("s") == "ok":
            PENDING_BUY_ORDERS.add(symbol)
            log(
                "LIVE_ORDER",
                f"{symbol} | BUY TRIGGER PLACED | Qty={qty} | "
                f"Price={price} | Trigger={trigger_price} | SL={low}"
            )
        else:
            log("ORDER_FAIL", f"{symbol} | {res}")

    except Exception as e:
        log("ORDER_ERROR", f"{symbol} | {e}")
