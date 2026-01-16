# ============================================================
# RajanTradeAutomation
# STEP-1 FINAL
# Volume + Color + Bias + Signal Candle → Order Placement
# ============================================================

import time
from datetime import datetime
import pytz

from sector_engine import run_sector_bias
from signal_candle_order import place_signal_order

# ============================================================
# TIMEZONE
# ============================================================
IST = pytz.timezone("Asia/Kolkata")

# ============================================================
# SETTINGS (FROM GOOGLE SHEET VIA WebApp.gs)
# ============================================================
SETTINGS = get_settings_from_sheet()

MODE = SETTINGS.get("MODE", "PAPER")                  # PAPER / LIVE
BIAS_TIME = SETTINGS.get("BIAS_TIME")                 # HH:MM:SS
PER_TRADE_RISK = int(SETTINGS.get("PER_TRADE_RISK")) # e.g. 500

# ============================================================
# LOGGER
# ============================================================
def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    final_msg = f"[{ts}] {msg}"
    print(final_msg)
    push_log_to_sheet(ts, msg)

# ============================================================
# UTILS
# ============================================================
def candle_color(o, c):
    if c > o:
        return "GREEN"
    if c < o:
        return "RED"
    return "DOJI"

# ============================================================
# SIGNAL + ORDER LOGIC
# ============================================================
def process_live_candle(
    *,
    symbol,
    candle_index,      # LIVE3, LIVE4...
    candle,
    prev_min_volume,
    bias,              # B / S
    fyers
):
    """
    candle = {
        open, high, low, close, volume
    }
    """

    color = candle_color(candle["open"], candle["close"])
    is_lowest = candle["volume"] < prev_min_volume

    log(
        f"VOLCHK | {symbol} | LIVE{candle_index} | "
        f"vol={candle['volume']} | prev_min={prev_min_volume} | "
        f"is_lowest={is_lowest} | {color} {bias} | MODE={MODE}"
    )

    # ========================================================
    # BUY SIGNAL (ONLY STEP-1)
    # ========================================================
    if (
        bias == "B"
        and color == "RED"
        and is_lowest
    ):
        log(f"SIGNAL_FOUND | BUY | {symbol} | LIVE{candle_index}")

        place_signal_order(
            fyers=fyers,
            symbol=symbol,
            side="BUY",
            high=candle["high"],
            low=candle["low"],
            per_trade_risk=PER_TRADE_RISK,
            mode=MODE,
            log_fn=log
        )

    return min(prev_min_volume, candle["volume"])

# ============================================================
# MAIN ENGINE
# ============================================================
def main():

    log("SYSTEM STARTED")
    log(f"MODE={MODE}")
    log(f"WAITING FOR BIAS_TIME={BIAS_TIME}")

    # --------------------------------------------------------
    # WAIT FOR BIAS TIME
    # --------------------------------------------------------
    while datetime.now(IST).strftime("%H:%M:%S") < BIAS_TIME:
        time.sleep(1)

    log(f"BIAS_TIME HIT = {BIAS_TIME}")

    # --------------------------------------------------------
    # RUN SECTOR ENGINE
    # --------------------------------------------------------
    sector_result = run_sector_bias()

    log("SECTOR SNAPSHOT")
    for s in sector_result["strong_sectors"]:
        log(
            f"SECTOR | {s['sector']} | {s['bias']} | "
            f"ADV%={s['up_pct']} DEC%={s['down_pct']}"
        )

    buy_stocks = [
        s for s in sector_result["selected_stocks"]
    ]

    log(f"BUY_STOCKS_SELECTED = {buy_stocks}")

    # --------------------------------------------------------
    # INITIAL MIN VOLUME FROM HISTORY (C1 + C2)
    # --------------------------------------------------------
    prev_min_volume_map = {}
    for symbol in buy_stocks:
        c1 = HISTORY_CANDLES[symbol][0]["volume"]
        c2 = HISTORY_CANDLES[symbol][1]["volume"]
        prev_min_volume_map[symbol] = min(c1, c2)

        log(
            f"HISTORY_MIN | {symbol} | "
            f"C1={c1} C2={c2} MIN={prev_min_volume_map[symbol]}"
        )

    log("HISTORY COMPLETE (C1, C2 only)")

    # --------------------------------------------------------
    # LIVE CANDLE LOOP (LIVE3 onward)
    # --------------------------------------------------------
    while True:
        for symbol in buy_stocks:
            candle = get_latest_5min_candle(symbol)

            if not candle or not candle["completed"]:
                continue

            live_index = candle["index"]  # 3,4,5...

            prev_min_volume_map[symbol] = process_live_candle(
                symbol=symbol,
                candle_index=live_index,
                candle=candle,
                prev_min_volume=prev_min_volume_map[symbol],
                bias="B",
                fyers=FYERS
            )

        time.sleep(1)

# ============================================================
# BOOT
# ============================================================
if __name__ == "__main__":
    main()
