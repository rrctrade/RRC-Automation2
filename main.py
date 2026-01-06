def fetch_two_history_candles(symbol, end_ts):
    start_ts = end_ts - 600

    log(
        "HISTORY_FETCH",
        f"{symbol} | {fmt_ist(start_ts)}→{fmt_ist(end_ts)} IST"
    )

    try:
        res = fyers.history({
            "symbol": symbol,
            "resolution": "5",
            "date_format": "0",
            "range_from": int(start_ts),
            "range_to": int(end_ts - 1),  # 🔒 KEY FIX
            "cont_flag": "1"
        })

        if res.get("s") == "ok":
            candles = res.get("candles", [])
            log("HISTORY_RESULT", f"{symbol} | candles_count={len(candles)}")
            return candles

        log("HISTORY_ERROR", f"{symbol} | response={res}")

    except Exception as e:
        log("ERROR", f"History exception {symbol}: {e}")

    return []
