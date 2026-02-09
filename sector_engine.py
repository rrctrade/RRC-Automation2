# ============================================================
# sector_engine.py
# Sector Bias + Stock Selection
# STEP-A : FINAL FIXED VERSION (BUY + SELL)
# ============================================================

import requests
import time
from datetime import datetime
from sector_mapping import SECTOR_MAP

# ------------------------------------------------------------
# NSE SECTOR NAMES (AS-IS)
# ------------------------------------------------------------
SECTOR_LIST = {
    "NIFTY AUTO": "AUTO",
    "NIFTY FINANCIAL SERVICES": "FINANCIAL_SERVICES",
    "NIFTY FIN SERVICE EX BANK": "FIN_SERVICES_EX_BANK",
    "NIFTY FMCG": "FMCG",
    "NIFTY IT": "IT",
    "NIFTY MEDIA": "MEDIA",
    "NIFTY METAL": "METAL",
    "NIFTY PHARMA": "PHARMA",
    "NIFTY PSU BANK": "PSU_BANK",
    "NIFTY PRIVATE BANK": "PRIVATE_BANK",
    "NIFTY REALTY": "REALTY",
    "NIFTY CONSUMER DURABLES": "CONSUMER_DURABLES",
    "NIFTY OIL & GAS": "OIL_GAS",
    "NIFTY CHEMICALS": "CHEMICALS",
    "NIFTY BANK": "BANK",
    "NIFTY 50": "NIFTY50",
}

# ------------------------------------------------------------
# NSE SESSION
# ------------------------------------------------------------
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com",
}

SESSION = requests.Session()
SESSION.headers.update(NSE_HEADERS)


def warmup():
    try:
        SESSION.get("https://www.nseindia.com", timeout=5)
    except Exception:
        pass


def fetch_sector_stocks(sector_name):
    url = "https://www.nseindia.com/api/equity-stockIndices"

    try:
        res = SESSION.get(url, params={"index": sector_name}, timeout=10)
        data = res.json()
    except Exception:
        return {}

    stocks = {}
    for row in data.get("data", []):
        sym = row.get("symbol")
        chg = row.get("pChange")

        if not sym or sym.upper() == sector_name.upper():
            continue

        if isinstance(chg, (int, float)):
            stocks[sym.upper()] = float(chg)

    return stocks


def run_sector_bias():
    """
    🔒 PUBLIC INTERFACE FUNCTION
    main.py will call ONLY this.
    """
    strong_sectors = []
    selected_stocks = set()

    warmup()

    for nse_sector, map_key in SECTOR_LIST.items():
        stocks = fetch_sector_stocks(nse_sector)
        if not stocks:
            continue

        total = len(stocks)
        up = sum(1 for v in stocks.values() if v > 0)
        down = sum(1 for v in stocks.values() if v < 0)

        up_pct = (up / total) * 100 if total else 0
        down_pct = (down / total) * 100 if total else 0

        bias = None

        # ✅ FIX: allow both BUY and SELL sectors
        if up_pct >= 60:
            bias = "BUY"

        if down_pct >= 60:
            bias = "SELL"

        if not bias:
            continue

        strong_sectors.append({
            "sector": nse_sector,
            "bias": bias,
            "up_pct": round(up_pct, 2),
            "down_pct": round(down_pct, 2),
        })

        # FnO stocks allowed for this sector
        allowed_fno = {
            s.replace("NSE:", "").replace("-EQ", "")
            for s in SECTOR_MAP.get(map_key, [])
        }

        # Stock-level filter (+/- 2.5%)
        for sym, pct in stocks.items():
            if sym in allowed_fno and abs(pct) <= 5:
                selected_stocks.add(f"NSE:{sym}-EQ")

        time.sleep(0.2)

    return {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "strong_sectors": strong_sectors,
        "selected_stocks": sorted(selected_stocks),
    }


# ------------------------------------------------------------
# LOCAL / MANUAL TEST
# ------------------------------------------------------------
if __name__ == "__main__":
    print("\n🚀 SECTOR ENGINE FINAL TEST (BUY + SELL)\n")
    result = run_sector_bias()

    print("Time:", result["timestamp"])
    print("\nStrong Sectors:")
    for s in result["strong_sectors"]:
        print(s)

    print("\nSelected Stocks:")
    print(result["selected_stocks"])
