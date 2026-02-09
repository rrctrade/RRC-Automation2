import requests
import time

def run_nse_test():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com",
    }

    s = requests.Session()
    s.headers.update(headers)

    print("NSE TEST: WARMUP")
    try:
        s.get("https://www.nseindia.com", timeout=5)
        time.sleep(1)
    except Exception as e:
        print("NSE TEST: warmup error", e)

    url = "https://www.nseindia.com/api/equity-stockIndices"
    params = {"index": "NIFTY IT"}

    print("NSE TEST: FETCHING DATA")
    try:
        r = s.get(url, params=params, timeout=10)
        print("NSE TEST: HTTP STATUS =", r.status_code)

        try:
            data = r.json()
            rows = data.get("data", [])
            print("NSE TEST: ROW COUNT =", len(rows))
        except Exception as je:
            print("NSE TEST: JSON PARSE ERROR", je)

    except Exception as e:
        print("NSE TEST: REQUEST ERROR", e)
