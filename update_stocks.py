import requests
import json
import time
import os
from datetime import datetime, timedelta

# 若有 Token 可加在 Github Secrets 裡，沒設定就是空字串
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

def get_headers():
    return {"Authorization": f"Bearer {FINMIND_TOKEN}"} if FINMIND_TOKEN else {}

def fetch_all_stock_list():
    stocks = []
    seen_codes = set()

    # 抓上市
    try:
        url_twse = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        res = requests.get(url_twse, timeout=15)
        if res.status_code == 200:
            for item in res.json():
                code = str(item.get("Code", "")).strip()
                name = str(item.get("Name", "")).strip()
                if len(code) == 4 and code.isdigit() and code not in seen_codes:
                    stocks.append({"code": code, "name": name, "market": "上市"})
                    seen_codes.add(code)
    except Exception as e:
        print(f"上市清單失敗: {e}")

    # 抓上櫃
    try:
        url_tpex = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
        res = requests.get(url_tpex, timeout=15)
        if res.status_code == 200:
            for item in res.json():
                code = str(item.get("SecuritiesCompanyCode", "")).strip()
                name = str(item.get("CompanyName", "")).strip()
                if len(code) == 4 and code.isdigit() and code not in seen_codes:
                    stocks.append({"code": code, "name": name, "market": "上櫃"})
                    seen_codes.add(code)
    except Exception as e:
        print(f"上櫃清單失敗: {e}")

    return stocks

def fetch_stock_data_with_retry(stock_id, start_date, max_retries=3):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": "TaiwanStockPrice", "data_id": stock_id, "start_date": start_date}
    
    for attempt in range(max_retries):
        try:
            res = requests.get(url, params=params, headers=get_headers(), timeout=10)
            if res.status_code == 200:
                data = res.json().get("data", [])
                return sorted(data, key=lambda x: x["date"])
            elif res.status_code == 429 or res.status_code == 403:
                # 被限流，休息久一點
                time.sleep(5)
        except Exception:
            time.sleep(2)
    return []

def moving_average(values, period):
    if len(values) < period: return None
    return sum(values[-period:]) / period

def rolling_ma(values, period, count):
    res = []
    if len(values) < period + count - 1: return res
    for i in range(count):
        end = len(values) - count + i + 1
        start = end - period
        if start < 0: return []
        res.append(sum(values[start:end]) / period)
    return res

def is_ma200_up_10days(closes):
    ma_values = rolling_ma(closes, 200, 10)
    if len(ma_values) < 10: return False
    for i in range(1, 10):
        if ma_values[i] <= ma_values[i - 1]: return False
    return True

def calculate_strategy(stock_rows, stock_info):
    if len(stock_rows) < 220: return None
    
    closes, volumes = [], []
    for row in stock_rows:
        try:
            closes.append(float(row["close"]))
            volumes.append(float(row["Trading_Volume"]))
        except:
            continue

    if len(closes) < 220 or len(volumes) < 220: return None

    close = closes[-1]
    ma5 = moving_average(closes, 5)
    ma20 = moving_average(closes, 20)
    ma60 = moving_average(closes, 60)
    ma200 = moving_average(closes, 200)
    lowest_close_20 = min(closes[-20:])
    volume = volumes[-1] / 1000
    ma200_up_10days = is_ma200_up_10days(closes)

    if None in [ma5, ma20, ma60, ma200]: return None

    passed = (
        close > ma5 and 
        close > ma20 and 
        close > ma60 and
        lowest_close_20 < ma20 and
        volume > 500 and
        close < ma200 * 1.4 and
        ma200_up_10days
    )

    if passed:
        return {
            "code": stock_info["code"],
            "name": stock_info["name"],
            "market": stock_info["market"],
            "close": round(close, 2),
            "ma5": round(ma5, 2),
            "ma20": round(ma20, 2),
            "ma60": round(ma60, 2),
            "ma200": round(ma200, 2),
            "lowestClose20": round(lowest_close_20, 2),
            "volume": round(volume, 2),
        }
    return None

def main():
    print("=== 開始獲取台股清單 ===")
    stocks = fetch_all_stock_list()
    
    if not stocks:
        print("無法取得股票清單")
        return

    print(f"共取得 {len(stocks)} 檔普通股。開始緩慢爬取 (預計需要 30 分鐘以上)...")

    start_date = (datetime.today() - timedelta(days=400)).strftime("%Y-%m-%d")
    results = []
    failed_count = 0
    
    for idx, stock in enumerate(stocks):
        rows = fetch_stock_data_with_retry(stock["code"], start_date)
        
        if not rows:
            failed_count += 1
            print(f"[{idx+1}/{len(stocks)}] {stock['code']} 抓取失敗或無資料")
            # 失敗也要休息，避免被徹底封鎖
            time.sleep(1)
            continue
            
        res = calculate_strategy(rows, stock)
        
        if res:
            results.append(res)
            print(f"🔥 [{idx+1}/{len(stocks)}] 找到符合標的: {stock['code']} {stock['name']}")
        else:
            if (idx + 1) % 50 == 0:
                print(f"[{idx+1}/{len(stocks)}] 掃描進度正常...")
        
        # 最核心的防鎖機制：每查一檔，強迫休息 1 秒鐘
        # 1700 檔 * 1 秒 = 1700 秒 = 大約 28 分鐘
        # GitHub Actions 免費額度一次可以跑 360 分鐘，絕對夠用
        time.sleep(1.0)

    # 寫入 json
    output_data = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "checked_count": len(stocks),
        "matched_count": len(results),
        "failed_count": failed_count,
        "stocks": results
    }

    with open("stocks.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print("\n=== 掃描完成 ===")
    print(f"總計掃描: {len(stocks)} 檔")
    print(f"無法解析: {failed_count} 檔")
    print(f"符合策略: {len(results)} 檔")

if __name__ == "__main__":
    main()
