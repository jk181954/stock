import requests
import json
import time
import os
from datetime import datetime, timedelta

FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

def get_headers():
    return {"Authorization": f"Bearer {FINMIND_TOKEN}"} if FINMIND_TOKEN else {}

def fetch_all_stock_list():
    stocks = []
    seen_codes = set()
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
            elif res.status_code in [429, 403]:
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

# 不再做過濾，只負責計算並回傳所有資料
def process_stock(stock_rows, stock_info):
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
        "ma200_up_10days": ma200_up_10days
    }

def main():
    print("=== 開始獲取台股清單 ===")
    stocks = fetch_all_stock_list()
    if not stocks:
        print("無法取得股票清單")
        return

    print(f"共取得 {len(stocks)} 檔普通股。開始計算技術指標 (約需 30 分鐘)...")
    start_date = (datetime.today() - timedelta(days=400)).strftime("%Y-%m-%d")
    all_stocks_data = []
    failed_count = 0
    
    for idx, stock in enumerate(stocks):
        rows = fetch_stock_data_with_retry(stock["code"], start_date)
        if not rows:
            failed_count += 1
            time.sleep(1)
            continue
            
        res = process_stock(rows, stock)
        if res:
            all_stocks_data.append(res)
            
        if (idx + 1) % 50 == 0:
            print(f"進度: {idx+1} / {len(stocks)}")
        
        time.sleep(1.0)

    # 寫入 json (注意檔名改為 all_stocks_data.json)
    output_data = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_valid_stocks": len(all_stocks_data),
        "stocks": all_stocks_data
    }

    with open("all_stocks_data.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print("\n=== 掃描完成 ===")
    print(f"成功儲存 {len(all_stocks_data)} 檔股票的技術指標至 all_stocks_data.json！")

if __name__ == "__main__":
    main()
