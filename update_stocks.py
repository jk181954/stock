import requests
import json
import time
import os
from datetime import datetime, timedelta

# 若有 Token 可加在環境變數
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

def get_headers():
    return {"Authorization": f"Bearer {FINMIND_TOKEN}"} if FINMIND_TOKEN else {}

# 1. 抓取上市 + 上櫃清單
def fetch_all_stock_list():
    stocks = []
    
    # 抓上市
    try:
        res = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX_ALL_BUT_0999", timeout=15)
        if res.status_code == 200:
            for item in res.json():
                code = str(item.get("Code", "")).strip()
                if len(code) == 4 and code.isdigit():
                    stocks.append({"code": code, "name": str(item.get("Name", "")).strip(), "market": "上市"})
    except Exception as e:
        print(f"抓取上市清單失敗: {e}")

    # 抓上櫃
    try:
        res = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=15)
        if res.status_code == 200:
            for item in res.json():
                code = str(item.get("SecuritiesCompanyCode", "")).strip()
                if len(code) == 4 and code.isdigit():
                    stocks.append({"code": code, "name": str(item.get("CompanyName", "")).strip(), "market": "上櫃"})
    except Exception as e:
        print(f"抓取上櫃清單失敗: {e}")

    return stocks

# 2. 抓取個股歷史價量
def fetch_stock_data(stock_id, start_date):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": "TaiwanStockPrice", "data_id": stock_id, "start_date": start_date}
    res = requests.get(url, params=params, headers=get_headers(), timeout=10)
    if res.status_code == 200:
        data = res.json().get("data", [])
        return sorted(data, key=lambda x: x["date"])
    return []

# 均線計算與邏輯
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
    for i in range(1, len(ma_values)):
        if ma_values[i] <= ma_values[i - 1]: return False
    return True

# 3. 策略計算
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

    # 你的策略條件
    passed = (
        close > ma5 and close > ma20 and close > ma60 and
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
    print("開始獲取台股清單...")
    stocks = fetch_all_stock_list()
    print(f"共取得 {len(stocks)} 檔普通股。開始掃描...")

    start_date = (datetime.today() - timedelta(days=500)).strftime("%Y-%m-%d")
    results = []
    
    for idx, stock in enumerate(stocks):
        try:
            rows = fetch_stock_data(stock["code"], start_date)
            res = calculate_strategy(rows, stock)
            if res:
                results.append(res)
                print(f"[{idx+1}/{len(stocks)}] {stock['code']} 符合條件！")
            else:
                if idx % 50 == 0:
                    print(f"[{idx+1}/{len(stocks)}] 掃描中...")
            
            # 為了避免被 FinMind 封鎖，每抓一檔休息 0.2 秒
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Error on {stock['code']}: {e}")
            time.sleep(1)

    # 將結果寫入 stocks.json
    output_data = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "checked_count": len(stocks),
        "matched_count": len(results),
        "stocks": results
    }

    with open("stocks.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print("掃描完成，已儲存至 stocks.json！")

if __name__ == "__main__":
    main()
