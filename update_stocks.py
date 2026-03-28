import requests
import json
import time
import twstock
import pandas as pd
from datetime import datetime

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

def is_ma200_up_10days(ma200_series):
    last_10 = ma200_series[-10:]
    if len(last_10) < 10 or pd.isna(last_10).any():
        return False
    for i in range(1, 10):
        if last_10[i] <= last_10[i-1]:
            return False
    return True

def main():
    print("=== 開始獲取台股清單 ===")
    stocks_info = fetch_all_stock_list()
    
    if not stocks_info:
        print("無法取得股票清單")
        return

    print(f"共取得 {len(stocks_info)} 檔普通股。開始透過 twstock 下載 (自動防鎖機制)...")

    all_stocks_data = []
    checked_count = 0
    failed_count = 0

    for idx, s in enumerate(stocks_info):
        code = s["code"]
        checked_count += 1
        
        if checked_count % 10 == 0:
            print(f"進度: 處理第 {checked_count} / {len(stocks_info)} 檔...")

        try:
            # twstock.Stock 會自動抓取最近 31 天的資料，並支援 fetch_from 抓取歷史
            # 我們需要 220 天來算 200MA，抓 10 個月
            stock = twstock.Stock(code)
            
            # 抓取過去 10 個月的資料 (確保有 220 天營業日)
            target_date = datetime.now()
            year = target_date.year
            month = target_date.month
            
            # 往前推 10 個月
            fetch_month = month - 10
            fetch_year = year
            if fetch_month <= 0:
                fetch_month += 12
                fetch_year -= 1
                
            # twstock 會自動處理爬蟲並合併資料
            stock.fetch_from(fetch_year, fetch_month)
            
            if len(stock.price) < 220:
                failed_count += 1
                time.sleep(1) # 短暫休息
                continue

            # 使用 pandas 計算指標
            df = pd.DataFrame({
                'Close': stock.price,
                'Volume': stock.capacity # twstock 的 capacity 是股數
            })
            
            close_series = df['Close']
            volume_series = df['Volume']

            ma5 = close_series.rolling(window=5).mean()
            ma20 = close_series.rolling(window=20).mean()
            ma60 = close_series.rolling(window=60).mean()
            ma200 = close_series.rolling(window=200).mean()
            lowest_close_20 = close_series.rolling(window=20).min()

            latest_close = close_series.iloc[-1]
            latest_vol = volume_series.iloc[-1] / 1000 # 轉張數
            
            c_ma5 = ma5.iloc[-1]
            c_ma20 = ma20.iloc[-1]
            c_ma60 = ma60.iloc[-1]
            c_ma200 = ma200.iloc[-1]
            c_low20 = lowest_close_20.iloc[-2]

            if pd.isna(c_ma5) or pd.isna(c_ma20) or pd.isna(c_ma60) or pd.isna(c_ma200):
                continue

            ma200_up = is_ma200_up_10days(ma200.tolist())

            all_stocks_data.append({
                "code": s["code"],
                "name": s["name"],
                "market": s["market"],
                "close": round(float(latest_close), 2),
                "ma5": round(float(c_ma5), 2),
                "ma20": round(float(c_ma20), 2),
                "ma60": round(float(c_ma60), 2),
                "ma200": round(float(c_ma200), 2),
                "lowestClose20": round(float(c_low20), 2),
                "volume": round(float(latest_vol), 2),
                "ma200_up_10days": ma200_up
            })
            
            # twstock 內建有防鎖機制，但我們保險起見再加一點延遲
            time.sleep(1)

        except Exception as e:
            failed_count += 1
            # 遇到錯誤多休一下
            time.sleep(3)
            continue

    # 寫入 json
    output_data = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_valid_stocks": len(all_stocks_data),
        "stocks": all_stocks_data
    }

    with open("all_stocks_data.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print("\n=== 掃描完成 ===")
    print(f"總計掃描: {checked_count} 檔")
    print(f"無法解析 (無資料/下市/剛上市/連線錯誤): {failed_count} 檔")
    print(f"成功儲存 {len(all_stocks_data)} 檔股票的技術指標至 all_stocks_data.json！")

if __name__ == "__main__":
    main()
