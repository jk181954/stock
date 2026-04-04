import requests
import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

DB_FILE = "historical_prices.json"
OUTPUT_FILE = "all_stocks_data.json"

def get_today_quotes():
    today_data = {}
    tw_today = datetime.now(tz=pytz.timezone("Asia/Taipei")).strftime("%Y-%m-%d")
    actual_date = None

    # ✅ 先抓 TPEX（有 Date 欄位），取得實際交易日
    try:
        res = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=15)
        for item in res.json():
            code = str(item.get("SecuritiesCompanyCode", "")).strip()
            close = str(item.get("Close", "")).replace(',', '')
            vol = str(item.get("TradingShares", "")).replace(',', '')
            date_str = str(item.get("Date", "")).strip()  # 格式：115/04/02
            if close and vol and close.replace('.', '', 1).isdigit() and len(code) == 4:
                today_data[code] = {"close": float(close), "volume": float(vol) / 1000}
                # ✅ 取第一筆有效日期，民國轉西元
                if actual_date is None and "/" in date_str:
                    parts = date_str.split("/")
                    if len(parts) == 3:
                        year = int(parts[0]) + 1911
                        actual_date = f"{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
    except Exception as e:
        print(f"獲取上櫃今日行情失敗: {e}")

    # 再抓 TWSE（無 Date 欄位，日期已從 TPEX 取得）
    try:
        res = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=15)
        for item in res.json():
            code = str(item.get("Code", "")).strip()
            close = str(item.get("ClosingPrice", "")).replace(',', '')
            vol = str(item.get("TradeVolume", "")).replace(',', '')
            if close and vol and close.replace('.', '', 1).isdigit() and len(code) == 4:
                today_data[code] = {"close": float(close), "volume": float(vol) / 1000}
    except Exception as e:
        print(f"獲取上市今日行情失敗: {e}")

    # fallback：TPEX 也沒有日期時才用今天
    if actual_date is None:
        actual_date = tw_today
        print(f"⚠️ 無法從 API 取得交易日，使用程式執行日: {actual_date}")

    return today_data, actual_date

def is_ma200_up_10days(ma200_list):
    if len(ma200_list) < 10: return False
    last_10 = ma200_list[-10:]
    for i in range(1, 10):
        if last_10[i] <= last_10[i-1]:
            return False
    return True

def calculate_kd(df, n=9):
    low_min = df['close'].rolling(window=n, min_periods=1).min()
    high_max = df['close'].rolling(window=n, min_periods=1).max()
    rsv = (df['close'] - low_min) / (high_max - low_min + 1e-8) * 100
    K = np.zeros(len(df))
    D = np.zeros(len(df))
    for i in range(len(df)):
        if i == 0:
            K[i] = 50
            D[i] = 50
        else:
            K[i] = K[i-1] * 2/3 + rsv.iloc[i] * 1/3
            D[i] = D[i-1] * 2/3 + K[i] * 1/3
    return pd.Series(K, index=df.index), pd.Series(D, index=df.index)

def main():
    print("=== 開始每日極速增量更新 ===")

    if not os.path.exists(DB_FILE):
        print(f"找不到 {DB_FILE}，請先上傳歷史資料庫！")
        return

    with open(DB_FILE, "r", encoding="utf-8") as f:
        db = json.load(f)

    today_quotes, actual_data_date = get_today_quotes()
    if not today_quotes:
        print("今日無資料或 API 異常，結束更新。")
        return

    print(f"API 實際交易日期: {actual_data_date}")

    all_stocks_result = []
    updated_count = 0

    for code, info in db.items():
        if code in today_quotes:
            new_quote = today_quotes[code]
            if info["history"] and info["history"][-1]["date"] == actual_data_date:
                info["history"][-1] = {"date": actual_data_date, "close": new_quote["close"], "volume": new_quote["volume"]}
            else:
                info["history"].append({"date": actual_data_date, "close": new_quote["close"], "volume": new_quote["volume"]})
            info["history"] = info["history"][-250:]
            updated_count += 1

        history = info["history"]
        if len(history) < 220:
            continue

        df = pd.DataFrame(history)
        closes = df['close']
        volumes = df['volume']

        ma5 = closes.rolling(window=5).mean()
        ma20 = closes.rolling(window=20).mean()
        ma60 = closes.rolling(window=60).mean()
        ma200 = closes.rolling(window=200).mean()
        low20 = closes.rolling(window=20).min()

        ma200_up = is_ma200_up_10days(ma200.dropna().tolist())

        ma20_today = ma20.iloc[-1]
        ma20_yesterday = ma20.iloc[-2] if len(ma20) > 1 else ma20_today

        vol_ma20 = volumes.rolling(window=20).mean()
        last_10_vols = volumes.iloc[-10:]
        last_10_vol_ma20 = vol_ma20.iloc[-10:]
        has_vol_burst = any(last_10_vols.iloc[i] > (last_10_vol_ma20.iloc[i] * 2) for i in range(len(last_10_vols)))

        pct_change = closes.pct_change() * 100
        has_price_burst = any(pct_change.iloc[-10:] > 5.0)

        high5 = closes.rolling(window=5).max()

        bias20 = abs(closes.iloc[-1] - ma20_today) / ma20_today * 100 if ma20_today > 0 else 0

        vol_ma5 = volumes.rolling(window=5).mean()

        max_vol_10 = volumes.iloc[-10:].max()

        K, D = calculate_kd(df)
        k_value = K.iloc[-1]

        all_stocks_result.append({
            "code": code,
            "name": info["name"],
            "market": info["market"],
            "close": round(float(closes.iloc[-1]), 2),
            "volume": round(float(volumes.iloc[-1]), 2),
            "ma5": round(float(ma5.iloc[-1]), 2),
            "ma20": round(float(ma20_today), 2),
            "ma60": round(float(ma60.iloc[-1]), 2),
            "ma200": round(float(ma200.iloc[-1]), 2),
            "lowestClose20": round(float(low20.iloc[-2] if len(low20) >= 2 else low20.iloc[-1]), 2),
            "ma200_up_10days": ma200_up,
            "ma20_yesterday": round(float(ma20_yesterday), 2),
            "has_vol_burst_10d": bool(has_vol_burst),
            "has_price_burst_10d": bool(has_price_burst),
            "highestClose5": round(float(high5.iloc[-1]), 2),
            "bias20": round(float(bias20), 2),
            "vol_ma5": round(float(vol_ma5.iloc[-1]), 2),
            "max_vol_10d": round(float(max_vol_10), 2),
            "k_value": round(float(k_value), 2)
        })

    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False)

    tw_tz = pytz.timezone("Asia/Taipei")
    tw_now = datetime.now(tz=tw_tz)

    output_data = {
        "updated_at": tw_now.strftime("%Y-%m-%d %H:%M:%S CST"),
        "data_date": actual_data_date,
        "total_valid_stocks": len(all_stocks_result),
        "stocks": all_stocks_result
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"=== 更新完成 ===")
    print(f"今天共更新 {updated_count} 檔股票價格")
    print(f"實際資料日期: {actual_data_date}")
    print(f"成功儲存 {len(all_stocks_result)} 檔符合天數的股票指標！")

if __name__ == "__main__":
    main()
