import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

DB_FILE = "historical_prices.json"
OUTPUT_FILE = "all_stocks_data.json"
TW_TZ = ZoneInfo("Asia/Taipei")


def get_today_quotes():
    today_data = {}

    try:
        res = requests.get(
            "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
            timeout=15
        )
        res.raise_for_status()

        for item in res.json():
            code = str(item.get("Code", "")).strip()
            close = str(item.get("ClosingPrice", "")).replace(",", "")
            vol = str(item.get("TradeVolume", "")).replace(",", "")

            if close and vol and close.replace(".", "", 1).isdigit() and len(code) == 4:
                today_data[code] = {
                    "close": float(close),
                    "volume": float(vol) / 1000
                }
    except Exception as e:
        print(f"獲取上市今日行情失敗: {e}")

    try:
        res = requests.get(
            "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
            timeout=15
        )
        res.raise_for_status()

        for item in res.json():
            code = str(item.get("SecuritiesCompanyCode", "")).strip()
            close = str(item.get("Close", "")).replace(",", "")
            vol = str(item.get("TradingShares", "")).replace(",", "")

            if close and vol and close.replace(".", "", 1).isdigit() and len(code) == 4:
                today_data[code] = {
                    "close": float(close),
                    "volume": float(vol) / 1000
                }
    except Exception as e:
        print(f"獲取上櫃今日行情失敗: {e}")

    return today_data


def is_ma200_up_10days(ma200_list):
    if len(ma200_list) < 10:
        return False

    last_10 = ma200_list[-10:]
    for i in range(1, 10):
        if last_10[i] <= last_10[i - 1]:
            return False
    return True


def calculate_kd(df, n=9):
    low_min = df["close"].rolling(window=n, min_periods=1).min()
    high_max = df["close"].rolling(window=n, min_periods=1).max()

    rsv = (df["close"] - low_min) / (high_max - low_min + 1e-8) * 100

    k = np.zeros(len(df))
    d = np.zeros(len(df))

    for i in range(len(df)):
        if i == 0:
            k[i] = 50
            d[i] = 50
        else:
            k[i] = k[i - 1] * 2 / 3 + rsv.iloc[i] * 1 / 3
            d[i] = d[i - 1] * 2 / 3 + k[i] * 1 / 3

    return pd.Series(k, index=df.index), pd.Series(d, index=df.index)


def main():
    print("=== 開始每日極速增量更新 ===")

    if not os.path.exists(DB_FILE):
        print(f"找不到 {DB_FILE}，請先上傳歷史資料庫！")
        return

    with open(DB_FILE, "r", encoding="utf-8") as f:
        db = json.load(f)

    today_quotes = get_today_quotes()
    if not today_quotes:
        print("今日無資料或 API 異常，結束更新。")
        return

    tw_now = datetime.now(TW_TZ)
    today_str = tw_now.strftime("%Y-%m-%d")

    all_stocks_result = []
    updated_count = 0

    for code, info in db.items():
        if code in today_quotes:
            new_quote = today_quotes[code]

            if info["history"] and info["history"][-1]["date"] == today_str:
                info["history"][-1] = {
                    "date": today_str,
                    "close": new_quote["close"],
                    "volume": new_quote["volume"]
                }
            else:
                info["history"].append({
                    "date": today_str,
                    "close": new_quote["close"],
                    "volume": new_quote["volume"]
                })

            info["history"] = info["history"][-250:]
            updated_count += 1

        history = info.get("history", [])
        if len(history) < 220:
            continue

        df = pd.DataFrame(history)
        closes = df["close"]
        volumes = df["volume"]

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
        has_vol_burst = any(
            last_10_vols.iloc[i] > (last_10_vol_ma20.iloc[i] * 2)
            for i in range(len(last_10_vols))
            if pd.notna(last_10_vol_ma20.iloc[i])
        )

        pct_change = closes.pct_change() * 100
        has_price_burst = bool((pct_change.iloc[-10:] > 5.0).any())

        high5 = closes.rolling(window=5).max()

        bias20 = (
            abs(closes.iloc[-1] - ma20_today) / ma20_today * 100
            if pd.notna(ma20_today) and ma20_today > 0
            else 0
        )

        vol_ma5 = volumes.rolling(window=5).mean()
        max_vol_10 = volumes.iloc[-10:].max()

        k_series, d_series = calculate_kd(df)
        k_value = k_series.iloc[-1]

        all_stocks_result.append({
            "code": code,
            "name": info.get("name", ""),
            "market": info.get("market", ""),
            "close": round(float(closes.iloc[-1]), 2),
            "volume": round(float(volumes.iloc[-1]), 2),
            "ma5": round(float(ma5.iloc[-1]), 2),
            "ma20": round(float(ma20_today), 2),
            "ma60": round(float(ma60.iloc[-1]), 2),
            "ma200": round(float(ma200.iloc[-1]), 2),
            "lowestClose20": round(
                float(low20.iloc[-2] if len(low20) >= 2 else low20.iloc[-1]),
                2
            ),
            "ma200up10days": bool(ma200_up),
            "ma20yesterday": round(float(ma20_yesterday), 2),
            "hasvolburst10d": bool(has_vol_burst),
            "haspriceburst10d": bool(has_price_burst),
            "highestClose5": round(float(high5.iloc[-1]), 2),
            "bias20": round(float(bias20), 2),
            "volma5": round(float(vol_ma5.iloc[-1]), 2),
            "maxvol10d": round(float(max_vol_10), 2),
            "kvalue": round(float(k_value), 2)
        })

    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False)

    output_data = {
        "updatedat": tw_now.strftime("%Y-%m-%d %H:%M:%S"),
        "totalvalidstocks": len(all_stocks_result),
        "stocks": all_stocks_result
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print("=== 更新完成 ===")
    print(f"今天共更新 {updated_count} 檔股票價格")
    print(f"成功儲存 {len(all_stocks_result)} 檔符合天數的股票指標！")
    print(f"資料更新時間（台灣）: {output_data['updatedat']}")


if __name__ == "__main__":
    main()
