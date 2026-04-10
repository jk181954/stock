import requests
import json
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

DB_FILE = "historical_prices.json"
OUTPUT_FILE = "all_stocks_data.json"


def parse_tpex_date(date_str):
    """將 TPEX 民國日期 1150402 轉為西元 2026-04-02"""
    date_str = str(date_str).strip()
    if len(date_str) == 7 and date_str.isdigit():
        year = int(date_str[:3]) + 1911
        month = date_str[3:5]
        day = date_str[5:7]
        return f"{year}-{month}-{day}"
    return None


def get_last_trading_date_from_twse():
    """
    防呆：從 TWSE 月交易日曆 API 取得最近的交易日。
    即使今天是假日或休市，也能正確回傳上一個真實交易日。
    """
    tw_now = datetime.now(tz=pytz.timezone("Asia/Taipei"))
    # 往前最多查 2 個月，確保能找到交易日
    for month_offset in range(2):
        check_dt = tw_now - timedelta(days=30 * month_offset)
        ym = check_dt.strftime("%Y%m")
        try:
            url = f"https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date={ym}01&response=json"
            res = requests.get(url, timeout=10)
            data = res.json()
            # data["data"] 是 [[民國年月日, ...], ...] 的月資料
            rows = data.get("data", [])
            if not rows:
                continue
            # 取最後一筆日期（最近交易日），格式如 "115/04/10"
            for row in reversed(rows):
                date_raw = str(row[0]).strip()  # "115/04/10"
                parts = date_raw.split("/")
                if len(parts) == 3:
                    year = int(parts[0]) + 1911
                    candidate = f"{year}-{parts[1]}-{parts[2]}"
                    # 必須 <= 今天
                    if candidate <= tw_now.strftime("%Y-%m-%d"):
                        return candidate
        except Exception as e:
            print(f"TWSE 月曆查詢失敗 (offset={month_offset}): {e}")
    return None


def get_today_quotes():
    today_data = {}
    actual_date = None

    # ① 先抓 TPEX，解析實際交易日
    try:
        res = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=15)
        for item in res.json():
            code = str(item.get("SecuritiesCompanyCode", "")).strip()
            close = str(item.get("Close", "")).replace(",", "")
            vol = str(item.get("TradingShares", "")).replace(",", "")
            date_str = str(item.get("Date", "")).strip()
            if close and vol and close.replace(".", "", 1).isdigit() and len(code) == 4:
                today_data[code] = {"close": float(close), "volume": float(vol) / 1000}
                if actual_date is None:
                    actual_date = parse_tpex_date(date_str)
    except Exception as e:
        print(f"獲取上櫃今日行情失敗: {e}")

    # ② 再抓 TWSE，同時解析日期
    try:
        res = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=15)
        for item in res.json():
            code = str(item.get("Code", "")).strip()
            close = str(item.get("ClosingPrice", "")).replace(",", "")
            vol = str(item.get("TradeVolume", "")).replace(",", "")
            date_str = str(item.get("Date", "")).strip()
            if close and vol and close.replace(".", "", 1).isdigit() and len(code) == 4:
                today_data[code] = {"close": float(close), "volume": float(vol) / 1000}
                if actual_date is None:
                    actual_date = parse_tpex_date(date_str)
    except Exception as e:
        print(f"獲取上市今日行情失敗: {e}")

    # ③ 防呆 fallback：兩個 API 都取不到日期時，查 TWSE 月曆取得最近真實交易日
    if actual_date is None:
        print("⚠️ 無法從即時 API 取得交易日，嘗試查詢 TWSE 月曆...")
        actual_date = get_last_trading_date_from_twse()
        if actual_date:
            print(f"✅ 月曆查詢成功，使用最近交易日: {actual_date}")
        else:
            # 最後手段：往前找 DB 中最新的日期，避免寫入錯誤日期
            print("⚠️ 月曆查詢也失敗，今日可能為非交易日，跳過更新。")
            return {}, None

    return today_data, actual_date


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
    K = np.zeros(len(df))
    D = np.zeros(len(df))
    for i in range(len(df)):
        if i == 0:
            K[i] = 50
            D[i] = 50
        else:
            K[i] = K[i - 1] * 2 / 3 + rsv.iloc[i] * 1 / 3
            D[i] = D[i - 1] * 2 / 3 + K[i] * 1 / 3
    return pd.Series(K, index=df.index), pd.Series(D, index=df.index)


def main():
    print("=== 開始每日極速增量更新 ===")

    if not os.path.exists(DB_FILE):
        print(f"找不到 {DB_FILE}，請先上傳歷史資料庫！")
        return

    with open(DB_FILE, "r", encoding="utf-8") as f:
        db = json.load(f)

    today_quotes, actual_data_date = get_today_quotes()

    # 防呆：actual_data_date 為 None 代表今天非交易日，直接結束
    if not today_quotes or actual_data_date is None:
        print("今日無資料或為非交易日，結束更新。")
        return

    print(f"API 實際交易日期: {actual_data_date}")

    # 防呆：若 DB 中已有此日期的資料，且今日報價筆數很少，可能是 API 還未完全更新
    already_updated = sum(
        1 for info in db.values()
        if info.get("history") and info["history"][-1]["date"] == actual_data_date
    )
    if already_updated > len(db) * 0.8:
        print(f"✅ 已有 {already_updated} 檔資料為 {actual_data_date}，資料已是最新，跳過更新。")
        return

    all_stocks_result = []
    updated_count = 0

    for code, info in db.items():
        if code in today_quotes:
            new_quote = today_quotes[code]
            if info["history"] and info["history"][-1]["date"] == actual_data_date:
                # 同一交易日重複執行 → 覆蓋，不重複插入
                info["history"][-1] = {"date": actual_data_date, "close": new_quote["close"], "volume": new_quote["volume"]}
            else:
                info["history"].append({"date": actual_data_date, "close": new_quote["close"], "volume": new_quote["volume"]})
            info["history"] = info["history"][-250:]
            updated_count += 1

        history = info["history"]
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
            "k_value": round(float(k_value), 2),
        })

    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False)

    tw_tz = pytz.timezone("Asia/Taipei")
    tw_now = datetime.now(tz=tw_tz)

    output_data = {
        "updated_at": tw_now.strftime("%Y-%m-%d %H:%M:%S CST"),
        "data_date": actual_data_date,
        "total_valid_stocks": len(all_stocks_result),
        "stocks": all_stocks_result,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"=== 更新完成 ===")
    print(f"今天共更新 {updated_count} 檔股票價格")
    print(f"實際資料日期: {actual_data_date}")
    print(f"成功儲存 {len(all_stocks_result)} 檔符合天數的股票指標！")


if __name__ == "__main__":
    main()
