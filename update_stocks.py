import requests
import json
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

DB_FILE = "historical_prices.json"
OUTPUT_FILE = "all_stocks_data.json"
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"


def parse_tpex_date(date_str):
    date_str = str(date_str).strip()
    if len(date_str) == 7 and date_str.isdigit():
        year = int(date_str[:3]) + 1911
        month = date_str[3:5]
        day = date_str[5:7]
        return f"{year}-{month}-{day}"
    return None


def get_last_trading_date_from_twse():
    tw_now = datetime.now(tz=pytz.timezone("Asia/Taipei"))
    for month_offset in range(2):
        check_dt = tw_now - timedelta(days=30 * month_offset)
        ym = check_dt.strftime("%Y%m")
        try:
            url = f"https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date={ym}01&response=json"
            res = requests.get(url, timeout=10)
            data = res.json()
            rows = data.get("data", [])
            if not rows:
                continue
            for row in reversed(rows):
                date_raw = str(row[0]).strip()
                parts = date_raw.split("/")
                if len(parts) == 3:
                    year = int(parts[0]) + 1911
                    candidate = f"{year}-{parts[1]}-{parts[2]}"
                    if candidate <= tw_now.strftime("%Y-%m-%d"):
                        return candidate
        except Exception as e:
            print(f"TWSE 月曆查詢失敗 (offset={month_offset}): {e}")
    return None


def get_today_quotes():
    today_data = {}
    actual_date = None

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

    if actual_date is None:
        print("⚠️ 無法從即時 API 取得交易日，嘗試查詢 TWSE 月曆...")
        actual_date = get_last_trading_date_from_twse()
        if actual_date:
            print(f"✅ 月曆查詢成功，使用最近交易日: {actual_date}")
        else:
            print("⚠️ 月曆查詢也失敗，今日可能為非交易日，跳過更新。")
            return {}, None

    return today_data, actual_date


def is_data_duplicate(history):
    """檢查最後一筆是否為重複寫入（close + volume 與前一筆完全相同）"""
    if len(history) < 2:
        return False
    prev = history[-2]
    last = history[-1]
    return (round(prev["close"], 2) == round(last["close"], 2) and
            round(prev["volume"], 2) == round(last["volume"], 2))


def clean_duplicate_entries(db, actual_data_date):
    """清除 DB 中當日的重複資料，還原成前一日狀態，讓程式重新更新"""
    cleaned = 0
    for info in db.values():
        history = info.get("history", [])
        if (history and history[-1]["date"] == actual_data_date
                and is_data_duplicate(history)):
            info["history"] = history[:-1]
            cleaned += 1
    return cleaned


def fetch_finmind(code, start_date, end_date, token=""):
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": code,
        "start_date": start_date,
        "end_date": end_date,
    }
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        res = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=20)
        data = res.json()
        if data.get("status") != 200:
            return []
        rows = []
        for item in data.get("data", []):
            date_val = item.get("date", "")
            close_val = item.get("close")
            vol_val = item.get("Trading_Volume")
            if date_val and close_val is not None:
                rows.append({
                    "date": date_val,
                    "close": round(float(close_val), 2),
                    "volume": round(float(vol_val) / 1000, 2) if vol_val else 0.0,
                })
        return rows
    except Exception as e:
        print(f"  [{code}] FinMind 失敗: {e}")
        return []


def backfill_with_finmind(db, actual_data_date, token=""):
    stale = []
    for code, info in db.items():
        history = info.get("history", [])
        if not history:
            continue
        if history[-1]["date"] < actual_data_date:
            stale.append((code, history[-1]["date"]))

    if not stale:
        print("✅ 所有股票已是最新，無需 FinMind 補齊。")
        return db

    print(f"⚙️  FinMind 補齊開始，共 {len(stale)} 檔缺漏...")
    filled = 0
    skipped = 0
    sleep_sec = 6 if token else 12

    for i, (code, last_date) in enumerate(stale):
        start_dt = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        new_rows = fetch_finmind(code, start_dt, actual_data_date, token=token)

        if new_rows:
            existing_dates = {r["date"] for r in db[code]["history"]}
            for row in new_rows:
                if row["date"] not in existing_dates:
                    db[code]["history"].append(row)
                else:
                    # 覆蓋已存在但可能錯誤的資料
                    for j, h in enumerate(db[code]["history"]):
                        if h["date"] == row["date"]:
                            db[code]["history"][j] = row
                            break
            db[code]["history"] = sorted(db[code]["history"], key=lambda x: x["date"])[-250:]
            filled += 1
        else:
            skipped += 1

        if (i + 1) % 50 == 0:
            print(f"  進度: {i+1}/{len(stale)} | 補齊 {filled} | 無資料 {skipped}")

        time.sleep(sleep_sec)

    print(f"✅ FinMind 補齊完成：成功 {filled} 檔，無資料 {skipped} 檔")
    return db


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

    # --- STEP 1: 取得今日交易日與報價 ---
    today_quotes, actual_data_date = get_today_quotes()

    if not today_quotes or actual_data_date is None:
        print("今日無資料或為非交易日，結束更新。")
        return

    print(f"API 實際交易日期: {actual_data_date}")

    # --- STEP 2: 偵測並清除重複寫入的錯誤資料 ---
    already_updated = sum(
        1 for info in db.values()
        if info.get("history") and info["history"][-1]["date"] == actual_data_date
    )

    if already_updated > len(db) * 0.8:
        # 大部分已是今日日期，再檢查有沒有重複資料
        duplicate_count = sum(
            1 for info in db.values()
            if info.get("history") and info["history"][-1]["date"] == actual_data_date
            and is_data_duplicate(info["history"])
        )
        if duplicate_count > len(db) * 0.1:
            print(f"⚠️  偵測到 {duplicate_count} 筆重複寫入（API 未更新），清除後重新更新...")
            cleaned = clean_duplicate_entries(db, actual_data_date)
            print(f"   清除 {cleaned} 筆，重新從 API 更新...")
        else:
            print(f"✅ 已有 {already_updated} 檔資料為 {actual_data_date}，資料已是最新，跳過更新。")
            return

    # --- STEP 3: TPEX / TWSE 即時 API 寫入 ---
    updated_count = 0
    duplicate_skipped = 0

    for code, info in db.items():
        if code not in today_quotes:
            continue

        new_quote = today_quotes[code]
        history = info["history"]

        # 今日報價與最後一筆完全相同 → API 尚未更新，跳過（留給 FinMind 補）
        if history:
            prev = history[-1]
            if (round(prev["close"], 2) == round(new_quote["close"], 2) and
                    round(prev["volume"], 2) == round(new_quote["volume"], 2)):
                duplicate_skipped += 1
                continue

        if history and history[-1]["date"] == actual_data_date:
            history[-1] = {"date": actual_data_date, "close": new_quote["close"], "volume": new_quote["volume"]}
        else:
            history.append({"date": actual_data_date, "close": new_quote["close"], "volume": new_quote["volume"]})

        info["history"] = history[-250:]
        updated_count += 1

    print(f"TPEX/TWSE 更新完成：{updated_count} 檔")
    if duplicate_skipped:
        print(f"資料重複跳過（API 未更新）：{duplicate_skipped} 檔，將交由 FinMind 補齊")

    # --- STEP 4: FinMind 補齊仍缺漏的股票 ---
    finmind_token = os.environ.get("FINMIND_TOKEN", "")
    if finmind_token:
        print("偵測到 FINMIND_TOKEN，啟用 FinMind 補齊...")
        db = backfill_with_finmind(db, actual_data_date, token=finmind_token)
    else:
        print("未設定 FINMIND_TOKEN，跳過 FinMind 補齊。")

    # --- STEP 5: 計算技術指標 ---
    all_stocks_result = []
    for code, info in db.items():
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

    # --- STEP 6: 儲存結果 ---
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
