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
    防呆：從 TWSE 月曆 API 取得最近的真實交易日。
    即使今天是假日或休市，也能正確回傳上一個交易日。
    """
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
                date_raw = str(row[0]).strip()  # "115/04/10"
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

    # ① TPEX 即時報價 + 解析交易日
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

    # ② TWSE 即時報價 + 解析交易日
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

    # ③ 防呆 fallback：查 TWSE 月曆
    if actual_date is None:
        print("⚠️ 無法從即時 API 取得交易日，嘗試查詢 TWSE 月曆...")
        actual_date = get_last_trading_date_from_twse()
        if actual_date:
            print(f"✅ 月曆查詢成功，使用最近交易日: {actual_date}")
        else:
            print("⚠️ 月曆查詢也失敗，今日可能為非交易日，跳過更新。")
            return {}, None

    return today_data, actual_date


def fetch_finmind(code, market, start_date, end_date, token=""):
    """使用 FinMind 補齊指定股票的缺漏歷史資料"""
    # FinMind 上市用原始代碼，上櫃也相同
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": code,
        "start_date": start_date,
        "end_date": end_date,
    }
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
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
    """
    掃描 db，找出仍落後 actual_data_date 的股票，用 FinMind 補齊。
    只補缺漏的股票，不會對已更新的股票發出請求。
    """
    stale = []
    for code, info in db.items():
        history = info.get("history", [])
        if not history:
            continue
        last_date = history[-1]["date"]
        if last_date < actual_data_date:
            stale.append((code, info.get("market", "TWSE"), last_date))

    if not stale:
        print("✅ 所有股票已是最新，無需 FinMind 補齊。")
        return db

    print(f"⚙️  FinMind 補齊開始，共 {len(stale)} 檔缺漏...")
    filled = 0
    skipped = 0
    # 有 token 600 req/hr → 每 6 秒；無 token 300 req/hr → 每 12 秒
    sleep_sec = 6 if token else 12

    for i, (code, market, last_date) in enumerate(stale):
        start_dt = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        new_rows = fetch_finmind(code, market, start_dt, actual_data_date, token=token)

        if new_rows:
            existing_dates = {r["date"] for r in db[code]["history"]}
            for row in new_rows:
                if row["date"] not in existing_dates:
                    db[code]["history"].append(row)
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

    # --- STEP 1: TPEX / TWSE 即時 API ---
    today_quotes, actual_data_date = get_today_quotes()

    if not today_quotes or actual_data_date is None:
        print("今日無資料或為非交易日，結束更新。")
        return

    print(f"API 實際交易日期: {actual_data_date}")

    # 防呆：若大部分股票已是今日日期，視為已更新，跳過
    already_updated = sum(
        1 for info in db.values()
        if info.get("history") and info["history"][-1]["date"] == actual_data_date
    )
    if already_updated > len(db) * 0.8:
        print(f"✅ 已有 {already_updated} 檔資料為 {actual_data_date}，資料已是最新，跳過更新。")
        return

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

    print(f"TPEX/TWSE 更新完成：{updated_count} 檔")

    # --- STEP 2: FinMind 補齊仍缺漏的股票 ---
    finmind_token = os.environ.get("FINMIND_TOKEN", "")
    if finmind_token:
        print("偵測到 FINMIND_TOKEN，啟用 FinMind 補齊...")
        db = backfill_with_finmind(db, actual_data_date, token=finmind_token)
    else:
        print("未設定 FINMIND_TOKEN，跳過 FinMind 補齊。")

    # --- STEP 3: 計算技術指標 ---
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

    # --- STEP 4: 儲存結果 ---
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
