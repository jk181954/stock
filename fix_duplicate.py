import json
import os

DB_FILE = "historical_prices.json"
TARGET_DATE = "2026-04-10"

print(f"=== 清除 {TARGET_DATE} 的錯誤資料 ===")

with open(DB_FILE, "r", encoding="utf-8") as f:
    db = json.load(f)

removed = 0
for code, info in db.items():
    history = info.get("history", [])
    if history and history[-1]["date"] == TARGET_DATE:
        # 檢查是否與前一筆完全相同（重複寫入的錯誤資料）
        if len(history) >= 2:
            prev = history[-2]
            last = history[-1]
            if (round(prev["close"], 2) == round(last["close"], 2) and
                    round(prev["volume"], 2) == round(last["volume"], 2)):
                info["history"] = history[:-1]  # 移除最後一筆
                removed += 1

with open(DB_FILE, "w", encoding="utf-8") as f:
    json.dump(db, f, ensure_ascii=False)

print(f"清除完成：移除 {removed} 筆重複的 {TARGET_DATE} 資料")
print(f"現在可以重新執行 python update_stocks.py")
