#!/usr/bin/env python3
"""
pullback_predictions 行情回填脚本
用 Tushare 补充 20 只缺失股票的日线数据。
覆盖范围：trigger_date 再往前推 60 个交易日（确保有足够历史计算收益）
"""
import sqlite3, os, sys, time
from datetime import datetime, timedelta

DB_PATH = "/root/.openclaw/workspace/smart-invest/data/smart_invest.db"
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")

TARGET_STOCKS = [
    ("000608.SZ", "20260318"),
    ("000711.SZ", "20260327"),
    ("000815.SZ", "20260319"),
    ("600468.SH", "20260318"),
    ("300207.SZ", "20260319"),
    ("603789.SH", "20260316"),
    ("603701.SH", "20260318"),
    ("603398.SH", "20260319"),
    ("300502.SZ", "20260318"),
    ("300394.SZ", "20260318"),
    ("603098.SH", "20260318"),
    ("603683.SH", "20260310"),  # earliest trigger
    ("603655.SH", "20260327"),
    ("301191.SZ", "20260318"),  # note: 301191.SH → .SZ (typo in data)
    ("603992.SH", "20260317"),
    ("605162.SH", "20260318"),
    ("688109.SH", "20260327"),
    ("688226.SH", "20260319"),
    ("301128.SZ", "20260318"),
    ("301396.SZ", "20260312"),
]

def ts_code_to_trade_date(ts_code: str) -> str:
    """tushare 证券代码 → 交易日期范围 start_date"""
    return "20250101"  # 覆盖到至少半年前

def main():
    import tushare as ts
    pro = ts.pro_api(TUSHARE_TOKEN)

    conn = sqlite3.connect(DB_PATH)
    total_inserted = 0
    total_updated = 0
    errors = []

    for stock_code, trigger_date in TARGET_STOCKS:
        # start_date: 取 trigger_date 往前推 60 个交易日 ≈ 3 个月
        # 直接用 20251001 保证够用
        start_date = "20251001"
        end_date = "20260402"  # 今天

        ts_code = stock_code  # tushare 直接用同代码

        try:
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                print(f"  ⚠️ {stock_code}: 无数据")
                errors.append((stock_code, "no data"))
                continue

            rows = 0
            for _, row in df.iterrows():
                trade_date = str(row["trade_date"])  # 20260318
                conn.execute("""
                    INSERT OR REPLACE INTO prices
                    (stock_code, trade_date, open_price, high_price, low_price,
                     close_price, change_pct, volume, turnover)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    stock_code,
                    trade_date,
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row["pct_chg"]) if "pct_chg" in row and row["pct_chg"] is not None else 0.0,
                    float(row["vol"]) if "vol" in row and row["vol"] is not None else 0.0,
                    float(row["amount"]) if "amount" in row and row["amount"] is not None else 0.0,
                ))
                rows += 1

            conn.commit()
            print(f"  ✅ {stock_code}: {rows} 条（含 {trigger_date} 触发日）")
            total_inserted += rows

        except Exception as e:
            print(f"  ❌ {stock_code}: {e}")
            errors.append((stock_code, str(e)))

        time.sleep(0.15)  # Tushare 频率限制

    # 验证
    print("\n验证：")
    for stock_code, trigger_date in TARGET_STOCKS:
        row = conn.execute(
            "SELECT trade_date, close_price FROM prices WHERE stock_code=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 3",
            (stock_code, trigger_date)
        ).fetchall()
        status = "✅" if row else "❌"
        print(f"  {status} {stock_code} trigger={trigger_date}: {row[:2] if row else '无数据'}")

    conn.close()
    print(f"\n完成。共计插入/更新 {total_inserted} 条。失败 {len(errors)} 只。")

if __name__ == "__main__":
    main()
