#!/usr/bin/env python3
"""
全市场行情修复脚本
=====================
数据源：Tushare Pro `daily` 接口（单次调用返回全市场 ~5500 条/日）

策略：
- 增量：拉近期 N 个交易日（默认60）的全市场批量行情
  每次 pro.daily(trade_date=X) → 全市场 ~5500 条/日
  60交易日 ≈ 60次 × 0.3s ≈ 20秒全量
- 首次全量 (--full)：补 stocks 表有但 prices 表无的 5891 只股票近 N 日行情

使用方式：
    python3 scripts/fill_prices.py              # 增量补最近60交易日
    python3 scripts/fill_prices.py --days 120  # 补最近120交易日
    python3 scripts/fill_prices.py --full       # 首次全量（5891只 × 60日）
"""

import sys
import os
import time
import json
import sqlite3
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import date, timedelta

# ── 日志 ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fill_prices")

DB_PATH = Path(__file__).parent.parent / "data" / "smart_invest.db"
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "")
MAX_WORKERS = 6          # Tushare 并发（积分保护）


def _safe_float(v, default=0.0):
    try:
        return float(v) if v not in (None, "", "None") else default
    except (ValueError, TypeError):
        return default


def _fetch_daily_batch(trade_date: str) -> list:
    """用 Tushare pro.daily(trade_date=) 拉全市场单日行情"""
    import tushare as ts
    if not TUSHARE_TOKEN:
        return []
    try:
        pro = ts.pro_api(TUSHARE_TOKEN)
        df = pro.daily(trade_date=trade_date)
        if df is None or df.empty:
            return []
        results = []
        for _, row in df.iterrows():
            results.append({
                "stock_code": str(row.get("ts_code", "")),
                "trade_date": str(row.get("trade_date", "")),
                "open_price": _safe_float(row.get("open")),
                "high_price": _safe_float(row.get("high")),
                "low_price": _safe_float(row.get("low")),
                "close_price": _safe_float(row.get("close")),
                "volume": _safe_float(row.get("vol")),
                "turnover": _safe_float(row.get("amount")),
                "change_pct": _safe_float(row.get("pct_chg")),
                "turnover_rate": 0.0,
            })
        return results
    except Exception as e:
        logger.error(f"[Tushare daily {trade_date}] 失败: {e}")
        return []


def _fetch_stock_klines(code: str, start_dt: str, end_dt: str) -> list:
    """用 Tushare pro.daily(ts_code=) 拉单只股票近 N 日 K 线"""
    import tushare as ts
    if not TUSHARE_TOKEN:
        return []
    try:
        pro = ts.pro_api(TUSHARE_TOKEN)
        df = pro.daily(ts_code=code, start_date=start_dt, end_date=end_dt)
        if df is None or df.empty:
            return []
        results = []
        for _, row in df.iterrows():
            results.append({
                "stock_code": code,
                "trade_date": str(row.get("trade_date", "")),
                "open_price": _safe_float(row.get("open")),
                "high_price": _safe_float(row.get("high")),
                "low_price": _safe_float(row.get("low")),
                "close_price": _safe_float(row.get("close")),
                "volume": _safe_float(row.get("vol")),
                "turnover": _safe_float(row.get("amount")),
                "change_pct": _safe_float(row.get("pct_chg")),
                "turnover_rate": 0.0,
            })
        return results
    except Exception:
        return []


def fill_prices(days: int = 60, full: bool = False) -> dict:
    """
    全/增量修复行情数据

    Args:
        days: 近 N 个交易日（默认60）
        full: True = 首次全量（补缺失的 5891 只股票）
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ① stocks 表全量 A 股
    cur.execute("SELECT code FROM stocks WHERE code LIKE '%.SZ' OR code LIKE '%.SH'")
    all_codes = set(r[0] for r in cur.fetchall())
    logger.info(f"stocks 表 A 股: {len(all_codes)} 只")

    # ② 已有行情的股票
    cur.execute("SELECT DISTINCT stock_code FROM prices")
    have_codes = {r[0] for r in cur.fetchall()}
    missing_codes = sorted(c for c in all_codes if c not in have_codes)
    logger.info(f"已有行情: {len(have_codes)} 只 | 缺行情: {len(missing_codes)} 只")

    # ③ 已有行情的交易日
    cur.execute("SELECT DISTINCT trade_date FROM prices ORDER BY trade_date DESC")
    have_dates = sorted([r[0] for r in cur.fetchall()], reverse=True)
    logger.info(f"已有交易日: {len(have_dates)} 个，最新={have_dates[0] if have_dates else '-'}")

    # ④ 确定要补的交易日窗口
    end_dt = (date.today() - timedelta(days=1)).strftime("%Y%m%d")  # 昨天（今天可能未结束）
    start_dt = (date.today() - timedelta(days=days * 2)).strftime("%Y%m%d")

    # 近期真实交易日列表（排除周末，排除今天，排除未来）
    recent_trade_dates = []
    d = date.today()
    while d.strftime("%Y%m%d") > "19900101" and len(recent_trade_dates) < days:
        d -= timedelta(days=1)
        if d.weekday() < 5 and d.strftime("%Y%m%d") <= end_dt:
            recent_trade_dates.append(d.strftime("%Y%m%d"))
    logger.info(f"目标补 {len(recent_trade_dates)} 个交易日: {recent_trade_dates[-1]} ~ {recent_trade_dates[0]}")
    # 过滤：已有完整行情的日期跳过（通过 recently_dates 有多少条判断）
    dates_need_fill = []
    for ds in recent_trade_dates:
        cur.execute("SELECT COUNT(*) FROM prices WHERE trade_date = ?", (ds,))
        cnt = cur.fetchone()[0]
        if cnt < len(all_codes) * 0.9:  # <90% 完整度 → 需要补
            dates_need_fill.append(ds)
    logger.info(f"需要补全的交易日: {len(dates_need_fill)} 个 {dates_need_fill[-1] if dates_need_fill else '-'} ~ {dates_need_fill[0] if dates_need_fill else '-'}")

    inserted = 0
    lock = Lock()

    # ⑤ 补交易日行情（全市场批量，每次一个完整交易日）
    if dates_need_fill:
        def _fetch_date(dt: str):
            return dt, _fetch_daily_batch(dt)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_date, d): d for d in dates_need_fill}
            done = 0
            for future in as_completed(futures):
                done += 1
                dt, klines = future.result()
                if not klines:
                    continue
                batch = [
                    (k["stock_code"], k["trade_date"],
                     k["open_price"], k["high_price"], k["low_price"],
                     k["close_price"], k["volume"], k["turnover"],
                     k["change_pct"], k.get("turnover_rate", 0))
                    for k in klines
                ]
                with lock:
                    cur.executemany("""
                        INSERT OR IGNORE INTO prices
                        (stock_code, trade_date, open_price, high_price, low_price,
                         close_price, volume, turnover, change_pct, turnover_rate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    conn.commit()
                    inserted += len(batch)

                if done % 10 == 0 or done == len(dates_need_fill):
                    logger.info(f"  交易日进度 {done}/{len(dates_need_fill)} | 累计 {inserted} 条")

    # ⑥ 补 missing_codes 近期行情（--full 时启用）
    if full and missing_codes:
        logger.info(f"全量补缺失股票: {len(missing_codes)} 只，每只拉近 {days} 日")
        done = 0
        for code in missing_codes:
            done += 1
            klines = _fetch_stock_klines(code, start_dt, end_dt)
            if klines:
                batch = [
                    (k["stock_code"], k["trade_date"],
                     k["open_price"], k["high_price"], k["low_price"],
                     k["close_price"], k["volume"], k["turnover"],
                     k["change_pct"], k.get("turnover_rate", 0))
                    for k in klines
                ]
                with lock:
                    cur.executemany("""
                        INSERT OR IGNORE INTO prices
                        (stock_code, trade_date, open_price, high_price, low_price,
                         close_price, volume, turnover, change_pct, turnover_rate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    conn.commit()
                    inserted += len(batch)

            if done % 500 == 0 or done == len(missing_codes):
                logger.info(f"  股票进度 {done}/{len(missing_codes)} | 累计 {inserted} 条")

    conn.close()

    # ⑦ 统计
    conn2 = sqlite3.connect(DB_PATH)
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(DISTINCT stock_code) FROM prices")
    stocks_now = cur2.fetchone()[0]
    cur2.execute("SELECT COUNT(*) FROM prices")
    total_now = cur2.fetchone()[0]
    cur2.execute("SELECT COUNT(DISTINCT trade_date) FROM prices WHERE trade_date >= ?", (start_dt,))
    dates_now = cur2.fetchone()[0]
    conn2.close()

    logger.info(f"✅ 完成")
    logger.info(f"   新增记录: {inserted}")
    logger.info(f"   有行情股票: {stocks_now} (A 股总数 ~5500)")
    logger.info(f"   总记录数: {total_now}")
    logger.info(f"   近期交易日: {dates_now} 个")

    return {
        "inserted": inserted,
        "stocks_with_data": stocks_now,
        "total_rows": total_now,
        "recent_trade_dates": dates_now,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="全市场行情修复")
    parser.add_argument("--days", type=int, default=60, help="补最近N交易日（默认60）")
    parser.add_argument("--full", action="store_true", help="首次全量：补5891只缺失股票")
    args = parser.parse_args()

    r = fill_prices(days=args.days, full=args.full)
    print(json.dumps(r, ensure_ascii=False, indent=2))
