#!/usr/bin/env python3
"""
扩充 stocks 表 — 从东方财富拉全量 A 股列表
覆盖：上证主板 + 深证主板 + 创业板 + 科创板 + 北交所
"""
import sqlite3
import requests
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = "/app/data/smart_invest.db"

# 东方财富市场参数
MARKETS = {
    '上证主板': 'm:1+t:2,m:1+t:23',
    '深证主板': 'm:0+t:6,m:0+t:80',
    '创业板': 'm:0+t:81+s:2048',
    '科创板': 'm:1+t:23',  # 科创板包含在上证
    '北交所': 'm:0+t:81+s:8192',
}


def fetch_market_stocks(fs):
    """从东方财富拉取一个市场的全部股票"""
    url = 'https://push2.eastmoney.com/api/qt/clist/get'
    all_stocks = []
    page = 1

    while True:
        params = {
            'pn': page, 'pz': 5000, 'po': 1, 'np': 1,
            'fltt': 2, 'invt': 2, 'fid': 'f3',
            'fs': fs,
            'fields': 'f12,f14,f100'
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()
        except Exception as e:
            print(f"  请求失败 page={page}: {e}")
            break

        if not data.get('data') or not data['data'].get('diff'):
            break

        stocks = data['data']['diff']
        if not stocks:
            break

        all_stocks.extend(stocks)

        total = data['data'].get('total', 0)
        if len(all_stocks) >= total:
            break
        page += 1

    return all_stocks


def get_suffix(code):
    """根据代码判断后缀"""
    code = str(code)
    if code.startswith('6'):
        return '.SH'
    elif code.startswith(('0', '3')):
        return '.SZ'
    elif code.startswith(('8', '4')):
        return '.BJ'  # 北交所用 .BJ 后缀
    else:
        return '.SZ'


def main():
    conn = sqlite3.connect(DB_PATH)

    # 确保表存在
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            industry TEXT,
            updated_at TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    conn.commit()

    # 统计现有数据
    before = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    print(f"stocks 表当前: {before} 只")

    total_added = 0
    total_updated = 0

    for market_name, fs in MARKETS.items():
        print(f"\n拉取 {market_name}...")
        stocks = fetch_market_stocks(fs)
        print(f"  获取 {len(stocks)} 只")

        for s in stocks:
            code = str(s.get('f12', ''))
            name = s.get('f14', '')
            industry = s.get('f100', '')

            if not code or not name:
                continue

            # 加后缀
            ts_code = code + get_suffix(code)

            # 检查是否存在
            existing = conn.execute("SELECT name FROM stocks WHERE code=?", (ts_code,)).fetchone()

            if existing:
                # 更新名称（如果以前是北交所XXXX）
                if existing[0].startswith('北交所') and not name.startswith('北交所'):
                    conn.execute("UPDATE stocks SET name=?, industry=?, updated_at=datetime('now','localtime') WHERE code=?",
                               (name, industry, ts_code))
                    total_updated += 1
            else:
                conn.execute("INSERT OR IGNORE INTO stocks (code, name, industry) VALUES (?, ?, ?)",
                           (ts_code, name, industry))
                total_added += 1

    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    print(f"\n{'='*40}")
    print(f"stocks 表: {before} → {after} (+{total_added}, 更新{total_updated})")

    # 按市场统计
    print("\n按市场分布:")
    rows = conn.execute("""
        SELECT
            CASE
                WHEN code LIKE '%.SH' AND code NOT LIKE '688%' THEN '上证主板'
                WHEN code LIKE '688%.SH' THEN '科创板'
                WHEN code LIKE '%.SZ' AND code LIKE '3%' THEN '创业板'
                WHEN code LIKE '%.SZ' THEN '深证主板'
                WHEN code LIKE '%.BJ' THEN '北交所'
                ELSE '其他'
            END as market,
            COUNT(*)
        FROM stocks
        GROUP BY market
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]}")

    conn.close()
    print("\n✅ 完成")


if __name__ == "__main__":
    main()
