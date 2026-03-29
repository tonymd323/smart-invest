#!/usr/bin/env python3
"""
数据回调脚本 — 补填 event_tracking 缺失字段 + 清理新股/B股
用途：一次性修复 + 后续可定期跑
"""
import sqlite3
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.data_provider import QuoteProvider

DB_PATH = os.path.expanduser("~/.openclaw/workspace/smart-invest/data/smart_invest.db")


def fix_names(conn):
    """补填北交所股票名称（从 stocks 表查）"""
    print("=== 补填公司名称 ===")
    
    # 先尝试从 stocks 表查
    rows = conn.execute("""
        SELECT et.stock_code, s.name
        FROM event_tracking et
        LEFT JOIN stocks s ON et.stock_code = s.code
        WHERE (et.stock_name LIKE '北交所%' OR et.stock_name = et.stock_code)
        AND s.name IS NOT NULL
    """).fetchall()
    
    fixed = 0
    for code, name in rows:
        conn.execute("UPDATE event_tracking SET stock_name=? WHERE stock_code=?", (name, code))
        fixed += 1
        print(f"  {code} → {name}")
    
    # 剩余的用腾讯行情 API 查
    remaining = conn.execute("""
        SELECT DISTINCT stock_code FROM event_tracking
        WHERE stock_name LIKE '北交所%' OR stock_name = stock_code
    """).fetchall()
    
    if remaining:
        provider = QuoteProvider()
        for (code,) in remaining:
            try:
                info = provider.get_stock_name(code)
                if info and info.get('name'):
                    conn.execute("UPDATE event_tracking SET stock_name=? WHERE stock_code=?", 
                               (info['name'], code))
                    conn.execute("UPDATE discovery_pool SET stock_name=? WHERE stock_code=?",
                               (info['name'], code))
                    fixed += 1
                    print(f"  {code} → {info['name']} (API)")
            except Exception as e:
                print(f"  {code}: 查询失败 ({e})")
    
    conn.commit()
    print(f"  修复: {fixed} 只")


def fill_financial_data(conn):
    """从 discovery_pool 补填财务数据到 event_tracking"""
    print("\n=== 补填财务数据 ===")
    
    rows = conn.execute("""
        SELECT et.id, et.stock_code, dp.detail
        FROM event_tracking et
        JOIN discovery_pool dp ON et.stock_code = dp.stock_code
        WHERE et.actual_yoy IS NULL AND dp.status = 'active'
        AND dp.detail IS NOT NULL
    """).fetchall()
    
    filled = 0
    for et_id, code, detail_str in rows:
        try:
            detail = json.loads(detail_str)
            report_date = detail.get("report_date", "")
            report_period = report_date.replace("-", "") if report_date else None
            actual_yoy = detail.get("actual_yoy")
            expected_yoy = detail.get("expected_yoy")
            profit_diff = detail.get("beat_diff")
            
            if actual_yoy is not None:
                conn.execute("""
                    UPDATE event_tracking 
                    SET report_period=?, actual_yoy=?, expected_yoy=?, profit_diff=?
                    WHERE id=? AND actual_yoy IS NULL
                """, (report_period, actual_yoy, expected_yoy, profit_diff, et_id))
                filled += 1
        except Exception as e:
            pass
    
    conn.commit()
    print(f"  补填: {filled} 条")


def filter_special_stocks(conn):
    """过滤新股和B股"""
    print("\n=== 过滤新股/B股 ===")
    
    # 统计
    special = conn.execute("""
        SELECT stock_code, stock_name FROM event_tracking
        WHERE stock_code LIKE 'A%' 
           OR stock_code LIKE '900%' 
           OR stock_code LIKE '200%'
    """).fetchall()
    
    for code, name in special:
        print(f"  删除: {code} {name}")
    
    # 删除
    conn.execute("""
        DELETE FROM event_tracking
        WHERE stock_code LIKE 'A%' 
           OR stock_code LIKE '900%' 
           OR stock_code LIKE '200%'
    """)
    
    # 也从 discovery_pool 删除
    conn.execute("""
        DELETE FROM discovery_pool
        WHERE stock_code LIKE 'A%' 
           OR stock_code LIKE '900%' 
           OR stock_code LIKE '200%'
    """)
    
    conn.commit()
    print(f"  删除: {len(special)} 条")


def backfill_from_json(conn):
    """从 btiq_history.json 补填 market_snapshots（如果需要）"""
    print("\n=== 检查 market_snapshots ===")
    count = conn.execute("SELECT COUNT(*) FROM market_snapshots").fetchone()[0]
    print(f"  当前: {count} 条")
    if count == 0:
        print("  需要运行 btiq_backfill.py 补填")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print("数据回调脚本 v1.0")
    print("=" * 40)
    
    # 1. 补填公司名称
    fix_names(conn)
    
    # 2. 补填财务数据
    fill_financial_data(conn)
    
    # 3. 过滤新股/B股
    filter_special_stocks(conn)
    
    # 4. 检查 market_snapshots
    backfill_from_json(conn)
    
    # 最终统计
    print("\n" + "=" * 40)
    total = conn.execute("SELECT COUNT(*) FROM event_tracking").fetchone()[0]
    with_yoy = conn.execute("SELECT COUNT(*) FROM event_tracking WHERE actual_yoy IS NOT NULL").fetchone()[0]
    with_period = conn.execute("SELECT COUNT(*) FROM event_tracking WHERE report_period IS NOT NULL AND report_period != ''").fetchone()[0]
    
    print(f"event_tracking: {total} 条")
    print(f"  有 actual_yoy: {with_yoy} ({with_yoy/total*100:.0f}%)")
    print(f"  有 report_period: {with_period} ({with_period/total*100:.0f}%)")
    
    conn.close()
    print("\n✅ 完成")


if __name__ == "__main__":
    main()
