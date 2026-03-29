#!/usr/bin/env python3
"""
数据回调脚本 — 补填 event_tracking 缺失字段 + 清理新股/B股
用途：一次性修复 + 后续可定期跑
"""
import sqlite3
import json
import requests

DB_PATH = "/app/data/smart_invest.db"


def fix_bj_names(conn):
    """补填北交所股票名称（东方财富 API + Tushare 兜底）"""
    print("=== 补填北交所名称 ===")
    
    rows = conn.execute("""
        SELECT DISTINCT stock_code FROM event_tracking
        WHERE stock_name LIKE '北交所%'
    """).fetchall()
    
    fixed = 0
    failed_codes = []
    
    for (code,) in rows:
        num = code.split('.')[0]
        name = None
        
        # 1. 东方财富 API
        try:
            url = f'https://push2.eastmoney.com/api/qt/stock/get?secid=0.{num}&fields=f58'
            resp = requests.get(url, timeout=5)
            data = resp.json()
            name = data.get('data', {}).get('f58', '')
        except Exception:
            pass
        
        # 2. Tushare 兜底
        if not name:
            try:
                import tushare as ts
                pro = ts.pro_api()
                df = pro.stock_basic(ts_code=code, fields='ts_code,name')
                if df is not None and not df.empty:
                    name = df.iloc[0]['name']
            except Exception:
                pass
        
        if name:
            conn.execute("UPDATE event_tracking SET stock_name=? WHERE stock_code=?", (name, code))
            conn.execute("UPDATE discovery_pool SET stock_name=? WHERE stock_code=?", (name, code))
            fixed += 1
            print(f"  {code} → {name}")
        else:
            failed_codes.append(code)
            print(f"  {code}: 未找到")
    
    conn.commit()
    print(f"  修复: {fixed} 只, 失败: {len(failed_codes)} 只")
    return failed_codes


def fill_financial_data(conn):
    """从 discovery_pool 补填财务数据到 event_tracking"""
    print("\n=== 补填财务数据 ===")
    
    rows = conn.execute("""
        SELECT et.id, et.stock_code, dp.detail
        FROM event_tracking et
        JOIN discovery_pool dp ON et.stock_code = dp.stock_code
        WHERE et.actual_yoy IS NULL AND dp.status = 'active'
        AND dp.detail IS NOT NULL AND dp.detail != ''
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
        except Exception:
            pass
    
    conn.commit()
    print(f"  补填: {filled} 条")


def filter_special_stocks(conn):
    """过滤新股和B股"""
    print("\n=== 过滤新股/B股 ===")
    
    special = conn.execute("""
        SELECT stock_code, stock_name FROM event_tracking
        WHERE stock_code LIKE 'A%' 
           OR stock_code LIKE '900%' 
           OR stock_code LIKE '200%'
    """).fetchall()
    
    for code, name in special:
        print(f"  删除: {code} {name}")
    
    conn.execute("""
        DELETE FROM event_tracking
        WHERE stock_code LIKE 'A%' 
           OR stock_code LIKE '900%' 
           OR stock_code LIKE '200%'
    """)
    
    conn.execute("""
        DELETE FROM discovery_pool
        WHERE stock_code LIKE 'A%' 
           OR stock_code LIKE '900%' 
           OR stock_code LIKE '200%'
    """)
    
    conn.commit()
    print(f"  删除: {len(special)} 条")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print("数据回调脚本 v1.1")
    print("=" * 40)
    
    fix_bj_names(conn)
    fill_financial_data(conn)
    filter_special_stocks(conn)
    
    # 最终统计
    print("\n" + "=" * 40)
    total = conn.execute("SELECT COUNT(*) FROM event_tracking").fetchone()[0]
    with_yoy = conn.execute("SELECT COUNT(*) FROM event_tracking WHERE actual_yoy IS NOT NULL").fetchone()[0]
    with_period = conn.execute("SELECT COUNT(*) FROM event_tracking WHERE report_period IS NOT NULL AND report_period != ''").fetchone()[0]
    no_name = conn.execute("SELECT COUNT(*) FROM event_tracking WHERE stock_name LIKE '北交所%'").fetchone()[0]
    
    print(f"event_tracking: {total} 条")
    print(f"  有 actual_yoy: {with_yoy} ({with_yoy/total*100:.0f}%)")
    print(f"  有 report_period: {with_period} ({with_period/total*100:.0f}%)")
    print(f"  缺名称: {no_name}")
    
    conn.close()
    print("\n✅ 完成")


if __name__ == "__main__":
    main()
