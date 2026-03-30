#!/usr/bin/env python3
"""
存量数据标准化清洗脚本 — v2.23

功能：
  1. 日期格式统一 → YYYY-MM-DD
  2. 股票代码统一 → canonical_code（000001.SZ）
  3. 合并重复 stocks 记录
  4. 修复已知 Bug#21（T+N 收益计算错误）

用法：
  python3 scripts/clean_normalize.py [--dry-run] [--db-path /app/data/smart_invest.db]
"""

import sqlite3
import argparse
import sys
import os
from pathlib import Path

# 添加项目根目录到 path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.data_normalizer import normalizer
from core.stock_resolver import StockResolver


def normalize_dates(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """统一日期格式"""
    stats = {'updated': 0, 'tables': {}}

    # 需要标准化的表和字段
    date_fields = [
        ('event_tracking', 'event_date'),
        ('earnings', 'report_date'),
        ('discovery_pool', 'discovered_at'),
        ('discovery_pool', 'expires_at'),
        ('discovery_pool', 'updated_at'),
    ]

    for table, field in date_fields:
        try:
            # 只更新含有连字符的日期（已经是 ISO 格式不需要更新）
            # 或者更新含有紧凑格式的（需要加连字符）
            rows = conn.execute(
                f"SELECT rowid, {field} FROM {table} WHERE {field} IS NOT NULL"
            ).fetchall()

            updated = 0
            for rowid, raw_val in rows:
                normalized = normalizer.normalize_date(raw_val)
                if normalized and normalized != raw_val:
                    if not dry_run:
                        conn.execute(
                            f"UPDATE {table} SET {field} = ? WHERE rowid = ?",
                            (normalized, rowid)
                        )
                    updated += 1

            stats['tables'][f'{table}.{field}'] = updated
            stats['updated'] += updated
            if updated > 0:
                print(f'  {table}.{field}: {updated} 条更新')

        except Exception as e:
            print(f'  ⚠️ {table}.{field}: {e}')

    return stats


def normalize_codes(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """统一股票代码格式"""
    stats = {'updated': 0}

    # 需要标准化的表和字段
    code_fields = [
        ('prices', 'stock_code'),
        ('earnings', 'stock_code'),
        ('analysis_results', 'stock_code'),
        ('discovery_pool', 'stock_code'),
        ('event_tracking', 'stock_code'),
        ('events', 'stock_code'),
    ]

    for table, field in code_fields:
        try:
            rows = conn.execute(
                f"SELECT rowid, {field} FROM {table} WHERE {field} IS NOT NULL"
            ).fetchall()

            updated = 0
            for rowid, raw_val in rows:
                normalized = normalizer.normalize_code(raw_val)
                if normalized and normalized != raw_val:
                    if not dry_run:
                        conn.execute(
                            f"UPDATE {table} SET {field} = ? WHERE rowid = ?",
                            (normalized, rowid)
                        )
                    updated += 1

            if updated > 0:
                print(f'  {table}.{field}: {updated} 条更新')
                stats['updated'] += updated

        except Exception as e:
            print(f'  ⚠️ {table}.{field}: {e}')

    return stats


def cleanup_stocks(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """合并重复 stocks 记录"""
    db_path = conn.execute("PRAGMA database_list").fetchone()[2]
    if not db_path or db_path == '':
        db_path = str(PROJECT_ROOT / 'data' / 'smart_invest.db')

    resolver = StockResolver(db_path)

    if not dry_run:
        stats = resolver.cleanup_duplicates()
    else:
        # 统计有多少需要合并
        bare_codes = conn.execute("""
            SELECT code FROM stocks 
            WHERE code NOT LIKE '%.%' AND length(code) = 6 AND code GLOB '[0-9]*'
        """).fetchall()
        stats = {'merged': len(bare_codes), 'deleted': 0, 'errors': 0}
        print(f'  发现 {len(bare_codes)} 个无后缀代码需要标准化')

    return stats


def fix_date_comparisons(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """
    修复 Bug#21：统一 event_tracking 中 event_date 格式为 YYYY-MM-DD，
    确保与 prices.trade_date（YYYYMMDD）比较时能正确工作。
    
    这一步在 normalize_dates 之后执行，确保格式一致。
    """
    stats = {'reset_returns': 0}

    # 找出 event_date > 最新价格日期的记录（这些记录的收益计算是错误的）
    bad_records = conn.execute("""
        SELECT et.id, et.stock_code, et.event_date,
               (SELECT MAX(trade_date) FROM prices p WHERE p.stock_code = et.stock_code) as max_price_date
        FROM event_tracking et
        WHERE et.return_1d IS NOT NULL
          AND et.event_date IS NOT NULL
    """).fetchall()

    reset_ids = []
    for row in bad_records:
        rid, code, event_date, max_price_date = row
        if not event_date or not max_price_date:
            continue

        # 统一格式后比较
        event_compact = normalizer.normalize_date_compact(event_date)
        if event_compact and max_price_date and event_compact > max_price_date:
            reset_ids.append(rid)

    if reset_ids and not dry_run:
        placeholders = ','.join('?' * len(reset_ids))
        conn.execute(f"""
            UPDATE event_tracking
            SET return_1d = NULL, return_5d = NULL, return_10d = NULL, return_20d = NULL,
                alpha_5d = NULL, alpha_20d = NULL,
                benchmark_1d = NULL, benchmark_5d = NULL,
                benchmark_10d = NULL, benchmark_20d = NULL,
                tracking_status = 'tracking'
            WHERE id IN ({placeholders})
        """, reset_ids)

    stats['reset_returns'] = len(reset_ids)
    if reset_ids:
        print(f'  重置 {len(reset_ids)} 条错误收益数据')

    return stats


def main():
    parser = argparse.ArgumentParser(description='存量数据标准化清洗')
    parser.add_argument('--dry-run', action='store_true', help='只统计不修改')
    parser.add_argument('--db-path', default=str(PROJECT_ROOT / 'data' / 'smart_invest.db'))
    args = parser.parse_args()

    print(f'{"🔍 DRY RUN 模式" if args.dry_run else "🔧 执行清洗"}')
    print(f'数据库: {args.db_path}')
    print()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    total_stats = {}

    # Step 1: 合并重复 stocks
    print('Step 1: 合并重复 stocks 记录')
    total_stats['stocks'] = cleanup_stocks(conn, args.dry_run)
    print(f'  结果: {total_stats["stocks"]}')
    print()

    # Step 2: 标准化日期
    print('Step 2: 标准化日期格式 → YYYY-MM-DD')
    total_stats['dates'] = normalize_dates(conn, args.dry_run)
    print(f'  总计: {total_stats["dates"]["updated"]} 条')
    print()

    # Step 3: 标准化股票代码
    print('Step 3: 标准化股票代码 → canonical_code')
    total_stats['codes'] = normalize_codes(conn, args.dry_run)
    print(f'  总计: {total_stats["codes"]["updated"]} 条')
    print()

    # Step 4: 修复错误收益数据
    print('Step 4: 修复日期比较导致的错误收益数据')
    total_stats['returns'] = fix_date_comparisons(conn, args.dry_run)
    print(f'  结果: {total_stats["returns"]}')
    print()

    if not args.dry_run:
        conn.commit()
        print('✅ 清洗完成，已提交')
    else:
        print('🔍 Dry run 完成，未修改数据')

    conn.close()


if __name__ == '__main__':
    main()
