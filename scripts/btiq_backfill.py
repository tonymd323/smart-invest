#!/usr/bin/env python3
"""
BTIQ 历史数据回填
==================
用 Tushare daily 接口获取最近 N 个交易日的涨跌家数，
计算 BTIQ 并写入 btiq_history.json，使 MA5 立即可用。
"""

import os; os.environ['TZ'] = 'Asia/Shanghai'
import sys
import json
import time; time.tzset()
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "oversold")
HISTORY_FILE = os.path.join(DATA_DIR, "btiq_history.json")


def backfill(days=5):
    """回填最近 N 个交易日的 BTIQ 数据"""
    import tushare as ts

    pro = ts.pro_api()

    # 获取最近交易日列表
    end = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=days * 2)).strftime('%Y%m%d')  # 多取几天防止节假日
    cal = pro.trade_cal(exchange='', start_date=start, end_date=end, is_open='1')
    if cal is None or cal.empty:
        print("获取交易日历失败")
        return

    trade_dates = sorted(cal[cal['is_open'] == 1]['cal_date'].tolist())[-days:]
    print(f"回填交易日: {trade_dates}")

    # 加载现有历史
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
    else:
        history = []

    existing_dates = set(h['time'][:10].replace('-', '') for h in history)

    added = 0
    for date in trade_dates:
        if date in existing_dates:
            print(f"  {date}: 已存在，跳过")
            continue

        try:
            df = pro.daily(trade_date=date, fields='ts_code,pct_chg')
            if df is None or df.empty:
                print(f"  {date}: 无数据")
                continue

            df = df.drop_duplicates('ts_code')
            up = len(df[df['pct_chg'] > 0])
            down = len(df[df['pct_chg'] < 0])
            flat = len(df[df['pct_chg'] == 0])
            total = up + down
            btiq = round(up / total * 100, 2) if total > 0 else 0

            # 格式化日期
            date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}"
            entry = {
                'time': f"{date_fmt} 15:00:00",  # 收盘时间
                'up': up,
                'down': down,
                'flat': flat,
                'total': len(df),
                'btiq': btiq,
                'top_gainers': [],
                'top_losers': [],
            }
            history.append(entry)
            added += 1
            print(f"  {date}: BTIQ={btiq}% (up={up} down={down}) ✅")
        except Exception as e:
            print(f"  {date}: Error: {e}")

    # 保存 JSON
    history.sort(key=lambda x: x['time'])
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    # 同步写入数据库（market_snapshots 表）
    _sync_to_db(history)

    print(f"\n回填完成，新增 {added} 条，总计 {len(history)} 条")

    # 计算 MA5
    daily_last = {}
    for h in history:
        day = h['time'][:10]
        daily_last[day] = h['btiq']
    days_sorted = sorted(daily_last.keys(), reverse=True)[:5]
    if len(days_sorted) >= 2:
        values = [daily_last[d] for d in days_sorted]
        ma5 = round(sum(values) / len(values), 2)
        print(f"MA5 = {ma5}% {'🔴 < 30 买入信号!' if ma5 < 30 else ''}")


def _sync_to_db(history):
    """将 JSON 数据同步到 market_snapshots 表"""
    import sqlite3

    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "smart_invest.db")
    if not os.path.exists(db_path):
        print("  数据库不存在，跳过同步")
        return

    conn = sqlite3.connect(db_path)

    # 获取已有时间
    existing = set(r[0] for r in conn.execute(
        "SELECT snapshot_time FROM market_snapshots WHERE source='backfill'"
    ).fetchall())

    btiq_values = []
    added = 0
    for entry in history:
        if entry['time'] in existing:
            btiq_values.append(entry['btiq'])
            continue

        btiq_values.append(entry['btiq'])
        ma5 = round(sum(btiq_values[-5:]) / len(btiq_values[-5:]), 2) if btiq_values else None

        signal = None
        if entry['btiq'] < 15:
            signal = 'buy'
        elif entry['btiq'] < 25:
            signal = 'warn'
        elif entry['btiq'] > 80:
            signal = 'hot'

        conn.execute('''
            INSERT INTO market_snapshots (snapshot_time, btiq, up_count, down_count, flat_count, total_count, ma5, signal, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'backfill')
        ''', (
            entry['time'], entry['btiq'], entry['up'], entry['down'],
            entry['flat'], entry['total'], ma5, signal
        ))
        added += 1

    conn.commit()
    conn.close()
    if added:
        print(f"  数据库同步: +{added} 条")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='BTIQ 历史数据回填')
    parser.add_argument('--days', type=int, default=5, help='回填天数')
    args = parser.parse_args()
    backfill(args.days)
