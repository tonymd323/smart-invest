#!/usr/bin/env python3
"""
v2.2 → Bitable「数据表」回填
从 SQLite analysis_results 读取 v2.2 分析结果，同步到飞书多维表格
"""
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, '/root/.openclaw/workspace/smart-invest')
from core.bitable_sync import BitableSync, BitableManager

DB_PATH = '/root/.openclaw/workspace/smart-invest/data/smart_invest.db'
APP_TOKEN = 'CvTRbdVyfa9PnMsnzIXcCSNmnnb'
TABLE_ID = 'tbluSQrjOW0tppTP'


def date_to_ts(date_str: str) -> int:
    """日期字符串转毫秒时间戳"""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return None


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 读取 v2.2 的分析结果（只取 buy/watch/hold，不含 N/A）
    rows = conn.execute("""
        SELECT ar.stock_code, ar.analysis_type, ar.score, ar.signal, 
               ar.summary, ar.created_at,
               e.report_date, e.net_profit, e.net_profit_yoy, 
               e.revenue_yoy, e.gross_margin, e.roe, e.eps
        FROM analysis_results ar
        LEFT JOIN earnings e ON ar.stock_code = e.stock_code 
            AND e.report_date = (
                SELECT MAX(report_date) FROM earnings 
                WHERE stock_code = ar.stock_code
            )
        WHERE ar.signal IN ('buy', 'watch', 'hold')
          AND ar.analysis_type IN ('earnings_beat', 'profit_new_high', 'pullback_score')
        ORDER BY ar.score DESC, ar.created_at DESC
    """).fetchall()

    print(f"从 SQLite 读取 {len(rows)} 条分析结果")

    # 构造 Bitable 记录
    records = []
    seen = set()  # (stock_code, report_date) 去重

    for r in rows:
        stock_code = r['stock_code']
        report_date = r['report_date'] or ''
        key = (stock_code, report_date)

        if key in seen:
            # 同一只股票同一报告期，合并分析类型
            existing = next(
                (rec for rec in records if rec['fields']['股票代码'] == stock_code 
                 and rec['fields'].get('报告期') == date_to_ts(report_date)),
                None
            )
            if existing:
                analysis_type = r['analysis_type']
                if analysis_type == 'profit_new_high':
                    existing['fields']['是否扣非新高'] = True
                    existing['fields']['信号类型'] = existing['fields'].get('信号类型', '') + ' +扣非新高'
                elif analysis_type == 'pullback_score':
                    existing['fields']['信号类型'] = existing['fields'].get('信号类型', '') + ' +回调'
            continue

        seen.add(key)

        # 解析 analysis_type
        type_map = {
            'earnings_beat': '超预期',
            'profit_new_high': '扣非新高',
            'pullback_score': '回调买入',
        }
        signal_type = type_map.get(r['analysis_type'], r['analysis_type'])

        fields = {
            '股票代码': stock_code,
            '信号类型': signal_type,
            '扫描日期': date_to_ts(r['created_at'][:10] if r['created_at'] else ''),
        }

        # 超预期相关
        if r['analysis_type'] == 'earnings_beat':
            fields['是否超预期'] = r['signal'] in ('buy', 'watch')
        else:
            fields['是否超预期'] = False

        # 扣非新高
        if r['analysis_type'] == 'profit_new_high':
            fields['是否扣非新高'] = True
        else:
            fields['是否扣非新高'] = False

        # 财务数据
        if r['net_profit_yoy'] is not None:
            fields['利润增速'] = round(r['net_profit_yoy'], 1)
        if r['revenue_yoy'] is not None:
            fields['营收增速'] = round(r['revenue_yoy'], 1)
        if r['net_profit'] is not None:
            fields['扣非净利润(亿)'] = round(r['net_profit'], 2)
        if report_date:
            fields['报告期'] = date_to_ts(report_date)

        records.append({"fields": fields})

    print(f"生成 {len(records)} 条 Bitable 记录")

    if not records:
        print("无记录，退出")
        return

    # 同步到 Bitable
    sync = BitableSync(
        app_token=APP_TOKEN,
        table_id=TABLE_ID,
        dedup_keys=['股票代码', '报告期'],
    )
    new_count = sync.sync(records)
    print(f"同步完成: {len(records)} 条生成 → {new_count} 条新增")

    conn.close()


if __name__ == '__main__':
    main()
