#!/usr/bin/env python3
"""
backfill_bitable_v2.py - 修复并回填飞书多维表格数据

修复问题：
1. 事件表：补充 earnings_beat 事件，详情字段格式化为人类可读文本
2. 发现池：公司名称用真实名称，补充行业和总市值
3. T+N 跟踪：从 event_tracking 表生成

用法：
    python3 scripts/backfill_bitable_v2.py --dry-run  # 预览
    python3 scripts/backfill_bitable_v2.py             # 执行回填
"""

import json
import sqlite3
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────────────────────────────

APP_TOKEN = 'CvTRbdVyfa9PnMsnzIXcCSNmnnb'
TABLE_EVENTS = 'tblUgPIXejUOggWx'
TABLE_DISCOVERY = 'tblPKXYUsow2Pd6A'
TABLE_TRACKING = 'tblNZIrovX0WRmW3'

DB_PATH = Path(__file__).parent.parent / 'data' / 'smart_invest.db'
STOCKS_JSON = Path(__file__).parent.parent / 'config' / 'stocks.json'
INDUSTRY_MV_JSON = Path(__file__).parent.parent / 'data' / 'industry_mv_map.json'

CST = timezone(timedelta(hours=8))

# ── 工具函数 ──────────────────────────────────────────────────────────────────

def date_to_ts_ms(date_str):
    """日期字符串转毫秒时间戳"""
    if not date_str or date_str == 'None':
        return None
    try:
        ds = str(date_str).replace('-', '').replace('/', '')[:8]
        dt = datetime.strptime(ds, '%Y%m%d')
        return int(dt.timestamp()) * 1000
    except (ValueError, TypeError):
        return None

def parse_date_str(date_str):
    """解析日期为 YYYY-MM-DD"""
    if not date_str or date_str == 'None':
        return None
    ds = str(date_str).replace('-', '').replace('/', '')
    if len(ds) == 8:
        return f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
    return date_str

def now_ts_ms():
    return int(datetime.now().timestamp()) * 1000

# ── 数据加载 ──────────────────────────────────────────────────────────────────

def load_stocks():
    """从 stocks.json 加载股票名称映射"""
    with open(STOCKS_JSON, 'r', encoding='utf-8') as f:
        data = json.load(f)
    name_map = {}
    for s in data.get('holdings', []) + data.get('watchlist', []):
        name_map[s['code']] = s['name']
    return name_map

def load_industry_mv():
    """从 industry_mv_map.json 加载行业和市值"""
    with open(INDUSTRY_MV_JSON, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # data is a dict with keys: industry, mv, trade_date
    if isinstance(data, dict):
        industry_map = data.get('industry', {})
        mv_map = data.get('mv', {})
    elif isinstance(data, (list, tuple)):
        industry_map = data[0] if len(data) > 0 else {}
        mv_map = data[1] if len(data) > 1 else {}
    else:
        industry_map = {}
        mv_map = {}
    return industry_map, mv_map

def load_db_names():
    """从 SQLite 多表加载股票名称"""
    conn = sqlite3.connect(str(DB_PATH))
    name_map = {}
    
    # 1. 从 stocks 表
    cur = conn.execute("SELECT code, name FROM stocks")
    for row in cur:
        name_map[row[0]] = row[1]
    
    # 2. 从 event_tracking 表（补充更多名称）
    cur = conn.execute("""SELECT DISTINCT stock_code, stock_name FROM event_tracking 
                          WHERE stock_name IS NOT NULL AND stock_name != ''
                          AND stock_name NOT LIKE '%.%' """)
    for row in cur:
        if row[0] not in name_map:
            name_map[row[0]] = row[1]
    
    # 3. 从 earnings_beat_daily 的 summary JSON 中提取 name
    cur = conn.execute("""SELECT DISTINCT stock_code, summary FROM analysis_results 
                          WHERE analysis_type='earnings_beat_daily' LIMIT 500""")
    for row in cur:
        if row[0] not in name_map:
            try:
                import ast
                data = ast.literal_eval(row[1])
                name = data.get('name', '')
                if name and '.' not in name:
                    name_map[row[0]] = name
            except:
                pass
    
    conn.close()
    return name_map

# ── 格式化函数 ────────────────────────────────────────────────────────────────

def format_profit_new_high(data):
    """格式化扣非新高事件为人类可读文本"""
    code = data.get('stock_code', '')
    period = parse_date_str(data.get('report_date', data.get('report_period', '')))
    profit = data.get('quarterly_net_profit', data.get('quarterly_profit', 0))
    prev_high = data.get('prev_quarterly_high', 0)
    growth = data.get('growth_pct', 0)
    is_new = data.get('is_new_high', False)

    if profit is None:
        profit = 0
    if prev_high is None:
        prev_high = 0
    if growth is None:
        growth = 0

    profit_str = f"{profit:.2f}" if abs(profit) >= 0.01 else f"{profit:.4f}"
    growth_sign = '+' if growth >= 0 else ''

    if is_new:
        detail = f"单季度扣非净利润 {profit_str} 亿，创历史新高，环比增长 {growth_sign}{growth:.1f}%"
    else:
        detail = f"单季度扣非净利润 {profit_str} 亿，历史最高 {prev_high:.2f} 亿，环比增长 {growth_sign}{growth:.1f}%"
    
    return detail

def format_earnings_beat(data):
    """格式化超预期事件为人类可读文本"""
    code = data.get('stock_code', '')
    period = parse_date_str(data.get('report_date', data.get('report_period', '')))
    actual_yoy = data.get('actual_profit_yoy', data.get('actual_yoy', 0))
    expected_yoy = data.get('expected_profit_yoy', data.get('expected_yoy', 0))
    beat_diff = data.get('beat_diff_pct', data.get('beat_diff', 0))
    is_beat = data.get('is_beat', False)
    revenue_yoy = data.get('actual_rev_yoy')

    if actual_yoy is None:
        actual_yoy = 0
    if expected_yoy is None:
        expected_yoy = 0
    if beat_diff is None:
        beat_diff = 0

    parts = []
    if actual_yoy is not None:
        parts.append(f"净利润同比 {actual_yoy:+.1f}%")
    if expected_yoy is not None:
        parts.append(f"预期 {expected_yoy:+.1f}%")
    if revenue_yoy is not None:
        parts.append(f"营收同比 {revenue_yoy:+.1f}%")
    if is_beat and beat_diff:
        parts.append(f"超预期 {beat_diff:+.1f}个百分点")
    
    return "，".join(parts) if parts else "业绩数据更新"

# ── 事件表生成 ─────────────────────────────────────────────────────────────────

def generate_events(db_names, industry_map, mv_map):
    """从 SQLite analysis_results 生成事件记录"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    records = []
    
    # 1. profit_new_high 事件 - 只要 is_new_high = True 的
    cur = conn.execute("""
        SELECT stock_code, summary, created_at 
        FROM analysis_results 
        WHERE analysis_type = 'profit_new_high'
        ORDER BY created_at DESC
    """)
    
    for row in cur:
        try:
            data = json.loads(row['summary'].replace("'", '"'))
        except (json.JSONDecodeError, TypeError):
            try:
                import ast
                data = ast.literal_eval(row['summary'])
            except:
                continue
        
        if not data.get('is_new_high', False):
            continue
            
        code = data.get('stock_code', row['stock_code'])
        name = db_names.get(code, code)
        detail = format_profit_new_high(data)
        period = parse_date_str(data.get('report_date', data.get('report_period', '')))
        score = data.get('score', 0)
        
        severity = 'high' if score and score >= 90 else ('medium' if score and score >= 70 else 'low')
        
        profit = data.get('quarterly_net_profit', data.get('quarterly_profit', 0))
        profit_str = f"{profit:.2f}" if profit and abs(profit) >= 0.01 else f"{profit:.4f}" if profit else "N/A"
        
        records.append({
            'fields': {
                '股票代码': code,
                '公司名称': name,
                '事件类型': 'profit_new_high',
                '情感': 'positive',
                '严重程度': severity,
                '标题': f"{name} 单季度净利润新高 {profit_str}亿",
                '详情': detail,
                '检测时间': now_ts_ms(),
            }
        })

    # 2. earnings_beat 事件 - 只要 is_beat = True 的
    cur = conn.execute("""
        SELECT stock_code, summary, created_at 
        FROM analysis_results 
        WHERE analysis_type = 'earnings_beat'
        ORDER BY created_at DESC
    """)
    
    for row in cur:
        try:
            data = json.loads(row['summary'].replace("'", '"'))
        except (json.JSONDecodeError, TypeError):
            try:
                import ast
                data = ast.literal_eval(row['summary'])
            except:
                continue
        
        if not data.get('is_beat', False):
            continue
            
        code = data.get('stock_code', row['stock_code'])
        name = data.get('stock_name', db_names.get(code, code))
        # 跳过 stock_name 是代码的情况
        if name == code or '.' not in name:
            name = db_names.get(code, code)
            
        detail = format_earnings_beat(data)
        period = parse_date_str(data.get('report_date', data.get('report_period', '')))
        beat_diff = data.get('beat_diff_pct', data.get('beat_diff', 0))
        
        severity = 'high' if beat_diff and beat_diff > 10 else ('medium' if beat_diff and beat_diff > 5 else 'low')
        
        records.append({
            'fields': {
                '股票代码': code,
                '公司名称': name,
                '事件类型': 'earnings_beat',
                '情感': 'positive',
                '严重程度': severity,
                '标题': f"{name} 业绩超预期",
                '详情': detail,
                '检测时间': now_ts_ms(),
            }
        })

    conn.close()
    return records

# ── 发现池生成 ─────────────────────────────────────────────────────────────────

def generate_discovery(db_names, industry_map, mv_map):
    """从 SQLite discovery_pool 生成发现池记录"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    records = []
    cur = conn.execute("""
        SELECT stock_code, stock_name, source, score, signal, 
               detail, status, discovered_at, expires_at, industry
        FROM discovery_pool
        WHERE status = 'active'
        ORDER BY score DESC
    """)
    
    for row in cur:
        code = row['stock_code']
        name = db_names.get(code, row['stock_name'] or code)
        industry = row['industry'] or industry_map.get(code, '')
        
        # 市值（万元转亿元）
        mv = mv_map.get(code)
        mv_yi = round(mv / 10000, 2) if mv else None
        
        # 发现来源映射
        source_map = {
            'earnings_beat': '超预期',
            'profit_new_high': '扣非新高',
            'pullback_buy': '回调买入',
            'consensus_upgrade': '事件驱动',
        }
        source_cn = source_map.get(row['source'], row['source'])
        
        # 信号映射
        signal_map = {'buy': 'buy', 'watch': 'watch', 'hold': 'hold'}
        signal = signal_map.get(row['signal'], 'watch')
        
        # 过期时间
        expires_ts = date_to_ts_ms(row['expires_at']) if row['expires_at'] else now_ts_ms() + 30 * 24 * 3600 * 1000
        discovered_ts = date_to_ts_ms(row['discovered_at']) if row['discovered_at'] else now_ts_ms()
        
        fields = {
            '股票代码': code,
            '公司名称': name,
            '发现来源': source_cn,
            '评分': row['score'] or 0,
            '信号': signal,
            '发现时间': discovered_ts,
            '状态': 'active',
            '过期时间': expires_ts,
        }
        if industry:
            fields['行业'] = industry
        if mv_yi is not None:
            fields['总市值(亿)'] = mv_yi
            
        records.append({'fields': fields})
    
    conn.close()
    return records

# ── T+N 跟踪生成 ──────────────────────────────────────────────────────────────

def generate_tracking(db_names):
    """从 SQLite event_tracking 生成 T+N 跟踪记录"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    records = []
    cur = conn.execute("""
        SELECT stock_code, stock_name, event_type, event_date,
               entry_price, return_1d, return_5d, return_10d, return_20d,
               tracking_status, last_updated
        FROM event_tracking
        ORDER BY event_date DESC
    """)
    
    for row in cur:
        code = row['stock_code']
        name = row['stock_name'] or db_names.get(code, code)
        
        # 事件类型映射
        event_map = {
            'earnings_beat': '超预期',
            'profit_new_high': '扣非新高',
            'pullback_buy': '回调买入',
        }
        event_cn = event_map.get(row['event_type'], row['event_type'])
        
        # 状态映射
        status_map = {
            'pending': 'pending',
            'tracking': 'pending',
            'completed': 'completed',
            'expired': 'expired',
        }
        status = status_map.get(row['tracking_status'], 'pending')
        
        records.append({
            'fields': {
                '股票代码': code,
                '公司名称': name,
                '事件类型': event_cn,
                '入池日期': date_to_ts_ms(row['event_date']),
                '入池价': row['entry_price'],
                'T+1收益(%)': row['return_1d'],
                'T+5收益(%)': row['return_5d'],
                'T+10收益(%)': row['return_10d'],
                'T+20收益(%)': row['return_20d'],
                '状态': status,
                '最新更新': date_to_ts_ms(row['last_updated']),
            }
        })
    
    conn.close()
    return records

# ── 主程序 ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='回填飞书多维表格数据')
    parser.add_argument('--dry-run', action='store_true', help='预览模式，不写入')
    args = parser.parse_args()
    
    # 加载数据
    print("加载股票名称...")
    stocks_json_names = load_stocks()
    db_names = load_db_names()
    # 合并，db_names 优先
    name_map = {**stocks_json_names, **db_names}
    print(f"  stocks.json: {len(stocks_json_names)} 只")
    print(f"  SQLite stocks: {len(db_names)} 只")
    print(f"  合计: {len(name_map)} 只")
    
    print("\n加载行业和市值数据...")
    industry_map, mv_map = load_industry_mv()
    print(f"  行业: {len(industry_map)} 只")
    print(f"  市值: {len(mv_map)} 只")
    
    # 生成事件记录
    print("\n生成事件记录...")
    events = generate_events(name_map, industry_map, mv_map)
    print(f"  共 {len(events)} 条事件")
    
    # 统计事件类型
    event_types = {}
    for e in events:
        t = e['fields']['事件类型']
        event_types[t] = event_types.get(t, 0) + 1
    for t, c in event_types.items():
        print(f"    {t}: {c} 条")
    
    # 生成发现池记录
    print("\n生成发现池记录...")
    discovery = generate_discovery(name_map, industry_map, mv_map)
    print(f"  共 {len(discovery)} 条发现")
    
    # 生成 T+N 跟踪记录
    print("\n生成 T+N 跟踪记录...")
    tracking = generate_tracking(name_map)
    print(f"  共 {len(tracking)} 条跟踪")
    
    if args.dry_run:
        print("\n=== DRY RUN 模式，不写入 ===")
        # 显示前3条
        print("\n事件表前3条:")
        for e in events[:3]:
            print(json.dumps(e, ensure_ascii=False, indent=2))
        print("\n发现池前3条:")
        for d in discovery[:3]:
            print(json.dumps(d, ensure_ascii=False, indent=2))
        print("\n跟踪表前3条:")
        for t in tracking[:3]:
            print(json.dumps(t, ensure_ascii=False, indent=2))
        
        # 保存到文件供检查
        output = {
            'events': events,
            'discovery': discovery,
            'tracking': tracking,
        }
        out_path = Path(__file__).parent.parent / 'data' / 'bitable_backfill_v2_preview.json'
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, default=str)
        print(f"\n预览已保存到 {out_path}")
        return
    
    # 输出 JSON 供写入
    output = {
        'events': events,
        'discovery': discovery,
        'tracking': tracking,
    }
    out_path = Path(__file__).parent.parent / 'data' / 'bitable_backfill_v2.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, default=str)
    print(f"\n回填数据已保存到 {out_path}")
    print(f"  事件: {len(events)} 条")
    print(f"  发现: {len(discovery)} 条")
    print(f"  跟踪: {len(tracking)} 条")
    print("\n下一步：用 feishu_bitable_app_table_record batch_create 写入")

if __name__ == '__main__':
    main()
