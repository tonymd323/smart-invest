#!/usr/bin/env python3
"""修复 Bitable 数据质量问题 - 重新回填发现池/事件/T+N"""
import sqlite3, json, sys
sys.path.insert(0, '/root/.openclaw/workspace/smart-invest')

DB = '/root/.openclaw/workspace/smart-invest/data/smart_invest.db'
APP = 'CvTRbdVyfa9PnMsnzIXcCSNmnnb'

def ts(date_str):
    if not date_str: return None
    try:
        from datetime import datetime
        return int(datetime.strptime(date_str[:10], '%Y-%m-%d').timestamp() * 1000)
    except: return None

def get_name_map(conn):
    """从 stocks.json + analysis_results summary 获取公司名称映射"""
    name_map = {}
    # 从 stocks.json
    try:
        with open('/root/.openclaw/workspace/smart-invest/config/stocks.json') as f:
            stocks = json.load(f)
        for s in stocks.get('holdings', []) + stocks.get('watchlist', []):
            name_map[s['code']] = s['name']
    except: pass
    # 从 analysis_results summary 提取
    rows = conn.execute("SELECT stock_code, summary FROM analysis_results WHERE summary LIKE '%stock_name%' OR summary LIKE '%name%'").fetchall()
    for r in rows:
        try:
            import re
            m = re.search(r"'(?:stock_name|name)':\s*'([^']+)'", r[1])
            if m and r[0] not in name_map:
                name_map[r[0]] = m.group(1)
        except: pass
    # 从 event_tracking 补充
    rows = conn.execute("SELECT DISTINCT stock_code, stock_name FROM event_tracking WHERE stock_name IS NOT NULL AND stock_name != ''").fetchall()
    for r in rows:
        if r[0] not in name_map:
            name_map[r[0]] = r[1]
    return name_map

def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    name_map = get_name_map(conn)
    print(f"公司名称映射: {len(name_map)} 只")
    
    # ═══ 发现池 ═══
    pool_rows = conn.execute("""
        SELECT dp.*, e.gross_margin, e.roe
        FROM discovery_pool dp
        LEFT JOIN earnings e ON dp.stock_code = e.stock_code 
            AND e.report_date = (SELECT MAX(report_date) FROM earnings WHERE stock_code = dp.stock_code)
        ORDER BY dp.score DESC
    """).fetchall()
    
    pool_records = []
    for r in pool_rows:
        code = r['stock_code']
        name = name_map.get(code, code)
        source_map = {'earnings_beat': '超预期', 'profit_new_high': '扣非新高', 'pullback_score': '回调买入'}
        pool_records.append({"fields": {
            "股票代码": code,
            "公司名称": name,
            "发现来源": source_map.get(r['source'], r['source']),
            "评分": r['score'] or 0,
            "信号": dict(r).get('signal', 'watch') or 'watch',
            "发现时间": ts(dict(r).get('discovered_at', '')),
            "状态": dict(r).get('status', 'active') or 'active',
            "过期时间": ts(dict(r).get('expires_at', '')),
        }})
    print(f"发现池: {len(pool_records)} 条")
    
    # ═══ 事件 ═══
    # 从 discovery_pool + analysis_results 生成事件
    evt_records = []
    seen = set()
    
    # profit_new_high 事件
    high_rows = conn.execute("""
        SELECT ar.stock_code, ar.score, ar.summary, ar.created_at
        FROM analysis_results ar
        WHERE ar.analysis_type = 'profit_new_high' AND ar.signal = 'watch'
        ORDER BY ar.score DESC
    """).fetchall()
    for r in high_rows:
        code = r['stock_code']
        key = (code, 'profit_new_high')
        if key in seen: continue
        seen.add(key)
        name = name_map.get(code, code)
        # 从 summary 提取数据
        summary = dict(r).get('summary', '')
        profit = ''
        growth = ''
        try:
            import re
            pm = re.search(r"'quarterly_net_profit':\s*([\d.]+)", summary)
            gm = re.search(r"'growth_pct':\s*([\d.]+)", summary)
            if pm: profit = f"{float(pm.group(1)):.2f} 亿"
            if gm: growth = f"{float(gm.group(1)):.1f}%"
        except: pass
        
        detail = f"{name} 单季度扣非净利润创新高"
        if profit: detail += f"，达 {profit}"
        if growth: detail += f"，环比增长 {growth}"
        
        evt_records.append({"fields": {
            "股票代码": code,
            "公司名称": name,
            "事件类型": "利润新高",
            "情感": "positive",
            "严重程度": "medium",
            "标题": f"{name} 单季度净利润新高 {profit}",
            "详情": detail,
            "检测时间": ts(dict(r).get('created_at', '')[:10]) or ts('2026-03-28'),
        }})
    
    # earnings_beat 事件
    beat_rows = conn.execute("""
        SELECT ar.stock_code, ar.score, ar.signal, ar.summary, ar.created_at
        FROM analysis_results ar
        WHERE ar.analysis_type = 'earnings_beat' AND ar.signal IN ('buy', 'watch')
        ORDER BY ar.score DESC
    """).fetchall()
    for r in beat_rows:
        code = r['stock_code']
        key = (code, 'earnings_beat')
        if key in seen: continue
        seen.add(key)
        name = name_map.get(code, code)
        signal = r['signal']
        score = r['score'] or 0
        
        detail = f"{name} 财报分析结果：评分 {score:.0f}，信号 {signal}"
        severity = "high" if signal == 'buy' else "medium"
        
        evt_records.append({"fields": {
            "股票代码": code,
            "公司名称": name,
            "事件类型": "财报超预期",
            "情感": "positive",
            "严重程度": severity,
            "标题": f"{name} 财报超预期 (score={score:.0f})",
            "详情": detail,
            "检测时间": ts(dict(r).get('created_at', '')[:10]) or ts('2026-03-28'),
        }})
    
    print(f"事件: {len(evt_records)} 条")
    
    # ═══ T+N 跟踪 ═══
    track_rows = conn.execute("""
        SELECT et.*,
            (SELECT close_price FROM prices WHERE stock_code = et.stock_code ORDER BY trade_date DESC LIMIT 1) as latest_price
        FROM event_tracking et
        ORDER BY et.event_date DESC
        LIMIT 200
    """).fetchall()
    
    track_records = []
    for r in track_rows:
        code = r['stock_code']
        name = name_map.get(code, dict(r).get('stock_name') or code)
        entry_price = dict(r).get('entry_price')
        current_price = dict(r).get('latest_price')
        
        # 计算已有收益
        returns = {}
        for n in [1, 5, 10, 20, 60]:
            col = f'return_{n}d'
            val = dict(r).get(col)
            if val is not None:
                returns[f'T+{n}收益(%)'] = round(val, 2)
        
        track_records.append({"fields": {
            "股票代码": code,
            "公司名称": name,
            "事件类型": "超预期" if dict(r).get('event_type') == 'earnings_beat' else "扣非新高",
            "入池日期": ts(dict(r).get('event_date', '')),
            "入池价": round(entry_price, 2) if entry_price else None,
            "状态": dict(r).get('tracking_status', 'pending') or 'pending',
            "当前价格": round(current_price, 2) if current_price else None,
            "最新更新": ts('2026-03-28'),
            **returns,
        }})
    
    print(f"T+N 跟踪: {len(track_records)} 条")
    conn.close()
    
    # 写入文件供后续使用
    with open('/tmp/fix_pool.json', 'w') as f: json.dump(pool_records, f, ensure_ascii=False)
    with open('/tmp/fix_events.json', 'w') as f: json.dump(evt_records, f, ensure_ascii=False)
    with open('/tmp/fix_track.json', 'w') as f: json.dump(track_records, f, ensure_ascii=False)
    print("数据文件已生成")

if __name__ == '__main__':
    main()
