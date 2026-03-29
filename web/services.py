"""
数据服务层 — 数据库查询 + Pipeline 调用
v2.5: 解析 summary JSON → 可读文本, 英文→中文映射, JOIN stocks 补全名称
"""
import sqlite3
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = Path(__file__).parent.parent / "data" / "smart_invest.db"

# ============================================================
# 中文映射表
# ============================================================

ANALYSIS_TYPE_MAP = {
    'earnings_beat': '超预期',
    'earnings_beat_daily': '超预期',
    'profit_new_high': '扣非新高',
    'quarterly_profit_new_high_daily': '扣非新高',
    'pullback_buy_daily': '回调买入',
    'pullback_score': '回调评分',
    'oversold_btiq': '超跌监控',
}

SIGNAL_MAP = {
    'buy': '买入',
    'watch': '关注',
    'hold': '持有',
    'avoid': '回避',
    'sell': '卖出',
    'N/A': '无信号',
    'S': 'S级',
    'A': 'A级',
    'B': 'B级',
}

SOURCE_MAP = {
    'earnings_beat': '超预期',
    'profit_new_high': '扣非新高',
    'pullback': '回调买入',
    'oversold': '超跌',
}

EVENT_TYPE_MAP = {
    'earnings_beat': '超预期',
    'profit_new_high': '扣非新高',
    'new_contract': '新合同',
    'production_increase': '增产',
    'buyback': '回购',
    'merger': '并购',
    'policy': '政策',
    'risk': '风险',
}

TRACKING_STATUS_MAP = {
    'active': '跟踪中',
    'expired': '已过期',
    'completed': '已完成',
    'pending': '待处理',
}

DISCLOSURE_TYPE_MAP = {
    '业绩预告': '业绩预告',
    '财报': '财报',
    '业绩快报': '业绩快报',
}


# ============================================================
# 格式化工具
# ============================================================

def _parse_json_or_dict(s: str) -> dict:
    """解析 JSON 或 Python dict 字符串"""
    import ast
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        pass
    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        pass
    return {}


def format_summary(summary_str: str, analysis_type: str = '') -> str:
    """将 summary JSON/dict 字符串转为人类可读文本
    
    支持多种格式：
    - earnings_beat_daily: {"name": ..., "profit_diff": ...} (双引号 JSON)
    - earnings_beat (v2):  {'stock_code': ..., 'beat_diff_pct': ...} (单引号 Python dict)
    - quarterly_profit_new_high_daily: {"name": ..., "growth_vs_high": ...}
    - profit_new_high (v2): {'stock_code': ..., 'growth_pct': ...}
    """
    if not summary_str:
        return ''
    
    # 跳过非结构化文本
    if not summary_str.strip().startswith('{'):
        return summary_str
    
    data = _parse_json_or_dict(summary_str)
    if not data:
        return summary_str[:200]  # 解析失败，截断返回

    parts = []
    atype = data.get('analysis_type', analysis_type)

    if 'earnings_beat' in atype:
        # 超预期信号 — 兼容多种格式
        # daily 格式: {name, disclosure_type, actual_profit_yoy, expected_profit_yoy, profit_diff, ...}
        # v2 格式:    {stock_code, stock_name, actual_profit_yoy, beat_diff_pct, report_period, ...}
        name = data.get('name') or data.get('stock_name') or data.get('stock_code', '')
        # 过滤掉仍然是代码格式的名称
        if name and '.S' in name:
            name = data.get('name', name)
        dtype = DISCLOSURE_TYPE_MAP.get(data.get('disclosure_type', ''), data.get('disclosure_type', ''))
        actual = data.get('actual_profit_yoy') or data.get('actual_yoy')
        expected = data.get('expected_profit_yoy') or data.get('expected_yoy')
        diff = data.get('profit_diff') or data.get('beat_diff') or data.get('beat_diff_pct')
        rev_actual = data.get('actual_rev_yoy')
        rev_expected = data.get('expected_rev_yoy')
        report = data.get('report_date') or data.get('report_period', '')
        dedt = data.get('profit_dedt')
        is_beat = data.get('is_beat')
        score = data.get('score')

        if name and '.S' not in str(name):
            parts.append(f"📊 {name}")
        if dtype:
            parts.append(f"类型: {dtype}")
        if actual is not None:
            parts.append(f"实际利润增速: {actual:+.1f}%")
        if expected is not None:
            parts.append(f"预期增速: {expected:+.1f}%")
        if diff is not None:
            emoji = '🔥' if diff > 20 else '📈' if diff > 0 else '📉'
            parts.append(f"{emoji} 超预期差: {diff:+.1f}pp")
        if is_beat is True:
            parts.append("✅ 确认超预期")
        elif is_beat is False:
            parts.append("❌ 未超预期")
        if rev_actual is not None and rev_expected is not None:
            parts.append(f"营收: {rev_actual:+.1f}% vs 预期{rev_expected:+.1f}%")
        if dedt is not None:
            parts.append(f"扣非净利: {dedt:.2f}亿")
        if score is not None and score > 0:
            parts.append(f"评分: {score:.0f}")
        if report:
            parts.append(f"报告期: {report}")

    elif 'profit_new_high' in atype or 'new_high' in atype:
        # 扣非新高 — 兼容两种格式
        name = data.get('name') or data.get('stock_code', '')
        # 过滤掉代码格式的名称
        if name and '.S' in str(name):
            name = ''
        qnp = data.get('quarterly_net_profit') or data.get('quarterly_profit')
        prev = data.get('prev_quarterly_high') or data.get('prev_high')
        growth = data.get('growth_pct') or data.get('growth_vs_high')
        report = data.get('report_period') or data.get('report_date', '')
        close = data.get('close')
        pe = data.get('pe')

        if name:
            parts.append(f"📊 {name}")
        if qnp is not None:
            parts.append(f"单季度扣非: {qnp:.2f}亿")
        if prev is not None:
            parts.append(f"前高: {prev:.2f}亿")
        if growth is not None:
            parts.append(f"增长: {growth:+.1f}%")
        if close is not None:
            parts.append(f"收盘: ¥{close:.2f}")
        if pe is not None and pe < 1000:
            parts.append(f"PE: {pe:.1f}")
        if report:
            parts.append(f"报告期: {report}")

    elif 'pullback' in atype:
        # 回调买入
        code = data.get('stock_code', '')
        score = data.get('score')
        level = data.get('level', '')

        if code:
            parts.append(f"📊 {code}")
        if score is not None:
            parts.append(f"评分: {score:.0f}")
        if level:
            parts.append(f"级别: {level}")

    else:
        # 通用：取前几个有意义的字段
        skip = {'stock_code', 'analysis_type', 'created_at'}
        for k, v in data.items():
            if k in skip or v is None:
                continue
            if isinstance(v, float):
                parts.append(f"{k}: {v:.2f}")
            else:
                parts.append(f"{k}: {v}")
            if len(parts) >= 5:
                break

    return ' | '.join(parts) if parts else summary_str[:100]


def map_analysis_type(t: str) -> str:
    """分析类型 → 中文"""
    return ANALYSIS_TYPE_MAP.get(t, t) if t else ''


def map_signal(s: str) -> str:
    """信号类型 → 中文"""
    return SIGNAL_MAP.get(s, s) if s else ''


def map_source(s: str) -> str:
    """来源 → 中文"""
    return SOURCE_MAP.get(s, s) if s else ''


def map_event_type(t: str) -> str:
    """事件类型 → 中文"""
    return EVENT_TYPE_MAP.get(t, t) if t else ''


def map_tracking_status(s: str) -> str:
    """跟踪状态 → 中文"""
    return TRACKING_STATUS_MAP.get(s, s) if s else ''


# ============================================================
# 数据库连接
# ============================================================

def get_conn():
    """获取数据库连接"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_db_stats():
    """数据库统计"""
    conn = get_conn()
    stats = {}
    tables = ['earnings', 'analysis_results', 'discovery_pool', 'event_tracking',
              'backtest', 'stocks', 'consensus', 'prices']
    for t in tables:
        try:
            stats[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            stats[t] = 0
    conn.close()
    stats['db_size_mb'] = round(DB_PATH.stat().st_size / 1024 / 1024, 2) if DB_PATH.exists() else 0
    return stats


# ============================================================
# 查询函数（v2.5: 带中文映射 + 名称补全）
# ============================================================

def get_discovery_pool(signal_filter: Optional[list] = None, source_filter: Optional[list] = None):
    """获取发现池 — v2.5: JOIN stocks 补全名称 + 中文映射"""
    conn = get_conn()
    sql = """
        SELECT dp.stock_code,
               COALESCE(s.name, dp.stock_name, dp.stock_code) as stock_name,
               COALESCE(s.industry, dp.industry) as industry,
               dp.source, dp.score, dp.signal,
               dp.status, dp.discovered_at, dp.expires_at
        FROM discovery_pool dp
        LEFT JOIN stocks s ON dp.stock_code = s.code
        WHERE dp.status = 'active'
    """
    params = []
    if signal_filter:
        placeholders = ','.join(['?'] * len(signal_filter))
        sql += f" AND dp.signal IN ({placeholders})"
        params.extend(signal_filter)
    if source_filter:
        placeholders = ','.join(['?'] * len(source_filter))
        sql += f" AND dp.source IN ({placeholders})"
        params.extend(source_filter)
    sql += " ORDER BY dp.score DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d['signal_zh'] = map_signal(d.get('signal'))
        d['source_zh'] = map_source(d.get('source'))
        results.append(d)
    return results


def get_scan_results(days: int = 7, analysis_type: Optional[str] = None):
    """获取扫描结果 — v2.5: JOIN stocks + 中文映射 + 解析 summary"""
    conn = get_conn()
    sql = """
        SELECT ar.id, ar.stock_code,
               COALESCE(s.name, ar.stock_code) as stock_name,
               s.industry, ar.analysis_type, ar.score, ar.signal,
               ar.summary, ar.created_at
        FROM analysis_results ar
        LEFT JOIN stocks s ON ar.stock_code = s.code
        WHERE ar.created_at >= datetime('now', ?)
    """
    params = [f'-{days} days']
    if analysis_type:
        sql += " AND ar.analysis_type = ?"
        params.append(analysis_type)
    sql += " ORDER BY ar.created_at DESC, ar.score DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d['analysis_type_zh'] = map_analysis_type(d.get('analysis_type'))
        d['signal_zh'] = map_signal(d.get('signal'))
        d['summary_text'] = format_summary(d.get('summary', ''), d.get('analysis_type', ''))
        results.append(d)
    return results


def get_events(days: int = 7, event_type: Optional[str] = None, sentiment: Optional[str] = None):
    """获取事件流 — v2.5: JOIN stocks + 中文映射"""
    conn = get_conn()
    sql = """
        SELECT et.id, et.stock_code, et.event_type, et.event_date,
               et.report_period, et.actual_yoy, et.expected_yoy,
               et.profit_diff, et.entry_price, et.return_1d, et.return_5d,
               et.return_10d, et.return_20d, et.tracking_status,
               et.last_updated, et.created_at,
               COALESCE(s.name, et.stock_code) as stock_name
        FROM event_tracking et
        LEFT JOIN stocks s ON et.stock_code = s.code
        WHERE et.created_at >= datetime('now', ?)
    """
    params = [f'-{days} days']
    if event_type:
        sql += " AND et.event_type = ?"
        params.append(event_type)
    if sentiment:
        pass  # event_tracking 无 sentiment 列，暂不筛选
    sql += " ORDER BY et.created_at DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d['event_type_zh'] = map_event_type(d.get('event_type'))
        d['tracking_status_zh'] = map_tracking_status(d.get('tracking_status'))
        results.append(d)
    return results


def get_tn_tracking(status: Optional[str] = None):
    """获取 T+N 跟踪 — v2.5: JOIN stocks + 中文映射"""
    conn = get_conn()
    sql = """
        SELECT et.id, et.stock_code,
               COALESCE(s.name, et.stock_name, et.stock_code) as stock_name,
               et.event_type, et.event_date, et.entry_price,
               et.return_1d, et.return_5d, et.return_10d, et.return_20d,
               et.tracking_status, et.last_updated
        FROM event_tracking et
        LEFT JOIN stocks s ON et.stock_code = s.code
        WHERE et.entry_price IS NOT NULL
    """
    params = []
    if status:
        sql += " AND et.tracking_status = ?"
        params.append(status)
    sql += " ORDER BY et.event_date DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d['event_type_zh'] = map_event_type(d.get('event_type'))
        d['tracking_status_zh'] = map_tracking_status(d.get('tracking_status'))
        results.append(d)
    return results


def get_backtest_results(signal_type: Optional[str] = None):
    """获取回测结果 — v2.5: JOIN stocks + 中文映射"""
    conn = get_conn()
    sql = """
        SELECT b.stock_code, COALESCE(s.name, b.stock_code) as stock_name,
               b.event_type, b.event_date, b.entry_price,
               b.return_5d, b.return_10d, b.return_20d, b.return_60d,
               b.alpha_5d, b.alpha_20d, b.is_win
        FROM backtest b
        LEFT JOIN stocks s ON b.stock_code = s.code
        WHERE 1=1
    """
    params = []
    if signal_type:
        sql += " AND b.event_type = ?"
        params.append(signal_type)
    sql += " ORDER BY b.event_date DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d['event_type_zh'] = map_event_type(d.get('event_type'))
        results.append(d)
    return results


def get_signal_summary():
    """今日信号摘要"""
    conn = get_conn()
    summary = {}
    for atype, label in [('earnings_beat', '超预期'), ('profit_new_high', '扣非新高'), ('pullback_buy_daily', '回调买入')]:
        count = conn.execute(
            "SELECT COUNT(*) FROM analysis_results WHERE analysis_type=? AND date(created_at)=date('now')",
            (atype,)
        ).fetchone()[0]
        summary[label] = count
    conn.close()
    return summary


def get_position_snapshot():
    """持仓快照（从 stocks.json 读取目标配置）"""
    config_path = Path(__file__).parent.parent / "config" / "stocks.json"
    positions = []
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)
        from core.data_provider import QuoteProvider
        qp = QuoteProvider()
        for stock in config.get('stocks', []):
            code = stock.get('code', '')
            try:
                records = qp.fetch(code)
                if records:
                    q = records[0].to_dict()
                    positions.append({
                        'code': code,
                        'name': stock.get('name', code),
                        'price': q.get('price', 0),
                        'change_pct': q.get('change_pct', 0),
                        'target': stock.get('target'),
                        'stop_loss': stock.get('stop_loss'),
                        'entry': stock.get('entry'),
                    })
            except Exception:
                positions.append({
                    'code': code,
                    'name': stock.get('name', code),
                    'price': 0,
                    'change_pct': 0,
                    'target': stock.get('target'),
                    'stop_loss': stock.get('stop_loss'),
                    'entry': stock.get('entry'),
                })
    return positions


def get_strategy_performance():
    """策略胜率统计"""
    conn = get_conn()
    sql = """
        SELECT COALESCE(event_type, 'unknown') as signal_type,
               COUNT(*) as total,
               AVG(return_20d) as avg_return,
               SUM(CASE WHEN return_20d > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as win_rate,
               20 as avg_hold_days
        FROM backtest
        WHERE return_20d IS NOT NULL
        GROUP BY event_type
    """
    rows = conn.execute(sql).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d['signal_type_zh'] = map_event_type(d.get('signal_type'))
        results.append(d)
    return results
