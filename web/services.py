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
        for stock in config.get('holdings', []):
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


# ============================================================
# 今日行动 — 综合研判（v2.9 重构）
# ============================================================

def _load_positions():
    """加载持仓配置"""
    config_path = Path(__file__).parent.parent / "config" / "stocks.json"
    if not config_path.exists():
        return []
    with open(config_path) as f:
        config = json.load(f)
    return config.get('holdings', [])


def _get_current_prices(codes: list) -> dict:
    """批量获取实时行情"""
    if not codes:
        return {}
    try:
        from core.data_provider import QuoteProvider
        qp = QuoteProvider()
        prices = {}
        for code in codes:
            records = qp.fetch(code)
            if records:
                q = records[0].to_dict()
                prices[code] = {
                    'price': q.get('price', 0),
                    'change_pct': q.get('change_pct', 0),
                    'high': q.get('high', 0),
                    'low': q.get('low', 0),
                }
        return prices
    except Exception:
        return {}


def get_today_actions():
    """
    今日行动 — 从信号推导出具体操作建议

    返回结构:
    [
        {
            "priority": "buy" | "wait" | "adjust" | "none",
            "emoji": "🔥" | "⏳" | "⚠️" | "☕",
            "stock_code": "600660.SH",
            "stock_name": "福耀玻璃",
            "current_price": 57.85,
            "change_pct": 1.8,
            "reasons": ["超预期 +9.1pp", "回调评分 72", "发现池内"],
            "action_text": "建议买入 ¥56-58，目标 ¥68，止损 ¥50",
            "suggestion": "buy",
            "signals": [...],
        },
        ...
    ]
    """
    conn = get_conn()
    positions = _load_positions()
    pos_map = {p['code']: p for p in positions}
    pos_codes = [p['code'] for p in positions]

    # 1. 获取今日及近3天的信号（只看持仓股+发现池）
    signals_3d = get_scan_results(days=3)
    code_signals = {}
    signal_codes = set()
    for s in signals_3d:
        code = s['stock_code']
        if code not in code_signals:
            code_signals[code] = []
        code_signals[code].append(s)
        signal_codes.add(code)

    # 只查价格的范围：持仓股 + 有信号的股票（最多20只）
    all_codes = list(set(pos_codes) | signal_codes)[:20]

    # 2. 获取发现池状态
    pool = get_discovery_pool()
    pool_map = {p['stock_code']: p for p in pool}

    # 3. 获取实时行情
    prices = _get_current_prices(all_codes)

    # 4. 获取回调评分（只查有信号的股票）
    pullback_scores = {}
    try:
        if signal_codes:
            placeholders = ','.join(['?'] * len(signal_codes))
            rows = conn.execute(f"""
                SELECT stock_code, score, signal, summary
                FROM analysis_results
                WHERE analysis_type = 'pullback_score'
                AND stock_code IN ({placeholders})
                ORDER BY created_at DESC
            """, list(signal_codes)).fetchall()
            for r in rows:
                code = r[0]
                if code not in pullback_scores:
                    pullback_scores[code] = {'score': r[1], 'signal': r[2]}
    except Exception:
        pass

    # 5. 合成行动建议（只处理有信号或有持仓的股票）
    action_codes = set(signal_codes) | set(pos_codes)
    actions = []

    # 从 DB 查股票名称
    name_map = {}
    try:
        rows = conn.execute("SELECT code, name FROM stocks").fetchall()
        name_map = {r[0]: r[1] for r in rows if r[1]}
    except Exception:
        pass

    for code in action_codes:
        pos = pos_map.get(code, {})
        code_sigs = code_signals.get(code, [])
        in_pool = code in pool_map
        price_info = prices.get(code, {})
        current_price = price_info.get('price', 0)
        change_pct = price_info.get('change_pct', 0)
        pb = pullback_scores.get(code, {})
        target = pos.get('target')
        stop_loss = pos.get('stop_loss')
        is_holding = code in pos_codes  # 在持仓列表中即为持有
        entry_price = pos.get('cost') or pos.get('entry')  # cost=stocks.json字段名

        reasons = []
        signals_detail = []
        priority = 'none'
        action_text = ''

        # 分析信号
        has_beat = False
        has_new_high = False
        beat_diff = 0
        for sig in code_sigs:
            atype = sig.get('analysis_type', '')
            if 'earnings_beat' in atype:
                has_beat = True
                signals_detail.append(sig)
                st = sig.get('summary_text', '')
                reasons.append(st.split('|')[0].strip() if st else '超预期')
                # 提取 beat_diff
                try:
                    if '超预期差' in st:
                        diff_str = st.split('超预期差:')[1].split('pp')[0].strip().replace('+', '').replace('📈', '').replace('🔥', '').strip()
                        beat_diff = float(diff_str)
                except:
                    pass
            elif 'profit_new_high' in atype:
                has_new_high = True
                signals_detail.append(sig)
                reasons.append('扣非新高')

        # 发现池状态
        if in_pool:
            reasons.append('发现池内')

        # 回调评分
        pb_score = pb.get('score', 0)
        if pb_score and pb_score >= 60:
            reasons.append(f'回调评分 {pb_score:.0f}')

        # === 生成行动建议 ===

        if has_beat and beat_diff > 0:
            # 超预期信号
            if is_holding:
                # 已持仓
                if beat_diff > 20:
                    priority = 'buy'
                    if current_price > 0:
                        entry_low = current_price * 0.99
                        entry_high = current_price * 1.01
                        action_text = f'超预期强劲，可加仓 ¥{entry_low:.2f}-{entry_high:.2f}'
                    else:
                        action_text = '超预期强劲，可考虑加仓'
                    if target:
                        action_text += f'，目标 ¥{target:.2f}'
                else:
                    priority = 'wait'
                    action_text = '超预期但幅度有限，持有观望'
            else:
                # 未持仓
                if pb_score and pb_score >= 60 and current_price > 0:
                    priority = 'buy'
                    entry_low = current_price * 0.98
                    entry_high = current_price * 1.01
                    action_text = f'超预期+回调到位，建议买入 ¥{entry_low:.2f}-{entry_high:.2f}'
                    if target:
                        action_text += f'，目标 ¥{target:.2f}'
                    if stop_loss:
                        action_text += f'，止损 ¥{stop_loss:.2f}'
                elif in_pool and current_price > 0:
                    priority = 'buy'
                    entry_low = current_price * 0.98
                    entry_high = current_price * 1.01
                    action_text = f'超预期+发现池内，建议买入 ¥{entry_low:.2f}-{entry_high:.2f}'
                    if target:
                        action_text += f'，目标 ¥{target:.2f}'
                elif in_pool:
                    priority = 'buy'
                    action_text = f'超预期+发现池内，建议关注'
                else:
                    priority = 'wait'
                    action_text = f'超预期信号，先加入关注，等回调再买'

        elif has_new_high:
            # 扣非新高
            if pb_score and pb_score >= 60:
                priority = 'buy'
                action_text = f'扣非新高+回调到位，可考虑买入 ¥{current_price*0.98:.2f}-{current_price*1.01:.2f}'
            else:
                priority = 'wait'
                action_text = f'扣非新高但未回调到位，等回调至支撑位再考虑'

        elif is_holding and code in pos_map:
            # 已持仓但无新信号 — 检查风险
            hold_sigs = code_signals.get(code, [])
            has_avoid = any(s.get('signal') == 'avoid' for s in hold_sigs)
            if has_avoid:
                priority = 'adjust'
                action_text = f'信号偏弱，考虑减仓'
                if stop_loss:
                    action_text += f'，止损 ¥{stop_loss:.2f}'
                reasons = [r for r in reasons if '超预期' not in r] + ['低于预期']
            else:
                # 持有中无特殊信号
                pass

        # 生成最终行动项
        if priority != 'none':
            emoji_map = {'buy': '🔥', 'wait': '⏳', 'adjust': '⚠️', 'none': '☕'}
            action = {
                'priority': priority,
                'emoji': emoji_map.get(priority, '☕'),
                'stock_code': code,
                'stock_name': name_map.get(code) or pos.get('name', code),
                'current_price': current_price,
                'change_pct': change_pct,
                'reasons': reasons,
                'action_text': action_text,
                'suggestion': priority,
                'target': target,
                'stop_loss': stop_loss,
                'is_holding': is_holding,
                'entry_price': entry_price,
            }
            actions.append(action)

    # 排序: buy > adjust > wait > none
    priority_order = {'buy': 0, 'adjust': 1, 'wait': 2, 'none': 3}
    actions.sort(key=lambda x: (priority_order.get(x['priority'], 9), -x.get('current_price', 0)))

    conn.close()
    return actions
