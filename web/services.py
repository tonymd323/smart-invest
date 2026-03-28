"""
数据服务层 — 数据库查询 + Pipeline 调用
"""
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = Path(__file__).parent.parent / "data" / "smart_invest.db"


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
              'tn_tracking', 'backtest_results', 'stocks', 'sector_data']
    for t in tables:
        try:
            stats[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            stats[t] = 0
    conn.close()
    stats['db_size_mb'] = round(DB_PATH.stat().st_size / 1024 / 1024, 2) if DB_PATH.exists() else 0
    return stats


def get_discovery_pool(signal_filter: Optional[list] = None, source_filter: Optional[list] = None):
    """获取发现池"""
    conn = get_conn()
    sql = """
        SELECT stock_code, stock_name, industry, source, score, signal,
               status, discovered_at, expires_at, entry_price, target_price, stop_loss
        FROM discovery_pool
        WHERE status = 'active'
    """
    params = []
    if signal_filter:
        placeholders = ','.join(['?'] * len(signal_filter))
        sql += f" AND signal IN ({placeholders})"
        params.extend(signal_filter)
    sql += " ORDER BY score DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_scan_results(days: int = 7, analysis_type: Optional[str] = None):
    """获取扫描结果"""
    conn = get_conn()
    sql = """
        SELECT ar.id, ar.stock_code, COALESCE(s.name, ar.stock_code) as stock_name,
               s.industry, ar.analysis_type, ar.score, ar.signal, ar.summary,
               ar.created_at, ar.metadata
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
    return [dict(r) for r in rows]


def get_events(days: int = 7, event_type: Optional[str] = None, sentiment: Optional[str] = None):
    """获取事件流"""
    conn = get_conn()
    sql = """
        SELECT et.*, COALESCE(s.name, et.stock_code) as stock_name
        FROM event_tracking et
        LEFT JOIN stocks s ON et.stock_code = s.code
        WHERE et.created_at >= datetime('now', ?)
    """
    params = [f'-{days} days']
    if event_type:
        sql += " AND et.event_type = ?"
        params.append(event_type)
    if sentiment:
        sql += " AND et.sentiment = ?"
        params.append(sentiment)
    sql += " ORDER BY et.created_at DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_tn_tracking(status: Optional[str] = None):
    """获取 T+N 跟踪"""
    conn = get_conn()
    sql = """
        SELECT tt.*, COALESCE(s.name, tt.stock_code) as stock_name
        FROM tn_tracking tt
        LEFT JOIN stocks s ON tt.stock_code = s.code
        WHERE 1=1
    """
    params = []
    if status:
        sql += " AND tt.tracking_status = ?"
        params.append(status)
    sql += " ORDER BY tt.discovered_at DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_backtest_results(signal_type: Optional[str] = None):
    """获取回测结果"""
    conn = get_conn()
    sql = """
        SELECT br.*, COALESCE(s.name, br.stock_code) as stock_name
        FROM backtest_results br
        LEFT JOIN stocks s ON br.stock_code = s.code
        WHERE 1=1
    """
    params = []
    if signal_type:
        sql += " AND br.signal_type = ?"
        params.append(signal_type)
    sql += " ORDER BY br.signal_date DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_signal_summary():
    """今日信号摘要"""
    conn = get_conn()
    summary = {}
    for atype, label in [('earnings_beat', '超预期'), ('profit_new_high', '扣非新高'), ('pullback_buy', '回调买入')]:
        count = conn.execute(
            "SELECT COUNT(*) FROM analysis_results WHERE analysis_type=? AND date(created_at)=date('now')",
            (atype,)
        ).fetchone()[0]
        summary[label] = count
    conn.close()
    return summary


def get_position_snapshot():
    """持仓快照（从 stocks.json 读取目标配置）"""
    import json
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
        SELECT signal_type,
               COUNT(*) as total,
               AVG(actual_return) as avg_return,
               SUM(CASE WHEN actual_return > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as win_rate,
               AVG(hold_days) as avg_hold_days
        FROM backtest_results
        WHERE actual_return IS NOT NULL
        GROUP BY signal_type
    """
    rows = conn.execute(sql).fetchall()
    conn.close()
    return [dict(r) for r in rows]
