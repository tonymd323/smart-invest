from datetime import datetime
"""事件流页面路由 — 仅显示新闻/公告类事件（不含信号跟踪）"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional, List
import math

from web.services import get_db_stats, get_conn, map_event_type

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# 只显示新闻/公告类型（不含超预期和扣非新高，这些在信号看板）
NEWS_EVENT_TYPES = [
    ("finance_report", "财报公告"),
    ("finance_dividend", "分红派息"),
    ("major_contract", "重大合同"),
    ("capital_buy", "增持"),
    ("capital_sell", "减持"),
    ("policy利好", "政策利好"),
    ("policy利空", "政策利空"),
    ("industry_up", "行业景气"),
    ("industry_down", "行业下行"),
    ("risk_warning", "风险警示"),
    ("ops_restructure", "资产重组"),
]


@router.get("/events", response_class=HTMLResponse)
async def events_page(
    request: Request,
    event_type: Optional[List[str]] = Query(None),
    sentiment: Optional[str] = None,
    days: int = Query(7),
    search: Optional[str] = None,
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
    conn = get_conn()
    time_filter = f"-{days} days"
    page_size = 20

    # 只查新闻/公告事件（排除信号类）
    signal_types = ("earnings_beat", "profit_new_high", "earnings_beat_daily",
                    "quarterly_profit_new_high_daily", "pullback_score", "oversold_btiq")

    sql = """
        SELECT e.id, e.stock_code, e.event_type, e.title, e.content,
               e.source, e.url, e.sentiment, e.severity, e.published_at, e.created_at,
               COALESCE(s.name, e.stock_code) as stock_name
        FROM events e
        LEFT JOIN stocks s ON e.stock_code = s.code
        WHERE e.created_at >= datetime('now', ?)
          AND e.event_type NOT IN ('earnings_beat', 'profit_new_high')
    """
    params = [time_filter]
    if event_type:
        placeholders = ','.join(['?'] * len(event_type))
        sql += f" AND e.event_type IN ({placeholders})"
        params.extend(event_type)
    if sentiment:
        sql += " AND e.sentiment = ?"
        params.append(sentiment)
    if search:
        sql += " AND (e.stock_code LIKE ? OR s.name LIKE ? OR e.title LIKE ?)"
        params.extend([f"%{search}%"] * 3)

    # COUNT
    count_sql = f"SELECT COUNT(*) FROM ({sql})"
    total = conn.execute(count_sql, params).fetchone()[0]

    # 排序 + 分页
    sql += " ORDER BY e.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        rows = []

    conn.close()

    # 格式化
    all_events = []
    for r in rows:
        d = dict(r)
        d['event_type_zh'] = map_event_type(d.get('event_type'))
        all_events.append(d)

    total_pages = max(1, math.ceil(total / page_size))

    # 按日期分组
    from collections import OrderedDict
    grouped = OrderedDict()
    for ev in all_events:
        date_str = (ev.get('created_at') or '')[:10] or '未知日期'
        if date_str not in grouped:
            grouped[date_str] = []
        grouped[date_str].append(ev)

    return templates.TemplateResponse("events.html", {
        "request": request, "active": "events",
        "db_stats": db_stats,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "events": all_events,
        "grouped_events": grouped,
        "days": days,
        "current_event_types": event_type or [],
        "current_report_periods": [],
        "sentiment": sentiment or "",
        "search": search,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/events",
        "event_type_options": NEWS_EVENT_TYPES,
    })
