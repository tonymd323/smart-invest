"""事件流页面路由 — v2.18 多选筛选 + 报告期筛选"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional, List
import math

from web.services import get_db_stats, get_conn, map_event_type

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/events", response_class=HTMLResponse)
async def events_page(
    request: Request,
    event_type: Optional[List[str]] = Query(None),
    report_period: Optional[List[str]] = Query(None),
    sentiment: Optional[str] = None,
    days: int = Query(7),
    search: Optional[str] = None,
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
    conn = get_conn()
    time_filter = f"-{days} days"
    page_size = 20

    # 查询1: 新闻事件（events表）
    sql1 = """
        SELECT e.id, e.stock_code, e.event_type, e.title, e.content,
               e.source, e.url, e.sentiment, e.severity, e.published_at, e.created_at,
               COALESCE(s.name, e.stock_code) as stock_name, 'news' as source_type
        FROM events e
        LEFT JOIN stocks s ON e.stock_code = s.code
        WHERE e.created_at >= datetime('now', ?)
    """
    params1 = [time_filter]
    if event_type:
        placeholders = ','.join(['?'] * len(event_type))
        sql1 += f" AND e.event_type IN ({placeholders})"
        params1.extend(event_type)
    if sentiment:
        sql1 += " AND e.sentiment = ?"
        params1.append(sentiment)
    if search:
        sql1 += " AND (e.stock_code LIKE ? OR s.name LIKE ? OR e.title LIKE ?)"
        params1.extend([f"%{search}%"] * 3)

    # 查询2: 信号跟踪（event_tracking表）
    sql2 = """
        SELECT et.id, et.stock_code, et.event_type, et.event_date,
               et.report_period, et.actual_yoy, et.expected_yoy,
               et.profit_diff, et.entry_price, et.return_1d, et.return_5d,
               et.return_10d, et.return_20d, et.tracking_status,
               et.created_at,
               COALESCE(s.name, et.stock_code) as stock_name,
               COALESCE(s.industry, '') as industry,
               'signal' as source_type
        FROM event_tracking et
        LEFT JOIN stocks s ON et.stock_code = s.code
        WHERE et.created_at >= datetime('now', ?)
    """
    params2 = [time_filter]
    if event_type:
        placeholders = ','.join(['?'] * len(event_type))
        sql2 += f" AND et.event_type IN ({placeholders})"
        params2.extend(event_type)
    # 多选：报告期
    if report_period:
        placeholders = ','.join(['?'] * len(report_period))
        sql2 += f" AND et.report_period IN ({placeholders})"
        params2.extend(report_period)
    if search:
        sql2 += " AND (et.stock_code LIKE ? OR s.name LIKE ?)"
        params2.extend([f"%{search}%"] * 2)

    try:
        rows1 = conn.execute(sql1, params1).fetchall()
    except Exception:
        rows1 = []
    try:
        rows2 = conn.execute(sql2, params2).fetchall()
    except Exception:
        rows2 = []

    conn.close()

    # 合并并排序
    all_events = []
    for r in rows1:
        d = dict(r)
        d['event_type_zh'] = map_event_type(d.get('event_type'))
        all_events.append(d)
    for r in rows2:
        d = dict(r)
        d['event_type_zh'] = map_event_type(d.get('event_type'))
        all_events.append(d)

    # 按 created_at 降序
    all_events.sort(key=lambda x: x.get('created_at', ''), reverse=True)

    # 分页
    total = len(all_events)
    total_pages = max(1, math.ceil(total / page_size))
    start = (page - 1) * page_size
    events = all_events[start:start + page_size]

    # 按日期分组（只对分页后的数据分组）
    from collections import OrderedDict
    grouped = OrderedDict()
    for ev in events:
        date_str = (ev.get('created_at') or '')[:10] or '未知日期'
        if date_str not in grouped:
            grouped[date_str] = []
        grouped[date_str].append(ev)

    return templates.TemplateResponse("events.html", {
        "request": request, "active": "events",
        "db_stats": db_stats,
        "events": events,        # 保留分页后的列表（备用）
        "grouped_events": grouped,# 按日期分组的完整列表
        "days": days,
        "current_event_types": event_type or [],
        "current_report_periods": report_period or [],
        "sentiment": sentiment or "",
        "search": search,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/events",
    })
