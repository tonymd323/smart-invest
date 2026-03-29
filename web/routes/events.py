"""事件流页面路由 — v2.10 新闻事件"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_conn, paginate_query, map_event_type

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_WHITELIST = ['e.created_at', 'e.published_at', 'e.stock_code']


@router.get("/events", response_class=HTMLResponse)
async def events_page(
    request: Request,
    event_type: Optional[str] = None,
    sentiment: Optional[str] = None,
    days: int = Query(7),
    search: Optional[str] = None,
    sort: str = Query("e.created_at"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
    conn = get_conn()

    sql = """
        SELECT e.id, e.stock_code, e.event_type, e.title, e.content,
               e.source, e.url, e.sentiment, e.sentiment_score,
               e.severity, e.published_at, e.created_at,
               COALESCE(s.name, e.stock_code) as stock_name
        FROM events e
        LEFT JOIN stocks s ON e.stock_code = s.code
        WHERE e.created_at >= datetime('now', ?)
    """
    params = [f'-{days} days']

    if event_type:
        sql += " AND e.event_type = ?"
        params.append(event_type)

    if sentiment:
        sql += " AND e.sentiment = ?"
        params.append(sentiment)

    search_cols = ['e.stock_code', 's.name', 'e.title'] if search else None
    rows, total, total_pages = paginate_query(
        conn, sql, params, page, 20,
        search=search, search_cols=search_cols,
        sort=sort, order=order, sort_whitelist=SORT_WHITELIST,
    )
    conn.close()

    events = []
    for r in rows:
        d = dict(r)
        d['event_type_zh'] = map_event_type(d.get('event_type'))
        events.append(d)

    return templates.TemplateResponse("events.html", {
        "request": request, "active": "events",
        "db_stats": db_stats,
        "events": events,
        "days": days,
        "event_type": event_type or "",
        "sentiment": sentiment or "",
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/events",
    })
