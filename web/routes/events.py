"""事件流页面路由 — v2.10 列表增强"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_conn, paginate_query, map_event_type, map_tracking_status

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_WHITELIST = ['created_at', 'event_date', 'stock_code']


@router.get("/events", response_class=HTMLResponse)
async def events_page(
    request: Request,
    event_type: Optional[str] = None,
    days: int = Query(7),
    search: Optional[str] = None,
    sort: str = Query("created_at"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
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

    search_cols = ['et.stock_code', 's.name'] if search else None
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
        d['tracking_status_zh'] = map_tracking_status(d.get('tracking_status'))
        events.append(d)

    return templates.TemplateResponse("events.html", {
        "request": request, "active": "events",
        "db_stats": db_stats,
        "events": events,
        "days": days,
        "event_type": event_type or "",
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/events",
    })
