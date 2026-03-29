"""T+N 跟踪页面路由 — v2.10 列表增强"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_conn, paginate_query, map_event_type, map_tracking_status

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_WHITELIST = ['event_date', 'return_1d', 'return_5d', 'return_10d', 'return_20d', 'stock_code']


@router.get("/tracking", response_class=HTMLResponse)
async def tracking_page(
    request: Request,
    status: Optional[str] = None,
    search: Optional[str] = None,
    sort: str = Query("event_date"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
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

    search_cols = ['et.stock_code', 's.name'] if search else None
    rows, total, total_pages = paginate_query(
        conn, sql, params, page, 20,
        search=search, search_cols=search_cols,
        sort=sort, order=order, sort_whitelist=SORT_WHITELIST,
    )
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d['event_type_zh'] = map_event_type(d.get('event_type'))
        d['tracking_status_zh'] = map_tracking_status(d.get('tracking_status'))
        results.append(d)

    return templates.TemplateResponse("tracking.html", {
        "request": request, "active": "tracking",
        "db_stats": db_stats,
        "tracking": results,
        "status": status or "",
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/tracking",
    })
