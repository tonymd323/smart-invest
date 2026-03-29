"""发现池页面路由 — v2.10 列表增强"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_conn, paginate_query, map_signal, map_source

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_WHITELIST = ['score', 'discovered_at', 'stock_code']


@router.get("/discovery", response_class=HTMLResponse)
async def discovery_page(
    request: Request,
    signal: Optional[str] = None,
    search: Optional[str] = None,
    sort: str = Query("score"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
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

    if signal:
        sql += " AND dp.signal = ?"
        params.append(signal)

    search_cols = ['dp.stock_code', 's.name'] if search else None
    rows, total, total_pages = paginate_query(
        conn, sql, params, page, 20,
        search=search, search_cols=search_cols,
        sort=sort, order=order, sort_whitelist=SORT_WHITELIST,
    )
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d['signal_zh'] = map_signal(d.get('signal'))
        d['source_zh'] = map_source(d.get('source'))
        results.append(d)

    return templates.TemplateResponse("discovery.html", {
        "request": request, "active": "discovery",
        "db_stats": db_stats,
        "pool": results,
        "current_signal": signal or "",
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/discovery",
    })
