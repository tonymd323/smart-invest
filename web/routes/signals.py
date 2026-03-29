"""信号看板页面路由 — v2.10 列表增强"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_conn, paginate_query, map_analysis_type, map_signal, format_summary

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_WHITELIST = ['created_at', 'score', 'stock_code']


@router.get("/signals", response_class=HTMLResponse)
async def signals_page(
    request: Request,
    type: Optional[str] = None,
    days: int = Query(7),
    search: Optional[str] = None,
    sort: str = Query("created_at"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
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

    if type:
        sql += " AND ar.analysis_type = ?"
        params.append(type)

    search_cols = ['ar.stock_code', 's.name'] if search else None
    rows, total, total_pages = paginate_query(
        conn, sql, params, page, 20,
        search=search, search_cols=search_cols,
        sort=sort, order=order, sort_whitelist=SORT_WHITELIST,
    )
    conn.close()

    signals = []
    for r in rows:
        d = dict(r)
        d['analysis_type_zh'] = map_analysis_type(d.get('analysis_type'))
        d['signal_zh'] = map_signal(d.get('signal'))
        d['summary_text'] = format_summary(d.get('summary', ''), d.get('analysis_type', ''))
        signals.append(d)

    return templates.TemplateResponse("signals.html", {
        "request": request, "active": "signals",
        "db_stats": db_stats,
        "signals": signals,
        "current_type": type or "",
        "current_days": days,
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/signals",
    })
