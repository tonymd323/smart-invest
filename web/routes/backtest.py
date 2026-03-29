"""策略回测页面路由 — v2.10 列表增强"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_conn, paginate_query, map_event_type, get_strategy_performance

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_WHITELIST = ['event_date', 'return_5d', 'return_10d', 'return_20d', 'return_60d', 'stock_code']


@router.get("/backtest", response_class=HTMLResponse)
async def backtest_page(
    request: Request,
    signal_type: Optional[str] = None,
    search: Optional[str] = None,
    sort: str = Query("event_date"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
    performance = get_strategy_performance()
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

    search_cols = ['b.stock_code', 's.name'] if search else None
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
        results.append(d)

    return templates.TemplateResponse("backtest.html", {
        "request": request, "active": "backtest",
        "db_stats": db_stats,
        "backtest": results,
        "performance": performance,
        "signal_type": signal_type or "",
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/backtest",
    })
