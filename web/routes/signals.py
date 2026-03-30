from datetime import datetime
"""信号看板页面路由 — 筛选维度：分析类型 + 披露类型 + 报告期"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional, List

from web.services import get_db_stats, get_conn, paginate_query, map_analysis_type, map_signal, format_summary

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_WHITELIST = ['ar.created_at', 'ar.score', 'ar.stock_code']

# 报告期：直接用日期值匹配
VALID_PERIODS = [
    '2026-03-31', '2025-12-31', '2025-09-30', '2025-06-30', '2025-03-31',
    '2024-12-31', '2024-09-30', '2024-06-30', '2024-03-31',
    '2023-12-31', '2023-09-30', '2023-06-30', '2023-03-31',
]

# 披露类型中文名
DISCLOSURE_LABELS = {
    '财报': '财报',
    '业绩预告': '业绩预告',
    '业绩快报': '业绩快报',
}


@router.get("/signals", response_class=HTMLResponse)
async def signals_page(
    request: Request,
    type: Optional[List[str]] = Query(None),
    disclosure_type: Optional[List[str]] = Query(None),
    period: Optional[List[str]] = Query(None),
    days: int = Query(7),
    search: Optional[str] = None,
    sort: str = Query("ar.created_at"),
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

    # 分析类型筛选
    if type:
        placeholders = ','.join(['?'] * len(type))
        sql += f" AND ar.analysis_type IN ({placeholders})"
        params.extend(type)

    # 披露类型筛选（从 summary JSON 提取 disclosure_type）
    if disclosure_type:
        disc_conditions = []
        for dt in disclosure_type:
            disc_conditions.append("ar.summary LIKE ?")
            params.append(f'%"disclosure_type": "{dt}"%')
        sql += f" AND ({' OR '.join(disc_conditions)})"

    # 报告期筛选（JOIN earnings 表，按 report_date）
    if period:
        valid_periods = [p for p in period if p in VALID_PERIODS]
        if valid_periods:
            placeholders = ','.join(['?'] * len(valid_periods))
            sql += f""" AND ar.stock_code IN (
                SELECT DISTINCT stock_code FROM earnings WHERE report_date IN ({placeholders})
            )"""
            params.extend(valid_periods)

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
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "signals": signals,
        "current_types": type or [],
        "current_disclosure_types": disclosure_type or [],
        "current_periods": period or [],
        "current_days": days,
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/signals",
    })
