"""发现池页面路由 — v2.18 信息扩展 + 行业筛选 + 公告类型筛选"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional, List

from web.services import get_db_stats, get_conn, paginate_query, map_signal, map_source

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_WHITELIST = ['score', 'discovered_at', 'stock_code']

# 报告期格式化：2025-12-31 → 2025Q4，2025-09-30 → 2025Q3
def _format_report_period(report_date: str, report_type: str) -> str:
    if report_date:
        year = report_date[:4]
        month = int(report_date[5:7]) if len(report_date) >= 7 else 0
        if month <= 3:
            q = "Q1"
        elif month <= 6:
            q = "Q2"
        elif month <= 9:
            q = "Q3"
        else:
            q = "Q4"
        return f"{year}{q}"
    if report_type:
        return report_type
    return ""


@router.get("/discovery", response_class=HTMLResponse)
async def discovery_page(
    request: Request,
    signal: Optional[str] = None,
    source: Optional[List[str]] = Query(None),
    industry: Optional[List[str]] = Query(None),
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
               dp.status, dp.discovered_at, dp.expires_at,
               e.report_type, e.report_date,
               e.net_profit_yoy
        FROM discovery_pool dp
        LEFT JOIN stocks s ON dp.stock_code = s.code
        LEFT JOIN (
            SELECT stock_code, report_type, report_date, net_profit_yoy,
                   ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY report_date DESC) as rn
            FROM earnings
        ) e ON dp.stock_code = e.stock_code AND e.rn = 1
        WHERE dp.status = 'active'
    """
    params = []

    if signal:
        sql += " AND dp.signal = ?"
        params.append(signal)

    # 多选：来源（公告类型）
    if source:
        placeholders = ','.join(['?'] * len(source))
        sql += f" AND dp.source IN ({placeholders})"
        params.extend(source)

    # 多选：行业筛选
    if industry:
        placeholders = ','.join(['?'] * len(industry))
        sql += f" AND COALESCE(s.industry, dp.industry) IN ({placeholders})"
        params.extend(industry)

    search_cols = ['dp.stock_code', 's.name'] if search else None
    rows, total, total_pages = paginate_query(
        conn, sql, params, page, 20,
        search=search, search_cols=search_cols,
        sort=sort, order=order, sort_whitelist=SORT_WHITELIST,
    )

    # 获取行业列表供筛选（返回 (value, label) 元组列表）
    industries = conn.execute("""
        SELECT DISTINCT COALESCE(s.industry, dp.industry) as ind
        FROM discovery_pool dp
        LEFT JOIN stocks s ON dp.stock_code = s.code
        WHERE dp.status = 'active' AND COALESCE(s.industry, dp.industry) IS NOT NULL AND COALESCE(s.industry, dp.industry) != ''
        ORDER BY ind
    """).fetchall()
    conn.close()

    industry_options = [(r[0], r[0]) for r in industries]

    results = []
    for r in rows:
        d = dict(r)
        d['signal_zh'] = map_signal(d.get('signal'))
        d['source_zh'] = map_source(d.get('source'))
        d['report_period'] = _format_report_period(d.get('report_date'), d.get('report_type'))
        results.append(d)

    # 来源选项
    source_options = [("earnings_beat", "超预期"), ("profit_new_high", "扣非新高")]

    return templates.TemplateResponse("discovery.html", {
        "request": request, "active": "discovery",
        "db_stats": db_stats,
        "pool": results,
        "current_signal": signal or "",
        "current_sources": source or [],
        "source_list": source_options,
        "current_industries": industry or [],
        "industry_list": industry_options,
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages, "total": total,
        "base_url": "/discovery",
    })
