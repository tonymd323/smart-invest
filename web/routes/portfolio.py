"""持仓管理页面路由 — v2.7 + v2.10 列表增强"""
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime
from typing import Optional
import json

from web.services import get_db_stats, get_conn, paginate_query, map_signal, map_source

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

STOCKS_JSON = Path(__file__).parent.parent.parent / "config" / "stocks.json"
POOL_SORT_WHITELIST = ['score', 'discovered_at', 'stock_code']


def _load_stocks() -> dict:
    if STOCKS_JSON.exists():
        with open(STOCKS_JSON) as f:
            return json.load(f)
    return {"version": "1.0", "holdings": [], "watchlist": []}


def _save_stocks(data: dict):
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    data["updated_by"] = "web"
    STOCKS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(STOCKS_JSON, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@router.get("/portfolio", response_class=HTMLResponse)
async def portfolio_page(
    request: Request,
    search: Optional[str] = None,
    sort: str = Query("score"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
):
    db_stats = get_db_stats()
    stocks = _load_stocks()

    holding_codes = {h["code"] for h in stocks.get("holdings", [])}
    watchlist_codes = {w["code"] for w in stocks.get("watchlist", [])}

    # 发现池分页查询
    conn = get_conn()
    sql = """
        SELECT dp.stock_code,
               COALESCE(s.name, dp.stock_name, dp.stock_code) as stock_name,
               COALESCE(s.industry, dp.industry) as industry,
               dp.source, dp.score, dp.signal,
               dp.status, dp.discovered_at
        FROM discovery_pool dp
        LEFT JOIN stocks s ON dp.stock_code = s.code
        WHERE dp.status = 'active'
    """
    params = []

    # 排除已在持仓/关注池的
    exclude = holding_codes | watchlist_codes
    if exclude:
        placeholders = ','.join(['?'] * len(exclude))
        sql += f" AND dp.stock_code NOT IN ({placeholders})"
        params.extend(list(exclude))

    search_cols = ['dp.stock_code', 's.name'] if search else None
    rows, total, total_pages = paginate_query(
        conn, sql, params, page, 20,
        search=search, search_cols=search_cols,
        sort=sort, order=order, sort_whitelist=POOL_SORT_WHITELIST,
    )
    conn.close()

    pool_candidates = []
    for r in rows:
        d = dict(r)
        d['signal_zh'] = map_signal(d.get('signal'))
        d['source_zh'] = map_source(d.get('source'))
        pool_candidates.append(d)

    return templates.TemplateResponse("portfolio.html", {
        "request": request,
        "active": "portfolio",
        "db_stats": db_stats,
        "holdings": stocks.get("holdings", []),
        "watchlist": stocks.get("watchlist", []),
        "pool_candidates": pool_candidates,
        "pool_total": total,
        "search": search,
        "sort": sort, "order": order,
        "page": page, "total_pages": total_pages,
        "base_url": "/portfolio",
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


@router.post("/portfolio/holding/add")
async def add_holding(
    code: str = Form(...), name: str = Form(...),
    shares: int = Form(0), cost: float = Form(0),
    target: float = Form(None), stop_loss: float = Form(None),
):
    stocks = _load_stocks()
    for h in stocks.get("holdings", []):
        if h["code"] == code:
            return RedirectResponse("/portfolio", status_code=303)
    stocks.setdefault("holdings", []).append({
        "code": code, "name": name, "shares": shares, "cost": cost,
        "target": target, "stop_loss": stop_loss,
    })
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/holding/update")
async def update_holding(
    code: str = Form(...), shares: int = Form(0), cost: float = Form(0),
    target: float = Form(None), stop_loss: float = Form(None),
):
    stocks = _load_stocks()
    for h in stocks.get("holdings", []):
        if h["code"] == code:
            h["shares"] = shares
            h["cost"] = cost
            if target is not None: h["target"] = target
            if stop_loss is not None: h["stop_loss"] = stop_loss
            break
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/holding/delete")
async def delete_holding(code: str = Form(...)):
    stocks = _load_stocks()
    stocks["holdings"] = [h for h in stocks.get("holdings", []) if h["code"] != code]
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/watchlist/add")
async def add_watchlist(code: str = Form(...), name: str = Form(...), sector: str = Form("")):
    stocks = _load_stocks()
    for w in stocks.get("watchlist", []):
        if w["code"] == code:
            return RedirectResponse("/portfolio", status_code=303)
    stocks.setdefault("watchlist", []).append({"code": code, "name": name, "sector": sector})
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/watchlist/delete")
async def delete_watchlist(code: str = Form(...)):
    stocks = _load_stocks()
    stocks["watchlist"] = [w for w in stocks.get("watchlist", []) if w["code"] != code]
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/pool/promote")
async def promote_from_pool(code: str = Form(...), name: str = Form(...), target: str = Form("watchlist")):
    stocks = _load_stocks()
    if target == "holding":
        stocks.setdefault("holdings", []).append({"code": code, "name": name, "shares": 0, "cost": 0})
    else:
        stocks.setdefault("watchlist", []).append({"code": code, "name": name, "sector": ""})
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)
