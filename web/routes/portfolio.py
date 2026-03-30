"""持仓管理页面路由 — v2.18 搜索添加 + 按钮统一"""
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime
from typing import Optional
import json

from web.services import get_db_stats, get_conn, paginate_query, map_signal, map_source
from core.data_provider import QuoteProvider

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


@router.get("/api/stocks/search")
async def stocks_search(q: str = Query(..., min_length=1)):
    """股票搜索 API — 支持代码/名称模糊匹配"""
    conn = get_conn()
    rows = conn.execute("""
        SELECT code, name, industry, sector
        FROM stocks
        WHERE code LIKE ? OR name LIKE ?
        ORDER BY
            CASE WHEN code = ? THEN 0 WHEN name = ? THEN 1 ELSE 2 END,
            code
        LIMIT 20
    """, (f"%{q}%", f"%{q}%", q, q)).fetchall()
    conn.close()
    return JSONResponse([dict(r) for r in rows])


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

    # 持仓实时行情+盈亏
    holdings_with_price = []
    try:
        qp = QuoteProvider()
        for h in stocks.get("holdings", []):
            price_info = {'price': 0, 'change_pct': 0}
            try:
                records = qp.fetch(h['code'])
                if records:
                    q = records[0].to_dict()
                    price_info = {'price': q.get('price', 0), 'change_pct': q.get('change_pct', 0)}
            except Exception:
                pass
            cost = h.get('cost', 0) or 0
            shares = h.get('shares', 0) or 0
            price = price_info['price']
            market_value = price * shares if price and shares else 0
            cost_total = cost * shares if cost and shares else 0
            pnl = market_value - cost_total
            pnl_pct = (pnl / cost_total * 100) if cost_total > 0 else 0
            holdings_with_price.append({
                **h,
                'price': price,
                'change_pct': price_info['change_pct'],
                'market_value': market_value,
                'cost_total': cost_total,
                'pnl': pnl,
                'pnl_pct': pnl_pct,
            })
    except Exception:
        holdings_with_price = [{**h, 'price': 0, 'change_pct': 0, 'market_value': 0, 'cost_total': 0, 'pnl': 0, 'pnl_pct': 0} for h in stocks.get("holdings", [])]

    return templates.TemplateResponse("portfolio.html", {
        "request": request,
        "active": "portfolio",
        "db_stats": db_stats,
        "holdings": holdings_with_price,
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


@router.post("/portfolio/holding/sell")
async def sell_holding(code: str = Form(...)):
    stocks = _load_stocks()
    holding = next((h for h in stocks.get("holdings", []) if h["code"] == code), None)
    if holding:
        stocks["holdings"] = [h for h in stocks.get("holdings", []) if h["code"] != code]
        # 记录到已卖出
        sold = {**holding, "sold_at": datetime.now().strftime("%Y-%m-%d %H:%M")}
        stocks.setdefault("sold", []).append(sold)
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
