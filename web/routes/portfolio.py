"""持仓管理页面路由 — v2.7 新增"""
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime
import json

from web.services import get_db_stats, get_discovery_pool

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

STOCKS_JSON = Path(__file__).parent.parent.parent / "config" / "stocks.json"


def _load_stocks() -> dict:
    """加载 stocks.json"""
    if STOCKS_JSON.exists():
        with open(STOCKS_JSON) as f:
            return json.load(f)
    return {"version": "1.0", "holdings": [], "watchlist": []}


def _save_stocks(data: dict):
    """保存 stocks.json"""
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    data["updated_by"] = "web"
    STOCKS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(STOCKS_JSON, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@router.get("/portfolio", response_class=HTMLResponse)
async def portfolio_page(request: Request):
    """持仓管理页面"""
    db_stats = get_db_stats()
    stocks = _load_stocks()
    pool = get_discovery_pool()

    # 发现池中不在跟踪池的股票
    holding_codes = {h["code"] for h in stocks.get("holdings", [])}
    watchlist_codes = {w["code"] for w in stocks.get("watchlist", [])}
    pool_candidates = [p for p in pool if p["stock_code"] not in holding_codes and p["stock_code"] not in watchlist_codes]

    return templates.TemplateResponse("portfolio.html", {
        "request": request,
        "active": "portfolio",
        "db_stats": db_stats,
        "holdings": stocks.get("holdings", []),
        "watchlist": stocks.get("watchlist", []),
        "pool_candidates": pool_candidates,
        "pool_total": len(pool_candidates),
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


@router.post("/portfolio/holding/add")
async def add_holding(
    code: str = Form(...),
    name: str = Form(...),
    shares: int = Form(0),
    cost: float = Form(0),
    target: float = Form(None),
    stop_loss: float = Form(None),
):
    """添加持仓"""
    stocks = _load_stocks()
    # 检查是否已存在
    for h in stocks.get("holdings", []):
        if h["code"] == code:
            return RedirectResponse("/portfolio", status_code=303)
    stocks.setdefault("holdings", []).append({
        "code": code,
        "name": name,
        "shares": shares,
        "cost": cost,
        "target": target,
        "stop_loss": stop_loss,
    })
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/holding/update")
async def update_holding(
    code: str = Form(...),
    shares: int = Form(0),
    cost: float = Form(0),
    target: float = Form(None),
    stop_loss: float = Form(None),
):
    """更新持仓"""
    stocks = _load_stocks()
    for h in stocks.get("holdings", []):
        if h["code"] == code:
            h["shares"] = shares
            h["cost"] = cost
            if target is not None:
                h["target"] = target
            if stop_loss is not None:
                h["stop_loss"] = stop_loss
            break
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/holding/delete")
async def delete_holding(code: str = Form(...)):
    """删除持仓"""
    stocks = _load_stocks()
    stocks["holdings"] = [h for h in stocks.get("holdings", []) if h["code"] != code]
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/watchlist/add")
async def add_watchlist(
    code: str = Form(...),
    name: str = Form(...),
    sector: str = Form(""),
):
    """添加关注"""
    stocks = _load_stocks()
    for w in stocks.get("watchlist", []):
        if w["code"] == code:
            return RedirectResponse("/portfolio", status_code=303)
    stocks.setdefault("watchlist", []).append({
        "code": code,
        "name": name,
        "sector": sector,
    })
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/watchlist/delete")
async def delete_watchlist(code: str = Form(...)):
    """删除关注"""
    stocks = _load_stocks()
    stocks["watchlist"] = [w for w in stocks.get("watchlist", []) if w["code"] != code]
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)


@router.post("/portfolio/pool/promote")
async def promote_from_pool(
    code: str = Form(...),
    name: str = Form(...),
    target: str = Form("watchlist"),
):
    """从发现池升级到跟踪池/关注池"""
    stocks = _load_stocks()
    if target == "holding":
        stocks.setdefault("holdings", []).append({
            "code": code,
            "name": name,
            "shares": 0,
            "cost": 0,
        })
    else:
        stocks.setdefault("watchlist", []).append({
            "code": code,
            "name": name,
            "sector": "",
        })
    _save_stocks(stocks)
    return RedirectResponse("/portfolio", status_code=303)
