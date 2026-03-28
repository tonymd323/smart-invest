"""策略回测页面路由"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_backtest_results, get_strategy_performance

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/backtest", response_class=HTMLResponse)
async def backtest_page(request: Request, type: Optional[str] = None):
    results = get_backtest_results(signal_type=type)
    perf = get_strategy_performance()
    db_stats = get_db_stats()
    return templates.TemplateResponse("backtest.html", {
        "request": request, "active": "backtest",
        "db_stats": db_stats,
        "results": results, "perf": perf, "current_type": type,
    })
