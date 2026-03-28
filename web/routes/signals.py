"""信号看板页面路由"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime

from web.services import get_db_stats, get_scan_results

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/signals", response_class=HTMLResponse)
async def signals_page(request: Request, days: int = Query(7), type: str = Query(None)):
    db_stats = get_db_stats()
    beats = get_scan_results(days=days, analysis_type="earnings_beat" if type == "beat" else None)
    highs = get_scan_results(days=days, analysis_type="profit_new_high" if type == "high" else None)
    pulls = get_scan_results(days=days, analysis_type="pullback_buy" if type == "pull" else None)
    
    return templates.TemplateResponse("signals.html", {
        "request": request,
        "active": "signals",
        "db_stats": db_stats,
        "beats": beats, "highs": highs, "pulls": pulls,
        "days": days,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
