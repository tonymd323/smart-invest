"""信号看板页面路由"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_scan_results

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/signals", response_class=HTMLResponse)
async def signals_page(request: Request, type: Optional[str] = None, days: int = 7):
    beats = get_scan_results(days=days, analysis_type=type or "earnings_beat")
    db_stats = get_db_stats()
    return templates.TemplateResponse("signals.html", {
        "request": request, "active": "signals",
        "db_stats": db_stats,
        "signals": beats, "current_type": type or "earnings_beat", "current_days": days,
    })
