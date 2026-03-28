"""总览页面路由"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from web.services import get_db_stats, get_signal_summary, get_position_snapshot, get_events

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/", response_class=HTMLResponse)
@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    db_stats = get_db_stats()
    signal_summary = get_signal_summary()
    positions = get_position_snapshot()
    recent_events = get_events(days=1)[:5]
    
    from datetime import datetime
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "active": "dashboard",
        "db_stats": db_stats,
        "signal_summary": signal_summary,
        "positions": positions,
        "recent_events": recent_events,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
