"""总览页面路由 — v2.13 UX 重构"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime

from web.services import get_db_stats, get_dashboard_summary, get_events

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/", response_class=HTMLResponse)
@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    summary = get_dashboard_summary()
    recent_events = get_events(days=3)[:8]
    db_stats = get_db_stats()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "active": "dashboard",
        "summary": summary,
        "recent_events": recent_events,
        "db_stats": db_stats,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
