"""事件流页面路由"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime

from web.services import get_db_stats, get_events

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/events", response_class=HTMLResponse)
async def events_page(request: Request, days: int = Query(7), event_type: str = Query(None)):
    db_stats = get_db_stats()
    events = get_events(days=days, event_type=event_type)
    
    return templates.TemplateResponse("events.html", {
        "request": request,
        "active": "events",
        "db_stats": db_stats,
        "events": events,
        "days": days,
        "event_type": event_type,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
