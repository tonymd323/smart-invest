"""发现池页面路由"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime

from web.services import get_db_stats, get_discovery_pool

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/discovery", response_class=HTMLResponse)
async def discovery_page(request: Request, signal: str = Query(None)):
    db_stats = get_db_stats()
    filters = [signal] if signal else None
    pool = get_discovery_pool(signal_filter=filters)
    
    return templates.TemplateResponse("discovery.html", {
        "request": request,
        "active": "discovery",
        "db_stats": db_stats,
        "pool": pool,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
