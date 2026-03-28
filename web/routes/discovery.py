"""发现池页面路由"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional

from web.services import get_db_stats, get_discovery_pool

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/discovery", response_class=HTMLResponse)
async def discovery_page(request: Request, signal: Optional[str] = None, source: Optional[str] = None):
    signal_filter = [signal] if signal else None
    source_filter = [source] if source else None
    pool = get_discovery_pool(signal_filter=signal_filter, source_filter=source_filter)
    db_stats = get_db_stats()
    return templates.TemplateResponse("discovery.html", {
        "request": request, "active": "discovery",
        "db_stats": db_stats,
        "pool": pool, "current_signal": signal, "current_source": source,
    })
