"""T+N 跟踪页面路由"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime

from web.services import get_db_stats, get_tn_tracking

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/tracking", response_class=HTMLResponse)
async def tracking_page(request: Request, status: str = Query(None)):
    db_stats = get_db_stats()
    tracking = get_tn_tracking(status=status)
    
    return templates.TemplateResponse("tracking.html", {
        "request": request,
        "active": "tracking",
        "db_stats": db_stats,
        "tracking": tracking,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
