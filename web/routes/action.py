"""今日行动页面路由 — v2.9 重构：综合研判操作建议"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime

from web.services import get_db_stats, get_today_actions

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/action", response_class=HTMLResponse)
async def action_page(request: Request):
    db_stats = get_db_stats()
    actions = get_today_actions()

    return templates.TemplateResponse("action.html", {
        "request": request,
        "active": "action",
        "db_stats": db_stats,
        "actions": actions,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
