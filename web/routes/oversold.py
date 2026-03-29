"""超跌监控页面路由 — BTIQ 涨跌比趋势图 + 市场情绪信号"""
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.services import get_oversold_data, get_db_stats

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/oversold", response_class=HTMLResponse)
async def oversold_page(request: Request):
    data = get_oversold_data()
    db_stats = get_db_stats()
    return templates.TemplateResponse("oversold.html", {
        "request": request,
        "active": "oversold",
        "db_stats": db_stats,
        **data,
    })
