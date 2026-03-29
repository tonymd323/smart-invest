#!/usr/bin/env python3
"""
投资系统 2.2 — Web 前端
FastAPI + Jinja2 + HTMX + SSE
"""
import os
import sys
from pathlib import Path

# 确保项目根目录在 path 中
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from web.routes import dashboard, action, signals, discovery, events, tracking, backtest, system, portfolio

app = FastAPI(title="投资系统 2.2", version="2.3.0")

# 静态文件
app.mount("/static", StaticFiles(directory=str(PROJECT_ROOT / "web" / "static")), name="static")

# 模板
templates = Jinja2Templates(directory=str(PROJECT_ROOT / "web" / "templates"))

# 注册路由
app.include_router(dashboard.router)
app.include_router(action.router)
app.include_router(signals.router)
app.include_router(discovery.router)
app.include_router(events.router)
app.include_router(tracking.router)
app.include_router(backtest.router)
app.include_router(system.router)
app.include_router(portfolio.router)


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
