"""系统控制页面路由 — Pipeline 触发 + SSE 日志 + Cron 管理"""
import asyncio
import subprocess
import json
from pathlib import Path
from datetime import datetime

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from web.services import get_db_stats

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
PROJECT_ROOT = Path(__file__).parent.parent.parent


@router.get("/system", response_class=HTMLResponse)
async def system_page(request: Request):
    db_stats = get_db_stats()
    
    # 读取 Cron 信息
    import sqlite3
    conn = sqlite3.connect(str(PROJECT_ROOT / "data" / "smart_invest.db"))
    try:
        cron_status = conn.execute("SELECT name, schedule FROM cron_jobs").fetchall()
    except Exception:
        cron_status = []
    conn.close()
    
    return templates.TemplateResponse("system.html", {
        "request": request,
        "active": "system",
        "db_stats": db_stats,
        "cron_status": cron_status,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


@router.post("/api/pipeline/run")
async def run_pipeline(request: Request, body: dict = None):
    """触发 Pipeline 运行，返回 SSE 任务 ID"""
    body = body or {}
    window = body.get("window", "12h")
    as_of = body.get("as_of", "")
    dry_run = body.get("dry_run", False)
    
    # 生成任务 ID
    import uuid
    task_id = str(uuid.uuid4())[:8]
    
    return {"task_id": task_id, "window": window, "as_of": as_of, "dry_run": dry_run}


@router.get("/api/pipeline/stream/{task_id}")
async def pipeline_sse(task_id: str, window: str = "12h", as_of: str = "", dry_run: bool = False):
    """SSE 实时日志流"""
    
    async def generate():
        # 构建命令
        cmd = ["python3", str(PROJECT_ROOT / "scripts" / "run_pipeline.py"), "--window", window]
        if as_of:
            cmd.extend(["--as-of", as_of])
        if dry_run:
            cmd.append("--dry-run")
        cmd.extend(["--max-stocks", "10"])
        
        yield f"data: {json.dumps({'type': 'info', 'msg': f'🚀 启动 Pipeline (window={window})'})}\n\n"
        
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(PROJECT_ROOT),
            )
            
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode('utf-8', errors='replace').rstrip()
                if text:
                    # 分类日志级别
                    if '✅' in text or 'SUCCESS' in text:
                        log_type = 'success'
                    elif '⚠️' in text or 'WARN' in text:
                        log_type = 'warn'
                    elif '❌' in text or 'ERROR' in text:
                        log_type = 'error'
                    else:
                        log_type = 'info'
                    yield f"data: {json.dumps({'type': log_type, 'msg': text})}\n\n"
            
            await proc.wait()
            yield f"data: {json.dumps({'type': 'done', 'code': proc.returncode})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'msg': str(e)})}\n\n"
    
    return StreamingResponse(generate(), media_type="text/event-stream")


@router.post("/api/decision")
async def record_decision(request: Request):
    """记录决策"""
    body = await request.json()
    code = body.get("code", "")
    action = body.get("action", "")
    reason = body.get("reason", "")
    
    # 写入 SQLite
    import sqlite3
    conn = sqlite3.connect(str(PROJECT_ROOT / "data" / "smart_invest.db"))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS decision_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                action TEXT,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("INSERT INTO decision_log (stock_code, action, reason) VALUES (?, ?, ?)",
                     (code, action, reason))
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
    
    return f'<span class="text-xs text-gray-400">已记录: {action}</span>'
