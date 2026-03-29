"""系统控制页面路由 + Pipeline API + Cron 管理"""
import os
import json
import subprocess
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from web.services import get_db_stats

PROJECT_ROOT = Path(__file__).parent.parent.parent
router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ============================================================
# 预定义任务（中文名+固定命令）
# ============================================================
PRESET_TASKS = {
    "pipeline_eod": {
        "name": "盘后全量扫描",
        "command": "cd /app && /usr/bin/python3 scripts/run_pipeline.py --window 6h --max-stocks 300 >> data/logs/pipeline_eod.log 2>&1",
        "default_schedule": "35 15 * * 1-5",
    },
    "pipeline_evening": {
        "name": "晚间补充扫描",
        "command": "cd /app && /usr/bin/python3 scripts/run_pipeline.py --window 6h >> data/logs/pipeline_cron.log 2>&1",
        "default_schedule": "30 20 * * 1-5",
    },
    "pullback_scan": {
        "name": "回调买入信号",
        "command": "cd /app && /usr/bin/python3 -c \"from core.analyzer import PullbackAnalyzer; pa = PullbackAnalyzer(db_path='data/smart_invest.db'); pa.scan()\" >> data/logs/pullback_cron.log 2>&1",
        "default_schedule": "15 15 * * 1-5",
    },
    "backtest_weekly": {
        "name": "每周回测回填",
        "command": "cd /app && /usr/bin/python3 scripts/btiq_backfill.py >> data/logs/backtest_cron.log 2>&1",
        "default_schedule": "0 10 * * 0",
    },
}

# ============================================================
# Cron 解析/管理
# ============================================================
def _get_crontab_entries():
    """读取当前 crontab，解析 smart-invest 相关条目"""
    import re
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode != 0:
            crontab_file = "/app/data/crontab.txt"
            try:
                with open(crontab_file) as f:
                    content = f.read()
            except FileNotFoundError:
                return []
        else:
            content = result.stdout

        entries = []
        last_comment = None
        for line in content.splitlines():
            line = line.strip()
            if not line:
                last_comment = None
                continue
            if line.startswith('SHELL') or line.startswith('PATH'):
                continue
            if line.startswith('#'):
                last_comment = line.lstrip('#').strip()
                continue
            m = re.match(r'^(\S+\s+\S+\s+\S+\s+\S+\s+\S+)\s+(.+)$', line)
            if m:
                schedule = m.group(1)
                command = m.group(2)
                comment_name = last_comment if last_comment else ""

                # 匹配预定义任务
                task_key = None
                task_label = comment_name
                for k, v in PRESET_TASKS.items():
                    if v["command"].split(">>")[0].strip() in command or v["command"].split("&&")[1].strip().split(">>")[0].strip() in command:
                        task_key = k
                        task_label = v["name"]
                        break

                # 解析调度为可读格式
                readable = _schedule_to_readable(schedule)

                entries.append({
                    "key": task_key or "",
                    "name": task_label,
                    "comment": comment_name,
                    "schedule": schedule,
                    "readable": readable,
                    "command": command[:80],
                })
                last_comment = None
        return entries
    except Exception as e:
        import logging
        logging.getLogger('system').error(f"_get_crontab_entries error: {e}")
        return []


def _schedule_to_readable(schedule: str) -> str:
    """cron调度 → 可读中文"""
    parts = schedule.split()
    if len(parts) != 5:
        return schedule
    minute, hour, dom, month, dow = parts

    # 时间
    time_str = f"{int(hour):02d}:{int(minute):02d}"

    # 星期
    dow_map = {"0": "周日", "1": "周一", "2": "周二", "3": "周三", "4": "周四", "5": "周五", "6": "周六"}
    if dow == "*":
        day_str = "每天"
    elif "-" in dow:
        s, e = dow.split("-")
        day_str = f"{dow_map.get(s, s)}-{dow_map.get(e, e)}"
    elif "," in dow:
        day_str = "、".join(dow_map.get(d, d) for d in dow.split(","))
    else:
        day_str = dow_map.get(dow, dow)

    return f"{day_str} {time_str}"


def _save_crontab(lines: list):
    """保存 crontab（系统 + 文件双写）"""
    crontab_file = "/app/data/crontab.txt"
    SHELL_HEADER = "SHELL=/bin/bash\nPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"

    # 写系统 crontab（只取 cron 行）
    cron_lines = "\n".join(
        l for l in lines if l.strip() and not l.startswith("SHELL") and not l.startswith("PATH")
    ) + "\n"
    subprocess.run(["crontab", "-"], input=cron_lines, capture_output=True, text=True)

    # 备份到文件（含 SHELL/PATH）
    full_content = SHELL_HEADER + "\n".join(
        l for l in lines if l.strip()
    ) + "\n"
    with open(crontab_file, "w") as f:
        f.write(full_content)


# ============================================================
# 页面路由
# ============================================================
@router.get("/system", response_class=HTMLResponse)
async def system_page(request: Request):
    db_stats = get_db_stats()
    return templates.TemplateResponse("system.html", {
        "request": request, "active": "system",
        "db_stats": db_stats,
    })


# ============================================================
# Cron API
# ============================================================
from fastapi.responses import JSONResponse

# ============================================================
# Pipeline API
# ============================================================
@router.post("/api/pipeline/run")
async def run_pipeline(request: Request, body: dict = None):
    """触发 Pipeline 运行，返回 SSE 任务 ID"""
    body = body or {}
    window = body.get("window", "12h")
    as_of = body.get("as_of", "")
    dry_run = body.get("dry_run", False)
    import uuid
    task_id = str(uuid.uuid4())[:8]
    return {"task_id": task_id, "window": window, "as_of": as_of, "dry_run": dry_run}


@router.get("/api/pipeline/stream/{task_id}")
async def pipeline_sse(task_id: str, window: str = "12h", as_of: str = "", dry_run: bool = False):
    """SSE 实时日志流"""
    import asyncio
    async def generate():
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


# ============================================================
# Cron API
# ============================================================

@router.get("/api/cron")
async def get_cron():
    return JSONResponse(_get_crontab_entries())


@router.get("/api/cron/presets")
async def get_cron_presets():
    """返回预定义任务列表"""
    presets = [{"key": k, "name": v["name"]} for k, v in PRESET_TASKS.items()]
    return JSONResponse(presets)


@router.post("/api/cron/add")
async def add_cron(request: Request):
    body = await request.json()
    task_key = body.get("task_key", "")
    hour = int(body.get("hour", 15))
    minute = int(body.get("minute", 0))
    days = body.get("days", "1-5")  # "1-5" or "0" or "1,3,5"

    if task_key not in PRESET_TASKS:
        return JSONResponse({"ok": False, "error": f"未知任务: {task_key}"}, status_code=400)

    task = PRESET_TASKS[task_key]
    schedule = f"{minute} {hour} * * {days}"
    comment = f"# {task['name']}"
    entry = f"{schedule} {task['command']}"

    try:
        # 读取现有
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode == 0:
            existing_lines = result.stdout.splitlines()
        else:
            crontab_file = "/app/data/crontab.txt"
            if os.path.exists(crontab_file):
                with open(crontab_file) as f:
                    existing_lines = f.read().splitlines()
            else:
                existing_lines = ["SHELL=/bin/bash", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"]

        # 检查是否已存在同名任务
        for i, line in enumerate(existing_lines):
            if line.strip().startswith("#") and task["name"] in line:
                return JSONResponse({"ok": False, "error": f"任务「{task['name']}」已存在，请先删除再添加"}, status_code=400)

        # 添加
        existing_lines.append("")
        existing_lines.append(comment)
        existing_lines.append(entry)

        _save_crontab(existing_lines)
        return JSONResponse({"ok": True, "entries": _get_crontab_entries()})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.post("/api/cron/delete")
async def delete_cron(request: Request):
    body = await request.json()
    name = body.get("name", "")
    if not name:
        return JSONResponse({"ok": False, "error": "name 必填"}, status_code=400)

    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode == 0:
            existing_lines = result.stdout.splitlines()
        else:
            crontab_file = "/app/data/crontab.txt"
            if os.path.exists(crontab_file):
                with open(crontab_file) as f:
                    existing_lines = f.read().splitlines()
            else:
                return JSONResponse({"ok": False, "error": "无 crontab"}, status_code=404)

        new_lines = []
        deleted = False
        skip_next_cron = False
        for line in existing_lines:
            stripped = line.strip()
            if not deleted and stripped.startswith("#") and name in stripped:
                deleted = True
                skip_next_cron = True  # 下一行是对应的 cron 行，也跳过
                continue
            if skip_next_cron and not stripped.startswith("#") and not stripped.startswith("SHELL") and not stripped.startswith("PATH") and stripped:
                skip_next_cron = False
                continue
            new_lines.append(line)

        if not deleted:
            return JSONResponse({"ok": False, "error": f"未找到任务: {name}"}, status_code=404)

        _save_crontab(new_lines)
        return JSONResponse({"ok": True, "entries": _get_crontab_entries()})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
