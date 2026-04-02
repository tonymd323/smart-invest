"""系统控制页面路由 + Pipeline API + Cron 管理"""
import os
import time
import json
import subprocess
from datetime import datetime
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
        "command": "python3 scripts/run_with_log.py pipeline_eod 盘后全量扫描 -- python3 scripts/run_pipeline.py --window 6h --max-stocks 300 >> data/logs/pipeline_eod.log 2>&1",
        "default_schedule": "35 15 * * 1-5",
    },
    "pipeline_evening": {
        "name": "晚间补充扫描",
        "command": "python3 scripts/run_with_log.py pipeline_evening 晚间补充扫描 -- python3 scripts/run_pipeline.py --window 6h >> data/logs/pipeline_cron.log 2>&1",
        "default_schedule": "30 20 * * 1-5",
    },
    "pullback_scan": {
        "name": "回调买入信号",
        "command": "python3 scripts/run_with_log.py pullback 回调买入扫描 -- python3 -c \"from core.analyzer import PullbackAnalyzer; pa = PullbackAnalyzer(db_path='data/smart_invest.db'); pa.scan()\" >> data/logs/pullback_cron.log 2>&1",
        "default_schedule": "15 15 * * 1-5",
    },
    "backtest_weekly": {
        "name": "每周回测回填",
        "command": "python3 scripts/run_with_log.py btiq_backfill BTIQ历史回填 -- python3 scripts/btiq_backfill.py >> data/logs/backtest_cron.log 2>&1",
        "default_schedule": "0 10 * * 0",
    },
}

# ============================================================
# 可手动执行的任务面板
# ============================================================
TASK_PANEL = {
    "pipeline": {
        "name": "Pipeline 数据管道",
        "desc": "全量采集 → 分析 → 入池",
        "icon": "🚀",
        "group": "数据采集",
        "cmd": ["python3", "scripts/run_pipeline.py", "--window", "{window}", "--max-stocks", "{max_stocks}"],
        "params": [
            {"key": "window", "label": "时间窗口", "type": "select", "default": "12h",
             "options": ["2h", "4h", "6h", "12h", "18h", "24h", "720h"]},
            {"key": "max_stocks", "label": "最大股票数", "type": "select", "default": "300",
             "options": ["10", "100", "300", "1000", "5000"]},
        ],
    },
    "pullback": {
        "name": "回调买入扫描",
        "desc": "扫描跟踪池回调买入信号",
        "icon": "📉",
        "group": "分析扫描",
        "cmd": ["python3", "scripts/run_with_log.py", "pullback", "回调买入扫描", "--", "python3", "-c", "from core.analyzer import PullbackAnalyzer; pa = PullbackAnalyzer(db_path='data/smart_invest.db'); results = pa.scan(); print(f'扫描完成: {len(results)} 只')"],
        "params": [],
    },
    "btiq_backfill": {
        "name": "BTIQ 历史回填",
        "desc": "回填涨跌比历史数据，使 MA5 可用",
        "icon": "📊",
        "group": "数据采集",
        "cmd": ["python3", "scripts/run_with_log.py", "btiq_backfill", "BTIQ历史回填", "--", "python3", "scripts/btiq_backfill.py", "--days", "{days}"],
        "params": [
            {"key": "days", "label": "回填天数", "type": "number", "default": "5"},
        ],
    },
    "btiq_monitor": {
        "name": "超跌监控",
        "desc": "BTIQ 涨跌比全市场扫描",
        "icon": "🔴",
        "group": "分析扫描",
        "cmd": ["python3", "scripts/run_with_log.py", "btiq_monitor", "超跌监控", "--", "python3", "scripts/btiq_monitor.py"],
        "params": [],
    },
}

# 按 group 分组，保持顺序
TASK_GROUPS = ["数据采集", "分析扫描"]

# ============================================================
# Cron 解析/管理
# ============================================================
CONTAINER_NAME = "smart-invest"

def _is_in_docker():
    """检测是否运行在 Docker 容器内"""
    return os.path.exists('/.dockerenv')

def _crontab_read():
    """读取 crontab（容器内直接读，宿主机通过 docker exec）"""
    if _is_in_docker():
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    else:
        result = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "crontab", "-l"],
            capture_output=True, text=True,
        )
    return result

def _read_container_crontab():
    """读取容器 crontab 内容，回退到备份文件"""
    if _is_in_docker():
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True, timeout=10,
        )
    else:
        result = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "crontab", "-l"],
            capture_output=True, text=True, timeout=10,
        )
    if result.returncode == 0:
        return result.stdout.splitlines()
    # 回退到备份文件
    crontab_file = str(PROJECT_ROOT / "data" / "crontab.txt")
    if os.path.exists(crontab_file):
        with open(crontab_file) as f:
            return f.read().splitlines()
    return ["SHELL=/bin/bash", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"]

def _get_crontab_entries():
    """读取容器 crontab，解析 smart-invest 相关条目"""
    import re
    try:
        lines = _read_container_crontab()
        content = "\n".join(lines)

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
                    cmd_base = v["command"].split(">>")[0].strip()
                    if cmd_base in command:
                        task_key = k
                        task_label = v["name"]
                        break
                    if "&&" in v["command"]:
                        cmd_and = v["command"].split("&&")[1].strip().split(">>")[0].strip()
                        if cmd_and in command:
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
    """cron调度 → 可读中文，支持 */N、范围、逗号等 cron 语法"""
    parts = schedule.split()
    if len(parts) != 5:
        return schedule
    minute, hour, dom, month, dow = parts

    def _fmt_part(v):
        """格式化单个 cron 字段，支持 *, */N, N-M, N,M """
        if v == "*":
            return "*"
        if "/" in v:
            return v  # */5, 10/15 等
        if "-" in v:
            return v  # 9-11
        if "," in v:
            return v  # 1,3,5
        try:
            return f"{int(v):02d}"
        except ValueError:
            return v

    # 时间显示
    def _fmt_time(h, m):
        # */5 分钟
        if "/" in m:
            m_str = m
        else:
            try:
                m_str = f"{int(m):02d}"
            except ValueError:
                m_str = m

        if h == "*":
            return f"每{m_str.replace('*/', '')}分钟" if "/" in m else "每分钟"
        try:
            if "-" in h:
                s, e = h.split("-")
                return f"{int(s):02d}:{m_str}-{int(e):02d}:{m_str}"
            elif "," in h:
                return "、".join(f"{int(x):02d}:{m_str}" for x in h.split(","))
            return f"{int(h):02d}:{m_str}"
        except ValueError:
            return f"{h}:{m}"

    time_str = _fmt_time(hour, minute)

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
    """保存 crontab（容器 + 文件双写）"""
    crontab_file = str(PROJECT_ROOT / "data" / "crontab.txt")
    SHELL_HEADER = "CRON_TZ=Asia/Shanghai\nSHELL=/bin/bash\nPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"

    # 写 crontab（只取 cron 行）
    cron_lines = "\n".join(
        l for l in lines if l.strip() and not l.startswith("SHELL") and not l.startswith("PATH") and not l.startswith("CRON_TZ")
    ) + "\n"
    if _is_in_docker():
        subprocess.run(["crontab", "-"], input=cron_lines, capture_output=True, text=True, timeout=10)
    else:
        subprocess.run(
            ["docker", "exec", "-i", CONTAINER_NAME, "crontab", "-"],
            input=cron_lines, capture_output=True, text=True, timeout=10,
        )

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
# 任务面板 API（统一手动执行入口）
# ============================================================
@router.get("/api/tasks")
async def list_tasks():
    """返回所有可手动执行的任务列表（按组分类）"""
    groups = {}
    for gid in TASK_GROUPS:
        groups[gid] = []
    for key, t in TASK_PANEL.items():
        group = t.get("group", "其他")
        if group not in groups:
            groups[group] = []
        groups[group].append({
            "key": key,
            "name": t["name"],
            "desc": t.get("desc", ""),
            "icon": t.get("icon", "⚡"),
            "params": t.get("params", []),
        })
    return JSONResponse({"groups": groups, "group_order": TASK_GROUPS})


@router.get("/api/task/stream")
async def task_sse(request: Request, task_key: str = ""):
    """通用 SSE 日志流"""
    import asyncio

    if task_key not in TASK_PANEL:
        return JSONResponse({"error": f"未知任务: {task_key}"}, status_code=400)

    task = TASK_PANEL[task_key]

    # 构建命令，替换参数
    cmd = []
    for part in task["cmd"]:
        if part.startswith("{") and part.endswith("}"):
            param_key = part[1:-1]
            val = request.query_params.get(param_key, "")
            if not val:
                # 从 params 定义取默认值
                for p in task.get("params", []):
                    if p["key"] == param_key:
                        val = p.get("default", "")
                        break
            cmd.append(str(val))
        else:
            cmd.append(part)

    task_icon = task.get("icon", "⚡")
    task_name = task["name"]

    async def generate():
        # 记录到系统日志
        from core.system_logger import SystemLogger
        db = str(PROJECT_ROOT / "data" / "smart_invest.db")
        slog = SystemLogger(db_path=db)
        start_time = time.time()
        output_lines = []

        yield f"data: {json.dumps({'type': 'info', 'msg': f'{task_icon} 启动: {task_name}'})}\n\n"
        cmd_str = " ".join(cmd)
        yield f"data: {json.dumps({'type': 'info', 'msg': f'命令: {cmd_str}'})}\n\n"

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
                    output_lines.append(text)
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
            code = proc.returncode
            duration_ms = int((time.time() - start_time) * 1000)

            if code == 0:
                yield f"data: {json.dumps({'type': 'success', 'msg': '✅ 执行完成'})}\n\n"
                slog.success("manual", task_name, result=f"{len(output_lines)} 行输出 | {duration_ms}ms", duration_ms=duration_ms)
            else:
                yield f"data: {json.dumps({'type': 'error', 'msg': f'❌ 退出码: {code}'})}\n\n"
                slog.error("manual", task_name, error=f"退出码 {code}", detail="\n".join(output_lines[-10:]))
            yield f"data: {json.dumps({'type': 'done', 'code': code})}\n\n"
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            yield f"data: {json.dumps({'type': 'error', 'msg': str(e)})}\n\n"
            slog.error("manual", task_name, error=str(e))
            yield f"data: {json.dumps({'type': 'done', 'code': -1})}\n\n"

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
        # 读取容器 crontab
        existing_lines = _read_container_crontab()

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
        existing_lines = _read_container_crontab()

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


# ============================================================
# 系统日志 API
# ============================================================

@router.get("/api/logs")
async def get_logs_api(limit: int = 50, module: str = None, status: str = None):
    """获取系统运行日志"""
    from fastapi.responses import JSONResponse
    from core.system_logger import SystemLogger
    db = str(PROJECT_ROOT / "data" / "smart_invest.db")
    logger = SystemLogger(db_path=db)
    logs = logger.get_logs(limit=limit, module=module, status=status)
    return JSONResponse(logs)


@router.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request, module: str = None, status: str = None):
    """系统日志页面"""
    from core.system_logger import SystemLogger
    db = str(PROJECT_ROOT / "data" / "smart_invest.db")
    logger = SystemLogger(db_path=db)
    logs = logger.get_logs(limit=100, module=module, status=status)

    # 模块列表（用于过滤）
    import sqlite3
    conn = sqlite3.connect(db)
    modules = [r[0] for r in conn.execute("SELECT DISTINCT module FROM system_log ORDER BY module").fetchall()]
    conn.close()

    db_stats = get_db_stats()
    return templates.TemplateResponse("logs.html", {
        "request": request,
        "active": "logs",
        "db_stats": db_stats,
        "logs": logs,
        "modules": modules,
        "current_module": module or "",
        "current_status": status or "",
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


@router.post("/api/logs/cleanup")
async def cleanup_logs_api(keep_days: int = 30):
    """清理旧日志"""
    from fastapi.responses import JSONResponse
    from core.system_logger import SystemLogger
    db = str(PROJECT_ROOT / "data" / "smart_invest.db")
    logger = SystemLogger(db_path=db)
    deleted = logger.cleanup(keep_days=keep_days)
    return JSONResponse({"ok": True, "deleted": deleted})
