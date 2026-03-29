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
    """决策流转 — v2.10: 决策即行动
    
    - bought: 写入 stocks.json 持仓 + T+N 跟踪
    - skip: 记录决策, 3天内今日行动页不再出现
    - watch: 记录决策, 有新信号时再出现
    - sold: 从 stocks.json 移除 + 标记 T+N 完成
    """
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        body = await request.json()
    else:
        form = await request.form()
        body = dict(form)
    code = body.get("code", "")
    action = body.get("action", "")
    reason = body.get("reason", "")
    name = body.get("name", code)
    price = body.get("price", 0)

    import sqlite3
    import json
    from pathlib import Path
    from datetime import datetime

    db_path = str(PROJECT_ROOT / "data" / "smart_invest.db")
    stocks_json = PROJECT_ROOT / "config" / "stocks.json"

    conn = sqlite3.connect(db_path)
    try:
        # 确保 decision_log 表存在
        conn.execute("""
            CREATE TABLE IF NOT EXISTS decision_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT,
                signal_type TEXT,
                created_at TIMESTAMP DEFAULT (datetime('now', 'localtime'))
            )
        """)
        conn.execute(
            "INSERT INTO decision_log (stock_code, action, reason) VALUES (?, ?, ?)",
            (code, action, reason)
        )
        conn.commit()

        # === 已买入: 写入 stocks.json 持仓 ===
        if action == "bought" and stocks_json.exists():
            with open(stocks_json) as f:
                config = json.load(f)

            # 检查是否已在持仓
            existing = next((h for h in config.get("holdings", []) if h["code"] == code), None)
            if not existing:
                # 从关注池移除（如果有）
                config["watchlist"] = [w for w in config.get("watchlist", []) if w["code"] != code]
                # 添加到持仓
                config.setdefault("holdings", []).append({
                    "code": code,
                    "name": name,
                    "shares": 0,
                    "cost": float(price) if price else 0,
                })
                config["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                config["updated_by"] = "web:decision"
                with open(stocks_json, "w") as f:
                    json.dump(config, f, ensure_ascii=False, indent=2)

            # 创建 T+N 跟踪（如果 event_tracking 表有对应信号）
            conn.execute("""
                UPDATE event_tracking SET tracking_status = 'active', last_updated = datetime('now','localtime')
                WHERE stock_code = ? AND tracking_status = 'pending'
            """, (code,))
            conn.commit()

        # === 已卖出: 从 stocks.json 持仓移除 ===
        elif action == "sold" and stocks_json.exists():
            with open(stocks_json) as f:
                config = json.load(f)
            config["holdings"] = [h for h in config.get("holdings", []) if h["code"] != code]
            config["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            config["updated_by"] = "web:decision:sold"
            with open(stocks_json, "w") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            # 完成 T+N 跟踪
            conn.execute("""
                UPDATE event_tracking SET tracking_status = 'completed', last_updated = datetime('now','localtime')
                WHERE stock_code = ? AND tracking_status = 'active'
            """, (code,))
            conn.commit()

    except Exception as e:
        return HTMLResponse(f'<span class="text-xs text-red-400">记录失败: {e}</span>')
    finally:
        conn.close()

    # 返回结果
    action_labels = {"bought": "✅ 已买入", "sold": "💰 已卖出", "skip": "⏭️ 已跳过", "watch": "👀 观望中"}
    label = action_labels.get(action, action)

    # 表单提交 → 重定向回来源页
    if "application/json" not in content_type:
        from fastapi.responses import RedirectResponse
        referer = request.headers.get("referer", "/portfolio")
        return RedirectResponse(url=referer, status_code=303)

    return HTMLResponse(f'<span class="text-xs text-gray-400">{label}</span>')


# ============================================================
# 图表数据 API（v2.11 Plotly 集成）
# ============================================================

@router.get("/api/chart/tn_returns")
async def chart_tn_returns():
    """T+N 收益曲线数据"""
    from fastapi.responses import JSONResponse
    import sqlite3
    db = str(PROJECT_ROOT / "data" / "smart_invest.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT et.stock_code, COALESCE(s.name, et.stock_code) as stock_name,
               et.event_date, et.return_1d, et.return_5d, et.return_10d, et.return_20d
        FROM event_tracking et
        LEFT JOIN stocks s ON et.stock_code = s.code
        WHERE et.entry_price IS NOT NULL AND et.tracking_status = 'active'
        ORDER BY et.event_date DESC
        LIMIT 20
    """).fetchall()
    conn.close()

    traces = {}
    for r in rows:
        code = r['stock_code']
        name = r['stock_name']
        key = f"{name}({code[:6]})"
        if key not in traces:
            traces[key] = {'x': ['1日', '5日', '10日', '20日'], 'y': [], 'name': key}
        traces[key]['y'] = [
            r['return_1d'] or 0, r['return_5d'] or 0,
            r['return_10d'] or 0, r['return_20d'] or 0
        ]

    return JSONResponse(list(traces.values()))


@router.get("/api/chart/backtest_winrate")
async def chart_backtest_winrate():
    """回测胜率柱状图数据"""
    from fastapi.responses import JSONResponse
    import sqlite3
    db = str(PROJECT_ROOT / "data" / "smart_invest.db")
    conn = sqlite3.connect(db)
    rows = conn.execute("""
        SELECT event_type, COUNT(*) as total,
               SUM(CASE WHEN is_win=1 THEN 1 ELSE 0 END) as wins,
               AVG(return_20d) as avg_return
        FROM backtest WHERE return_20d IS NOT NULL
        GROUP BY event_type
    """).fetchall()
    conn.close()

    type_map = {'earnings_beat': '超预期', 'profit_new_high': '扣非新高'}
    data = {
        'labels': [type_map.get(r[0], r[0]) for r in rows],
        'win_rates': [round(r[2]/r[1]*100, 1) if r[1] else 0 for r in rows],
        'avg_returns': [round(r[3] or 0, 2) for r in rows],
        'totals': [r[1] for r in rows],
    }
    return JSONResponse(data)


@router.get("/api/chart/signal_trend")
async def chart_signal_trend():
    """近30天信号趋势"""
    from fastapi.responses import JSONResponse
    import sqlite3
    db = str(PROJECT_ROOT / "data" / "smart_invest.db")
    conn = sqlite3.connect(db)
    rows = conn.execute("""
        SELECT date(created_at) as day, analysis_type, COUNT(*) as cnt
        FROM analysis_results
        WHERE created_at >= datetime('now', '-30 days')
        GROUP BY day, analysis_type
        ORDER BY day
    """).fetchall()
    conn.close()

    type_map = {
        'earnings_beat': '超预期',
        'earnings_beat_daily': '超预期',
        'profit_new_high': '扣非新高',
        'quarterly_profit_new_high_daily': '扣非新高',
        'pullback_buy_daily': '回调买入',
        'pullback_score': '回调评分',
        'oversold_btiq': '超跌监控',
    }
    series = {}
    for r in rows:
        t = type_map.get(r[1], r[1])
        if t not in series:
            series[t] = {'x': [], 'y': [], 'name': t}
        series[t]['x'].append(r[0])
        series[t]['y'].append(r[2])

    return JSONResponse(list(series.values()))
