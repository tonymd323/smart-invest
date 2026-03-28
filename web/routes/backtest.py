"""策略回测页面路由"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime
import csv
import io

from web.services import get_db_stats, get_backtest_results, get_strategy_performance

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/backtest", response_class=HTMLResponse)
async def backtest_page(request: Request, signal_type: str = Query(None)):
    db_stats = get_db_stats()
    results = get_backtest_results(signal_type=signal_type)
    perf = get_strategy_performance()
    
    return templates.TemplateResponse("backtest.html", {
        "request": request,
        "active": "backtest",
        "db_stats": db_stats,
        "results": results,
        "perf": perf,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


@router.get("/api/export/backtest")
async def export_backtest_csv():
    results = get_backtest_results()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['股票代码', '股票名称', '信号类型', '信号日期', '收益%', '持有天数'])
    for r in results:
        writer.writerow([r.get('stock_code'), r.get('stock_name'), r.get('signal_type'),
                         r.get('signal_date'), r.get('actual_return'), r.get('hold_days')])
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=backtest_export.csv"}
    )
