"""今日行动页面路由"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime

from web.services import get_db_stats, get_scan_results, get_discovery_pool

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/action", response_class=HTMLResponse)
async def action_page(request: Request):
    db_stats = get_db_stats()
    
    # 获取今日信号并合成行动
    beats = get_scan_results(days=1, analysis_type="earnings_beat")
    highs = get_scan_results(days=1, analysis_type="profit_new_high")
    pool = get_discovery_pool(signal_filter=["buy", "watch"])
    
    # 合成行动列表
    actions = []
    
    # 超预期 + 在发现池内 = 高优先
    for b in beats:
        pool_match = next((p for p in pool if p['stock_code'] == b['stock_code']), None)
        if b.get('signal') in ('buy', 'watch') and pool_match:
            actions.append({
                'priority': 'high',
                'stock_code': b['stock_code'],
                'stock_name': b.get('stock_name', b['stock_code']),
                'reason': '超预期 + 发现池内',
                'score': b.get('score', 0),
                'signal': b.get('signal'),
                'target_price': pool_match.get('target_price'),
                'stop_loss': pool_match.get('stop_loss'),
            })
        elif b.get('signal') in ('buy', 'watch'):
            actions.append({
                'priority': 'medium',
                'stock_code': b['stock_code'],
                'stock_name': b.get('stock_name', b['stock_code']),
                'reason': '超预期信号',
                'score': b.get('score', 0),
                'signal': b.get('signal'),
            })
    
    # 扣非新高
    for h in highs:
        if h.get('signal') in ('buy', 'watch'):
            actions.append({
                'priority': 'medium',
                'stock_code': h['stock_code'],
                'stock_name': h.get('stock_name', h['stock_code']),
                'reason': '扣非净利润新高',
                'score': h.get('score', 0),
                'signal': h.get('signal'),
            })
    
    # 按优先级排序
    priority_order = {'high': 0, 'medium': 1, 'low': 2}
    actions.sort(key=lambda x: (priority_order.get(x['priority'], 9), -x.get('score', 0)))
    
    return templates.TemplateResponse("action.html", {
        "request": request,
        "active": "action",
        "db_stats": db_stats,
        "actions": actions,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
