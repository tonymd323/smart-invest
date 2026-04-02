"""图表 API — 供前端 Plotly 图表调用"""
import sqlite3
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import JSONResponse

DB_PATH = Path(__file__).parent.parent.parent / "data" / "smart_invest.db"
router = APIRouter(prefix="/api/chart")


@router.get("/tn_returns")
async def tn_returns():
    """T+N 收益曲线数据（活跃跟踪）"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row

        tracks = conn.execute("""
            SELECT id, stock_code, COALESCE(stock_name, stock_code) as name,
                   entry_price, event_date
            FROM event_tracking
            WHERE tracking_status IN ('tracking', 'active')
              AND entry_price IS NOT NULL AND entry_price > 0
            ORDER BY event_date ASC
            LIMIT 50
        """).fetchall()

        traces = []
        for t in tracks:
            code = t["stock_code"]
            event_date = t["event_date"].replace("-", "") if t["event_date"] else ""
            entry = t["entry_price"]

            prices = conn.execute("""
                SELECT trade_date, close_price FROM prices
                WHERE stock_code = ? AND trade_date > ?
                ORDER BY trade_date ASC LIMIT 20
            """, (code, event_date)).fetchall()

            if not prices:
                continue

            x = [f"D+{i+1}" for i in range(len(prices))]
            y = [round((p["close_price"] / entry - 1) * 100, 2) for p in prices]

            traces.append({"name": f"{t['name']}", "x": x, "y": y})

        conn.close()
        return JSONResponse(traces)
    except Exception as e:
        return JSONResponse([], status_code=500)


@router.get("/backtest_winrate")
async def backtest_winrate():
    """策略胜率柱状图数据"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT event_type,
                   COUNT(*) as total,
                   AVG(CASE WHEN is_win = 1 THEN 1.0 ELSE 0.0 END) as win_rate,
                   AVG(return_5d) as avg_return
            FROM backtest
            WHERE return_5d IS NOT NULL
            GROUP BY event_type
            ORDER BY total DESC
        """).fetchall()

        type_map = {
            'earnings_beat': '超预期',
            'profit_new_high': '扣非新高',
            'pullback_score': '回调评分',
        }

        labels = [type_map.get(r["event_type"], r["event_type"]) for r in rows]
        win_rates = [round((r["win_rate"] or 0) * 100, 1) for r in rows]
        avg_returns = [round(r["avg_return"] or 0, 2) for r in rows]

        conn.close()
        return JSONResponse({
            "labels": labels,
            "win_rates": win_rates,
            "avg_returns": avg_returns,
        })
    except Exception as e:
        return JSONResponse({"labels": [], "win_rates": [], "avg_returns": []}, status_code=500)
