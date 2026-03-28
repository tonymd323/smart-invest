"""
回测引擎
========
计算入池后 N 日收益 vs 沪深300 基准收益。
"""

import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'core'))
from database import get_connection

logger = logging.getLogger('backtest_engine')

# 观察周期（交易日）
PERIODS = [5, 10, 20, 60]
# 基准指数
BENCHMARK = '000300.SH'


def run_pending_backtests() -> int:
    """
    对尚未回测的入池记录，计算收益并写入 backtest 表。

    Returns:
        处理记录数
    """
    import tushare as ts
    pro = ts.pro_api()

    logger.info("="*50)
    logger.info("📈 回测引擎：计算入池后收益")
    logger.info("="*50)

    # 获取待回测记录
    pending = _get_pending_records()
    logger.info(f"  待回测记录: {len(pending)} 条")

    if not pending:
        return 0

    # 获取基准指数历史价格
    logger.info(f"  获取沪深300历史数据...")
    benchmark_prices = _get_benchmark_history(pro)
    if not benchmark_prices:
        logger.warning("  基准数据获取失败，跳过回测")
        return 0

    success_count = 0
    for i, rec in enumerate(pending):
        try:
            bt = _calculate_backtest(pro, rec, benchmark_prices)
            if bt:
                _save_backtest(bt)
                success_count += 1
        except Exception as e:
            logger.debug(f"  {rec['stock_code']} 回测失败: {e}")
            continue

        time.sleep(0.3)  # Tushare 限速

        if (i + 1) % 10 == 0:
            logger.info(f"  进度: {i+1}/{len(pending)} | 完成: {success_count}")

    logger.info(f"  ✅ 回测完成: {success_count}/{len(pending)} 条")
    return success_count


def _get_pending_records() -> list:
    """获取尚未回测的 analysis_results 记录"""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT ar.stock_code, ar.created_at, ar.signal, ar.summary
            FROM analysis_results ar
            WHERE ar.analysis_type IN ('earnings_beat_daily', 'quarterly_profit_new_high_daily')
              AND ar.created_at >= date('now', '-90 days')
              AND NOT EXISTS (
                  SELECT 1 FROM backtest b
                  WHERE b.stock_code = ar.stock_code AND b.event_date = date(ar.created_at)
              )
            ORDER BY ar.created_at DESC
        """).fetchall()

    records = []
    for r in rows:
        import json
        summary = json.loads(r['summary']) if r['summary'] else {}
        # 使用 ann_date 作为入池日，而非 created_at
        ann_date = summary.get('ann_date', '')
        if ann_date:
            event_date = str(ann_date).replace('-', '').replace('/', '')[:8]
            # 格式化为 YYYY-MM-DD
            if len(event_date) == 8:
                event_date = f"{event_date[:4]}-{event_date[4:6]}-{event_date[6:]}"
        else:
            event_date = r['created_at'][:10]
        
        records.append({
            'stock_code': r['stock_code'],
            'event_date': event_date,
            'event_type': r['signal'],
            'close': summary.get('close'),
        })

    return records


def _get_benchmark_history(pro) -> dict:
    """获取沪深300历史收盘价"""
    try:
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')
        df = pro.index_daily(ts_code=BENCHMARK, start_date=start_date, end_date=end_date,
                              fields='trade_date,close')
        if df is None or df.empty:
            return {}
        df = df.sort_values('trade_date')
        return dict(zip(df['trade_date'], df['close']))
    except Exception as e:
        logger.warning(f"  基准数据获取失败: {e}")
        return {}


def _calculate_backtest(pro, rec: dict, benchmark_prices: dict) -> dict:
    """计算单条记录的回测结果"""
    code = rec['stock_code']
    event_date = rec['event_date'].replace('-', '')

    # 获取入池后的股价历史
    end_date = datetime.now().strftime('%Y%m%d')
    df = pro.daily(ts_code=code, start_date=event_date, end_date=end_date,
                    fields='trade_date,close')
    if df is None or df.empty:
        return None

    df = df.sort_values('trade_date')

    # 入池价：event_date 当天或最近交易日收盘价
    entry_price = rec.get('close')
    if not entry_price or entry_price <= 0:
        # 找 event_date 之后最近的交易日
        entry_row = df[df['trade_date'] >= event_date.replace('-', '')].head(1)
        if entry_row.empty:
            # 如果 event_date 之后没有数据，用之前的最新数据
            entry_row = df.tail(1)
        if entry_row.empty:
            return None
        entry_price = float(entry_row.iloc[0]['close'])

    # 入池基准价
    entry_benchmark = None
    event_date_clean = event_date.replace('-', '')
    for d in sorted(benchmark_prices.keys()):
        if d >= event_date_clean:
            entry_benchmark = benchmark_prices[d]
            break
    if not entry_benchmark and benchmark_prices:
        entry_benchmark = list(benchmark_prices.values())[-1]
    if not entry_benchmark:
        return None

    # 计算各周期收益
    result = {
        'stock_code': code,
        'event_date': rec['event_date'],
        'event_type': rec.get('event_type', ''),
        'entry_price': entry_price,
    }

    trading_dates = df['trade_date'].tolist()
    prices = df['close'].tolist()
    benchmark_dates = sorted(benchmark_prices.keys())

    for period in PERIODS:
        # 个股收益
        target_idx = min(period, len(prices) - 1)
        if target_idx > 0 and prices[target_idx] and entry_price:
            stock_return = round((float(prices[target_idx]) / entry_price - 1) * 100, 2)
        else:
            stock_return = None

        # 基准收益
        target_date = trading_dates[target_idx] if target_idx < len(trading_dates) else None
        benchmark_return = None
        if target_date and entry_benchmark:
            bm_price = benchmark_prices.get(target_date)
            if bm_price:
                benchmark_return = round((bm_price / entry_benchmark - 1) * 100, 2)

        # Alpha
        alpha = None
        if stock_return is not None and benchmark_return is not None:
            alpha = round(stock_return - benchmark_return, 2)

        result[f'return_{period}d'] = stock_return
        result[f'benchmark_{period}d'] = benchmark_return
        result[f'alpha_{period}d'] = alpha

    # 判断是否跑赢（取 20 日 alpha）
    alpha_20d = result.get('alpha_20d')
    result['is_win'] = 1 if alpha_20d and alpha_20d > 0 else (0 if alpha_20d is not None else None)

    return result


def _save_backtest(bt: dict):
    """保存回测结果到数据库"""
    with get_connection() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO backtest
            (stock_code, event_date, event_type, entry_price,
             return_5d, return_10d, return_20d, return_60d,
             benchmark_5d, benchmark_10d, benchmark_20d, benchmark_60d,
             alpha_5d, alpha_10d, alpha_20d, alpha_60d, is_win)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            bt['stock_code'], bt['event_date'], bt.get('event_type'), bt.get('entry_price'),
            bt.get('return_5d'), bt.get('return_10d'), bt.get('return_20d'), bt.get('return_60d'),
            bt.get('benchmark_5d'), bt.get('benchmark_10d'), bt.get('benchmark_20d'), bt.get('benchmark_60d'),
            bt.get('alpha_5d'), bt.get('alpha_10d'), bt.get('alpha_20d'), bt.get('alpha_60d'),
            bt.get('is_win'),
        ))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s %(message)s')
    count = run_pending_backtests()
    print(f"\n回测完成: {count} 条")
