"""
回调买入预测引擎 v1.0
======================
收盘后运行，预判次日及未来几天的买入目标价。

核心逻辑：
  1. 扫描全市场（或指定股票池），找活跃烧香信号 + 备买信号
  2. 对每只信号股计算：建议买入价、买入区间、止损位、风险收益比
  3. 写入 predictions 表，供盘中轻检使用
  4. 对新产生的预告信号推飞书通知

输出格式（每条预测）：
  {
    code, name, signal_type, trigger_date,
    buy_target, buy_low, buy_high,
    stop_loss, risk_reward_ratio,
    current_close, sx_close, sx_low,
    expires_at, status
  }
"""

import sys
import json
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from core.database import get_connection, init_db
from scanners.formulas import calc_shaoxiang_signals, calc_beimai_signals

logger = logging.getLogger('pullback_predictor')

# ── 配置 ──────────────────────────────────────────────────────────────────────

BUY_TOLERANCE = 0.01      # 买入区间 ±1%
STOP_LOSS_RATIO = 0.05    # 止损 = 支撑位下方 5%
MAX_PREDICTION_DAYS = 5   # 预测有效期（交易日）
MIN_RISK_REWARD = 1.5     # 最低风险收益比


# ═══════════════════════════════════════════════════════════════════════════════
#  预测计算
# ═══════════════════════════════════════════════════════════════════════════════

def predict_shaoxiang_baifo(df: pd.DataFrame, code: str, name: str) -> dict:
    """
    烧香拜佛预测。

    条件：
    - 近13天内有烧香
    - Day 4+ 起连续3天站稳支撑（guanzhu确认）
    - 当前价格 > 支撑位（还没回踩到位）

    返回预测信息或 None。
    """
    n = len(df)
    if n < 7:
        return None

    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values

    # 使用公共公式计算烧香信号
    shaoxiang = calc_shaoxiang_signals(df)

    sx_positions = np.where(shaoxiang == 1)[0]
    if len(sx_positions) == 0:
        return None

    last_sx = sx_positions[-1]
    dysj = n - 1 - last_sx

    if dysj > 13 or dysj < 4:
        return None

    sx_close = closes[last_sx]
    sx_low = lows[last_sx]

    # 检查支撑确认：第4天起连续3天站稳
    check_start = last_sx + 4
    if check_start + 2 < n:
        confirmed = all(closes[j] > sx_close for j in range(check_start, check_start + 3))
        if not confirmed:
            confirmed = all(closes[j] > sx_low for j in range(check_start, check_start + 3))
        if not confirmed:
            return None
    elif check_start < n:
        return None

    buy_target = sx_close
    buy_low = round(buy_target * (1 - BUY_TOLERANCE), 2)
    buy_high = round(buy_target * (1 + BUY_TOLERANCE), 2)
    stop_loss = round(sx_low * (1 - STOP_LOSS_RATIO), 2)

    recent_high = max(highs[last_sx:])
    expected_gain = recent_high - buy_target
    expected_loss = buy_target - stop_loss
    risk_reward = round(expected_gain / expected_loss, 1) if expected_loss > 0 else 0

    cur_close = closes[-1]

    # 只在当前价 > 目标价时才发出（说明还没回踩到位）
    if cur_close <= buy_target * 1.02:
        # 已经在目标价附近或以下，不适合提前预告
        return None

    return {
        'signal_type': 'shaoxiang_baifo',
        'trigger_date': str(df['trade_date'].iloc[last_sx]),
        'buy_target': buy_target,
        'buy_low': buy_low,
        'buy_high': buy_high,
        'stop_loss': stop_loss,
        'risk_reward_ratio': risk_reward,
        'current_close': round(cur_close, 2),
        'sx_close': round(sx_close, 2),
        'sx_low': round(sx_low, 2),
        'recent_high': round(recent_high, 2),
    }


def predict_beimai(df: pd.DataFrame, code: str, name: str) -> dict:
    """
    备买预测。

    条件：
    - EMA5 连续走平/下行（快=0）
    - 但6日内EMA5整体偏多（慢>20）
    - 当前价高于EMA5

    计算目标价：EMA5当前值或前低作为支撑。
    """
    n = len(df)
    if n < 10:
        return None

    closes = df['close'].values

    # 使用公共公式
    signals = calc_beimai_signals(df)

    if not (signals['fast'] == 0 and signals['slow'] > 20):
        return None

    cur_close = closes[-1]
    cur_ema5 = signals['ema5']

    recent_low = min(closes[-5:])
    buy_target = min(cur_ema5, recent_low)
    buy_low = round(buy_target * (1 - BUY_TOLERANCE), 2)
    buy_high = round(buy_target * (1 + BUY_TOLERANCE), 2)

    stop_loss = round(recent_low * (1 - STOP_LOSS_RATIO), 2)

    recent_high = max(closes[-10:])
    expected_gain = recent_high - buy_target
    expected_loss = buy_target - stop_loss
    risk_reward = round(expected_gain / expected_loss, 1) if expected_loss > 0 else 0

    if cur_close <= buy_target * 1.02:
        return None

    return {
        'signal_type': 'beimai',
        'trigger_date': str(df['trade_date'].iloc[-1]),
        'buy_target': round(buy_target, 2),
        'buy_low': buy_low,
        'buy_high': buy_high,
        'stop_loss': stop_loss,
        'risk_reward_ratio': risk_reward,
        'current_close': round(cur_close, 2),
        'ema5': round(cur_ema5, 2),
        'recent_low': round(recent_low, 2),
        'recent_high': round(recent_high, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  批量预测 + 入库
# ═══════════════════════════════════════════════════════════════════════════════

def generate_predictions(stock_pool: list, lookback_days: int = 120) -> list:
    """
    对股票池执行预测，返回新产生的预告信号列表。

    新信号 = 今天刚生成、之前不存在的。
    """
    import tushare as ts
    pro = ts.pro_api()

    today = datetime.now().strftime('%Y%m%d')
    today_str = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')

    # 加载已有预测（避免重复）
    existing = set()
    try:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT stock_code, signal_type FROM pullback_predictions
                WHERE status IN ('active', 'triggered')
                AND created_at >= date('now', '-7 days')
            """).fetchall()
            for r in rows:
                existing.add((r['stock_code'], r['signal_type']))
    except Exception:
        pass

    predictions = []
    new_alerts = []
    total = len(stock_pool)

    for idx, stock in enumerate(stock_pool):
        code = stock['code']
        name = stock.get('name', '')

        if (idx + 1) % 100 == 0:
            logger.info(f"  预测进度: {idx+1}/{total}")

        try:
            df = pro.daily(ts_code=code, start_date=start, end_date=today,
                          fields='ts_code,trade_date,open,high,low,close,vol',
                          limit=120)
            if df is None or len(df) < 21:
                continue

            df = df.sort_values('trade_date').reset_index(drop=True)

            # 烧香拜佛预测
            sx_pred = predict_shaoxiang_baifo(df, code, name)
            if sx_pred and (code, 'shaoxiang_baifo') not in existing:
                sx_pred['code'] = code
                sx_pred['name'] = name
                predictions.append(sx_pred)
                new_alerts.append(sx_pred)

            # 备买预测
            bm_pred = predict_beimai(df, code, name)
            if bm_pred and (code, 'beimai') not in existing:
                bm_pred['code'] = code
                bm_pred['name'] = name
                predictions.append(bm_pred)
                new_alerts.append(bm_pred)

            time.sleep(0.12)

        except Exception as e:
            logger.debug(f"  {code} 预测失败: {e}")
            continue

    # 入库
    if predictions:
        _save_predictions(predictions, today_str)
        logger.info(f"  💾 已保存 {len(predictions)} 条预测")

    # 过滤出新信号
    logger.info(f"  ✅ 预测完成: {total} 只 → {len(predictions)} 条预测 → {len(new_alerts)} 条新信号")
    return new_alerts


def _save_predictions(predictions: list, today_str: str):
    """保存预测到数据库"""
    init_db()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    expires = (datetime.now() + timedelta(days=MAX_PREDICTION_DAYS)).strftime('%Y-%m-%d %H:%M:%S')

    with get_connection() as conn:
        # 确保表存在
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pullback_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                stock_name TEXT,
                signal_type TEXT NOT NULL,
                trigger_date TEXT,
                buy_target REAL,
                buy_low REAL,
                buy_high REAL,
                stop_loss REAL,
                risk_reward_ratio REAL,
                current_close REAL,
                detail TEXT,
                status TEXT DEFAULT 'active',
                expires_at TEXT,
                triggered_at TEXT,
                triggered_price REAL,
                created_at TEXT DEFAULT (datetime('now', 'localtime')),
                UNIQUE(stock_code, signal_type, trigger_date)
            )
        """)

        # 清理过期预测
        conn.execute("""
            UPDATE pullback_predictions
            SET status = 'expired'
            WHERE status = 'active' AND expires_at < datetime('now', 'localtime')
        """)

        for p in predictions:
            detail = json.dumps(p, ensure_ascii=False, default=str)
            conn.execute("""
                INSERT OR REPLACE INTO pullback_predictions
                (stock_code, stock_name, signal_type, trigger_date,
                 buy_target, buy_low, buy_high, stop_loss, risk_reward_ratio,
                 current_close, detail, status, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
            """, (
                p['code'], p.get('name', ''), p['signal_type'],
                p.get('trigger_date', ''),
                p.get('buy_target'), p.get('buy_low'), p.get('buy_high'),
                p.get('stop_loss'), p.get('risk_reward_ratio'),
                p.get('current_close'), detail, expires
            ))

        conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
#  盘中轻检
# ═══════════════════════════════════════════════════════════════════════════════

def check_intraday_triggers() -> list:
    """
    盘中轻检：用实时价格 vs 预测目标价，检测是否触发。

    返回刚触发的信号列表（用于推通知）。
    """
    init_db()
    triggered = []

    # 获取活跃预测
    try:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM pullback_predictions
                WHERE status = 'active'
                AND expires_at > datetime('now', 'localtime')
            """).fetchall()
    except Exception as e:
        logger.error(f"  读取预测失败: {e}")
        return []

    if not rows:
        return []

    # 批量获取实时价格（腾讯行情API）
    codes = [r['stock_code'] for r in rows]
    prices = _fetch_realtime_prices(codes)

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with get_connection() as conn:
        for row in rows:
            code = row['stock_code']
            cur_price = prices.get(code)
            if cur_price is None or cur_price <= 0:
                continue

            buy_low = row['buy_low']
            buy_high = row['buy_high']

            if buy_low <= cur_price <= buy_high:
                # 触发！
                conn.execute("""
                    UPDATE pullback_predictions
                    SET status = 'triggered', triggered_at = ?, triggered_price = ?
                    WHERE id = ?
                """, (now, cur_price, row['id']))

                detail = json.loads(row['detail']) if row['detail'] else {}
                detail['triggered_price'] = cur_price
                detail['triggered_at'] = now
                triggered.append({
                    'code': code,
                    'name': row['stock_name'],
                    'signal_type': row['signal_type'],
                    'buy_target': row['buy_target'],
                    'triggered_price': cur_price,
                    'stop_loss': row['stop_loss'],
                    'risk_reward_ratio': row['risk_reward_ratio'],
                })

        conn.commit()

    if triggered:
        logger.info(f"  🔔 触发 {len(triggered)} 条买入信号!")

    return triggered


def _fetch_realtime_prices(codes: list) -> dict:
    """
    批量获取实时价格。优先用腾讯行情API，备选Tushare。
    返回 {code: price} 映射。
    """
    prices = {}

    # 方案1：Tushare daily（当日已收盘时）
    try:
        import tushare as ts
        pro = ts.pro_api()
        today = datetime.now().strftime('%Y%m%d')

        for code in codes:
            try:
                df = pro.daily(ts_code=code, trade_date=today,
                              fields='ts_code,close')
                if df is not None and len(df) > 0:
                    prices[code] = df['close'].iloc[0]
                time.sleep(0.1)
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"  Tushare 行情获取失败: {e}")

    return prices


# ═══════════════════════════════════════════════════════════════════════════════
#  生成飞书通知内容
# ═══════════════════════════════════════════════════════════════════════════════

def format_alert(p: dict, is_triggered: bool = False) -> str:
    """格式化单条信号为飞书消息文本"""
    code_short = p['code'].replace('.SH', '').replace('.SZ', '')
    name = p.get('name', code_short)
    signal_name = '烧香拜佛' if p['signal_type'] == 'shaoxiang_baifo' else '备买'

    if is_triggered:
        return (
            f"🔔 **回调买入触发**\n\n"
            f"**{name}({code_short})**\n"
            f"信号：{signal_name}\n"
            f"触发价：¥{p.get('triggered_price', 0):.2f}\n"
            f"目标价：¥{p.get('buy_target', 0):.2f}\n"
            f"止损位：¥{p.get('stop_loss', 0):.2f}\n"
            f"风险收益比：1:{p.get('risk_reward_ratio', 0)}\n\n"
            f"⚠️ 建议设限价单买入，严格止损。"
        )
    else:
        return (
            f"📐 **回调买入预告**\n\n"
            f"**{name}({code_short})**\n"
            f"信号：{signal_name}\n"
            f"当前价：¥{p.get('current_close', 0):.2f}\n"
            f"目标买入价：¥{p.get('buy_target', 0):.2f}\n"
            f"买入区间：¥{p.get('buy_low', 0):.2f} ~ ¥{p.get('buy_high', 0):.2f}（±1%）\n"
            f"止损位：¥{p.get('stop_loss', 0):.2f}\n"
            f"风险收益比：1:{p.get('risk_reward_ratio', 0)}\n\n"
            f"⚠️ 设限价单，不追高。{MAX_PREDICTION_DAYS}天内未触发则信号作废。"
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  主入口
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """独立运行入口：收盘后预测 or 盘中轻检"""
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s [%(name)s] %(levelname)s %(message)s')

    if '--check' in sys.argv:
        # 盘中轻检模式
        logger.info("🔍 盘中轻检模式")
        triggered = check_intraday_triggers()
        if triggered:
            for t in triggered:
                print(format_alert(t, is_triggered=True))
                print("---")
        else:
            print("无触发信号")
    else:
        # 预测模式（收盘后运行）
        logger.info("📐 回调买入预测模式")

        import tushare as ts
        pro = ts.pro_api()

        # 获取全市场股票（或用持仓池）
        logger.info("  获取股票列表...")
        all_stocks = pro.stock_basic(exchange='', list_status='L',
                                     fields='ts_code,name')
        pool = [{'code': row['ts_code'], 'name': row['name']}
                for _, row in all_stocks.iterrows()]

        logger.info(f"  扫描池: {len(pool)} 只")
        new_alerts = generate_predictions(pool)

        if new_alerts:
            logger.info(f"\n📐 新产生 {len(new_alerts)} 条买入预告:")
            for a in new_alerts:
                print(format_alert(a))
                print("---")
        else:
            logger.info("  无新信号")


if __name__ == '__main__':
    main()
