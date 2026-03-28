"""
回调买入信号扫描器 v1.1
========================
基于「烧香拜佛」+「备买」融合的多因子回调买入评分系统。

四层漏斗：
  第一层：趋势确认（MA20>MA60, 收盘>MA20, 近20日涨幅>0）
  第二层：回调识别（烧香拜佛路径A / 备买路径B）
  第三层：多重共振确认（缩量、支撑位、动量、K线形态）
  第四层：风险过滤（放量破MA60、连续暴跌、ST）

输出：按评分排序的买入信号列表。
"""

import sys
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from scanners.formulas import (
    calc_shaoxiang_signals, calc_beimai_signals,
    calc_ma, calc_rsi, calc_macd_hist, calc_kdj,
)

logger = logging.getLogger('pullback_scanner')

# ── 评分权重 ──────────────────────────────────────────────────────────────────

SCORE_SHAOXIANG_BAIFO = 60   # 烧香拜佛命中
SCORE_BEIQIAN = 30           # 备买命中
SCORE_BOTH_PATHS = 80        # 双路径同时命中
SCORE_SHRINK_VOLUME = 15     # 缩量回调
SCORE_SUPPORT_PER = 10       # 每条支撑位共振（最多2条）
SCORE_MOMENTUM = 10          # 动量指标确认（2/3）
SCORE_KLINE_PATTERN = 5      # K线形态确认
SCORE_EARNINGS_BEAT = 15     # 超预期加持
PENALTY_BAD_ENV = -15        # 大盘环境差

GRADE_S = 80   # S级：重仓
GRADE_A = 60   # A级：标准买入
GRADE_B = 40   # B级：轻仓试探


# ═══════════════════════════════════════════════════════════════════════════════
#  烧香拜佛（使用 formulas 公共层）
# ═══════════════════════════════════════════════════════════════════════════════

def calc_shaoxiang_baifo(df: pd.DataFrame) -> dict:
    """
    烧香拜佛指标计算（收盘后检测模式）。

    使用 formulas.calc_shaoxiang_signals 获取每日烧香信号，
    然后判断跟踪/关注/拜佛状态。
    """
    n = len(df)
    if n < 7:
        return {'shaoxiang': 0, 'genzong': 0, 'guanzhu': None, 'baifo': 0}

    closes = df['close'].values
    lows = df['low'].values

    # 用公共公式计算烧香信号
    shaoxiang = calc_shaoxiang_signals(df)

    # 找最近一次烧香位置
    sx_positions = np.where(shaoxiang == 1)[0]
    if len(sx_positions) == 0:
        return {'shaoxiang': 0, 'genzong': 0, 'guanzhu': None, 'baifo': 0,
                'sx_date': None, 'sx_close': None, 'sx_low': None}

    last_sx = sx_positions[-1]
    dysj = n - 1 - last_sx

    if dysj > 13:
        return {'shaoxiang': 0, 'genzong': 0, 'guanzhu': None, 'baifo': 0,
                'sx_date': str(df['trade_date'].iloc[last_sx]),
                'sx_close': closes[last_sx], 'sx_low': lows[last_sx]}

    sx_close = closes[last_sx]
    sx_low = lows[last_sx]

    # 跟踪
    genzong = 0
    count_above = sum(1 for j in range(last_sx + 1, n) if closes[j] > sx_low)
    if count_above >= max(dysj - 1, 0):
        genzong = dysj

    # 关注
    guanzhu = None
    if dysj >= 4:
        recent_3 = closes[-3:]
        if all(c > sx_close for c in recent_3):
            guanzhu = sx_close
        elif all(c > sx_low for c in recent_3):
            guanzhu = sx_low

    # 拜佛
    baifo = 0
    if guanzhu is not None and genzong > 0:
        if lows[-1] < guanzhu * 1.01:
            baifo = 1

    return {
        'shaoxiang': int(shaoxiang[-1]),
        'genzong': genzong,
        'guanzhu': guanzhu,
        'baifo': baifo,
        'sx_date': str(df['trade_date'].iloc[last_sx]),
        'sx_close': sx_close,
        'sx_low': sx_low,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  备买（使用 formulas 公共层）
# ═══════════════════════════════════════════════════════════════════════════════

def calc_beimai(df: pd.DataFrame) -> dict:
    """备买指标计算，使用 formulas.calc_beimai_signals。"""
    signals = calc_beimai_signals(df)
    return {
        'beimai': signals['score'],
        'fast': signals['fast'],
        'slow': signals['slow'],
        'signal_type': signals['signal'],
        'ema5': signals['ema5'],
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  第三层：多重共振确认
# ═══════════════════════════════════════════════════════════════════════════════

def calc_volume_shrink(df: pd.DataFrame) -> dict:
    """成交量萎缩分析"""
    n = len(df)
    if n < 6:
        return {'shrink': False, 'ratio': 1.0}

    volumes = df['volume'].values
    cur_vol = volumes[-1]
    avg5 = np.mean(volumes[-6:-1])

    ratio = cur_vol / avg5 if avg5 > 0 else 1.0

    decreasing = False
    if n >= 4:
        last_3 = volumes[-3:]
        decreasing = all(last_3[i] >= last_3[i+1] for i in range(len(last_3)-1))

    return {
        'shrink': ratio < 0.7,
        'ratio': round(ratio, 2),
        'decreasing': decreasing,
        'volume_5d_avg': avg5,
    }


def calc_support_resonance(df: pd.DataFrame, sx_result: dict) -> dict:
    """支撑位共振分析"""
    n = len(df)
    if n < 21:
        return {'count': 0, 'levels': [], 'near_support': False}

    closes = df['close'].values
    lows = df['low'].values
    cur_close = closes[-1]

    ma20 = calc_ma(closes, 21)
    ma60 = calc_ma(closes, 61) if n >= 61 else calc_ma(closes, n)

    support_levels = []
    near_count = 0

    if abs(cur_close - ma20) / ma20 < 0.02:
        support_levels.append(('MA20', ma20))
        near_count += 1

    guanzhu = sx_result.get('guanzhu')
    if guanzhu is not None and abs(cur_close - guanzhu) / guanzhu < 0.02:
        support_levels.append(('烧香关注位', guanzhu))
        near_count += 1

    for i in range(max(0, n-21), n-1):
        if i < 1:
            continue
        day_ret = (closes[i] / closes[i-1] - 1) * 100 if closes[i-1] > 0 else 0
        if day_ret > 2:
            if abs(cur_close - lows[i]) / lows[i] < 0.03:
                support_levels.append((f'阳线低点({df["trade_date"].iloc[i]})', lows[i]))
                near_count += 1
                break

    return {
        'count': min(near_count, 2),
        'levels': support_levels,
        'near_support': near_count > 0,
        'ma20': ma20,
        'ma60': ma60,
    }


def calc_momentum(df: pd.DataFrame) -> dict:
    """动量指标分析（MACD + RSI + KDJ），使用 formulas 公共层"""
    n = len(df)
    if n < 30:
        return {'confirmed': 0, 'details': {}}

    closes = df['close'].values
    confirmed = 0
    details = {}

    # 1. MACD柱状线缩短
    macd_hist = calc_macd_hist(closes)
    if len(macd_hist) >= 3 and macd_hist[-1] < 0 and macd_hist[-2] < 0:
        if macd_hist[-1] > macd_hist[-2]:
            confirmed += 1
            details['macd'] = '负值收窄'
        elif macd_hist[-1] > macd_hist[-3]:
            confirmed += 1
            details['macd'] = '趋势收窄'

    # 2. RSI(6) 超卖回升
    if n >= 7:
        rsi_now = calc_rsi(closes, 6)
        rsi_prev = calc_rsi(closes[:-1], 6)
        if rsi_now < 40 and rsi_now > rsi_prev:
            confirmed += 1
            details['rsi6'] = round(rsi_now, 1)

    # 3. KDJ J值 < 0 后上穿
    _, _, j_vals = calc_kdj(df)
    if len(j_vals) >= 3 and j_vals[-2] < 0 and j_vals[-1] > j_vals[-2]:
        confirmed += 1
        details['kdj_j'] = round(j_vals[-1], 1)

    return {
        'confirmed': confirmed,
        'need': 2,
        'passed': confirmed >= 2,
        'details': details,
    }


def calc_kline_pattern(df: pd.DataFrame) -> dict:
    """K线形态分析"""
    n = len(df)
    if n < 3:
        return {'confirmed': False, 'pattern': None}

    opens = df['open'].values
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values

    o, c, h, l = opens[-1], closes[-1], highs[-1], lows[-1]
    body = abs(c - o)
    lower_shadow = min(o, c) - l

    pattern = None

    if body > 0 and lower_shadow / body > 1.0:
        pattern = '长下影线'

    avg_body = np.mean([abs(closes[i] - opens[i]) for i in range(-5, 0)])
    if avg_body > 0 and body / avg_body < 0.3:
        pattern = '十字星'

    if n >= 2:
        prev_o, prev_c = opens[-2], closes[-2]
        if prev_c < prev_o and c > o and o < prev_c and c > prev_o:
            pattern = '阳包阴'

    return {'confirmed': pattern is not None, 'pattern': pattern}


# ═══════════════════════════════════════════════════════════════════════════════
#  第四层：风险过滤
# ═══════════════════════════════════════════════════════════════════════════════

def calc_risk_filter(df: pd.DataFrame, stock_name: str = '') -> dict:
    """风险过滤"""
    n = len(df)
    if n < 6:
        return {'blocked': False, 'reasons': []}

    closes = df['close'].values
    volumes = df['volume'].values
    reasons = []

    if 'ST' in stock_name.upper():
        reasons.append('ST股票')

    if n >= 61:
        ma60 = calc_ma(closes, 61)
        avg5vol = np.mean(volumes[-6:-1])
        if closes[-1] < ma60 and volumes[-1] > avg5vol * 1.5:
            reasons.append('放量破MA60')

    if n >= 4:
        consecutive = sum(1 for i in range(-3, 0)
                         if closes[i] < closes[i-1] and volumes[i] > volumes[i-1])
        if consecutive >= 3:
            reasons.append('连续放量下跌')

    if n >= 6:
        five_day_ret = (closes[-1] / closes[-6] - 1) * 100
        if five_day_ret < -15:
            reasons.append(f'5日跌幅{five_day_ret:.1f}%')

    return {'blocked': len(reasons) > 0, 'reasons': reasons}


# ═══════════════════════════════════════════════════════════════════════════════
#  综合评分引擎
# ═══════════════════════════════════════════════════════════════════════════════

def calc_pullback_score(df: pd.DataFrame, stock_name: str = '',
                        is_earnings_beat: bool = False,
                        market_env_good: bool = True) -> dict:
    """综合计算回调买入评分。"""
    n = len(df)
    if n < 61:
        return {'score': 0, 'grade': 'C', 'passed': False, 'reason': '数据不足'}

    closes = df['close'].values

    # 第一层：趋势确认
    ma20 = calc_ma(closes, 21)
    ma60 = calc_ma(closes, 61)
    trend_up = ma20 > ma60
    above_ma20 = closes[-1] > ma20
    ret_20d = (closes[-1] / closes[-21] - 1) * 100 if n >= 21 else 0

    if not (trend_up and above_ma20):
        return {'score': 0, 'grade': 'C', 'passed': False,
                'reason': f'趋势不满足: MA20{"↑" if trend_up else "↓"}MA60, 收盘{"在" if above_ma20 else "破"}MA20'}

    # 第四层：风险过滤（提前）
    risk = calc_risk_filter(df, stock_name)
    if risk['blocked']:
        return {'score': 0, 'grade': 'C', 'passed': False,
                'reason': f'风险否决: {", ".join(risk["reasons"])}'}

    # 第二层：回调识别
    sx = calc_shaoxiang_baifo(df)
    bm = calc_beimai(df)

    path_a = sx['baifo'] == 1
    path_b = bm['beimai'] > 0

    base_score = 0
    path_desc = []
    if path_a and path_b:
        base_score = SCORE_BOTH_PATHS
        path_desc.append('烧香拜佛+备买双共振')
    elif path_a:
        base_score = SCORE_SHAOXIANG_BAIFO
        path_desc.append(f'烧香拜佛(烧香日:{sx.get("sx_date","")})')
    elif path_b:
        base_score = SCORE_BEIQIAN
        path_desc.append(f'备买({bm.get("signal_type","")})')

    if base_score == 0:
        return {'score': 0, 'grade': 'C', 'passed': False,
                'reason': '无回调买入信号', 'sx': sx, 'bm': bm}

    # 第三层：多重共振
    total = base_score

    vol = calc_volume_shrink(df)
    if vol['shrink']:
        total += SCORE_SHRINK_VOLUME
        path_desc.append(f'缩量({vol["ratio"]})')
    elif vol['decreasing']:
        total += SCORE_SHRINK_VOLUME * 0.6
        path_desc.append('量能递减')

    support = calc_support_resonance(df, sx)
    if support['count'] > 0:
        total += support['count'] * SCORE_SUPPORT_PER
        path_desc.append(f'支撑({"+".join(l[0] for l in support["levels"])})')

    momentum = calc_momentum(df)
    if momentum['passed']:
        total += SCORE_MOMENTUM
        path_desc.append(f'动量确认({momentum["confirmed"]}/3)')

    kline = calc_kline_pattern(df)
    if kline['confirmed']:
        total += SCORE_KLINE_PATTERN
        path_desc.append(f'K线({kline["pattern"]})')

    if is_earnings_beat:
        total += SCORE_EARNINGS_BEAT
        path_desc.append('超预期')

    if not market_env_good:
        total += PENALTY_BAD_ENV
        path_desc.append('大盘偏弱')

    # 评级
    grade = 'S' if total >= GRADE_S else 'A' if total >= GRADE_A else 'B' if total >= GRADE_B else 'C'

    return {
        'score': total, 'grade': grade, 'passed': grade in ('S', 'A', 'B'),
        'reason': ' | '.join(path_desc),
        'path_a': path_a, 'path_b': path_b,
        'sx': sx, 'bm': bm, 'volume': vol, 'support': support,
        'momentum': momentum, 'kline': kline, 'risk': risk,
        'trend': {'ma20': round(ma20, 2), 'ma60': round(ma60, 2), 'ret_20d': round(ret_20d, 2)},
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  批量扫描入口
# ═══════════════════════════════════════════════════════════════════════════════

def scan_pullback_buy(stock_pool: list, beat_codes: set = None,
                      lookback_days: int = 120, min_score: int = 40) -> list:
    """对股票池执行回调买入扫描。"""
    import tushare as ts
    pro = ts.pro_api()

    if beat_codes is None:
        beat_codes = set()

    today = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')
    market_env_good = _check_market_env(pro, today)

    results = []
    for idx, stock in enumerate(stock_pool):
        code = stock['code']
        name = stock.get('name', '')
        if (idx + 1) % 50 == 0:
            logger.info(f"  进度: {idx+1}/{len(stock_pool)}")
        try:
            df = pro.daily(ts_code=code, start_date=start, end_date=today,
                          fields='ts_code,trade_date,open,high,low,close,vol', limit=120)
            if df is None or len(df) < 61:
                continue
            df = df.sort_values('trade_date').reset_index(drop=True)
            df = df.rename(columns={'vol': 'volume'})

            result = calc_pullback_score(df, name, code in beat_codes, market_env_good)
            if result['passed']:
                result['code'] = code
                result['name'] = name
                result['close'] = round(df['close'].values[-1], 2)
                result['trade_date'] = str(df['trade_date'].iloc[-1])
                results.append(result)
            time.sleep(0.15)
        except Exception as e:
            logger.debug(f"  {code} 扫描失败: {e}")

    results.sort(key=lambda x: x['score'], reverse=True)
    logger.info(f"  ✅ 回调买入扫描完成: {len(stock_pool)} 只 → {len(results)} 只信号")
    return results


def _check_market_env(pro, today: str) -> bool:
    """检查大盘环境（沪深300 MA20方向）"""
    try:
        hs300 = pro.index_daily(ts_code='000300.SH',
                                start_date=(datetime.now() - timedelta(days=40)).strftime('%Y%m%d'),
                                end_date=today, fields='trade_date,close')
        if hs300 is None or len(hs300) < 21:
            return True
        hs300 = hs300.sort_values('trade_date')
        closes = hs300['close'].values
        return calc_ma(closes, 21) > calc_ma(closes[-22:-1], 21)
    except Exception:
        return True


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s %(message)s')
    import tushare as ts
    pro = ts.pro_api()

    test_code, test_name = '300054.SZ', '鼎龙股份'
    logger.info(f"🧪 测试: {test_code} {test_name}")

    today = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')
    df = pro.daily(ts_code=test_code, start_date=start, end_date=today,
                   fields='ts_code,trade_date,open,high,low,close,vol', limit=120)
    df = df.sort_values('trade_date').reset_index(drop=True)
    df = df.rename(columns={'vol': 'volume'})
    logger.info(f"  获取 {len(df)} 条日K数据")

    result = calc_pullback_score(df, test_name, is_earnings_beat=True)
    logger.info(f"\n📊 评分: {result['score']} | 等级: {result['grade']} | 原因: {result['reason']}")
    for k in ['sx', 'bm', 'volume', 'support', 'momentum']:
        if k in result:
            logger.info(f"  {k}: {result[k]}")
