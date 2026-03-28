"""
回调买入公共公式库
==================
烧香拜佛、备买等核心计算逻辑，scanner 和 predictor 共用。
"""

import numpy as np
import pandas as pd


def calc_ema(series: np.ndarray, period: int) -> np.ndarray:
    """计算 EMA（指数移动平均）"""
    ema = np.zeros(len(series))
    ema[0] = series[0]
    multiplier = 2.0 / (period + 1)
    for i in range(1, len(series)):
        ema[i] = (series[i] - ema[i-1]) * multiplier + ema[i-1]
    return ema


def calc_shaoxiang_signals(df: pd.DataFrame) -> np.ndarray:
    """
    计算每日烧香信号（向量化）。

    烧香条件（三选一）：
      1. 实体涨幅 > 3% 且为6日最强
      2. 涨幅 > 3% 且为6日最强
      3. 涨停（涨幅>=9% 且收盘=最高）

    返回：int 数组，1=烧香，0=无
    """
    n = len(df)
    if n < 6:
        return np.zeros(n, dtype=int)

    opens = df['open'].values
    closes = df['close'].values
    highs = df['high'].values

    st = (closes / opens - 1) * 100
    zf = np.zeros(n)
    zf[1:] = (closes[1:] / closes[:-1] - 1) * 100

    shaoxiang = np.zeros(n, dtype=int)
    for i in range(5, n):
        sx1 = st[i] > 3 and st[i] == max(st[i-5:i+1])
        sx2 = zf[i] > 3 and zf[i] == max(zf[i-5:i+1])
        sx3 = zf[i] >= 9 and closes[i] == highs[i]
        if sx1 or sx2 or sx3:
            shaoxiang[i] = 1

    return shaoxiang


def calc_beimai_signals(df: pd.DataFrame) -> dict:
    """
    计算备买信号参数。

    返回：
      {
        'fast': float,      # 近3日EMA5上涨占比（0-100）
        'slow': float,      # 近6日EMA5上涨占比（0-100）
        'ema5': float,      # 当前EMA5值
        'signal': str|None, # 'beiqian' / 'dimai' / None
        'score': int,       # 20=备钱 / 50=低买 / 0=无信号
      }
    """
    n = len(df)
    if n < 10:
        return {'fast': 0, 'slow': 0, 'ema5': 0, 'signal': None, 'score': 0}

    closes = df['close'].values
    qx = calc_ema(closes, 5)

    qx_rising = np.zeros(n, dtype=int)
    qx_rising[1:] = (qx[1:] > qx[:-1]).astype(int)

    fast_count = int(np.sum(qx_rising[-3:]))
    fast = fast_count / 3 * 100
    slow_count = int(np.sum(qx_rising[-6:]))
    slow = slow_count / 6 * 100

    signal = None
    score = 0
    if fast == 0 and slow > 20:
        signal = 'beiqian'
        score = 20
    elif fast == 0 and 1 <= slow <= 20:
        signal = 'dimai'
        score = 50

    return {
        'fast': fast,
        'slow': slow,
        'ema5': round(qx[-1], 4),
        'signal': signal,
        'score': score,
    }


def calc_ma(series: np.ndarray, period: int) -> float:
    """计算简单移动平均"""
    if len(series) < period:
        return np.mean(series)
    return np.mean(series[-period:])


def calc_rsi(closes: np.ndarray, period: int = 6) -> float:
    """计算 RSI"""
    n = len(closes)
    if n < period + 1:
        return 50.0

    gains = []
    losses = []
    for j in range(n - period, n):
        change = closes[j] - closes[j-1] if j > 0 else 0
        if change > 0:
            gains.append(change)
        else:
            losses.append(abs(change))
    avg_gain = np.mean(gains) if gains else 0
    avg_loss = np.mean(losses) if losses else 0.001
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_macd_hist(closes: np.ndarray) -> np.ndarray:
    """计算 MACD 柱状线"""
    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    dif = ema12 - ema26
    dea = calc_ema(dif, 9)
    return (dif - dea) * 2


def calc_kdj(df: pd.DataFrame, period: int = 9):
    """计算 KDJ 指标"""
    n = len(df)
    if n < period + 1:
        return np.zeros(n), np.zeros(n), np.zeros(n)

    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values

    k_vals = np.zeros(n)
    d_vals = np.zeros(n)
    j_vals = np.zeros(n)
    k_vals[period-1] = 50
    d_vals[period-1] = 50

    for i in range(period, n):
        hhv = max(highs[i-period+1:i+1])
        llv = min(lows[i-period+1:i+1])
        rsv = (closes[i] - llv) / (hhv - llv) * 100 if hhv != llv else 50
        k_vals[i] = k_vals[i-1] * 2/3 + rsv / 3
        d_vals[i] = d_vals[i-1] * 2/3 + k_vals[i] / 3
        j_vals[i] = 3 * k_vals[i] - 2 * d_vals[i]

    return k_vals, d_vals, j_vals
