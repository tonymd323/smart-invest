"""
任务B：扣非净利润新高扫描
========================
按 disclosure_date 获取当日财报披露公司，
用 fina_indicator.profit_dedt（累计扣非净利润）转单季度，对比历史最高。
"""

import sys
import time
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger('new_high_scanner')


def scan_quarterly_new_high(target_date: str = None) -> list:
    """
    扫描当日披露财报的公司中，单季度扣非净利润创历史新高的。

    数据流：
      Step 1: disclosure_date(pre_date=today, actual_date≠None) → 披露名单
      Step 2: fina_indicator(ts_code=xxx) → profit_dedt（累计值）8个季度
      Step 3: 累计转单季度 → 对比历史最高
    """
    import tushare as ts
    pro = ts.pro_api()

    if target_date is None:
        target_date = datetime.now().strftime('%Y%m%d')

    logger.info(f"{'='*50}")
    logger.info(f"💎 任务B：扣非净利润新高扫描 | 日期={target_date}")
    logger.info(f"{'='*50}")

    # Step 1: 获取当日已实际披露的公司
    logger.info("  Step 1: 获取财报披露名单...")
    try:
        dd = pro.disclosure_date(pre_date=target_date)
        if dd is None or dd.empty:
            logger.info("  今日无财报披露计划，跳过")
            return []

        dd = dd[dd['actual_date'].notna()] if 'actual_date' in dd.columns else dd
        if dd.empty:
            logger.info("  今日尚无已实际披露的财报，跳过")
            return []

        dd = dd.drop_duplicates('ts_code', keep='first')
        codes = dd['ts_code'].tolist()
        logger.info(f"  Step 1 完成：{len(codes)} 家已披露")
    except Exception as e:
        logger.warning(f"  披露计划查询失败: {e}")
        return []

    # Step 2-3: 逐只检查扣非净利润新高
    # 预先批量拉全量行情（PE/PB/收盘价），各线程只查 dict，不走网络
    logger.info("  Step 2: 预拉行情数据...")
    from core.data_provider import QuoteProvider
    quote_provider = QuoteProvider()
    quotes = quote_provider.fetch_batch(codes)  # {code: QuoteData}
    logger.info(f"  行情预拉：{len(quotes)}/{len(codes)} 只成功")

    logger.info("  Step 3: 检查扣非净利润新高...")
    new_highs = []
    for i, code in enumerate(codes):
        try:
            result = _check_single_new_high(pro, code, quotes)
            if result:
                new_highs.append(result)
                logger.info(f"    ✅ {code} {result['name']} 扣非新高！")
        except Exception:
            continue

        time.sleep(0.3)

        if (i + 1) % 20 == 0:
            logger.info(f"    进度: {i+1}/{len(codes)} | 已发现: {len(new_highs)}")

    logger.info(f"  ✅ 扣非净利润新高: {len(new_highs)} 只")
    return new_highs


def _check_single_new_high(pro, code: str, quotes: dict) -> dict:
    """检查单只股票的扣非净利润是否新高"""
    # 获取扣非净利润累计值（最近 12 个季度）
    fi = pro.fina_indicator(
        ts_code=code,
        fields='ts_code,ann_date,end_date,profit_dedt,dt_netprofit_yoy,roe_dt',
        limit=16
    )
    if fi is None or len(fi) < 5:
        return None

    fi = fi.sort_values('end_date').drop_duplicates('end_date', keep='first')

    # 提取累计扣非净利润
    cumulative = []
    for _, r in fi.iterrows():
        val = _safe_float(r.get('profit_dedt'))
        end_date = str(r.get('end_date', ''))
        if val is not None and val > 0 and end_date:
            cumulative.append({'date': end_date, 'cumulative': val})

    if len(cumulative) < 5:
        return None

    # 时效性：最新报告期不超过 120 天
    latest_date = cumulative[-1]['date']
    try:
        latest_dt = datetime.strptime(latest_date, '%Y%m%d')
        if (datetime.now() - latest_dt).days > 120:
            return None
    except ValueError:
        pass

    # 累计转单季度
    quarterly = _cumulative_to_quarterly(cumulative)
    if len(quarterly) < 4:
        return None

    # 判定：最新单季度是否历史新高
    latest_val = quarterly[-1]['value']
    historical_max = max(q['value'] for q in quarterly[:-1])

    if latest_val > historical_max and latest_val > 0:
        # 获取公司名称
        name = ''
        try:
            nb = pro.stock_basic(ts_code=code, fields='ts_code,name')
            if nb is not None and not nb.empty:
                name = nb.iloc[0]['name']
        except Exception:
            pass

        # 获取收盘价和 PE（从预拉的行情 dict 查，不走网络）
        q = quotes.get(code)
        close = q.price if q else None
        pe = q.pe if q else None
        total_mv = q.total_mv if q else None

        return {
            'code': code,
            'name': name,
            'quarterly_profit': round(latest_val / 1e8, 2),  # 转为亿元
            'prev_high': round(historical_max / 1e8, 2),
            'growth_vs_high': round((latest_val / historical_max - 1) * 100, 1),
            'report_date': latest_date,
            'close': close,
            'pe': pe,
            'total_mv': total_mv,
            'dt_netprofit_yoy': fi.iloc[-1].get('dt_netprofit_yoy'),
        }

    return None


def _cumulative_to_quarterly(records: list) -> list:
    """将累计值转换为单季度值"""
    quarterly = []
    for i, rec in enumerate(records):
        date = rec['date']
        cum = rec['cumulative']
        month = date[4:6]

        if month == '03':
            # Q1 = 累计值本身就是单季度
            quarterly.append({'date': date, 'value': cum})
        elif month in ('06', '09', '12'):
            # Q2/Q3/Q4 = 当期累计 - 上期累计
            prev_cum = records[i-1]['cumulative'] if i > 0 else 0
            quarterly.append({'date': date, 'value': cum - prev_cum})

    return quarterly


def _safe_float(val) -> float:
    if val is None:
        return None
    try:
        import math
        f = float(val)
        return f if not math.isnan(f) else None
    except (ValueError, TypeError):
        return None
