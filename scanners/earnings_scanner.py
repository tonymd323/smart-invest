"""
任务A：业绩超预期扫描（v2 - 并行化 + 假超预期过滤）
=====================================================
按 disclosure_date 获取当日财报披露公司，用 fina_indicator 获取实际增速，
对比 AkShare 一致预期。

v2 改进：
- ThreadPoolExecutor 并行获取实际数据和一致预期（5-10x 提速）
- 非经常性损益过滤（扣非净利润验证）
- 假超预期标记
"""

import sys
import time
import math
import logging
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

logger = logging.getLogger('earnings_scanner')

# 超预期阈值（实际增速 - 预期增速 >= 此值）
BEAT_THRESHOLD = 5

# 并发控制
MAX_WORKERS_ACTUALS = 12     # Tushare 并发数
MAX_WORKERS_CONSENSUS = 8    # AkShare 并发数（更保守，避免被封）


def scan_earnings_beat(target_date: str = None) -> list:
    """
    扫描当日业绩超预期的公司。

    数据流：
      Step 1: disclosure_date(pre_date=today) → 披露名单
      Step 2: fina_indicator(ts_code=xxx) → 实际增速(dt_netprofit_yoy, or_yoy)
      Step 3: AkShare 一致预期 → 预期增速(profit_25e, rev_25e)
      Step 4: 对比，diff >= 5pp → 超预期
    """
    import tushare as ts
    pro = ts.pro_api()

    if target_date is None:
        target_date = datetime.now().strftime('%Y%m%d')

    logger.info(f"{'='*50}")
    logger.info(f"📊 任务A：业绩超预期扫描 | 日期={target_date}")
    logger.info(f"{'='*50}")

    # Step 1: 获取当日已实际披露的公司
    logger.info("  Step 1: 获取披露名单...")
    disclosed = _fetch_disclosed_list(pro, target_date)
    logger.info(f"  Step 1 完成：{len(disclosed)} 家已披露")

    if not disclosed:
        logger.info("  今日无已披露公司，跳过")
        return []

    # Step 2: 获取实际增速（fina_indicator）
    logger.info("  Step 2: 获取实际财务数据...")
    actuals = _batch_fetch_actuals(pro, disclosed)
    logger.info(f"  Step 2 完成：{len(actuals)} 家有数据")

    # Step 3: 获取一致预期（AkShare）
    logger.info("  Step 3: 获取一致预期...")
    consensus = _batch_fetch_consensus(list(actuals.keys()), disclosed)
    logger.info(f"  Step 3 完成：{len(consensus)} 家有预期")

    # Step 4: 对比判定
    logger.info("  Step 4: 对比判定...")
    beats = []
    for code, actual in actuals.items():
        exp = consensus.get(code, {})
        result = _check_beat(code, actual, exp, disclosed.get(code, {}))
        if result:
            beats.append(result)

    logger.info(f"  ✅ 业绩超预期: {len(beats)} 只")
    return beats


def _fetch_disclosed_list(pro, target_date: str) -> dict:
    """获取当日已披露的公司名单（三层：预告+快报+财报）"""
    from datetime import datetime, timedelta

    result = {}

    # 生成最近 7 天的日期列表（年报季披露密集，窗口拉宽）
    try:
        dt = datetime.strptime(target_date, '%Y%m%d')
    except ValueError:
        dt = datetime.now()
    date_list = [(dt - timedelta(days=i)).strftime('%Y%m%d') for i in range(2)]  # 今天+昨天

    # 1. 业绩预告（查最近 7 天 + target_date+1 处理 Tushare 次日入库延迟）
    next_day = (dt + timedelta(days=1)).strftime('%Y%m%d')
    forecast_date_list = date_list + [next_day]
    for d in forecast_date_list:
        try:
            f = pro.forecast(ann_date=d, fields='ts_code,name,ann_date,end_date,type,p_change_min,p_change_max')
            if f is not None and not f.empty:
                f = f.drop_duplicates('ts_code', keep='first')
                for _, r in f.iterrows():
                    code = r['ts_code']
                    if code not in result:  # 不覆盖已有的（快报/财报优先）
                        ann = str(r.get('ann_date', d))
                        result[code] = {
                            'end_date': str(r.get('end_date', '')),
                            'ann_date': ann,
                            'disclosure_type': '业绩预告',
                            'name': r.get('name', ''),
                            'profit_change_min': _safe_float(r.get('p_change_min')),
                            'profit_change_max': _safe_float(r.get('p_change_max')),
                        }
        except Exception:
            pass
        time.sleep(0.1)

    # 补充：批量查缺失的公司名称（Tushare forecast 不返回 name）
    _fill_missing_names(pro, result)

    # 2. 业绩快报（覆盖预告）
    # 交叉验证：Tushare express 表混入了部分 forecast 数据
    # 如果某公司同一天既在 forecast 又在 express 中出现，以 forecast 为准（express 可能是脏数据）
    # 但如果该公司只在 express 中出现（不同日期的快报），则正常覆盖
    forecast_entries = {code: info.get('ann_date', '') for code, info in result.items()}
    express_date_list = date_list + [next_day]
    for d in express_date_list:
        try:
            e = pro.express(ann_date=d, fields='ts_code,name,ann_date,end_date,revenue,n_income')
            if e is not None and not e.empty:
                e = e.drop_duplicates('ts_code', keep='first')
                for _, r in e.iterrows():
                    code = r['ts_code']
                    express_ann_date = str(r.get('ann_date', d))
                    # 交叉验证：如果该股票同一天已在 forecast 中出现，express 可能是脏数据，跳过
                    if code in forecast_entries and forecast_entries[code] == express_ann_date:
                        logger.info(f"    跳过 {code} 的 express 记录（同日已存在于 forecast，Tushare 数据质量问题）")
                        continue
                    result[code] = {  # 快报覆盖预告
                        'end_date': str(r.get('end_date', '')),
                        'ann_date': express_ann_date,
                        'disclosure_type': '业绩快报',
                        'name': r.get('name', ''),
                        'revenue': _safe_float(r.get('revenue')),
                        'n_income': _safe_float(r.get('n_income')),
                    }
        except Exception:
            pass
        time.sleep(0.1)

    # 3. 财报（通过 disclosure_date，覆盖预告和快报）
    # 同样多查 target_date+1 处理 Tushare 次日入库延迟
    disc_date_list = date_list + [next_day]
    for d in disc_date_list:
        try:
            dd = pro.disclosure_date(pre_date=d)
            if dd is not None and not dd.empty:
                dd = dd.drop_duplicates('ts_code', keep='first')
                for _, r in dd.iterrows():
                    code = r['ts_code']
                    actual = str(r.get('actual_date', '')) if 'actual_date' in dd.columns else ''
                    if actual and actual != 'nan' and actual != 'None':
                        result[code] = {
                            'end_date': str(r.get('end_date', '')),
                            'ann_date': actual,
                            'disclosure_type': '财报',
                            'name': result.get(code, {}).get('name', ''),
                        }
        except Exception:
            pass
        time.sleep(0.1)

    # 3b. 明天的数据：批量查 forecast/express/fina_indicator，捞提前披露的
    # 公司可能晚间提前披露，Tushare 先入库但 disclosure_date 的 actual_date 还没更新
    # 3b-i. 业绩预告（ann_date=next_day）
    try:
        f_batch = pro.forecast(ann_date=next_day, fields='ts_code,name,ann_date,end_date,type,p_change_min,p_change_max')
        if f_batch is not None and not f_batch.empty:
            f_batch = f_batch.drop_duplicates('ts_code', keep='first')
            for _, r in f_batch.iterrows():
                code = r['ts_code']
                if code not in result:
                    result[code] = {
                        'end_date': str(r.get('end_date', '')),
                        'ann_date': str(r.get('ann_date', next_day)),
                        'disclosure_type': '业绩预告',
                        'name': r.get('name', ''),
                        'profit_change_min': _safe_float(r.get('p_change_min')),
                        'profit_change_max': _safe_float(r.get('p_change_max')),
                    }
    except Exception:
        pass
    time.sleep(0.1)

    # 3b-ii. 业绩快报（ann_date=next_day）
    try:
        e_batch = pro.express(ann_date=next_day, fields='ts_code,name,ann_date,end_date,revenue,n_income')
        if e_batch is not None and not e_batch.empty:
            e_batch = e_batch.drop_duplicates('ts_code', keep='first')
            forecast_entries = {code: info.get('ann_date', '') for code, info in result.items()}
            for _, r in e_batch.iterrows():
                code = r['ts_code']
                express_ann_date = str(r.get('ann_date', next_day))
                # 交叉验证：同日已在 forecast 中则跳过（Tushare 数据质量问题）
                if code in forecast_entries and forecast_entries[code] == express_ann_date:
                    continue
                if code not in result or result[code].get('disclosure_type') == '业绩预告':
                    result[code] = {
                        'end_date': str(r.get('end_date', '')),
                        'ann_date': express_ann_date,
                        'disclosure_type': '业绩快报',
                        'name': r.get('name', ''),
                        'revenue': _safe_float(r.get('revenue')),
                        'n_income': _safe_float(r.get('n_income')),
                    }
    except Exception:
        pass
    time.sleep(0.1)

    # 3b-iii. 财报年报（fina_indicator ann_date=next_day）
    try:
        dd_tomorrow = pro.disclosure_date(pre_date=next_day)
        if dd_tomorrow is not None and not dd_tomorrow.empty:
            not_disclosed = dd_tomorrow[
                dd_tomorrow['actual_date'].isna() if 'actual_date' in dd_tomorrow.columns else dd_tomorrow.index == dd_tomorrow.index
            ]['ts_code'].tolist()
            if not_disclosed:
                fi_batch = pro.fina_indicator(ann_date=next_day, fields='ts_code,ann_date,end_date')
                if fi_batch is not None and not fi_batch.empty:
                    fi_2025 = fi_batch[fi_batch['end_date'] == '20251231']
                    for _, r in fi_2025.iterrows():
                        code = r['ts_code']
                        if code in not_disclosed and code not in result:
                            result[code] = {
                                'end_date': '20251231',
                                'ann_date': str(r.get('ann_date', next_day)),
                                'disclosure_type': '财报',
                                'name': result.get(code, {}).get('name', ''),
                            }
    except Exception:
        pass

    # 4. AkShare 业绩预告（补充 Tushare 缺失的）
    # 注意：stock_yjyg_em 的 date 参数是报告期，不是公告日期
    # 它返回整个报告期的所有预告，需要按公告日期过滤到最近7天
    try:
        import akshare as ak
        # 根据当前日期推算可能的报告期
        current_year = dt.year
        report_periods = [
            f'{current_year}0331',    # Q1
            f'{current_year}0630',    # 半年报
            f'{current_year}0930',    # Q3
            f'{current_year-1}1231',  # 上年年报
        ]
        date_set = set(date_list + [next_day])  # 今天+昨天+明天（Q1预告可能提前一天入库）

        for period in report_periods:
            try:
                df = ak.stock_yjyg_em(date=period)
                if df is not None and not df.empty:
                    for _, r in df.iterrows():
                        code_raw = str(r.get('股票代码', ''))
                        if not code_raw:
                            continue

                        # 按公告日期过滤：只保留最近7天的
                        ann_date_raw = str(r.get('公告日期', ''))
                        ann_date_clean = ann_date_raw.replace('-', '')
                        if ann_date_clean not in date_set:
                            continue

                        # 转换为 Tushare 格式
                        if code_raw.startswith('6'):
                            ts_code = f"{code_raw}.SH"
                        elif code_raw.startswith('9'):
                            ts_code = f"{code_raw}.BJ"
                        else:
                            ts_code = f"{code_raw}.SZ"

                        ak_name = str(r.get('股票简称', ''))

                        if ts_code in result:
                            # Tushare 已有记录，补充缺失的名称
                            if ts_code in result and not result[ts_code].get('name', '').strip() and ak_name:
                                result[ts_code]['name'] = ak_name
                            continue

                        # stock_yjyg_em 无'报告日期'列，period 本身就是报告期日期
                        end_date_clean = period

                        # 变动幅度
                        change_val = r.get('业绩变动幅度')

                        result[ts_code] = {
                            'end_date': end_date_clean,
                            'ann_date': ann_date_clean,
                            'disclosure_type': '业绩预告',
                            'name': ak_name,
                            'profit_change_min': _safe_float(change_val),
                            'profit_change_max': _safe_float(change_val),
                        }
            except Exception:
                pass
            time.sleep(0.1)
    except ImportError:
        pass

    return result


def _batch_fetch_actuals(pro, disclosed: dict) -> dict:
    """批量获取实际财务数据（并行版）"""
    import tushare as ts
    result = {}
    result_lock = Lock()
    completed = [0]

    def _fetch_one(item):
        """处理单只股票"""
        code, info = item
        dtype = info.get('disclosure_type', '财报')
        name = info.get('name', '')

        try:
            # 每个线程独立 pro_api（Tushare 非线程安全）
            pro_local = ts.pro_api()

            if dtype == '业绩预告':
                min_val = info.get('profit_change_min')
                max_val = info.get('profit_change_max')
                if min_val is None and max_val is None:
                    return None
                mid = ((min_val or 0) + (max_val or 0)) / 2 if (min_val and max_val) else (min_val or max_val)

                if not name:
                    try:
                        nb = pro_local.stock_basic(ts_code=code, fields='ts_code,name')
                        if nb is not None and not nb.empty:
                            name = nb.iloc[0]['name']
                    except Exception:
                        pass

                close, pe, total_mv = _fetch_price(pro_local, code)
                return (code, {
                    'name': name,
                    'disclosure_type': '业绩预告',
                    'ann_date': info.get('ann_date', ''),
                    'end_date': info.get('end_date', ''),
                    'dt_netprofit_yoy': mid,
                    'or_yoy': None,
                    'roe_dt': None,
                    'close': close,
                    'pe': pe,
                    'total_mv': total_mv,
                    'profit_dedt': None,
                    '_is_forecast_mid': True,
                })

            else:
                fi = pro_local.fina_indicator(
                    ts_code=code,
                    fields='ts_code,ann_date,end_date,profit_dedt,netprofit_yoy,dt_netprofit_yoy,or_yoy,roe_dt',
                    limit=4
                )
                if fi is None or fi.empty:
                    return None
                fi = fi.sort_values('end_date')
                latest = fi.iloc[-1]

                disclosed_end_date = info.get('end_date', '')
                actual_end_date = str(latest.get('end_date', ''))
                if dtype == '业绩快报' and disclosed_end_date and actual_end_date and disclosed_end_date != actual_end_date:
                    return None

                if not name:
                    try:
                        nb = pro_local.stock_basic(ts_code=code, fields='ts_code,name')
                        if nb is not None and not nb.empty:
                            name = nb.iloc[0]['name']
                    except Exception:
                        pass

                close, pe, total_mv = _fetch_price(pro_local, code)
                return (code, {
                    'name': name,
                    'disclosure_type': dtype,
                    'ann_date': info.get('ann_date', ''),
                    'end_date': str(latest.get('end_date', '')),
                    'profit_dedt': _safe_float(latest.get('profit_dedt')),
                    'netprofit_yoy': _safe_float(latest.get('netprofit_yoy')),
                    'dt_netprofit_yoy': _safe_float(latest.get('dt_netprofit_yoy')),
                    'or_yoy': _safe_float(latest.get('or_yoy')),
                    'roe_dt': _safe_float(latest.get('roe_dt')),
                    'close': close,
                    'pe': pe,
                    'total_mv': total_mv,
                })
        except Exception:
            return None

    # 并行执行
    items = list(disclosed.items())
    total = len(items)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_ACTUALS) as executor:
        futures = {executor.submit(_fetch_one, item): item[0] for item in items}
        for future in as_completed(futures):
            completed[0] += 1
            try:
                r = future.result()
                if r:
                    code, data = r
                    with result_lock:
                        result[code] = data
            except Exception:
                pass

            if completed[0] % 20 == 0:
                logger.info(f"    实际数据进度: {completed[0]}/{total}")

    logger.info(f"    实际数据完成: {len(result)}/{total}")
    return result


def _fetch_price(pro, code: str) -> tuple:
    """获取收盘价、PE 和总市值"""
    try:
        daily = pro.daily_basic(ts_code=code, fields='ts_code,close,pe,total_mv', limit=1)
        if daily is not None and not daily.empty:
            return (_safe_float(daily.iloc[0].get('close')),
                    _safe_float(daily.iloc[0].get('pe')),
                    _safe_float(daily.iloc[0].get('total_mv')))
    except Exception:
        pass
    return None, None, None


def _pick_consensus_year(end_date: str) -> str:
    """根据报告期选择一致预期年份"""
    if not end_date:
        return '25E'
    try:
        year = int(end_date[:4])
        return f"{year % 100}E"  # 2025 → 25E, 2026 → 26E
    except (ValueError, IndexError):
        return '25E'


def _batch_fetch_consensus(codes: list, disclosed: dict = None) -> dict:
    """批量获取一致预期（并行版）"""
    import akshare as ak
    result = {}
    result_lock = Lock()
    completed = [0]
    total = min(len(codes), 200)

    def _fetch_one_consensus(code):
        """获取单只股票的一致预期"""
        short = code.split('.')[0]
        exchange = 'SH' if code.endswith('.SH') else 'SZ'
        try:
            df = ak.stock_zh_growth_comparison_em(symbol=f'{exchange}{short}')
            if df is None or df.empty:
                return None
            row = df[df['代码'] == short]
            if row.empty:
                return None
            r = row.iloc[0]

            end_date = ''
            if disclosed and code in disclosed:
                end_date = disclosed[code].get('end_date', '')
            year_suffix = _pick_consensus_year(end_date)

            return (code, {
                f'rev_{year_suffix.lower()}': _safe_float(r.get(f'营业收入增长率-{year_suffix}')),
                f'profit_{year_suffix.lower()}': _safe_float(r.get(f'净利润增长率-{year_suffix}')),
                '_year': year_suffix,
            })
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CONSENSUS) as executor:
        futures = {executor.submit(_fetch_one_consensus, code): code for code in codes[:total]}
        for future in as_completed(futures):
            completed[0] += 1
            try:
                r = future.result()
                if r:
                    code, data = r
                    with result_lock:
                        result[code] = data
            except Exception:
                pass

            if completed[0] % 50 == 0:
                logger.info(f"    预期进度: {completed[0]}/{total}")

    logger.info(f"    预期数据完成: {len(result)}/{total}")
    return result


def _check_beat(code: str, actual: dict, consensus: dict, disclosed: dict) -> dict:
    """检查是否超预期"""
    actual_profit = actual.get('dt_netprofit_yoy')  # 扣非净利润同比

    # 动态读取预期年份字段
    year_suffix = consensus.get('_year', '25E').lower()
    expected_profit = consensus.get(f'profit_{year_suffix}')

    # 没有预期数据 → 仍然返回记录，标记无预期
    has_consensus = not (expected_profit is None or (isinstance(expected_profit, float) and math.isnan(expected_profit)))

    if not has_consensus:
        # 如果连实际数据都没有，跳过
        if actual_profit is None:
            return None
        # 无一致预期，仍然入库展示
        profit_dedt = actual.get('profit_dedt')
        profit_dedt_yi = round(profit_dedt / 1e8, 2) if profit_dedt is not None else None
        return {
            'code': code,
            'name': actual.get('name', ''),
            'disclosure_type': actual.get('disclosure_type', '财报'),
            'report_type': actual.get('disclosure_type', '财报'),
            'ann_date': actual.get('ann_date', ''),
            'period': actual.get('end_date', ''),
            'report_date': actual.get('end_date', ''),
            'actual_profit_yoy': round(actual_profit, 1) if actual_profit is not None else None,
            'expected_profit_yoy': None,
            'profit_diff': None,
            'actual_rev_yoy': round(actual.get('or_yoy'), 1) if actual.get('or_yoy') is not None else None,
            'expected_rev_yoy': None,
            'rev_diff': None,
            'profit_dedt': profit_dedt_yi,
            'roe': actual.get('roe_dt'),
            'close': actual.get('close'),
            'pe': actual.get('pe'),
            'total_mv': actual.get('total_mv'),
            'consensus_available': False,
        }

    # 没有实际数据
    if actual_profit is None:
        return None

    profit_diff = actual_profit - expected_profit
    beat_profit = profit_diff >= BEAT_THRESHOLD

    # 营收对比（可选）
    actual_rev = actual.get('or_yoy')
    expected_rev = consensus.get(f'rev_{year_suffix}')
    rev_diff = None
    if actual_rev is not None and expected_rev is not None:
        if not (isinstance(expected_rev, float) and math.isnan(expected_rev)):
            rev_diff = actual_rev - expected_rev

    if beat_profit:
        # 扣非净利润（元 → 亿元）
        profit_dedt = actual.get('profit_dedt')
        profit_dedt_yi = round(profit_dedt / 1e8, 2) if profit_dedt is not None else None

        # 非经常性损益过滤
        is_non_recurring = False
        netprofit_yoy = actual.get('netprofit_yoy')
        dt_yoy = actual.get('dt_netprofit_yoy')
        if netprofit_yoy is not None and dt_yoy is not None:
            if netprofit_yoy > 50 and dt_yoy < 0:
                is_non_recurring = True
            elif netprofit_yoy > 100 and dt_yoy < netprofit_yoy * 0.3:
                is_non_recurring = True

        return {
            'code': code,
            'name': actual.get('name', ''),
            'disclosure_type': actual.get('disclosure_type', '财报'),
            'report_type': actual.get('disclosure_type', '财报'),  # 兼容 daily_scan.py
            'ann_date': actual.get('ann_date', ''),
            'period': actual.get('end_date', ''),
            'report_date': actual.get('end_date', ''),  # 兼容 daily_scan.py
            'actual_profit_yoy': round(actual_profit, 1),
            'expected_profit_yoy': round(expected_profit, 1),
            'profit_diff': round(profit_diff, 1),
            'actual_rev_yoy': round(actual_rev, 1) if actual_rev is not None else None,
            'expected_rev_yoy': round(expected_rev, 1) if expected_rev is not None else None,
            'rev_diff': round(rev_diff, 1) if rev_diff is not None else None,
            'profit_dedt': profit_dedt_yi,
            'roe': actual.get('roe_dt'),
            'close': actual.get('close'),
            'pe': actual.get('pe'),
            'total_mv': actual.get('total_mv'),
            'consensus_available': True,
            'is_non_recurring': is_non_recurring,
        }

    return None


def _fill_missing_names(pro, result: dict):
    """批量补充缺失的公司名称"""
    missing = [code for code, info in result.items() if not info.get('name', '').strip()]
    if not missing:
        return
    logger.info(f"  补充 {len(missing)} 只股票名称...")
    try:
        nb = pro.stock_basic(fields='ts_code,name')
        if nb is not None and not nb.empty:
            name_map = dict(zip(nb['ts_code'], nb['name']))
            for code in missing:
                if code in name_map:
                    result[code]['name'] = name_map[code]
    except Exception:
        # 降级：逐个查
        for code in missing:
            try:
                nb = pro.stock_basic(ts_code=code, fields='ts_code,name')
                if nb is not None and not nb.empty:
                    result[code]['name'] = nb.iloc[0]['name']
            except Exception:
                pass
            time.sleep(0.05)


def _safe_float(val) -> float:
    if val is None:
        return None
    try:
        f = float(val)
        return f if not math.isnan(f) else None
    except (ValueError, TypeError):
        return None
