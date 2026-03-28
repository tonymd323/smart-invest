#!/usr/bin/env python3
"""
智能投资系统 v1.5 — 每日扫描主入口
====================================
任务A：业绩超预期（三层数据源）
任务B：扣非净利润新高（仅财报）
任务C：数据入库（SQLite）
任务E：导出 Bitable 待写入 JSON
任务F：生成飞书卡片 JSON 输出到 stdout（由 cron agent 读取并推送）

用法：
  python3 daily_scan.py           # 正常扫描 + 输出卡片 JSON
  python3 daily_scan.py --quiet   # 仅扫描入库，不输出卡片
"""

import sys
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from core.database import get_connection, init_db
from scanners.earnings_scanner import scan_earnings_beat
from scanners.new_high_scanner import scan_quarterly_new_high
from scanners.pullback_scanner import scan_pullback_buy
from notifiers.card_generator import CardGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
)
logger = logging.getLogger('daily_scan_v2')


def main():
    quiet_mode = '--quiet' in sys.argv

    init_db()
    logger.info("="*60)
    logger.info("🚀 智能投资系统 v1.5 — 每日扫描")
    logger.info("="*60)
    start = time.time()

    # 任务A：业绩超预期
    logger.info("\n" + "="*40)
    beats = scan_earnings_beat()

    # 任务B：扣非净利润新高
    logger.info("\n" + "="*40)
    new_highs = scan_quarterly_new_high()

    # 任务B.5：批量获取行业信息（用于板块分布统计）
    industry_map = fetch_industry_map(beats, new_highs)

    # 任务C：回调买入信号扫描（烧香拜佛+备买融合）
    logger.info("\n" + "="*40)
    logger.info("📐 任务C：回调买入信号扫描")
    pullback_signals = scan_pullback_signals(beats, new_highs)
    logger.info(f"  回调信号: {len(pullback_signals)} 只")

    # 任务D：入库
    save_results(beats, new_highs)

    # 任务D：事件跟踪入库
    track_events(beats, new_highs)

    # 任务E：Bitable 同步（统一管理器：生成 → 去重 → 导出）
    try:
        from core.bitable_sync import BitableSync, BitableManager
        sync = BitableSync(
            app_token="CvTRbdVyfa9PnMsnzIXcCSNmnnb",
            scan_table_id="tbluSQrjOW0tppTP",
        )
        pending_records = sync.sync_scan_results(beats, new_highs, industry_map)
        if pending_records:
            mgr = BitableManager.from_preset('scan')
            new_count = mgr.sync(pending_records)
            logger.info(f"  📝 Bitable 同步: {len(pending_records)} 条生成 → {new_count} 条新增")
        else:
            logger.info("  📝 Bitable: 无新记录")
    except Exception as e:
        logger.warning(f"  Bitable 同步失败: {e}")

    # 任务F：生成卡片 JSON 输出到 stdout
    has_results = beats or new_highs or pullback_signals
    if not quiet_mode and has_results:
        card_gen = CardGenerator()
        card = card_gen.generate_daily_scan_card(beats, new_highs, industry_map,
                                                  pullback_signals=pullback_signals)
        # 分隔符标记，方便 agent 解析
        print("\n===FEISHU_CARD_JSON_START===")
        print(json.dumps(card, ensure_ascii=False))
        print("===FEISHU_CARD_JSON_END===")
    elif not quiet_mode:
        print("\n===NO_RESULTS===")

    elapsed = time.time() - start
    logger.info(f"\n✅ 扫描完成 | 耗时 {elapsed:.1f}s | 超预期={len(beats)} | 扣非新高={len(new_highs)} | 回调信号={len(pullback_signals)}")


def fetch_industry_map(beats: list, new_highs: list) -> dict:
    """批量获取行业信息，返回 {ts_code: industry}"""
    try:
        import tushare as ts
        pro = ts.pro_api()
        all_codes = set()
        for b in beats:
            all_codes.add(b['code'])
        for h in new_highs:
            all_codes.add(h['code'])

        if not all_codes:
            return {}

        # 一次性拉全量（~5000条），本地匹配，避免逐只查询
        all_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,industry')
        if all_stocks is None or all_stocks.empty:
            return {}
        industry_map = dict(zip(all_stocks['ts_code'], all_stocks['industry']))
        logger.info(f"  🏭 行业映射: 已加载 {len(industry_map)} 只")
        return industry_map
    except Exception as e:
        logger.warning(f"  行业映射获取失败: {e}")
        return {}


def scan_pullback_signals(beats: list, new_highs: list) -> list:
    """
    回调买入信号扫描。

    扫描范围：
      1. 超预期股票（有基本面催化）
      2. 扣非新高股票（有业绩支撑）
      3. 持仓/备选股（日常关注）

    扫描后入库，返回信号列表。
    """
    import tushare as ts
    from scanners.pullback_scanner import scan_pullback_buy

    pro = ts.pro_api()

    # 构建扫描池
    pool = []
    beat_codes = set()

    for b in beats:
        code = b['code']
        pool.append({'code': code, 'name': b.get('name', '')})
        beat_codes.add(code)

    for h in new_highs:
        code = h['code']
        if code not in beat_codes:
            pool.append({'code': code, 'name': h.get('name', '')})

    # 加入持仓/备选股（从 stocks.json 读取）
    try:
        from core.stock_config import get_all_codes
        existing = {s['code'] for s in pool}
        for wc in get_all_codes():
            if wc not in existing:
                pool.append({'code': wc, 'name': ''})
    except Exception:
        pass

    if not pool:
        logger.info("  扫描池为空，跳过")
        return []

    logger.info(f"  扫描池: {len(pool)} 只（超预期={len(beats)} + 新高={len(new_highs)} + 持仓）")

    # 执行扫描
    signals = scan_pullback_buy(pool, beat_codes=beat_codes, min_score=40)

    # 入库
    if signals:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        today = datetime.now().strftime('%Y-%m-%d')
        with get_connection() as conn:
            conn.execute("""
                DELETE FROM analysis_results
                WHERE analysis_type = 'pullback_buy_daily'
                AND created_at LIKE ?
            """, (today + '%',))

            for s in signals:
                detail = json.dumps({
                    'name': s.get('name', ''),
                    'score': s.get('score'),
                    'grade': s.get('grade'),
                    'reason': s.get('reason', ''),
                    'close': s.get('close'),
                    'path_a': s.get('path_a', False),
                    'path_b': s.get('path_b', False),
                    'trend': s.get('trend', {}),
                    'volume': s.get('volume', {}),
                    'support': {'count': s.get('support', {}).get('count', 0)},
                    'momentum': {'passed': s.get('momentum', {}).get('passed', False)},
                }, ensure_ascii=False, default=str)

                conn.execute("""
                    INSERT INTO analysis_results
                    (stock_code, analysis_type, score, signal, summary, detail, created_at)
                    VALUES (?, 'pullback_buy_daily', ?, ?, ?, ?, ?)
                """, (s['code'], s['score'], s['grade'], s.get('reason', ''), detail, now))

            conn.commit()
        logger.info(f"  💾 已入库 {len(signals)} 条回调信号")

    return signals


def save_results(beats: list, new_highs: list):
    """保存结果到数据库"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    today = datetime.now().strftime('%Y-%m-%d')
    saved = 0

    with get_connection() as conn:
        # 清除当日旧数据（同一天重复运行时先清再写，避免重复）
        conn.execute("""
            DELETE FROM analysis_results
            WHERE analysis_type IN ('earnings_beat_daily', 'quarterly_profit_new_high_daily')
            AND created_at LIKE ?
        """, (today + '%',))

        # 清除 7 天前的历史数据
        cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        conn.execute("""
            DELETE FROM analysis_results
            WHERE analysis_type IN ('earnings_beat_daily', 'quarterly_profit_new_high_daily')
            AND created_at < ?
        """, (cutoff,))

        # 保存超预期
        for b in beats:
            code = b['code']
            conn.execute("INSERT OR IGNORE INTO stocks (code, name) VALUES (?, ?)", (code, b.get('name', '')))
            has_consensus = b.get('consensus_available', True)
            detail = json.dumps({
                'name': b.get('name', ''),
                'disclosure_type': b.get('disclosure_type', ''),
                'actual_profit_yoy': b.get('actual_profit_yoy'),
                'expected_profit_yoy': b.get('expected_profit_yoy'),
                'actual_rev_yoy': b.get('actual_rev_yoy'),
                'expected_rev_yoy': b.get('expected_rev_yoy'),
                'profit_diff': b.get('profit_diff'),
                'profit_dedt': b.get('profit_dedt'),
                'report_date': b.get('period', ''),
                'end_date': b.get('period', ''),
                'ann_date': b.get('ann_date', ''),
                'report_type': b.get('disclosure_type', '财报'),
                'consensus_available': has_consensus,
            }, ensure_ascii=False, default=str)

            conn.execute("""
                INSERT INTO analysis_results
                (stock_code, analysis_type, score, signal, summary, detail, created_at)
                VALUES (?, 'earnings_beat_daily', ?, ?, ?, ?, ?)
            """, (code, 80 if has_consensus else 60,
                  '超预期' if has_consensus else '首次覆盖',
                  detail, detail, now))
            saved += 1

        # 保存扣非新高
        for h in new_highs:
            code = h['code']
            conn.execute("INSERT OR IGNORE INTO stocks (code, name) VALUES (?, ?)", (code, h.get('name', '')))
            detail = json.dumps({
                'name': h.get('name', ''),
                'quarterly_profit': h.get('quarterly_profit'),
                'prev_high': h.get('prev_high'),
                'growth_vs_high': h.get('growth_vs_high'),
                'report_date': h.get('report_date', ''),
                'ann_date': h.get('report_date', ''),
                'report_type': '财报',
                'close': h.get('close'),
                'pe': h.get('pe'),
            }, ensure_ascii=False, default=str)

            conn.execute("""
                INSERT INTO analysis_results
                (stock_code, analysis_type, score, signal, summary, detail, created_at)
                VALUES (?, 'quarterly_profit_new_high_daily', 85, '历史新高', ?, ?, ?)
            """, (code, detail, detail, now))
            saved += 1

        conn.commit()

    logger.info(f"  💾 入库: {saved} 条")


def track_events(beats: list, new_highs: list):
    """将新发现的超预期事件写入跟踪表，并更新已有事件的 T+N 表现"""
    event_date = datetime.now().strftime('%Y-%m-%d')
    inserted = 0

    with get_connection() as conn:
        # 1. 插入新事件
        for b in beats:
            if not b.get('consensus_available', True):
                continue  # 首次覆盖不跟踪
            code = b['code']
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO event_tracking
                    (stock_code, stock_name, event_type, event_date, report_period,
                     actual_yoy, expected_yoy, profit_diff, is_non_recurring,
                     entry_price, entry_pe, tracking_status)
                    VALUES (?, ?, 'earnings_beat', ?, ?, ?, ?, ?, ?, ?, ?, 'tracking')
                """, (
                    code, b.get('name', ''), event_date,
                    b.get('period', ''),
                    b.get('actual_profit_yoy'), b.get('expected_profit_yoy'),
                    b.get('profit_diff'),
                    1 if b.get('is_non_recurring') else 0,
                    b.get('close'), b.get('pe'),
                ))
                inserted += 1
            except Exception:
                pass

        for h in new_highs:
            code = h['code']
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO event_tracking
                    (stock_code, stock_name, event_type, event_date, report_period,
                     entry_price, entry_pe, tracking_status)
                    VALUES (?, ?, 'profit_new_high', ?, ?, ?, ?, 'tracking')
                """, (
                    code, h.get('name', ''), event_date,
                    h.get('report_date', ''), h.get('close'), h.get('pe'),
                ))
                inserted += 1
            except Exception:
                pass

        # 2. 更新已有事件的 T+N 表现（仅更新 pending/tracking 状态的）
        try:
            import tushare as ts
            pro = ts.pro_api()

            pending = conn.execute("""
                SELECT id, stock_code, event_date, entry_price
                FROM event_tracking
                WHERE tracking_status IN ('pending', 'tracking')
                AND entry_price IS NOT NULL
                ORDER BY event_date DESC
                LIMIT 200
            """).fetchall()

            for row in pending:
                tid, code, ev_date, entry_price = row
                if not entry_price or entry_price <= 0:
                    continue

                try:
                    # 获取事件日后的行情
                    daily = pro.daily(
                        ts_code=code,
                        start_date=ev_date.replace('-', ''),
                        fields='ts_code,trade_date,close',
                        limit=25
                    )
                    if daily is None or daily.empty:
                        continue

                    daily = daily.sort_values('trade_date')
                    closes = {r['trade_date']: r['close'] for _, r in daily.iterrows()}
                    trade_dates = sorted(closes.keys())

                    # 计算 T+N 收益率
                    updates = {}
                    for n in [1, 5, 10, 20]:
                        if n < len(trade_dates):
                            close_n = closes[trade_dates[n]]
                            ret = (close_n / entry_price - 1) * 100
                            updates[f'return_{n}d'] = round(ret, 2)

                    if updates:
                        set_clause = ', '.join(f'{k} = ?' for k in updates.keys())
                        # 判断是否完成跟踪
                        status = 'completed' if 'return_20d' in updates else 'tracking'
                        conn.execute(f"""
                            UPDATE event_tracking
                            SET {set_clause}, tracking_status = ?, last_updated = datetime('now','localtime')
                            WHERE id = ?
                        """, (*updates.values(), status, tid))

                except Exception:
                    pass

        except Exception as e:
            logger.warning(f"  T+N 跟踪更新失败: {e}")

        conn.commit()

    if inserted:
        logger.info(f"  📊 事件跟踪: 新增 {inserted} 个事件")


if __name__ == '__main__':
    main()
