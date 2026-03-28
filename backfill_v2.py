#!/usr/bin/env python3
"""
投资系统 2.0 全量回填脚本
从 2/26 开始全量回填数据，走完整 Pipeline → Analyzer → Bitable 全流程
"""
import sys
import json
import time
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
)
logger = logging.getLogger('backfill')

DB_PATH = "data/smart_invest.db"

# ── Step 1: 获取待扫描列表 ───────────────────────────────────────────────────
def step1_get_codes():
    logger.info("=" * 60)
    logger.info("Step 1: DisclosureScanner 获取 2/26 以来全部新披露")
    logger.info("=" * 60)
    from core.disclosure_scanner import DisclosureScanner
    scanner = DisclosureScanner(db_path=DB_PATH)
    new_codes = scanner.get_scan_list(since_hours=720)  # 30天
    logger.info(f"总披露: {len(new_codes)} 家")
    return new_codes

# ── Step 2: Pipeline 批量采集 ────────────────────────────────────────────────
def step2_pipeline(new_codes):
    logger.info("=" * 60)
    logger.info(f"Step 2: Pipeline 批量采集 ({len(new_codes)} 只)")
    logger.info("=" * 60)
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline

    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])

    batch_size = 50
    total_ok = 0
    total_empty = 0
    total_error = 0

    for i in range(0, len(new_codes), batch_size):
        batch = new_codes[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(new_codes) + batch_size - 1) // batch_size
        logger.info(f"批次 {batch_num}/{total_batches}: 处理 {len(batch)} 只...")

        results = pipe.run(batch)
        ok = sum(1 for r in results.values() if r['status'] == 'ok')
        empty = sum(1 for r in results.values() if r['status'] == 'empty')
        error = sum(1 for r in results.values() if r['status'] == 'error')
        total_ok += ok
        total_empty += empty
        total_error += error
        logger.info(f"  批次 {batch_num}: ok={ok}, empty={empty}, error={error}")

        if i + batch_size < len(new_codes):
            time.sleep(1)  # 批次间 sleep 1 秒

    logger.info(f"Pipeline 完成: ok={total_ok}, empty={total_empty}, error={total_error}")
    return total_ok

# ── Step 3: Analyzer 全量分析 ────────────────────────────────────────────────
def step3_analyzer():
    logger.info("=" * 60)
    logger.info("Step 3: Analyzer 全量分析")
    logger.info("=" * 60)
    from core.analyzer import EarningsAnalyzer, EventAnalyzer

    analyzer = EarningsAnalyzer(db_path=DB_PATH)

    # 超预期
    logger.info("扫描超预期...")
    beats = analyzer.scan_beat_expectation()
    logger.info(f"超预期: {len(beats)} 条")
    buy_watch = [b for b in beats if b.get('signal') in ('buy', 'watch')]
    logger.info(f"  buy/watch: {len(buy_watch)} 条")

    # 扣非新高
    logger.info("扫描扣非新高...")
    highs = analyzer.scan_new_high()
    logger.info(f"扣非新高: {len(highs)} 条")
    new_high_true = [h for h in highs if h.get('is_new_high')]
    logger.info(f"  真正新高: {len(new_high_true)} 条")

    # 发现池自动入场
    logger.info("发现池自动入场...")
    pool = analyzer.auto_discover_pool(beats=beats, new_highs=highs)
    logger.info(f"发现池入场: {len(pool)} 只")

    # T+N 跟踪
    tracking = None
    if pool:
        pool_codes = [p['stock_code'] for p in pool]
        logger.info(f"创建 T+N 跟踪: {len(pool_codes)} 只")
        analyzer.create_tn_tracking(pool_codes, 'earnings_beat')
        logger.info("T+N 跟踪创建完成")

    # 事件检测
    logger.info("事件检测...")
    evt = EventAnalyzer(db_path=DB_PATH)
    events = evt.detect_from_pipeline(beats, highs)
    logger.info(f"Pipeline 事件: {len(events)} 条")

    # 更新 T+N 跟踪
    logger.info("更新 T+N 跟踪收益...")
    updated = analyzer.update_tn_tracking()
    logger.info(f"T+N 跟踪更新: {len(updated)} 条")

    # 回测
    logger.info("执行回测...")
    from core.pipeline import run_backtest
    bt = run_backtest(DB_PATH)
    logger.info(f"回测: {bt}")

    return beats, highs, pool, events

# ── Step 4: Bitable 同步 ─────────────────────────────────────────────────────
def step4_bitable(beats, highs, pool, events):
    logger.info("=" * 60)
    logger.info("Step 4: Bitable 同步")
    logger.info("=" * 60)

    app_token = "CvTRbdVyfa9PnMsnzIXcCSNmnnb"
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # ── 4a: 扫描结果同步到旧表（scan） ──
    try:
        from core.bitable_sync import BitableSync, BitableManager
        sync = BitableSync(
            app_token=app_token,
            scan_table_id="tbluSQrjOW0tppTP",
        )
        # 获取行业映射
        industry_map = {}
        try:
            import tushare as ts
            pro = ts.pro_api()
            all_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,industry')
            if all_stocks is not None and not all_stocks.empty:
                industry_map = dict(zip(all_stocks['ts_code'], all_stocks['industry']))
                logger.info(f"行业映射: {len(industry_map)} 只")
        except Exception as e:
            logger.warning(f"行业映射获取失败: {e}")

        pending_records = sync.sync_scan_results(beats, highs, industry_map)
        if pending_records:
            mgr = BitableManager.from_preset('scan')
            new_count = mgr.sync(pending_records)
            logger.info(f"扫描结果: {len(pending_records)} 条生成 → {new_count} 条新增")
        else:
            logger.info("扫描结果: 无新记录")
    except Exception as e:
        logger.error(f"扫描结果同步失败: {e}")

    # ── 4b: 发现池同步 ──
    logger.info("同步发现池到 Bitable...")
    try:
        pool_rows = conn.execute("""
            SELECT dp.stock_code, dp.stock_name, dp.source, dp.score, dp.signal,
                   dp.discovered_at, dp.status, dp.expires_at, dp.updated_at,
                   dp.industry, s.name as stock_name2
            FROM discovery_pool dp
            LEFT JOIN stocks s ON dp.stock_code = s.code
            ORDER BY dp.score DESC
        """).fetchall()

        if pool_rows:
            records = []
            for row in pool_rows:
                fields = {
                    '股票代码': row['stock_code'],
                    '公司名称': row['stock_name'] or row['stock_name2'] or '',
                    '发现来源': row['source'] or '',
                    '评分': row['score'],
                    '信号': row['signal'] or '',
                    '状态': row['status'] or '',
                }
                # 日期字段用毫秒时间戳
                if row['discovered_at']:
                    try:
                        dt = datetime.strptime(row['discovered_at'][:19], '%Y-%m-%d %H:%M:%S')
                        fields['发现时间'] = int(dt.timestamp()) * 1000
                    except ValueError:
                        pass
                if row['expires_at']:
                    try:
                        dt = datetime.strptime(row['expires_at'][:19], '%Y-%m-%d %H:%M:%S')
                        fields['过期时间'] = int(dt.timestamp()) * 1000
                    except ValueError:
                        pass
                if row['updated_at']:
                    try:
                        dt = datetime.strptime(row['updated_at'][:19], '%Y-%m-%d %H:%M:%S')
                        fields['升级时间'] = int(dt.timestamp()) * 1000
                    except ValueError:
                        pass
                if row['industry']:
                    fields['行业'] = row['industry']
                records.append({"fields": fields})

            # 去重后写入
            sync_pool = BitableSync.from_preset('discovery_pool')
            # 清除旧缓存（回填场景）
            cache_path = str(Path(__file__).parent / 'data' / f'bitable_existing_{sync_pool.table_id}.json')
            if Path(cache_path).exists():
                Path(cache_path).unlink()
            filtered = sync_pool.dedup_records(records, existing_keys=set())
            if filtered:
                pending_path = str(Path(__file__).parent / 'data' / 'bitable_pending.json')
                with open(pending_path, 'w', encoding='utf-8') as f:
                    json.dump(filtered, f, ensure_ascii=False, default=str)
                sync_pool.save_existing_keys({sync_pool.extract_key(r) for r in filtered})
                logger.info(f"发现池: {len(filtered)} 条待写入 ({pending_path})")
            else:
                logger.info("发现池: 无新记录")
        else:
            logger.info("发现池: 表为空")
    except Exception as e:
        logger.error(f"发现池同步失败: {e}")

    # ── 4c: 事件同步 ──
    logger.info("同步事件到 Bitable...")
    try:
        evt_rows = conn.execute("""
            SELECT e.stock_code, e.event_type, e.title, e.content, e.source, e.url,
                   e.sentiment, e.severity, e.published_at, e.created_at,
                   s.name as stock_name
            FROM events e
            LEFT JOIN stocks s ON e.stock_code = s.code
            ORDER BY e.created_at DESC
        """).fetchall()

        if evt_rows:
            records = []
            for row in evt_rows:
                fields = {
                    '股票代码': row['stock_code'] or '',
                    '公司名称': row['stock_name'] or '',
                    '事件类型': row['event_type'] or '',
                    '情感': row['sentiment'] or 'neutral',
                    '严重程度': row['severity'] or 'normal',
                    '标题': row['title'] or '',
                    '详情': (row['content'] or '')[:500],
                }
                if row['url']:
                    fields['来源链接'] = row['url']
                if row['published_at']:
                    try:
                        dt = datetime.fromisoformat(row['published_at'].replace('Z', '+00:00'))
                        fields['检测时间'] = int(dt.timestamp()) * 1000
                    except ValueError:
                        try:
                            dt = datetime.strptime(row['published_at'][:19], '%Y-%m-%d %H:%M:%S')
                            fields['检测时间'] = int(dt.timestamp()) * 1000
                        except ValueError:
                            pass
                records.append({"fields": fields})

            sync_events = BitableSync.from_preset('events')
            cache_path = str(Path(__file__).parent / 'data' / f'bitable_existing_{sync_events.table_id}.json')
            if Path(cache_path).exists():
                Path(cache_path).unlink()
            filtered = sync_events.dedup_records(records, existing_keys=set())
            if filtered:
                pending_path = str(Path(__file__).parent / 'data' / 'bitable_pending_events.json')
                with open(pending_path, 'w', encoding='utf-8') as f:
                    json.dump(filtered, f, ensure_ascii=False, default=str)
                sync_events.save_existing_keys({sync_events.extract_key(r) for r in filtered})
                logger.info(f"事件: {len(filtered)} 条待写入 ({pending_path})")
            else:
                logger.info("事件: 无新记录")
        else:
            logger.info("事件: 表为空")
    except Exception as e:
        logger.error(f"事件同步失败: {e}")

    # ── 4d: T+N 跟踪同步 ──
    logger.info("同步 T+N 跟踪到 Bitable...")
    try:
        track_rows = conn.execute("""
            SELECT et.stock_code, et.event_type, et.event_date, et.entry_price,
                   et.return_1d, et.return_5d, et.return_10d, et.return_20d,
                   et.tracking_status, et.last_updated, et.created_at,
                   et.stock_name, s.name as stock_name2
            FROM event_tracking et
            LEFT JOIN stocks s ON et.stock_code = s.code
            ORDER BY et.event_date DESC
        """).fetchall()

        if track_rows:
            records = []
            for row in track_rows:
                fields = {
                    '股票代码': row['stock_code'] or '',
                    '公司名称': row['stock_name'] or row['stock_name2'] or '',
                    '事件类型': row['event_type'] or '',
                    '状态': row['tracking_status'] or 'pending',
                }
                if row['event_date']:
                    try:
                        dt = datetime.strptime(row['event_date'], '%Y-%m-%d')
                        fields['入池日期'] = int(dt.timestamp()) * 1000
                    except ValueError:
                        pass
                if row['entry_price'] is not None:
                    fields['入池价'] = row['entry_price']
                if row['return_1d'] is not None:
                    fields['T+1收益(%)'] = round(row['return_1d'], 2)
                if row['return_5d'] is not None:
                    fields['T+5收益(%)'] = round(row['return_5d'], 2)
                if row['return_10d'] is not None:
                    fields['T+10收益(%)'] = round(row['return_10d'], 2)
                if row['return_20d'] is not None:
                    fields['T+20收益(%)'] = round(row['return_20d'], 2)
                if row['last_updated']:
                    try:
                        dt = datetime.strptime(row['last_updated'][:19], '%Y-%m-%d %H:%M:%S')
                        fields['最新更新'] = int(dt.timestamp()) * 1000
                    except ValueError:
                        pass
                records.append({"fields": fields})

            sync_tracking = BitableSync.from_preset('tracking')
            cache_path = str(Path(__file__).parent / 'data' / f'bitable_existing_{sync_tracking.table_id}.json')
            if Path(cache_path).exists():
                Path(cache_path).unlink()
            filtered = sync_tracking.dedup_records(records, existing_keys=set())
            if filtered:
                pending_path = str(Path(__file__).parent / 'data' / 'bitable_pending_tracking.json')
                with open(pending_path, 'w', encoding='utf-8') as f:
                    json.dump(filtered, f, ensure_ascii=False, default=str)
                sync_tracking.save_existing_keys({sync_tracking.extract_key(r) for r in filtered})
                logger.info(f"T+N 跟踪: {len(filtered)} 条待写入 ({pending_path})")
            else:
                logger.info("T+N 跟踪: 无新记录")
        else:
            logger.info("T+N 跟踪: 表为空")
    except Exception as e:
        logger.error(f"T+N 跟踪同步失败: {e}")

    conn.close()

# ── Step 5: 输出对比报告 ─────────────────────────────────────────────────────
def step5_report():
    logger.info("=" * 60)
    logger.info("Step 5: 输出对比报告")
    logger.info("=" * 60)
    conn = sqlite3.connect(DB_PATH)

    print("\n=== 回填完成报告 ===")
    for t in ['earnings', 'analysis_results', 'discovery_pool', 'events', 'event_tracking', 'backtest']:
        try:
            cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t}: {cnt} 条")
        except Exception as e:
            print(f"  {t}: 查询失败 - {e}")

    print("\n=== analysis_results 按类型 ===")
    rows = conn.execute("SELECT analysis_type, COUNT(*) FROM analysis_results GROUP BY analysis_type").fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]}")

    print("\n=== 超预期 TOP 10 ===")
    rows = conn.execute("""
        SELECT stock_code, score, signal, summary 
        FROM analysis_results 
        WHERE analysis_type='earnings_beat' AND signal IN ('buy','watch')
        ORDER BY score DESC LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}: score={r[1]}, signal={r[2]}")

    print("\n=== 扣非新高 TOP 10 ===")
    rows = conn.execute("""
        SELECT stock_code, score, signal 
        FROM analysis_results 
        WHERE analysis_type='profit_new_high' AND signal='watch'
        ORDER BY score DESC LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}: score={r[1]}, signal={r[2]}")

    print("\n=== 发现池 ===")
    rows = conn.execute("SELECT stock_code, source, score, status FROM discovery_pool ORDER BY score DESC LIMIT 10").fetchall()
    for r in rows:
        print(f"  {r[0]}: source={r[1]}, score={r[2]}, status={r[3]}")

    print("\n=== 事件 ===")
    rows = conn.execute("SELECT stock_code, event_type, title, sentiment FROM events ORDER BY created_at DESC LIMIT 10").fetchall()
    for r in rows:
        title = (r[2] or '')[:40]
        print(f"  {r[0]}: {r[1]} | {title} | {r[3]}")

    print("\n=== T+N 跟踪 TOP 10 ===")
    rows = conn.execute("""
        SELECT stock_code, event_type, event_date, entry_price, return_5d, return_20d, tracking_status
        FROM event_tracking
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]} | {r[2]} | 入池价={r[3]} | T+5={r[4]} | T+20={r[5]} | {r[6]}")

    conn.close()

# ── 主流程 ───────────────────────────────────────────────────────────────────
def main():
    start = time.time()
    logger.info("🚀 投资系统 2.0 全量回填开始")
    logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Step 1
    new_codes = step1_get_codes()
    if not new_codes:
        logger.warning("无新披露股票，退出")
        return

    # Step 2
    ok_count = step2_pipeline(new_codes)

    # Step 3
    beats, highs, pool, events = step3_analyzer()

    # Step 4
    step4_bitable(beats, highs, pool, events)

    # Step 5
    step5_report()

    elapsed = time.time() - start
    logger.info(f"\n✅ 回填完成 | 总耗时 {elapsed:.1f}s ({elapsed/60:.1f}min)")

if __name__ == '__main__':
    main()
