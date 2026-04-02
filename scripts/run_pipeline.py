#!/usr/bin/env python3
"""投资系统 2.2 — Pipeline Runner"""
import os; os.environ['TZ'] = 'Asia/Shanghai'
import sys, json, time, argparse, sqlite3
time.tzset()
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
DB_PATH = PROJECT_ROOT / "data" / "smart_invest.db"

def parse_window(w):
    return int(w[:-1]) if w.endswith('h') else int(w)

def run(args):
    from core.system_logger import SystemLogger
    logger = SystemLogger(db_path=str(DB_PATH))

    results = {}
    t_total = time.time()
    hours = parse_window(args.window)
    max_stocks = args.max_stocks
    print(f'🚀 投资系统 v2.2 — Pipeline')
    print(f'窗口: {hours}h | 实际时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print()

    with logger.run("pipeline", f"全量扫描 window={hours}h max={max_stocks}") as log:

        # Step 0: 批量更新跟踪股行情
        from core.database import init_db
        init_db(str(DB_PATH))

        print(f'📈 Step 0: 批量更新跟踪股行情')
        t0 = time.time()
        conn = sqlite3.connect(str(DB_PATH))

        # 收集所有需要持续跟踪的股票
        track_codes = set()

        # 1) 发现池（active）
        for r in conn.execute("SELECT DISTINCT stock_code FROM discovery_pool WHERE status='active'").fetchall():
            track_codes.add(r[0])

        # 2) T+N 跟踪（tracking/active）
        for r in conn.execute("SELECT DISTINCT stock_code FROM event_tracking WHERE tracking_status IN ('tracking','active')").fetchall():
            track_codes.add(r[0])

        # 3) 持仓（stocks.json）
        config_path = PROJECT_ROOT / "config" / "stocks.json"
        if config_path.exists():
            with open(config_path) as f:
                config = json.load(f)
            for s in config.get('stocks', []):
                if s.get('holding') or s.get('tracking'):
                    track_codes.add(s['code'])

        track_codes = sorted(track_codes)
        conn.close()

        if track_codes:
            from datetime import timedelta
            today_str = datetime.now().strftime('%Y%m%d')

            conn = sqlite3.connect(str(DB_PATH))

            # 分类：已有历史数据（只需今日更新） vs 完全无数据（需完整历史）
            has_history = []
            no_history = []
            for code in track_codes:
                row = conn.execute(
                    "SELECT MAX(trade_date) FROM prices WHERE stock_code=?", (code,)
                ).fetchone()
                if not row[0]:
                    no_history.append(code)   # 完全无数据
                elif row[0] < today_str:
                    has_history.append(code)  # 有历史但今日未更新
                # row[0] >= today_str → 已是最新，跳过
            conn.close()

            updated = 0

            # ── 批量更新：腾讯行情 API（一次请求覆盖全部）──
            if has_history:
                import urllib.request
                import re as _re
                print(f'   腾讯批量更新: {len(has_history)} 只')

                # 转换代码格式: 600660.SH → sh600660
                def _to_tx_code(c):
                    c = c.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')
                    if c.startswith(('6', '9')):
                        return f'sh{c}'
                    return f'sz{c}'

                tx_codes = [_to_tx_code(c) for c in has_history]
                batch_size = 800
                tx_data = {}  # {stock_code: {...}}

                for start in range(0, len(tx_codes), batch_size):
                    batch = tx_codes[start:start + batch_size]
                    url = f"https://qt.gtimg.cn/q={','.join(batch)}"
                    try:
                        req = urllib.request.Request(url)
                        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                        with opener.open(req, timeout=15) as resp:
                            raw = resp.read().decode('gbk', errors='ignore')
                        for line in raw.split(';'):
                            if '~' not in line:
                                continue
                            parts = line.split('~')
                            if len(parts) < 40:
                                continue
                            code_raw = parts[2]  # e.g. 600660
                            # 归一化到 SH/SZ
                            if code_raw.startswith(('6', '9')):
                                sc = f'{code_raw}.SH'
                            else:
                                sc = f'{code_raw}.SZ'
                            try:
                                close = float(parts[3]) if parts[3] else 0
                                open_p = float(parts[5]) if parts[5] else close
                                high = float(parts[33]) if parts[33] else close
                                low = float(parts[34]) if parts[34] else close
                                change_pct = float(parts[32]) if parts[32] else 0
                                volume = float(parts[37]) if parts[37] else 0  # 手
                                turnover = float(parts[38]) if parts[38] else 0  # 亿
                                if close > 0:
                                    tx_data[sc] = {
                                        'close': close, 'open': open_p,
                                        'high': high, 'low': low,
                                        'change_pct': change_pct,
                                        'volume': volume,
                                        'turnover': round(turnover * 1e8, 2),  # 亿→元
                                    }
                            except (ValueError, IndexError):
                                continue
                    except Exception as e:
                        print(f'   ⚠️ 腾讯行情批次失败: {e}')

                # 写入 prices（仅今日记录）
                conn = sqlite3.connect(str(DB_PATH))
                for code in has_history:
                    if code not in tx_data:
                        continue
                    d = tx_data[code]
                    conn.execute("""
                        INSERT OR REPLACE INTO prices
                        (stock_code, trade_date, open_price, high_price, low_price, close_price,
                         volume, turnover, change_pct)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (code, today_str, d['open'], d['high'], d['low'], d['close'],
                          d['volume'], d['turnover'], d['change_pct']))
                    updated += 1
                conn.commit()
                conn.close()
                print(f'   ✅ 腾讯更新: {updated} 只')

            # ── 无历史数据的股票：KlineProvider 补完整历史 ──
            if no_history:
                print(f'   KlineProvider 补历史: {len(no_history)} 只')
                from core.data_provider import KlineProvider
                kline_provider = KlineProvider()
                ok = 0
                for code in no_history:
                    try:
                        klines = kline_provider.fetch(code, limit=120)
                        if klines:
                            conn = sqlite3.connect(str(DB_PATH))
                            for k in klines:
                                conn.execute("""INSERT OR REPLACE INTO prices
                                    (stock_code, trade_date, open_price, high_price, low_price, close_price,
                                     volume, turnover, change_pct)
                                    VALUES (?,?,?,?,?,?,?,?,?)""",
                                    (code, k.trade_date, k.open_price, k.high_price, k.low_price, k.close_price,
                                     k.volume, k.amount, k.change_pct))
                            conn.commit()
                            conn.close()
                            ok += 1
                    except Exception as e:
                        print(f'   ⚠️ {code}: {e}')
                print(f'   ✅ 历史补全: {ok}/{len(no_history)} 只')
                updated += ok

            step0_ms = int((time.time() - t0) * 1000)
            results['step0'] = {'track': len(track_codes), 'updated': updated, 'ms': step0_ms}
            print(f'   📊 Step 0 完成: {updated}/{len(track_codes)} 更新 ({step0_ms}ms)')

        # Step 1: DisclosureScanner
        print(f'📡 Step 1: DisclosureScanner (窗口 {hours}h)')
        t0 = time.time()
        from core.database import init_db
        init_db(str(DB_PATH))
        from core.disclosure_scanner import DisclosureScanner
        scanner = DisclosureScanner(db_path=str(DB_PATH))
        new_codes = scanner.get_scan_list(since_hours=hours)
        scan_ms = int((time.time() - t0) * 1000)
        results['scanner'] = {'codes': len(new_codes), 'ms': scan_ms}
        print(f'   ✅ {len(new_codes)} 家新披露 ({scan_ms}ms)')

        if not new_codes:
            print('   无新披露，跳过采集步骤')
            test_codes = []
        else:
            test_codes = new_codes[:max_stocks]
            print(f'   测试范围: {len(test_codes)} 只')

        # 分析结果默认值（无新披露时也保证变量存在）
        beats, highs, auto_pool, tn, events, fetched = [], [], [], [], [], 0

        if test_codes:
            # Step 2: Pipeline 采集
            print(f'\n📊 Step 2: Pipeline 采集')
            t0 = time.time()
            from core.pipeline import Pipeline
            from core.data_provider import FinancialProvider
            pipeline = Pipeline(db_path=str(DB_PATH), providers=[FinancialProvider()])
            run_result = pipeline.run(test_codes)
            pipe_ms = int((time.time() - t0) * 1000)
            fetched = run_result.get('stocks_fetched', 0) if isinstance(run_result, dict) else 0
            results['pipeline'] = {'collected': fetched, 'ms': pipe_ms}
            print(f'   ✅ {fetched}/{len(test_codes)} 采集成功 ({pipe_ms}ms)')

            # Step 3: Analyzer
            print(f'\n🔍 Step 3: Analyzer')
            t0 = time.time()
            from core.analyzer import EarningsAnalyzer
            ea = EarningsAnalyzer(db_path=str(DB_PATH))

            beats = ea.scan_beat_expectation(test_codes)
            print(f'   ├─ 超预期: {len(beats)} 条')
            for b in beats[:3]:
                print(f'      {b.get("stock_code","?")}: {b.get("signal","?")} ({b.get("beat_strength","?")})')

            highs = ea.scan_new_high(test_codes)
            print(f'   ├─ 扣非新高: {len(highs)} 条')

            auto_pool = ea.auto_discover_pool(beats, highs)
            print(f'   ├─ 发现池: {len(auto_pool)} 入池')

            if auto_pool:
                try:
                    for entry in auto_pool:
                        code = entry.get("stock_code")
                        source = entry.get("source")
                        if code and source:
                            ea.create_tn_tracking([code], source)
                    print(f'   ├─ T+N 创建: {len(auto_pool)} 条')
                except Exception as e:
                    print(f'   ├─ T+N 创建: 跳过 ({e})')

            try:
                from core.analyzer import PullbackAnalyzer
                pa = PullbackAnalyzer(db_path=str(DB_PATH))
                pullback_results = pa.scan(test_codes)
                print(f'   ├─ 回调买入: {len(pullback_results)} 条')
            except Exception as e:
                print(f'   ├─ 回调买入: 跳过 ({e})')
                pullback_results = []

            try:
                from core.analyzer import EventAnalyzer
                eva = EventAnalyzer(db_path=str(DB_PATH))
                events = eva.detect_from_pipeline(beats=beats, new_highs=highs)
                print(f'   └─ 事件(pipeline): {len(events)} 条')
            except Exception as e:
                print(f'   └─ 事件(pipeline): 跳过 ({e})')
                events = []


            # Step 3b: 新闻事件采集
            print(f'\n📰 Step 3b: 新闻事件采集')
            t0_news = time.time()
            try:
                conn = sqlite3.connect(str(DB_PATH))
                news_codes = set(test_codes)

                config_path = PROJECT_ROOT / "config" / "stocks.json"
                if config_path.exists():
                    with open(config_path) as f:
                        config = json.load(f)
                    for s in config.get('stocks', []):
                        if s.get('holding'):
                            news_codes.add(s['code'])

                pool_rows = conn.execute(
                    "SELECT DISTINCT stock_code FROM discovery_pool WHERE status = 'active'"
                ).fetchall()
                for r in pool_rows:
                    news_codes.add(r[0])
                conn.close()

                news_codes = list(news_codes)[:30]
                news_events = eva.detect_from_codes(news_codes, limit=5)
                news_ms = int((time.time() - t0_news) * 1000)
                results['news_events'] = {'codes': len(news_codes), 'events': len(news_events), 'ms': news_ms}
                print(f'   ✅ {len(news_codes)} 只股票扫描，{len(news_events)} 条新闻事件 ({news_ms}ms)')
            except Exception as e:
                print(f'   ⚠️ 新闻事件采集失败: {e}')
                results['news_events'] = {'error': str(e)}

            ana_ms = int((time.time() - t0) * 1000)
            results['analyzer'] = {
                'beats': len(beats), 'highs': len(highs),
                'pool': len(auto_pool), 'tn': len(tn), 'events': len(events),
                'pullback': len(pullback_results), 'ms': ana_ms,
            }

            # Step 3c: 新发现股K线（Step 0 未覆盖的新增股票）
            print(f'\n📈 Step 3c: 新发现股K线采集')
            new_from_pipeline = set()
            for b in beats:
                c = b.get('code')
                if c: new_from_pipeline.add(c)
            for h in highs:
                c = h.get('stock_code')
                if c: new_from_pipeline.add(c)
            # 排除已更新的
            if new_from_pipeline and results.get('step0', {}).get('track', 0) > 0:
                conn = sqlite3.connect(str(DB_PATH))
                existing = set(r[0] for r in conn.execute("SELECT DISTINCT stock_code FROM prices").fetchall())
                conn.close()
                new_codes_to_fetch = [c for c in new_from_pipeline if c not in existing]
            else:
                new_codes_to_fetch = []

            if new_codes_to_fetch:
                print(f'   需采集: {len(new_codes_to_fetch)} 只')
                from core.data_provider import KlineProvider
                kline_provider = KlineProvider()
                kline_ok = 0
                for code in new_codes_to_fetch:
                    try:
                        klines = kline_provider.fetch(code, limit=120)
                        if klines:
                            conn = sqlite3.connect(str(DB_PATH))
                            for k in klines:
                                conn.execute("""INSERT OR REPLACE INTO prices
                                    (stock_code, trade_date, open_price, high_price, low_price, close_price,
                                     volume, turnover, change_pct)
                                    VALUES (?,?,?,?,?,?,?,?,?)""",
                                    (code, k.trade_date, k.open_price, k.high_price, k.low_price, k.close_price,
                                     k.volume, k.amount, k.change_pct))
                            conn.commit()
                            conn.close()
                            kline_ok += 1
                    except Exception as e:
                        print(f'   ⚠️ {code}: {e}')
                print(f'   ✅ K线采集完成: {kline_ok}/{len(new_codes_to_fetch)}')
            else:
                print(f'   无新增股票需要采集')

            beats, highs, auto_pool, fetched = [], [], [], 0

        # T+N 跟踪更新（无条件执行）
        try:
            from core.analyzer import EarningsAnalyzer
            ea_tn = EarningsAnalyzer(db_path=str(DB_PATH))
            tn = ea_tn.update_tn_tracking()
            print(f'\n📈 T+N 跟踪更新: {len(tn)} 条')
        except Exception as e:
            print(f'\n⚠️ T+N 更新失败: {e}')
            tn = []

        # 一致预期补充（跟踪池中无预期数据的股票）
        print(f'\n📊 一致预期补充')
        t0 = time.time()
        try:
            conn_tmp = sqlite3.connect(str(DB_PATH))
            need_consensus = [r[0] for r in conn_tmp.execute("""
                SELECT DISTINCT stock_code FROM event_tracking
                WHERE tracking_status IN ('tracking','active')
                AND (expected_yoy IS NULL OR expected_yoy = 0)
            """).fetchall()]
            conn_tmp.close()
            if need_consensus:
                from core.pipeline import fetch_and_apply_consensus
                cr = fetch_and_apply_consensus(str(DB_PATH), need_consensus)
                consensus_ms = int((time.time() - t0) * 1000)
                print(f'   ✅ {len(need_consensus)} 只待补, fetched={cr.get("fetched",0)} updated={cr.get("updated",0)} ({consensus_ms}ms)')
                # 同步更新 event_tracking 的 expected_yoy
                conn_tmp = sqlite3.connect(str(DB_PATH))
                conn_tmp.execute("""
                    UPDATE event_tracking SET expected_yoy = (
                        SELECT c.net_profit_yoy FROM consensus c
                        WHERE c.stock_code = event_tracking.stock_code
                        AND c.year = SUBSTR(event_tracking.report_period, 3, 2) || 'E'
                        AND c.net_profit_yoy IS NOT NULL
                        ORDER BY c.fetched_at DESC LIMIT 1
                    )
                    WHERE tracking_status IN ('tracking','active')
                    AND (expected_yoy IS NULL OR expected_yoy = 0)
                """)
                conn_tmp.commit()
                conn_tmp.close()
            else:
                print(f'   ✅ 无需补充')
        except Exception as e:
            print(f'   ⚠️ 一致预期补充失败: {e}')

        # Step 3d: 回调买入评分
        print('\n📊 Step 3d: 回调买入评分')
        from core.analyzer import PullbackAnalyzer
        pa = PullbackAnalyzer(db_path=str(DB_PATH))
        pullback_results = pa.scan()
        pb_signals = [r for r in pullback_results if r.get('score', 0) > 0]
        print(f'   扫描: {len(pullback_results)} 只 | 信号: {len(pb_signals)} 只')
        for s in pb_signals[:5]:
            print(f"   → {s['stock_code']} {s.get('stock_name','')} 分={s['score']} {s['grade']}")
        results['pullback'] = len(pb_signals)

        # Step 4: 数据质量
        print(f'\n🔍 Step 4: 数据质量验证')
        conn = sqlite3.connect(str(DB_PATH))
        name_ok = conn.execute("SELECT COUNT(*) FROM discovery_pool WHERE stock_name IS NOT NULL AND stock_name != ''").fetchone()[0]
        name_total = conn.execute("SELECT COUNT(*) FROM discovery_pool").fetchone()[0]
        conn.close()
        print(f'   发现池名称: {name_ok}/{name_total}')
        results['quality'] = {'pool_names': name_ok, 'pool_total': name_total}

        total_ms = int((time.time() - t_total) * 1000)
        results['total_ms'] = total_ms
        summary = f"扫描 {len(new_codes)} | 采集 {fetched}/{len(test_codes)} | beats={len(beats)} pool={len(auto_pool)} | {total_ms}ms"
        log.result(summary)
        print(f'\n{"="*50}')
        print(f'✅ 完成 | {summary}')
        print(f'{"="*50}')

        # ── Step 5: 飞书推送 ──────────────────────────────────────────────────
        if not args.quiet:
            try:
                _push_feishu_notifications(beats, highs, pb_signals)
            except Exception as e:
                print(f'⚠️ 飞书推送异常（不影响Pipeline）: {e}')

    return results


def _push_feishu_notifications(beats, highs, pullback_signals):
    """
    Pipeline 完成后推送飞书卡片通知。

    触发条件：
    - 有超预期信号（含一致预期的 beats）
    - 有扣非净利润新高
    - 有回调买入信号（S/A 级）
    """
    import sys, os, sqlite3
    sys.path.insert(0, str(PROJECT_ROOT))
    from notifiers.feishu_pusher import FeishuPusher

    # ── 筛选有意义的信号 ──
    true_beats = [b for b in beats if b.get('signal') in ('buy', 'watch') and b.get('actual_profit_yoy') is not None]
    real_highs = [h for h in highs if h.get('is_new_high')]
    top_pullback = [p for p in pullback_signals if p.get('grade') in ('S', 'A')]

    total_signals = len(true_beats) + len(real_highs) + len(top_pullback)
    if total_signals == 0:
        print(f'\n📢 飞书推送：无新信号，跳过')
        return

    print(f'\n📢 飞书推送：检测到 {total_signals} 个信号，准备推送...')

    # ── 构建行业映射 ──
    industry_map = {}
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute("SELECT code, industry FROM stocks WHERE industry IS NOT NULL").fetchall()
        for r in rows:
            industry_map[r[0]] = r[1]
        conn.close()
    except Exception:
        pass

    # ── 格式转换：analyzer → card_generator ──
    card_beats = []
    for b in true_beats:
        code = b.get('stock_code', '')
        ann = b.get('report_period', '')
        card_beats.append({
            'code': code,
            'name': b.get('stock_name', ''),
            'consensus_available': True,
            'actual_profit_yoy': b.get('actual_profit_yoy'),
            'expected_profit_yoy': None,  # 从 beat_diff 反推
            'actual_rev_yoy': None,
            'expected_rev_yoy': None,
            'is_non_recurring': b.get('is_forecast', 0) == 1,
            'report_type': _infer_report_type(ann),
            'ann_date': ann.replace('-', '') if '-' in ann else ann,
        })
        # 反推 expected_profit_yoy
        diff = b.get('beat_diff_pct')
        actual = b.get('actual_profit_yoy')
        if diff is not None and actual is not None:
            card_beats[-1]['expected_profit_yoy'] = round(actual - diff, 1)

    card_highs = []
    for h in real_highs:
        code = h.get('stock_code', '')
        profit = h.get('quarterly_net_profit', 0)
        # 从 discovery_pool 获取名称
        name = ''
        pe = None
        try:
            conn2 = sqlite3.connect(str(DB_PATH))
            r = conn2.execute("SELECT stock_name FROM discovery_pool WHERE stock_code = ? LIMIT 1", (code,)).fetchone()
            if r:
                name = r[0]
            conn2.close()
        except Exception:
            pass
        ann = h.get('report_period', '')
        card_highs.append({
            'code': code,
            'name': name,
            'quarterly_profit': round(profit / 1e8, 2) if profit else 0,  # 元 → 亿
            'growth_vs_high': h.get('growth_pct', 0),
            'pe': pe,
            'report_type': _infer_report_type(ann),
            'ann_date': ann.replace('-', '') if '-' in ann else ann,
        })

    card_pullback = []
    for p in top_pullback:
        card_pullback.append({
            'code': p.get('stock_code', ''),
            'name': p.get('stock_name', ''),
            'grade': p.get('grade', 'C'),
            'score': p.get('score', 0),
            'close': p.get('close'),
            'reason': p.get('reason', '')[:30],
        })

    # ── 发送 ──
    pusher = FeishuPusher()
    success = pusher.push_daily_scan_card(card_beats, card_highs, industry_map, card_pullback)
    if success:
        print(f'   ✅ 飞书卡片推送成功 (超预期{len(card_beats)} / 新高{len(card_highs)} / 回调{len(card_pullback)})')
    else:
        print(f'   ❌ 飞书卡片推送失败')


def _infer_report_type(report_date: str) -> str:
    """从报告期推断类型"""
    if not report_date:
        return '财报'
    try:
        md = report_date.replace('-', '')[4:8] if len(report_date.replace('-', '')) >= 8 else ''
        if md in ('0331', '0630', '0930', '1231'):
            return {'0331': 'Q1', '0630': 'Q2', '0930': 'Q3', '1231': '年报'}[md]
    except Exception:
        pass
    return '财报'

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--window', default='12h')
    p.add_argument('--max-stocks', type=int, default=10)
    p.add_argument('--quiet', action='store_true')
    args = p.parse_args()
    results = run(args)
    if not args.quiet and results:
        print(json.dumps(results, indent=2, ensure_ascii=False))
