#!/usr/bin/env python3
"""投资系统 2.2 — Pipeline Runner"""
import sys, json, time, argparse, sqlite3
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
DB_PATH = PROJECT_ROOT / "data" / "smart_invest.db"

def parse_window(w):
    return int(w[:-1]) if w.endswith('h') else int(w)

def run(args):
    results = {}
    t_total = time.time()
    hours = parse_window(args.window)
    print(f'🚀 投资系统 v2.2 — Pipeline')
    print(f'窗口: {hours}h | 实际时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print()

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
        print('   无新披露，结束')
        return results

    test_codes = new_codes[:args.max_stocks]
    print(f'   测试范围: {len(test_codes)} 只')

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

    try:
        tn = ea.update_tn_tracking()
        print(f'   ├─ T+N 更新: {len(tn)} 条')
    except Exception as e:
        print(f'   ├─ T+N 更新: 跳过 ({e})')
        tn = []

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

    # Step 3b: 新闻事件采集（持仓+发现池+有信号的股票）
    print(f'\n📰 Step 3b: 新闻事件采集')
    t0_news = time.time()
    try:
        # 收集需要扫描新闻的股票代码
        conn = sqlite3.connect(str(DB_PATH))
        news_codes = set(test_codes)  # 本次扫描的股票

        # 加入持仓股
        config_path = PROJECT_ROOT / "config" / "stocks.json"
        if config_path.exists():
            with open(config_path) as f:
                config = json.load(f)
            for s in config.get('stocks', []):
                if s.get('holding'):
                    news_codes.add(s['code'])

        # 加入发现池（最近7天的）
        pool_rows = conn.execute(
            "SELECT DISTINCT stock_code FROM discovery_pool WHERE status = 'active'"
        ).fetchall()
        for r in pool_rows:
            news_codes.add(r[0])

        conn.close()

        news_codes = list(news_codes)[:30]  # 限制30只
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
        'pullback': len(pullback_results),
        'ms': ana_ms,
    }

    # Step 4: 数据质量验证
    print(f'\n🔍 Step 4: 数据质量验证')
    conn = sqlite3.connect(str(DB_PATH))
    name_ok = conn.execute("SELECT COUNT(*) FROM discovery_pool WHERE stock_name IS NOT NULL AND stock_name != ''").fetchone()[0]
    name_total = conn.execute("SELECT COUNT(*) FROM discovery_pool").fetchone()[0]
    q_ok = conn.execute("SELECT COUNT(*) FROM earnings WHERE quarterly_net_profit IS NOT NULL").fetchone()[0]
    q_total = conn.execute("SELECT COUNT(*) FROM earnings").fetchone()[0]
    conn.close()
    print(f'   ├─ 发现池名称: {name_ok}/{name_total}')
    print(f'   └─ quarterly_net_profit: {q_ok}/{q_total}')
    results['quality'] = {'pool_names': name_ok, 'pool_total': name_total}

    total_ms = int((time.time() - t_total) * 1000)
    results['total_ms'] = total_ms
    print(f'\n{"="*50}')
    print(f'✅ 完成 | {total_ms}ms | 扫描 {len(new_codes)} | 采集 {fetched}/{len(test_codes)}')
    print(f'{"="*50}')
    return results

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--window', default='12h')
    p.add_argument('--max-stocks', type=int, default=10)
    p.add_argument('--quiet', action='store_true')
    args = p.parse_args()
    results = run(args)
    if not args.quiet and results:
        print(json.dumps(results, indent=2, ensure_ascii=False))
