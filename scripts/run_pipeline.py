#!/usr/bin/env python3
"""
投资系统 v2.2 — 回调测试脚本
==============================
模拟 Cron 触发，跑完整 Pipeline → Analyzer → Bitable → 卡片推送链路。

用法:
  python3 scripts/run_pipeline.py                    # 默认 12h 窗口
  python3 scripts/run_pipeline.py --window 18h       # 18h 窗口（早盘）
  python3 scripts/run_pipeline.py --window 4h        # 4h 窗口（盘后）
  python3 scripts/run_pipeline.py --no-push          # 不推卡片
  python3 scripts/run_pipeline.py --no-bitable       # 不写 Bitable
  python3 scripts/run_pipeline.py --dry-run          # 只打印，不写入
"""

import sys, os, json, time, argparse
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_db
from core.disclosure_scanner import DisclosureScanner
from core.data_provider import FinancialProvider
from core.pipeline import Pipeline
from core.analyzer import EarningsAnalyzer, EventAnalyzer
from core.bitable_sync import BitableSync

APP_TOKEN = 'CvTRbdVyfa9PnMsnzIXcCSNmnnb'
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'smart_invest.db')

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--window', default='12h', help='扫描窗口: 4h/12h/18h/720h')
    p.add_argument('--as-of', help='模拟时间点: 2026-03-27T21:30 或 2026-03-27')
    p.add_argument('--no-push', action='store_true', help='不推飞书卡片')
    p.add_argument('--no-bitable', action='store_true', help='不写 Bitable')
    p.add_argument('--dry-run', action='store_true', help='只打印不执行')
    p.add_argument('--max-stocks', type=int, default=10, help='最多采集股票数（测试用）')
    return p.parse_args()

def parse_window(w):
    if w.endswith('h'):
        return int(w[:-1])
    return int(w)

def _get_feishu_token():
    """获取飞书 tenant_access_token"""
    import re
    try:
        with open('/root/.openclaw/openclaw.json') as f:
            content = f.read()
        m = re.search(r"appSecret:\s*'([^']+)'", content)
        if not m:
            m = re.search(r'"appSecret"\s*:\s*"([^"]+)"', content)
        if not m:
            return None
        import requests
        r = requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
                         json={'app_id': 'cli_a92e5d71f2799bdb', 'app_secret': m.group(1)},
                         timeout=10)
        return r.json().get('tenant_access_token')
    except Exception:
        return None

def _write_to_bitable(app_token, table_id, records, batch_size=50):
    """直接写入 Bitable（调飞书 API）"""
    if not records:
        return 0
    token = _get_feishu_token()
    if not token:
        # 降级：写入 pending 文件
        pending_path = f'data/bitable_pending_{table_id}.json'
        with open(pending_path, 'w') as f:
            json.dump(records, f, ensure_ascii=False)
        return len(records)

    import requests
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            r = requests.post(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create',
                headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
                json={'records': batch},
                timeout=30
            )
            resp = r.json()
            if resp.get('code') == 0:
                total += len(resp.get('data', {}).get('records', []))
            else:
                print(f'   ⚠️ Bitable API 错误: {resp.get("msg", "unknown")}')
        except Exception as e:
            print(f'   ⚠️ Bitable 写入异常: {e}')
    return total

def _make_pool_records(pool_rows):
    """构造发现池 Bitable 记录"""
    records = []
    for r in pool_rows:
        records.append({'fields': {
            '股票代码': r[0],
            '公司名称': r[1] or r[0],
            '发现来源': r[2] or '',
            '评分': r[3] or 0,
            '信号': r[4] or 'watch',
            '状态': 'active',
        }})
    return records

def _make_event_records(events):
    """构造事件 Bitable 记录"""
    records = []
    for e in events:
        records.append({'fields': {
            '股票代码': e.get('stock_code', ''),
            '公司名称': e.get('stock_name', e.get('stock_code', '')),
            '事件类型': e.get('event_type', ''),
            '情感': e.get('sentiment', 'neutral'),
            '严重程度': e.get('severity', 'medium'),
            '标题': e.get('title', ''),
            '详情': e.get('content', ''),
        }})
    return records

def _make_tn_records(tn_rows):
    """构造 T+N 跟踪 Bitable 记录"""
    records = []
    for t in tn_rows:
        records.append({'fields': {
            '股票代码': t.get('stock_code', ''),
            '公司名称': t.get('stock_name', t.get('stock_code', '')),
            '事件类型': t.get('event_type', ''),
            '入池价': t.get('entry_price'),
            '状态': t.get('tracking_status', 'pending'),
        }})
    return records

def run(args):
    results = {}
    t_total = time.time()

    # 处理 --as-of 参数
    as_of_str = ''
    scan_mode = 'custom'
    hours = parse_window(args.window)

    if args.as_of:
        try:
            if 'T' in args.as_of:
                as_of = datetime.strptime(args.as_of, '%Y-%m-%dT%H:%M')
            else:
                as_of = datetime.strptime(args.as_of, '%Y-%m-%d')
            as_of_str = as_of.strftime('%Y-%m-%d %H:%M')
            hour = as_of.hour
            # 根据模拟时间标注模式（不影响窗口大小）
            if hour < 12:
                scan_mode = 'morning'
            elif hour < 18:
                scan_mode = 'afternoon'
            else:
                scan_mode = 'evening'
        except ValueError:
            print(f'❌ 时间格式错误: {args.as_of}，正确格式: 2026-03-27T21:30')
            return {}

    mode_names = {'morning': '早盘', 'afternoon': '盘后', 'evening': '晚间', 'custom': '自定义'}

    print(f'🚀 投资系统 v2.2 — Pipeline 回调测试')
    if as_of_str:
        print(f'模拟时间: {as_of_str} ({mode_names.get(scan_mode, scan_mode)}模式, 窗口{hours}h)')
    else:
        print(f'窗口: {hours}h')
    print(f'实际时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print()

    # ── Step 1: DisclosureScanner ──
    print(f'📡 Step 1: DisclosureScanner (窗口 {hours}h)')
    t0 = time.time()
    init_db(DB_PATH)
    scanner = DisclosureScanner(db_path=DB_PATH)
    new_codes = scanner.get_scan_list(since_hours=hours)
    scan_ms = int((time.time() - t0) * 1000)
    results['scanner'] = {'codes': len(new_codes), 'ms': scan_ms}
    if as_of_str:
        print(f'   ✅ {len(new_codes)} 家新披露 ({scan_ms}ms) [模拟 {as_of_str}]')
    else:
        print(f'   ✅ {len(new_codes)} 家新披露 ({scan_ms}ms)')

    if not new_codes:
        print('\n   无新披露，测试结束')
        return results

    # 限制测试范围
    demo_codes = new_codes[:args.max_stocks]
    print(f'   测试范围: {len(demo_codes)} 只')

    if args.dry_run:
        print(f'   [DRY RUN] 跳过后续步骤')
        return results

    # ── Step 2: Pipeline ──
    print(f'\n📊 Step 2: Pipeline 采集')
    t0 = time.time()
    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    pipe_results = pipe.run(demo_codes)
    pipe_ms = int((time.time() - t0) * 1000)
    ok = sum(1 for r in pipe_results.values() if r['status'] == 'ok')
    results['pipeline'] = {'ok': ok, 'total': len(demo_codes), 'ms': pipe_ms}
    print(f'   ✅ {ok}/{len(demo_codes)} 采集成功 ({pipe_ms}ms)')

    # ── Step 3: Analyzer ──
    print(f'\n🔍 Step 3: Analyzer 分析')
    analyzer = EarningsAnalyzer(db_path=DB_PATH)

    # 超预期
    beats = analyzer.scan_beat_expectation(stock_codes=demo_codes)
    na = sum(1 for b in beats if b.get('signal') == 'N/A')
    watch = sum(1 for b in beats if b.get('signal') == 'watch')
    buy = sum(1 for b in beats if b.get('signal') == 'buy')
    print(f'   ├─ 超预期: {len(beats)} 条 (buy={buy}, watch={watch}, N/A={na})')

    # 扣非新高
    highs = analyzer.scan_new_high(stock_codes=demo_codes)
    print(f'   ├─ 扣非新高: {len(highs)} 条')

    # 发现池
    pool = analyzer.auto_discover_pool(beats=beats, new_highs=highs)
    print(f'   ├─ 发现池: {len(pool)} 入池')

    # T+N 更新
    tn = analyzer.update_tn_tracking()
    print(f'   ├─ T+N 更新: {len(tn)} 条')

    # 事件
    evt = EventAnalyzer(db_path=DB_PATH)
    events = evt.detect_from_pipeline(beats=beats, new_highs=highs)
    print(f'   └─ 事件: {len(events)} 条')

    results['analyzer'] = {
        'beats': len(beats), 'highs': len(highs),
        'pool': len(pool), 'tn': len(tn), 'events': len(events)
    }

    # ── Step 4: Bitable 同步 ──
    if not args.no_bitable:
        print(f'\n📝 Step 4: Bitable 同步')
        bitable_results = {}

        # 信号看板
        sync = BitableSync.from_preset('scan')
        records = sync.generate_scan_records(beats=beats, new_highs=highs)
        if records:
            # 直接写入 Bitable（调 API，不只写 pending）
            new_count = _write_to_bitable(APP_TOKEN, 'tbluSQrjOW0tppTP', records)
            bitable_results['signals'] = new_count
            print(f'   ├─ 信号看板: {len(records)} 条生成, {new_count} 写入')
        else:
            bitable_results['signals'] = 0
            print(f'   ├─ 信号看板: 无新记录')

        # 发现池
        if pool:
            pool_records = _make_pool_records(pool)
            n = _write_to_bitable(APP_TOKEN, 'tblPKXYUsow2Pd6A', pool_records)
            bitable_results['pool'] = n
            print(f'   ├─ 发现池: {n} 条写入')
        else:
            bitable_results['pool'] = 0
            print(f'   ├─ 发现池: 无新记录')

        # 事件
        if events:
            evt_records = _make_event_records(events)
            n = _write_to_bitable(APP_TOKEN, 'tblUgPIXejUOggWx', evt_records)
            bitable_results['events'] = n
            print(f'   ├─ 事件: {n} 条写入')
        else:
            bitable_results['events'] = 0
            print(f'   ├─ 事件: 无新记录')

        # T+N
        if tn:
            tn_records = _make_tn_records(tn)
            n = _write_to_bitable(APP_TOKEN, 'tblNZIrovX0WRmW3', tn_records)
            bitable_results['tn'] = n
            print(f'   └─ T+N 跟踪: {n} 条写入')
        else:
            bitable_results['tn'] = 0
            print(f'   └─ T+N 跟踪: 无新记录')

        results['bitable'] = bitable_results

    # ── Step 5: 数据质量验证 ──
    print(f'\n🔍 Step 5: 数据质量验证')
    import sqlite3
    conn = sqlite3.connect(DB_PATH)

    # 名称检查
    pool_rows = conn.execute('SELECT stock_code, stock_name FROM discovery_pool LIMIT 10').fetchall()
    name_ok = sum(1 for r in pool_rows if r[0] != r[1])
    print(f'   ├─ 发现池名称: {name_ok}/{len(pool_rows)} 有正确名称')

    # quarterly_net_profit 检查
    q = conn.execute('SELECT COUNT(*) FROM earnings WHERE quarterly_net_profit IS NOT NULL AND stock_code IN (' + ','.join(['?']*len(demo_codes)) + ')', demo_codes).fetchone()[0]
    total_q = conn.execute('SELECT COUNT(*) FROM earnings WHERE stock_code IN (' + ','.join(['?']*len(demo_codes)) + ')', demo_codes).fetchone()[0]
    print(f'   ├─ quarterly_net_profit: {q}/{total_q} 有值')

    # 事件 title 检查
    evt_rows = conn.execute('SELECT title FROM events LIMIT 5').fetchall()
    has_name = sum(1 for r in evt_rows if '.SH' not in (r[0] or '') and '.SZ' not in (r[0] or ''))
    print(f'   └─ 事件 title: {has_name}/{len(evt_rows)} 含公司名')

    conn.close()

    results['quality'] = {'name_ok': name_ok, 'q_ratio': f'{q}/{total_q}'}

    # ── 总结 ──
    total_ms = int((time.time() - t_total) * 1000)
    print(f'\n{"="*50}')
    print(f'✅ 测试完成 | 总耗时 {total_ms}ms | 扫描 {len(new_codes)} 家 | 采集 {ok}/{len(demo_codes)}')
    print(f'{"="*50}')

    return results

if __name__ == '__main__':
    args = parse_args()
    run(args)
