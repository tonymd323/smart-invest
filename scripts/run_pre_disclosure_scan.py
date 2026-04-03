#!/usr/bin/env python3
"""
投资系统 v2.2 — 前夜预获取扫描
================================
每天晚间运行（21:00），在财报披露日前一晚预捕捉信号。

流程：
  1. fetch_pre_disclosure(明天) → 获取次日披露公司列表
  2. fetch_by_em_code → 直接调东财 API 获取财务数据
  3. scan_new_high / scan_beat_expectation → 判断利润新高/超预期
  4. 写入 analysis_results + discovery_pool + event_tracking
  5. 推送飞书通知

效果：
  披露日当天早上推送信号，比 Tushare 批量入库早 1-2 天。
"""
import os; os.environ['TZ'] = 'Asia/Shanghai'
import sys, json, time, argparse, sqlite3
from datetime import datetime, timedelta
from pathlib import Path

time.tzset()

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
DB_PATH = PROJECT_ROOT / "data" / "smart_invest.db"


def run(args):
    from core.system_logger import SystemLogger
    logger = SystemLogger(db_path=str(DB_PATH))

    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")

    print(f"🚀 投资系统 v2.2 — 前夜预获取扫描")
    print(f"目标日期: {tomorrow_str} | 运行时间: {today.strftime('%Y-%m-%d %H:%M')}")
    print()

    with logger.run("pre_disclosure", f"前夜预获取 target={tomorrow_str}") as log:
        from core.database import init_db
        init_db(str(DB_PATH))

        # ── Step 1: 获取明日披露公司 ────────────────────────────────────────
        print(f"📡 Step 1: DisclosureScanner.fetch_pre_disclosure({tomorrow_str})")
        t0 = time.time()
        from core.disclosure_scanner import DisclosureScanner
        scanner = DisclosureScanner(db_path=str(DB_PATH))
        disclosures = scanner.fetch_pre_disclosure(tomorrow_str)
        step1_ms = int((time.time() - t0) * 1000)
        print(f"   ✅ {len(disclosures)} 家公司将于 {tomorrow_str} 披露财报 ({step1_ms}ms)")

        if not disclosures:
            print("   无明日披露公司，跳过")
            log.result("无明日披露")
            return {"codes": 0}

        # 显示前5条
        for d in disclosures[:5]:
            print(f"   → {d['stock_code']} {d['stock_name']} 报告期:{d['report_date']}")
        if len(disclosures) > 5:
            print(f"   ... 还有 {len(disclosures) - 5} 家")

        # ── Step 2: 直接调东财 API 获取财务数据 ────────────────────────────
        print(f"\n📊 Step 2: FinancialProvider.fetch_by_em_code (直接API)")
        t0 = time.time()
        from core.data_provider import FinancialProvider
        provider = FinancialProvider()

        fetched_data = {}  # {em_code: FinancialData}
        fetch_fail = []

        for d in disclosures:
            em_code = d["em_code"]
            report_date = d["report_date"]
            try:
                results = provider.fetch_by_em_code(em_code, report_date)
                if results:
                    # 取最新一期（就是指定报告期）
                    fetched_data[em_code] = results[0]
                else:
                    fetch_fail.append(em_code)
            except Exception as e:
                print(f"   ⚠️ {d['stock_code']} fetch失败: {e}")
                fetch_fail.append(em_code)

        step2_ms = int((time.time() - t0) * 1000)
        print(f"   ✅ 财务数据获取: {len(fetched_data)}/{len(disclosures)} 成功 ({step2_ms}ms)")
        if fetch_fail:
            print(f"   ⚠️ 失败: {fetch_fail[:3]}{'...' if len(fetch_fail) > 3 else ''}")

        if not fetched_data:
            print("   无有效财务数据，跳过")
            log.result("无财务数据")
            return {"codes": len(disclosures), "fetched": 0}

        # ── Step 2b: 业绩预告（现有系统已有 fetch_forecast，补上前夜扫描）─────
        print(f"\n📡 Step 2b: FinancialProvider.fetch_forecast (业绩预告)")
        t0f = time.time()
        forecast_records = []
        for d in disclosures:
            if d.get("disclosure_type") != "forecast":
                continue
            code = d["stock_code"]
            try:
                recs = provider.fetch_forecast(code)
                if recs:
                    # 只保留与目标日期匹配的
                    matched = [r for r in recs if r.get("report_date") == d["report_date"]]
                    forecast_records.extend(matched or recs[:1])
            except Exception as ex:
                print(f"   ⚠️ {code} fetch_forecast失败: {ex}")
        step2b_ms = int((time.time() - t0f) * 1000)
        print(f"   ✅ 业绩预告获取: {len(forecast_records)} 条 ({step2b_ms}ms)")
        if forecast_records:
            for f in forecast_records[:5]:
                print(f"   → {f['stock_code']} {f['forecast_type']} 同比:{f.get('net_profit_yoy')} 预告内容:{f.get('content','')[:40]}...")

        # ── Step 3: 信号分析 ─────────────────────────────────────────────────
        print(f"\n🔍 Step 3: 信号分析")
        t0 = time.time()
        from core.analyzer import EarningsAnalyzer
        ea = EarningsAnalyzer(db_path=str(DB_PATH))

        # 构造 analyzer 需要的格式：List[dict]
        # 每个 dict 至少需要 stock_code, report_date, net_profit, koufei_net_profit
        enriched = []
        for d in disclosures:
            em_code = d["em_code"]
            if em_code not in fetched_data:
                continue
            fd = fetched_data[em_code]
            enriched.append({
                "stock_code": d["stock_code"],
                "stock_name": d["stock_name"],
                "report_date": d["report_date"],
                "notice_date": d["notice_date"],
                "em_code": em_code,
                "net_profit": fd.net_profit,          # 亿元
                "koufei_net_profit": fd.koufei_net_profit,  # 亿元
                "net_profit_yoy": fd.net_profit_yoy,  # %
                "revenue_yoy": fd.revenue_yoy,         # %
                "roe": fd.roe,
                "gross_margin": fd.gross_margin,
            })

        if not enriched:
            print("   无有效数据，跳过")
            log.result("无有效enriched数据")
            return {"codes": len(disclosures), "fetched": len(fetched_data)}

        # 构建 code → enriched dict
        code_to_data = {e["stock_code"]: e for e in enriched}

        # 构造假/mock格式让 scan_new_high 可以用
        # scan_new_high 内部会从 db 查 quarterly_net_profit，我们把数据直接写入 db
        # 先把数据写入 earnings 表（如果不存在）
        conn = sqlite3.connect(str(DB_PATH))
        for e in enriched:
            exists = conn.execute(
                "SELECT 1 FROM earnings WHERE stock_code=? AND report_date=?",
                (e["stock_code"], e["report_date"])
            ).fetchone()
            if not exists:
                conn.execute("""
                    INSERT OR IGNORE INTO earnings
                    (stock_code, report_date, net_profit, net_profit_yoy,
                     revenue, revenue_yoy, roe, gross_margin, koufei_net_profit,
                     created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))
                """, (
                    e["stock_code"], e["report_date"],
                    e["net_profit"], e["net_profit_yoy"],
                    e.get("revenue", 0), e.get("revenue_yoy", 0),
                    e["roe"], e["gross_margin"],
                    e["koufei_net_profit"],
                ))
        conn.commit()
        conn.close()
        print(f"   ✅ 写入 earnings 表: {len(enriched)} 条（预数据）")

        # 调用 scan_new_high
        codes = [e["stock_code"] for e in enriched]
        try:
            highs = ea.scan_new_high(codes)
            print(f"   ├─ 扣非净利润新高: {len(highs)} 条")
            for h in highs[:3]:
                code = h.get("stock_code", "?")
                profit = h.get("quarterly_net_profit", 0)
                growth = h.get("growth_pct", 0)
                print(f"      {code}: 季度利润={profit:.4f}亿 vs 高点={growth:+.1f}%")
        except Exception as ex:
            print(f"   ├─ scan_new_high 失败: {ex}")
            highs = []

        # 调用 scan_beat_expectation（如果有 consensus 数据的话）
        try:
            beats = ea.scan_beat_expectation(codes)
            print(f"   ├─ 超预期信号: {len(beats)} 条")
            for b in beats[:3]:
                code = b.get("stock_code", "?")
                sig = b.get("signal", "?")
                diff = b.get("beat_diff_pct", 0)
                if diff is not None:
                    print(f"      {code}: signal={sig} beat_diff={diff:+.1f}%")
                else:
                    print(f"      {code}: signal={sig} beat_diff=N/A")
        except Exception as ex:
            print(f"   ├─ scan_beat_expectation 失败（不影响主流程）: {ex}")
            beats = []

        step3_ms = int((time.time() - t0) * 1000)

        # ── Step 3b: 业绩预告信号分析（预告无 quarterly_net_profit，走单独打分）──
        forecast_signals = []
        if forecast_records:
            conn = sqlite3.connect(str(DB_PATH))
            for f in forecast_records:
                stock_code = f.get("stock_code")
                report_date = f.get("report_date")
                forecast_type = f.get("forecast_type", "")
                net_profit_yoy = f.get("net_profit_yoy")
                # 预告类型打分
                score = 0
                signal = "watch"
                if forecast_type in ("预增", "扭亏", "大幅扭亏"):
                    score = 70 + min(int(net_profit_yoy or 0) if net_profit_yoy else 0, 30)
                    signal = "buy"
                elif forecast_type in ("略增", "续盈"):
                    score = 55
                    signal = "watch"
                elif forecast_type in ("预减", "首亏", "续亏", "大幅减亏"):
                    score = 30
                    signal = "avoid"
                # 写入发现池
                try:
                    exists = conn.execute(
                        "SELECT 1 FROM discovery_pool WHERE stock_code=? AND source='forecast'",
                        (stock_code,)
                    ).fetchone()
                    if not exists:
                        conn.execute("""
                            INSERT INTO discovery_pool
                            (stock_code, source, signal, score, detail, discovered_at)
                            VALUES (?, 'forecast', ?, ?, ?, datetime('now'))
                        """, (
                            stock_code, signal, score,
                            json.dumps(f, ensure_ascii=False)
                        ))
                        forecast_signals.append({
                            "stock_code": stock_code,
                            "forecast_type": forecast_type,
                            "signal": signal,
                            "score": score,
                            "net_profit_yoy": net_profit_yoy,
                        })
                except Exception as ex:
                    print(f"   ⚠️ 预告信号写入失败 {stock_code}: {ex}")
            conn.commit()
            conn.close()
        if forecast_signals:
            print(f"   ├─ 业绩预告信号: {len(forecast_signals)} 条")
            for f in forecast_signals[:5]:
                print(f"      {f['stock_code']} [{f['forecast_type']}] signal={f['signal']} score={f['score']}")

        # ── Step 4: 写入发现池 + T+N 跟踪 ───────────────────────────────────
        auto_pool = ea.auto_discover_pool(beats, highs)
        print(f"   ├─ 发现池入池: {len(auto_pool)} 条")

        if auto_pool:
            try:
                for entry in auto_pool:
                    code = entry.get("stock_code")
                    source = entry.get("source")
                    if code and source:
                        ea.create_tn_tracking([code], source)
                print(f"   ├─ T+N 跟踪创建: {len(auto_pool)} 条")
            except Exception as ex:
                print(f"   ├─ T+N 跟踪创建失败: {ex}")

        # ── Step 5: 推送飞书通知 ─────────────────────────────────────────────
        if not args.quiet and (highs or beats):
            try:
                _push_feishu_pre_disclosure(highs, beats, code_to_data)
            except Exception as ex:
                print(f"   ⚠️ 飞书推送失败: {ex}")

        total_ms = int((time.time() - t0) * 1000)
        summary = (f"前夜预获取 | 披露{len(disclosures)}家 | "
                   f"获取{len(fetched_data)}家财务 | "
                   f"新高{len(highs)} | 超预期{len(beats)} | "
                   f"入池{len(auto_pool)} | {total_ms}ms")
        log.result(summary)
        print(f"\n{'='*50}")
        print(f"✅ {summary}")
        print(f"{'='*50}")

        return {
            "disclosures": len(disclosures),
            "fetched": len(fetched_data),
            "highs": len(highs),
            "beats": len(beats),
            "auto_pool": len(auto_pool),
            "total_ms": total_ms,
        }


def _push_feishu_pre_disclosure(highs, beats, code_to_data):
    """推送前夜预获取结果到飞书"""
    try:
        from notifiers.feishu_pusher import FeishuPusher
        pusher = FeishuPusher()

        # 构造新高信号
        card_highs = []
        for h in highs:
            code = h.get("stock_code", "")
            data = code_to_data.get(code, {})
            card_highs.append({
                "code": code,
                "name": data.get("stock_name", ""),
                "quarterly_profit": h.get("quarterly_net_profit", 0),
                "growth_vs_high": h.get("growth_pct", 0),
                "pe": None,
                "report_type": _infer_report_type(data.get("report_date", "")),
                "ann_date": data.get("notice_date", "").replace("-", ""),
            })

        # 构造超预期信号
        card_beats = []
        for b in beats:
            code = b.get("stock_code", "")
            data = code_to_data.get(code, {})
            card_beats.append({
                "code": code,
                "name": data.get("stock_name", ""),
                "consensus_available": False,
                "actual_profit_yoy": b.get("actual_profit_yoy"),
                "expected_profit_yoy": None,
                "actual_rev_yoy": data.get("revenue_yoy"),
                "expected_rev_yoy": None,
                "is_non_recurring": False,
                "report_type": _infer_report_type(data.get("report_date", "")),
                "ann_date": data.get("notice_date", "").replace("-", ""),
            })

        if card_highs or card_beats:
            pusher.push_daily_scan_card(card_beats, card_highs, {}, [])
            print(f"   ✅ 飞书推送成功 (新高{len(card_highs)}/超预期{len(card_beats)})")
    except Exception as e:
        print(f"   ⚠️ 飞书推送异常: {e}")


def _infer_report_type(report_date: str) -> str:
    if not report_date:
        return "财报"
    try:
        md = report_date.replace("-", "")[4:8]
        return {"0331": "Q1", "0630": "Q2", "0930": "Q3", "1231": "年报"}.get(md, "财报")
    except Exception:
        return "财报"


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--quiet", action="store_true", help="静默模式，不推送飞书")
    p.add_argument("--target", default=None, help="目标日期 YYYY-MM-DD（默认明天）")
    args = p.parse_args()

    if args.target:
        # 支持手动指定目标日期（用于测试）
        import sys as _sys
        _sys.path.insert(0, str(PROJECT_ROOT))
        from core.disclosure_scanner import DisclosureScanner
        scanner = DisclosureScanner(db_path=str(DB_PATH))
        result = scanner.fetch_pre_disclosure(args.target)
        print(f"fetch_pre_disclosure({args.target}) → {len(result)} 条:")
        for r in result:
            print(f"  {r['stock_code']} {r['stock_name']} 报告期:{r['report_date']}")
    else:
        results = run(args)
        print(json.dumps(results, indent=2, ensure_ascii=False))
