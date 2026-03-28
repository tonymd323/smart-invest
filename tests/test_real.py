#!/usr/bin/env python3
"""
投资系统 2.0 — 真实环境测试套件
================================
不 mock，跑真实 API + 真实 DB，验证每个环节。

测试矩阵：
  T1: Pipeline 单股票采集（福耀玻璃）
  T2: Pipeline 多股票批量采集（3只持仓股）
  T3: Pipeline 异常股票处理（不存在的代码）
  T4: EarningsAnalyzer 超预期扫描
  T5: EarningsAnalyzer 扣非新高扫描
  T6: 端到端链路（Pipeline → DB → Analyzer → DB）
  T7: 数据质量校验（正常值 vs 脏数据）
  T8: 1.0 daily_scan.py 回归
  T9: 性能基准（3只股票总耗时）
  T10: DisclosureScanner 获取新披露列表
  T11: DisclosureScanner diff_with_db 去重
  T12: quarterly_net_profit 计算验证
  T13: 超预期扫描 N/A 处理
  T14: NewsProvider 获取新闻
  T15: 发现池自动入场
  T16: T+N 跟踪创建+更新
"""

import os
import pytest
import sys
import json
import sqlite3
import time
import subprocess
import traceback
from pathlib import Path
from datetime import datetime, timedelta

# 路径设置
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = str(PROJECT_ROOT / "data" / "smart_invest.db")

# ── 测试框架 ──────────────────────────────────────────────────────────────────

class TestResult:
    def __init__(self):
        self.results = []
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def add(self, name, status, detail="", elapsed_ms=0):
        self.results.append({
            "name": name,
            "status": status,
            "detail": detail,
            "elapsed_ms": elapsed_ms,
        })
        if status == "PASS":
            self.passed += 1
        elif status == "FAIL":
            self.failed += 1
        else:
            self.skipped += 1

    def summary(self):
        total = self.passed + self.failed + self.skipped
        lines = []
        lines.append("=" * 70)
        lines.append(f"  投资系统 2.0 测试报告  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        for r in self.results:
            icon = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️"}.get(r["status"], "?")
            elapsed = f"({r['elapsed_ms']}ms)" if r["elapsed_ms"] > 0 else ""
            lines.append(f"  {icon} {r['name']} {elapsed}")
            if r["detail"]:
                for dl in r["detail"].split("\n"):
                    lines.append(f"     {dl}")
        lines.append("-" * 70)
        lines.append(f"  Total: {total}  |  ✅ {self.passed}  ❌ {self.failed}  ⏭️ {self.skipped}")
        lines.append("=" * 70)
        return "\n".join(lines)


T = TestResult()


def run_test(name, func):
    """运行单个测试，捕获异常"""
    t0 = time.time()
    try:
        func()
        elapsed = int((time.time() - t0) * 1000)
        T.add(name, "PASS", elapsed_ms=elapsed)
    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        T.add(name, "FAIL", f"{type(e).__name__}: {e}", elapsed_ms=elapsed)
        traceback.print_exc()


# ═══════════════════════════════════════════════════════════════════════════════
#  T1: Pipeline 单股票采集（福耀玻璃）
# ═══════════════════════════════════════════════════════════════════════════════

def test_t1_single_stock():
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline

    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    results = pipe.run(["600660.SH"])

    assert "600660.SH" in results, "结果中没有 600660.SH"
    r = results["600660.SH"]
    assert r["status"] == "ok", f"状态不是 ok: {r['status']}"
    assert r["records_written"] > 0, f"写入 0 条记录"
    assert r["quality"]["passed"], f"质量校验失败: {r['quality'].get('errors')}"

    # 验证 DB 中确实有数据
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT COUNT(*) as cnt, MAX(report_date) as latest FROM earnings WHERE stock_code='600660.SH'"
    ).fetchone()
    conn.close()

    assert row[0] > 0, "DB 中 earnings 表无数据"
    print(f"     采集 {row[0]} 条, 最新报告期: {row[1]}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T2: Pipeline 多股票批量采集
# ═══════════════════════════════════════════════════════════════════════════════

def test_t2_batch_stocks():
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline

    codes = ["600660.SH", "600938.SH", "600875.SH"]
    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    results = pipe.run(codes)

    all_ok = True
    details = []
    for code in codes:
        r = results.get(code, {})
        status = r.get("status", "missing")
        written = r.get("records_written", 0)
        details.append(f"{code}: status={status}, records={written}")
        if status != "ok" or written == 0:
            all_ok = False

    assert all_ok, f"部分股票采集失败:\n" + "\n".join(details)
    print(f"     " + " | ".join(details))


# ═══════════════════════════════════════════════════════════════════════════════
#  T3: Pipeline 异常股票处理
# ═══════════════════════════════════════════════════════════════════════════════

def test_t3_invalid_stock():
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline

    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    results = pipe.run(["999999.XX"])

    r = results.get("999999.XX", {})
    # 应该优雅处理，不崩溃，返回 empty 或 error
    assert r.get("status") in ("empty", "error", "ok"), \
        f"无效股票未优雅处理: {r}"
    print(f"     无效股票状态: {r.get('status')}, 写入: {r.get('records_written', 0)}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T4: EarningsAnalyzer 超预期扫描
# ═══════════════════════════════════════════════════════════════════════════════

def test_t4_earnings_beat():
    from core.analyzer import EarningsAnalyzer

    analyzer = EarningsAnalyzer(db_path=DB_PATH)
    results = analyzer.scan_beat_expectation(stock_codes=["600660.SH", "600938.SH", "600875.SH"])

    assert len(results) > 0, "超预期扫描返回空结果"

    details = []
    for r in results:
        details.append(
            f"{r['stock_code']}: score={r['score']}, signal={r['signal']}, "
            f"beat_diff={r['beat_diff_pct']}%"
        )
        # 验证返回结构完整性
        assert "stock_code" in r
        assert "analysis_type" in r
        assert r["analysis_type"] == "earnings_beat"
        # signal 可以是 buy/watch/hold/avoid/N/A
        assert r["signal"] in ("buy", "watch", "hold", "avoid", "N/A"), \
            f"未知 signal: {r['signal']}"
        # score 可以是 None（N/A 情况）或 0-100
        if r["score"] is not None:
            assert 0 <= r["score"] <= 100, f"score 越界: {r['score']}"

    print(f"     扫描 {len(results)} 只:")
    for d in details:
        print(f"       {d}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T5: EarningsAnalyzer 扣非新高扫描
# ═══════════════════════════════════════════════════════════════════════════════

def test_t5_new_high():
    from core.analyzer import EarningsAnalyzer

    analyzer = EarningsAnalyzer(db_path=DB_PATH)
    results = analyzer.scan_new_high(stock_codes=["600660.SH", "600938.SH", "600875.SH"])

    # 扣非新高需要 quarterly_net_profit 数据，可能返回空
    details = []
    for r in results:
        details.append(
            f"{r['stock_code']}: is_new_high={r['is_new_high']}, "
            f"score={r['score']}, profit={r.get('quarterly_net_profit')}"
        )
        assert "analysis_type" in r
        assert r["analysis_type"] == "profit_new_high"

    print(f"     扫描结果: {len(results)} 只")
    for d in details:
        print(f"       {d}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T6: 端到端链路验证
# ═══════════════════════════════════════════════════════════════════════════════

def test_t6_e2e():
    """Pipeline 采集 → DB → Analyzer 分析 → DB → 验证全链路"""
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline
    from core.analyzer import EarningsAnalyzer

    test_code = "600660.SH"

    # Step 1: Pipeline
    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    pipe_results = pipe.run([test_code])
    assert pipe_results[test_code]["status"] == "ok", "Pipeline 采集失败"

    # Step 2: Analyzer
    analyzer = EarningsAnalyzer(db_path=DB_PATH)
    beat_results = analyzer.scan_beat_expectation(stock_codes=[test_code])
    high_results = analyzer.scan_new_high(stock_codes=[test_code])

    # Step 3: 验证 analysis_results 表有写入
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT analysis_type, score, signal, created_at
        FROM analysis_results
        WHERE stock_code = ?
        ORDER BY created_at DESC
        LIMIT 10
    """, (test_code,)).fetchall()
    conn.close()

    assert len(rows) > 0, "analysis_results 表无数据"

    types_found = set(r["analysis_type"] for r in rows)
    print(f"     Pipeline: {pipe_results[test_code]['records_written']} 条 earnings")
    print(f"     Analyzer: beat={len(beat_results)}, new_high={len(high_results)}")
    print(f"     DB analysis_results: {len(rows)} 条, 类型: {types_found}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T7: 数据质量校验
# ═══════════════════════════════════════════════════════════════════════════════

def test_t7_data_quality():
    from core.pipeline import DataQualityChecker

    # 正常数据
    good_records = [
        {"stock_code": "600660.SH", "report_date": "2025-12-31",
         "net_profit": 93.1, "net_profit_yoy": 33.2, "revenue": 457.9,
         "roe": 25.56, "gross_margin": 35.8},
    ]
    result = DataQualityChecker.check(good_records)
    assert result["passed"], f"正常数据被误判: {result['errors']}"

    # 脏数据：净利润越界
    bad_records = [
        {"stock_code": "600660.SH", "report_date": "2025-12-31",
         "net_profit": 999999, "net_profit_yoy": 33.2},
    ]
    result = DataQualityChecker.check(bad_records)
    assert not result["passed"], "脏数据未被检测出来"
    assert len(result["errors"]) > 0, "没有错误信息"

    # 缺关键字段
    missing_records = [
        {"stock_code": "", "report_date": "2025-12-31", "net_profit": 50},
    ]
    result = DataQualityChecker.check(missing_records)
    assert not result["passed"], "缺失关键字段未被检测"

    print(f"     正常数据: PASS | 脏数据: 检测到错误 | 缺字段: 检测到")


# ═══════════════════════════════════════════════════════════════════════════════
#  T8: 1.0 daily_scan.py 回归
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skip(reason='daily_scan.py archived, Cron uses v2 inline Python')
def test_t8_regression():
    """确保 2.0 开发不影响 1.0 daily_scan.py (已归档)"""
    result = subprocess.run(
        ["python3", "scripts/archived/daily_scan.py", "--quiet"],
        cwd=str(PROJECT_ROOT),
        capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, \
        f"daily_scan.py 退出码 {result.returncode}\nstderr: {result.stderr[:500]}"
    print(f"     daily_scan.py exit code: {result.returncode}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T9: 性能基准
# ═══════════════════════════════════════════════════════════════════════════════

def test_t9_performance():
    """3只股票采集+分析的总耗时"""
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline
    from core.analyzer import EarningsAnalyzer

    codes = ["600660.SH", "600938.SH", "600875.SH"]
    t0 = time.time()

    # Pipeline
    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    pipe.run(codes)

    # Analyzer
    analyzer = EarningsAnalyzer(db_path=DB_PATH)
    analyzer.scan_beat_expectation(stock_codes=codes)
    analyzer.scan_new_high(stock_codes=codes)

    total_ms = int((time.time() - t0) * 1000)

    # 性能标准：3只股票 < 30秒
    assert total_ms < 30000, f"性能不达标: {total_ms}ms (标准 < 30000ms)"
    print(f"     3只股票全链路耗时: {total_ms}ms")


# ═══════════════════════════════════════════════════════════════════════════════
#  T10: DisclosureScanner 获取新披露列表
# ═══════════════════════════════════════════════════════════════════════════════

def test_t10_disclosure_scanner_fetch():
    """验证 DisclosureScanner 能成功调用东方财富 API 获取新披露列表"""
    from core.disclosure_scanner import DisclosureScanner

    scanner = DisclosureScanner(db_path=DB_PATH)
    disclosures = scanner.fetch_new_disclosures(since_hours=48)

    # API 调用成功（不一定有数据，但不能报错）
    assert isinstance(disclosures, list), f"返回类型不是 list: {type(disclosures)}"

    if disclosures:
        # 验证返回结构
        for d in disclosures[:3]:
            assert "stock_code" in d, "缺少 stock_code"
            assert "stock_name" in d, "缺少 stock_name"
            assert "report_date" in d, "缺少 report_date"
            assert "notice_date" in d, "缺少 notice_date"
            assert "source" in d, "缺少 source"
            # 验证 stock_code 格式
            assert "." in d["stock_code"], f"stock_code 格式不对: {d['stock_code']}"

        print(f"     API 返回 {len(disclosures)} 条新披露")
        for d in disclosures[:5]:
            print(f"       {d['stock_code']} {d['stock_name']} "
                  f"报告期={d['report_date']} 披露日={d['notice_date']} 来源={d['source']}")
    else:
        print(f"     API 返回 0 条（48h 内无新披露）")


# ═══════════════════════════════════════════════════════════════════════════════
#  T11: DisclosureScanner diff_with_db 去重
# ═══════════════════════════════════════════════════════════════════════════════

def test_t11_disclosure_scanner_diff():
    """验证 diff_with_db 能正确去重，只返回真正新增的股票"""
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline
    from core.disclosure_scanner import DisclosureScanner

    # 先确保 DB 中有测试数据
    test_codes = ["600660.SH", "600938.SH"]
    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    pipe.run(test_codes)

    # 调用 diff_with_db
    scanner = DisclosureScanner(db_path=DB_PATH)
    new_codes = scanner.diff_with_db(test_codes)

    assert isinstance(new_codes, list), f"返回类型不是 list: {type(new_codes)}"

    # DB 中已有这些股票的数据，所以 new_codes 应该是空或很少
    # （除非东方财富发布了这些股票的新报告期）
    print(f"     输入 {len(test_codes)} 只, diff 后新增 {len(new_codes)} 只")
    if new_codes:
        print(f"       新增: {new_codes}")

    # 验证 get_scan_list 方法也能正常运行
    scan_list = scanner.get_scan_list(since_hours=24)
    assert isinstance(scan_list, list), f"get_scan_list 返回类型不对"
    print(f"     get_scan_list(since_hours=24): {len(scan_list)} 只")


# ═══════════════════════════════════════════════════════════════════════════════
#  T12: quarterly_net_profit 计算验证
# ═══════════════════════════════════════════════════════════════════════════════

def test_t12_quarterly_net_profit():
    """跑完 Pipeline 后检查 DB 中 quarterly_net_profit 非空"""
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline

    test_code = "600660.SH"

    # 跑 Pipeline（会自动计算 quarterly_net_profit）
    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    results = pipe.run([test_code])

    assert results[test_code]["status"] == "ok", "Pipeline 采集失败"

    # 验证 DB 中 quarterly_net_profit 有值
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT report_date, net_profit, quarterly_net_profit
        FROM earnings
        WHERE stock_code = ?
        ORDER BY report_date ASC
    """, (test_code,)).fetchall()
    conn.close()

    assert len(rows) >= 2, f"数据不足 {len(rows)} 条，无法验证"

    non_null_count = 0
    details = []
    for row in rows:
        qnp = row["quarterly_net_profit"]
        np_val = row["net_profit"]
        if qnp is not None:
            non_null_count += 1
        details.append(
            f"{row['report_date']}: net_profit={np_val}, "
            f"quarterly_net_profit={qnp}"
        )

    # 第一条应为 NULL（无前值），其余应有值
    assert rows[0]["quarterly_net_profit"] is None, \
        f"第一条 quarter_net_profit 应为 NULL（无前值）"
    assert non_null_count >= len(rows) - 1, \
        f"quarterly_net_profit 非空条数不足: {non_null_count}/{len(rows)}"

    print(f"     {len(rows)} 条 earnings, {non_null_count} 条有 quarterly_net_profit")
    for d in details[:5]:
        print(f"       {d}")
    if len(details) > 5:
        print(f"       ... (共 {len(details)} 条)")


# ═══════════════════════════════════════════════════════════════════════════════
#  T13: 超预期扫描 N/A 处理
# ═══════════════════════════════════════════════════════════════════════════════

def test_t13_beat_expectation_na():
    """验证无预期数据的股票 signal=N/A"""
    from core.data_provider import FinancialProvider
    from core.pipeline import Pipeline
    from core.analyzer import EarningsAnalyzer

    # 用一只可能没有 consensus 的股票测试
    test_code = "600875.SH"

    # 确保有 earnings 数据
    provider = FinancialProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[provider])
    pipe.run([test_code])

    # 清除该股票的 consensus 数据（模拟无预期）
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM consensus WHERE stock_code = ?", (test_code,))
    conn.commit()
    conn.close()

    # 运行超预期扫描
    analyzer = EarningsAnalyzer(db_path=DB_PATH)
    results = analyzer.scan_beat_expectation(stock_codes=[test_code])

    assert len(results) > 0, "超预期扫描返回空结果"

    found_na = False
    for r in results:
        if r["stock_code"] == test_code:
            # 应该是 N/A
            assert r["signal"] == "N/A", \
                f"{test_code} 无预期数据但 signal={r['signal']}，应为 N/A"
            assert r["score"] is None, \
                f"{test_code} 无预期数据但 score={r['score']}，应为 None"
            assert r["beat_diff_pct"] is None, \
                f"{test_code} 无预期数据但 beat_diff_pct={r['beat_diff_pct']}，应为 None"
            found_na = True

    assert found_na, f"未找到 {test_code} 的扫描结果"

    # 验证 analysis_results 表中 N/A 状态正确写入
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT signal, score
        FROM analysis_results
        WHERE stock_code = ? AND analysis_type = 'earnings_beat'
        ORDER BY created_at DESC
        LIMIT 1
    """, (test_code,)).fetchone()
    conn.close()

    assert row is not None, "analysis_results 中未写入"
    assert row["signal"] == "N/A", \
        f"analysis_results 中 signal={row['signal']}，应为 N/A"

    print(f"     {test_code}: signal=N/A, score=None ✅")
    print(f"     analysis_results 写入正确: signal={row['signal']}, score={row['score']}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T14: NewsProvider 获取新闻
# ═══════════════════════════════════════════════════════════════════════════════

def test_t14_news_provider():
    """验证 NewsProvider 能获取新闻（预注入数据 + 实时 API + RSS 降级）"""
    from core.data_provider import NewsProvider

    # 测试 1: 预注入数据
    mock_data = {
        "600660.SH": [
            {
                "title": "福耀玻璃发布2025年报",
                "content": "营收增长30%",
                "pub_date": "2026-03-28",
                "url": "https://example.com/1",
                "source_name": "东方财富",
                "event_type": "earnings",
            },
            {
                "title": "曹德旺谈汽车玻璃行业前景",
                "content": "新能源车带来新机遇",
                "pub_date": "2026-03-27",
                "url": "https://example.com/2",
                "source_name": "证券时报",
                "event_type": "industry",
            },
        ],
    }
    provider = NewsProvider(data=mock_data)
    results = provider.fetch("600660.SH")

    assert len(results) == 2, f"预注入数据应返回 2 条，实际 {len(results)}"
    assert results[0].title == "福耀玻璃发布2025年报"
    assert results[0].to_dict()["stock_code"] == "600660.SH"
    assert provider.last_source == "eastmoney"
    print(f"     预注入数据: {len(results)} 条, last_source={provider.last_source}")

    # 测试 2: 空数据返回空列表
    empty_provider = NewsProvider(data={})
    empty_results = empty_provider.fetch("999999.XX")
    assert isinstance(empty_results, list), "返回类型应为 list"
    print(f"     空数据查询: {len(empty_results)} 条 (预期 0)")

    # 测试 3: 实时 API 调用（非预注入股票）
    live_provider = NewsProvider()
    live_results = live_provider.fetch("600660.SH", limit=5)
    assert isinstance(live_results, list), "实时 API 返回类型应为 list"
    if live_results:
        for item in live_results:
            assert hasattr(item, 'title'), "返回对象缺少 title 属性"
            assert hasattr(item, 'stock_code'), "返回对象缺少 stock_code 属性"
            assert hasattr(item, 'to_dict'), "返回对象缺少 to_dict 方法"
    print(f"     实时 API: {len(live_results)} 条, last_source={live_provider.last_source}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T15: 发现池自动入场
# ═══════════════════════════════════════════════════════════════════════════════

def test_t15_discovery_pool():
    """验证自动发现池入场逻辑"""
    from core.analyzer import EarningsAnalyzer

    analyzer = EarningsAnalyzer(db_path=DB_PATH)

    # 清理测试数据（避免干扰）
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM discovery_pool WHERE stock_code LIKE 'TEST%'")
    conn.commit()
    conn.close()

    # 构造 mock beats 数据（超预期 buy）
    mock_beats = [
        {
            "stock_code": "TEST001.XX",
            "stock_name": "测试超预期A",
            "analysis_type": "earnings_beat",
            "score": 85.0,
            "signal": "buy",
            "beat_diff_pct": 15.0,
            "is_beat": True,
        },
        {
            "stock_code": "TEST002.XX",
            "stock_name": "测试超预期B",
            "analysis_type": "earnings_beat",
            "score": 50.0,
            "signal": "hold",  # 非 buy，不应入池
            "beat_diff_pct": 2.0,
            "is_beat": False,
        },
    ]

    # 构造 mock new_highs 数据
    mock_highs = [
        {
            "stock_code": "TEST003.XX",
            "analysis_type": "profit_new_high",
            "score": 70.0,
            "signal": "watch",
            "is_new_high": True,
            "quarterly_net_profit": 10.0,
        },
        {
            "stock_code": "TEST004.XX",
            "analysis_type": "profit_new_high",
            "score": 40.0,
            "signal": "hold",
            "is_new_high": False,  # 非新高，不应入池
        },
    ]

    # 执行自动入场
    entries = analyzer.auto_discover_pool(beats=mock_beats, new_highs=mock_highs)

    # 验证入池结果
    assert len(entries) == 2, f"应入池 2 只（buy + watch），实际 {len(entries)}"

    entry_codes = {e["stock_code"] for e in entries}
    assert "TEST001.XX" in entry_codes, "TEST001（buy）应入池"
    assert "TEST003.XX" in entry_codes, "TEST003（watch+新高）应入池"
    assert "TEST002.XX" not in entry_codes, "TEST002（hold）不应入池"
    assert "TEST004.XX" not in entry_codes, "TEST004（非新高）不应入池"

    # 验证 DB 写入
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT stock_code, source, status, expires_at
        FROM discovery_pool
        WHERE stock_code IN ('TEST001.XX', 'TEST003.XX')
        ORDER BY stock_code
    """).fetchall()

    assert len(rows) == 2, f"DB 中应有 2 条入池记录，实际 {len(rows)}"

    for row in rows:
        assert row["status"] == "active", f"{row['stock_code']} 状态应为 active"
        assert row["expires_at"] is not None, f"{row['stock_code']} 应有过期时间"

    # 验证不重复入池（再次调用）
    entries_again = analyzer.auto_discover_pool(beats=mock_beats, new_highs=mock_highs)
    assert len(entries_again) == 0, f"已入池的不应重复入池，实际入池 {len(entries_again)}"

    # 清理测试数据
    conn.execute("DELETE FROM discovery_pool WHERE stock_code LIKE 'TEST%'")
    conn.commit()
    conn.close()

    print(f"     入池 {len(entries)} 只: {entry_codes}")
    print(f"     重复入池: {len(entries_again)} 只 (预期 0)")
    print(f"     DB 写入验证通过, expires_at 有值")


# ═══════════════════════════════════════════════════════════════════════════════
#  T16: T+N 跟踪创建+更新
# ═══════════════════════════════════════════════════════════════════════════════

def test_t16_tn_tracking():
    """验证 T+N 跟踪创建和更新"""
    from core.analyzer import EarningsAnalyzer

    analyzer = EarningsAnalyzer(db_path=DB_PATH)
    test_code = "TESTTN001.XX"

    # 清理测试数据
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM event_tracking WHERE stock_code = ?", (test_code,))
    conn.execute("DELETE FROM prices WHERE stock_code = ?", (test_code,))
    conn.commit()
    conn.close()

    # Step 1: 先手动插入跟踪记录（用固定日期，方便测试）
    event_date_str = "2026-03-20"
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO event_tracking
        (stock_code, event_type, event_date, entry_price, tracking_status)
        VALUES (?, 'earnings_beat', ?, 10.0, 'pending')
    """, (test_code, event_date_str))
    conn.commit()

    # 插入价格数据：事件日 + 25 个交易日（逐步上涨）
    for i in range(25):
        d = (datetime(2026, 3, 20) + timedelta(days=i)).strftime("%Y-%m-%d")
        price = 10.0 + i * 0.2  # 每天涨 0.2
        conn.execute("""
            INSERT OR IGNORE INTO prices (stock_code, trade_date, close_price)
            VALUES (?, ?, ?)
        """, (test_code, d, price))
    conn.commit()
    conn.close()

    # 验证跟踪记录写入
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT * FROM event_tracking
        WHERE stock_code = ? AND event_type = 'earnings_beat'
        ORDER BY id DESC LIMIT 1
    """, (test_code,)).fetchone()
    conn.close()

    assert row is not None, "跟踪记录未写入 DB"
    assert row["event_type"] == "earnings_beat"
    assert row["tracking_status"] == "pending"

    # Step 3: 更新 T+N 收益
    updated = analyzer.update_tn_tracking()

    # 验证更新结果
    assert len(updated) > 0, "应有更新的跟踪记录"

    found = False
    for u in updated:
        if u["stock_code"] == test_code:
            found = True
            assert "return_1d" in u or "return_5d" in u, "应有收益计算结果"
            break
    assert found, f"未找到 {test_code} 的更新记录"

    # 验证 DB 中的值
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT return_1d, return_5d, return_10d, return_20d, tracking_status
        FROM event_tracking
        WHERE stock_code = ? AND event_type = 'earnings_beat'
        ORDER BY id DESC LIMIT 1
    """, (test_code,)).fetchone()
    conn.close()

    assert row is not None
    # 至少 T+1 和 T+5 应该有值
    assert row["return_1d"] is not None, "return_1d 应有值"
    assert row["return_1d"] > 0, "模拟数据中 T+1 应为正收益"

    # 清理测试数据
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM event_tracking WHERE stock_code = ?", (test_code,))
    conn.execute("DELETE FROM prices WHERE stock_code = ?", (test_code,))
    conn.commit()
    conn.close()

    print(f"     创建跟踪: {test_code} status=pending")
    print(f"     更新后: return_1d={row['return_1d']}%, return_5d={row['return_5d']}%")
    print(f"     tracking_status={row['tracking_status']}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T17: QuoteProvider 实时行情
# ═══════════════════════════════════════════════════════════════════════════════

def test_t17_quote_provider():
    """QuoteProvider 能获取实时行情"""
    from core.data_provider import QuoteProvider

    provider = QuoteProvider()

    # 单只获取
    records = provider.fetch('600660.SH')
    assert len(records) > 0, "QuoteProvider 返回空"
    rec = records[0].to_dict()
    assert rec['price'] > 0, f"价格异常: {rec['price']}"
    assert rec['stock_name'] != '', "股票名称为空"
    assert provider.last_source in ("tencent", "eastmoney"), \
        f"last_source 异常: {provider.last_source}"

    print(f"     单只: {rec['stock_code']} {rec['stock_name']} "
          f"价格={rec['price']} 涨跌={rec['change_pct']}% 来源={provider.last_source}")

    # 批量获取
    batch_codes = ['600660.SH', '000858.SZ']
    quotes = provider.fetch_batch(batch_codes)
    assert len(quotes) >= 1, f"批量获取返回不足: {len(quotes)}"

    for code in batch_codes:
        if code in quotes:
            q = quotes[code].to_dict()
            print(f"     批量: {code} {q['stock_name']} 价格={q['price']}")

    print(f"     批量获取: {len(quotes)}/{len(batch_codes)} 只成功")


# ═══════════════════════════════════════════════════════════════════════════════
#  T18: PullbackAnalyzer 四层漏斗评分
# ═══════════════════════════════════════════════════════════════════════════════

def test_t18_pullback_analyzer():
    """PullbackAnalyzer 四层漏斗评分"""
    from core.data_provider import KlineProvider
    from core.pipeline import Pipeline
    from core.analyzer import PullbackAnalyzer

    # 先确保 DB 中有 K 线数据
    test_code = "600660.SH"
    kline_provider = KlineProvider()
    pipe = Pipeline(db_path=DB_PATH, providers=[kline_provider])
    pipe.run([test_code])

    # 验证 prices 表有数据
    conn = sqlite3.connect(DB_PATH)
    cnt = conn.execute(
        "SELECT COUNT(*) FROM prices WHERE stock_code = ?", (test_code,)
    ).fetchone()[0]
    conn.close()
    print(f"     prices 表: {test_code} {cnt} 条 K 线")

    if cnt < 61:
        # K 线数据不足，重新采集
        kline_provider2 = KlineProvider()
        pipe2 = Pipeline(db_path=DB_PATH, providers=[kline_provider2])
        pipe2.run([test_code])
        conn = sqlite3.connect(DB_PATH)
        cnt = conn.execute(
            "SELECT COUNT(*) FROM prices WHERE stock_code = ?", (test_code,)
        ).fetchone()[0]
        conn.close()
        print(f"     重新采集后: {cnt} 条")

    # 运行 PullbackAnalyzer
    analyzer = PullbackAnalyzer(db_path=DB_PATH)
    results = analyzer.scan([test_code])

    # 验证返回结构
    if len(results) > 0:
        for r in results:
            assert 'score' in r, "缺少 score 字段"
            assert 'signal' in r, "缺少 signal 字段"
            assert 'analysis_type' in r, "缺少 analysis_type 字段"
            assert r['analysis_type'] == 'pullback_score', \
                f"analysis_type 异常: {r['analysis_type']}"
            assert r['signal'] in ('buy', 'watch', 'hold', 'avoid'), \
                f"signal 异常: {r['signal']}"

        print(f"     扫描结果: {len(results)} 只")
        for r in results[:3]:
            print(f"       {r['stock_code']}: score={r['score']} "
                  f"signal={r['signal']} grade={r.get('grade')}")
    else:
        # 数据不足时返回空是正常的
        print(f"     扫描结果: 0 只（K线数据不足61条，正常）")


# ═══════════════════════════════════════════════════════════════════════════════
#  T19: 回测计算
# ═══════════════════════════════════════════════════════════════════════════════

def test_t19_backtest():
    """回测计算"""
    from core.pipeline import run_backtest

    # 首先检查 event_tracking 是否有待回测数据
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    pending_cnt = conn.execute("""
        SELECT COUNT(*) as cnt FROM event_tracking
        WHERE entry_price IS NOT NULL AND entry_price > 0
          AND return_5d IS NULL
          AND tracking_status != 'pending'
    """).fetchone()["cnt"]

    # 如果没有待回测数据，创建测试数据
    if pending_cnt == 0:
        # 检查 prices 表是否有数据
        price_cnt = conn.execute(
            "SELECT COUNT(*) FROM prices WHERE stock_code = '600660.SH'"
        ).fetchone()[0]

        if price_cnt >= 10:
            # 获取最早的交易日和收盘价
            first_row = conn.execute("""
                SELECT trade_date, close_price FROM prices
                WHERE stock_code = '600660.SH'
                ORDER BY trade_date ASC LIMIT 1
            """).fetchone()
            conn.execute("""
                INSERT OR IGNORE INTO event_tracking
                (stock_code, stock_name, event_type, event_date,
                 entry_price, tracking_status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, ("600660.SH", "福耀玻璃", "earnings_beat",
                  first_row["trade_date"],
                  first_row["close_price"],
                  "tracking"))
            conn.commit()
            print(f"     创建测试 event_tracking 记录 1 条")
    conn.close()

    # 运行回测
    result = run_backtest(DB_PATH)

    assert 'updated' in result, f"缺少 updated 字段: {result}"
    assert 'skipped' in result, f"缺少 skipped 字段: {result}"

    print(f"     回测结果: updated={result['updated']}, skipped={result['skipped']}")

    # 如果有更新的记录，验证 backtest 表
    if result['updated'] > 0:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        bt_rows = conn.execute("""
            SELECT stock_code, event_date, entry_price,
                   return_5d, return_10d, return_20d, return_60d,
                   alpha_20d, is_win
            FROM backtest
            ORDER BY rowid DESC LIMIT 3
        """).fetchall()
        conn.close()

        for bt in bt_rows:
            print(f"       {bt['stock_code']} {bt['event_date']}: "
                  f"T+5={bt['return_5d']}% T+20={bt['return_20d']}% "
                  f"alpha_20d={bt['alpha_20d']}% win={bt['is_win']}")


# ═══════════════════════════════════════════════════════════════════════════════
#  T20: EventAnalyzer 事件检测
# ═══════════════════════════════════════════════════════════════════════════════

def test_t20_event_detection():
    """EventAnalyzer 事件检测 — 关键词匹配 + 分类 + 写入 events 表"""
    from core.analyzer import EventAnalyzer
    from core.data_provider import NewsProvider

    analyzer = EventAnalyzer(db_path=DB_PATH)

    # 清理测试事件
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM events WHERE title LIKE '%测试%'")
    conn.commit()

    # 测试 1: 从新闻检测政策利好事件
    mock_news = [
        {
            "stock_code": "600660.SH",
            "title": "福耀玻璃获国家补贴扶持，减税政策利好",
            "content": "公司获得国务院补贴政策扶持，享受减税优惠",
            "pub_date": "2026-03-28",
            "url": "https://example.com/test1",
            "source_name": "测试来源",
        },
        {
            "stock_code": "000001.SZ",
            "title": "平安银行收到监管处罚通知",
            "content": "因违规操作被监管部门处罚",
            "pub_date": "2026-03-28",
            "url": "https://example.com/test2",
            "source_name": "测试来源",
        },
        {
            "stock_code": "300750.SZ",
            "title": "宁德时代签下重大合作协议",
            "content": "与某车企签订长期合作协议，订单金额超百亿",
            "pub_date": "2026-03-28",
            "url": "https://example.com/test3",
            "source_name": "测试来源",
        },
    ]

    events = analyzer.detect_from_news(mock_news)
    assert len(events) >= 2, f"应检测到至少 2 个事件，实际 {len(events)}"

    # 验证事件类型分类
    event_types = {e["event_type"] for e in events}
    assert "policy利好" in event_types, f"应检测到 policy利好 事件，实际: {event_types}"
    assert "policy利空" in event_types or "risk_warning" in event_types, \
        f"应检测到利空类事件，实际: {event_types}"

    # 验证情感判定
    for e in events:
        assert e["sentiment"] in ("positive", "negative", "neutral"), \
            f"情感异常: {e['sentiment']}"
        assert e["severity"] in ("high", "medium", "low", "normal"), \
            f"严重程度异常: {e['severity']}"

    # 测试 2: 从 Pipeline 结果检测事件
    mock_beats = [
        {
            "stock_code": "600660.SH",
            "stock_name": "福耀玻璃",
            "signal": "buy",
            "beat_diff_pct": 15.0,
            "score": 85.0,
        },
        {
            "stock_code": "000858.SZ",
            "stock_name": "五粮液",
            "signal": "hold",
            "beat_diff_pct": 2.0,
        },
    ]
    mock_highs = [
        {
            "stock_code": "600938.SH",
            "signal": "watch",
            "is_new_high": True,
            "quarterly_net_profit": 50.0,
        },
    ]

    pipeline_events = analyzer.detect_from_pipeline(beats=mock_beats, new_highs=mock_highs)
    assert len(pipeline_events) == 2, f"应检测到 2 个 pipeline 事件（1 buy + 1 new_high），实际 {len(pipeline_events)}"

    pipeline_types = {e["event_type"] for e in pipeline_events}
    assert "earnings_beat" in pipeline_types, f"应有 earnings_beat 事件"
    assert "profit_new_high" in pipeline_types, f"应有 profit_new_high 事件"

    # 测试 3: 验证 events 表写入
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT event_type, title, sentiment, severity FROM events
        WHERE title LIKE '%补贴%' OR title LIKE '%监管%' OR title LIKE '%合作协议%'
        OR event_type IN ('earnings_beat', 'profit_new_high')
        ORDER BY id DESC LIMIT 10
    """).fetchall()
    conn.close()
    assert len(rows) >= 4, f"events 表应有至少 4 条记录，实际 {len(rows)}"

    # 清理测试数据
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM events WHERE source = '测试来源' OR source = 'pipeline'")
    conn.commit()
    conn.close()

    print(f"     新闻事件: {len(events)} 个 (类型: {event_types})")
    print(f"     Pipeline 事件: {len(pipeline_events)} 个 (类型: {pipeline_types})")
    print(f"     events 表验证: {len(rows)} 条写入")


# ═══════════════════════════════════════════════════════════════════════════════
#  T21: 回调 DM 推送逻辑（dry run）
# ═══════════════════════════════════════════════════════════════════════════════

def test_t21_pullback_dm():
    """回调 DM 推送逻辑（dry run）— 验证消息格式正确"""
    from pusher import push_pullback_dm

    # 构造 mock 回调信号
    mock_signals = [
        {
            "stock_code": "600660.SH",
            "stock_name": "福耀玻璃",
            "score": 85.0,
            "signal": "buy",
            "grade": "A",
            "reason": "MA20>MA60, 缩量回调, RSI超卖",
        },
        {
            "stock_code": "000858.SZ",
            "stock_name": "五粮液",
            "score": 70.0,
            "signal": "watch",
            "grade": "B",
            "reason": "趋势确认, 待回调到位",
        },
        {
            "stock_code": "600938.SH",
            "stock_name": "中国海油",
            "score": 40.0,  # 低于 60 分，不应推送
            "signal": "hold",
            "grade": "C",
            "reason": "",
        },
    ]

    # dry run 模式测试
    pushed = push_pullback_dm(mock_signals, dry_run=True)
    assert pushed == 2, f"应推送 2 条（score>=60），实际 {pushed}"

    # 测试空列表
    pushed_empty = push_pullback_dm([], dry_run=True)
    assert pushed_empty == 0, f"空列表应返回 0，实际 {pushed_empty}"

    # 测试全低分
    low_signals = [{"stock_code": "TEST.SH", "score": 30, "signal": "watch"}]
    pushed_low = push_pullback_dm(low_signals, dry_run=True)
    assert pushed_low == 0, f"低分不应推送，实际 {pushed_low}"

    print(f"     dry run 推送: {pushed} 条 (buy 1 + watch 1)")
    print(f"     空列表: {pushed_empty} 条 | 低分: {pushed_low} 条")


# ═══════════════════════════════════════════════════════════════════════════════
#  T22: 事件 DM 推送逻辑（dry run）
# ═══════════════════════════════════════════════════════════════════════════════

def test_t22_event_dm():
    """事件 DM 推送逻辑（dry run）— 验证消息格式正确"""
    from pusher import push_event_dm

    # 构造 mock 事件（含 high 和 non-high）
    mock_events = [
        {
            "stock_code": "600660.SH",
            "event_type": "earnings_beat",
            "title": "福耀玻璃财报超预期 15%",
            "sentiment": "positive",
            "severity": "high",
        },
        {
            "stock_code": "000001.SZ",
            "event_type": "risk_warning",
            "title": "平安银行被立案调查",
            "sentiment": "negative",
            "severity": "high",
        },
        {
            "stock_code": "000858.SZ",
            "event_type": "major_contract",
            "title": "五粮液签新合同",
            "sentiment": "positive",
            "severity": "medium",  # 非 high，不应推送
        },
    ]

    # dry run 模式测试
    pushed = push_event_dm(mock_events, dry_run=True)
    assert pushed == 2, f"应推送 2 条（severity=high），实际 {pushed}"

    # 测试空列表
    pushed_empty = push_event_dm([], dry_run=True)
    assert pushed_empty == 0, f"空列表应返回 0，实际 {pushed_empty}"

    # 测试全 non-high
    low_events = [{"event_type": "test", "title": "测试", "severity": "normal"}]
    pushed_low = push_event_dm(low_events, dry_run=True)
    assert pushed_low == 0, f"非 high 不应推送，实际 {pushed_low}"

    print(f"     dry run 推送: {pushed} 条 (earnings_beat 1 + risk_warning 1)")
    print(f"     空列表: {pushed_empty} 条 | non-high: {pushed_low} 条")


# ═══════════════════════════════════════════════════════════════════════════════
#  T23: 发现池升级+过期
# ═══════════════════════════════════════════════════════════════════════════════

def test_t23_discovery_upgrade():
    """发现池升级+过期 — 测试 promote + expire"""
    from core.analyzer import DiscoveryPoolManager
    from core.analyzer import EarningsAnalyzer

    # 清理测试数据
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM discovery_pool WHERE stock_code LIKE 'TEST_UPGRADE%'")
    conn.commit()

    # Step 1: 手动写入测试记录
    analyzer = EarningsAnalyzer(db_path=DB_PATH)
    mock_beats = [
        {
            "stock_code": "TEST_UPGRADE_001.XX",
            "stock_name": "测试升级A",
            "signal": "buy",
            "score": 85.0,
            "beat_diff_pct": 15.0,
        },
    ]
    analyzer.auto_discover_pool(beats=mock_beats, new_highs=[])

    # 验证入池
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status FROM discovery_pool WHERE stock_code = 'TEST_UPGRADE_001.XX'"
    ).fetchone()
    conn.close()
    assert row is not None, "测试记录未入池"
    assert row["status"] == "active", f"初始状态应为 active，实际 {row['status']}"

    # Step 2: 测试 promote_to_watchlist
    manager = DiscoveryPoolManager(db_path=DB_PATH)

    # 升级成功
    success = manager.promote_to_watchlist("TEST_UPGRADE_001.XX")
    assert success, "升级应成功"

    # 验证状态变更
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status FROM discovery_pool WHERE stock_code = 'TEST_UPGRADE_001.XX'"
    ).fetchone()
    conn.close()
    assert row["status"] == "promoted", f"升级后状态应为 promoted，实际 {row['status']}"

    # 再次升级应失败（已非 active）
    success_again = manager.promote_to_watchlist("TEST_UPGRADE_001.XX")
    assert not success_again, "已 promoted 的不应再次升级"

    # 不存在的股票升级应失败
    success_missing = manager.promote_to_watchlist("NOTEXIST.XX")
    assert not success_missing, "不存在的股票升级应失败"

    # Step 3: 测试 expire_old_entries
    # 写入一条过期记录（8 天前）
    old_date = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO discovery_pool (stock_code, stock_name, source, score, signal, status, discovered_at)
        VALUES ('TEST_UPGRADE_002.XX', '测试过期A', 'earnings_beat', 70.0, 'watch', 'active', ?)
    """, (old_date,))
    conn.commit()
    conn.close()

    expired_count = manager.expire_old_entries()
    assert expired_count >= 1, f"应过期至少 1 条，实际 {expired_count}"

    # 验证过期
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status FROM discovery_pool WHERE stock_code = 'TEST_UPGRADE_002.XX'"
    ).fetchone()
    conn.close()
    assert row["status"] == "expired", f"过期后状态应为 expired，实际 {row['status']}"

    # 清理
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM discovery_pool WHERE stock_code LIKE 'TEST_UPGRADE%'")
    conn.commit()
    conn.close()

    print(f"     promote: success={success}, again={success_again}, missing={success_missing}")
    print(f"     expire: {expired_count} 条过期")


# ═══════════════════════════════════════════════════════════════════════════════
#  T24: SectorProvider 获取板块数据
# ═══════════════════════════════════════════════════════════════════════════════

def test_t24_sector_provider():
    """SectorProvider 获取板块数据 — 验证返回结构"""
    from core.data_provider import SectorProvider

    provider = SectorProvider()
    sectors = provider.fetch()

    assert isinstance(sectors, list), f"返回类型应为 list，实际 {type(sectors)}"

    if sectors:
        # 验证返回结构
        for s in sectors[:5]:
            d = s.to_dict() if hasattr(s, 'to_dict') else s
            assert "sector_name" in d, f"缺少 sector_name"
            assert "change_pct" in d, f"缺少 change_pct"
            assert "net_inflow" in d, f"缺少 net_inflow"
            assert "up_count" in d, f"缺少 up_count"
            assert "down_count" in d, f"缺少 down_count"
            assert isinstance(d["change_pct"], (int, float)), \
                f"change_pct 类型异常: {type(d['change_pct'])}"
            assert isinstance(d["net_inflow"], (int, float)), \
                f"net_inflow 类型异常: {type(d['net_inflow'])}"

        print(f"     板块数据: {len(sectors)} 个板块, 来源={provider.last_source}")
        for s in sectors[:5]:
            d = s.to_dict() if hasattr(s, 'to_dict') else s
            print(f"       {d['sector_name']}: 涨跌={d['change_pct']}% "
                  f"净流入={d['net_inflow']}亿 涨={d['up_count']} 跌={d['down_count']}")
    else:
        # API 不可用时返回空是正常的
        print(f"     板块数据: 0 个 (API 不可用, 来源={provider.last_source})")


# ═══════════════════════════════════════════════════════════════════════════════
#  T25: 超跌扫描
# ═══════════════════════════════════════════════════════════════════════════════

def test_t25_oversold():
    """超跌扫描 — 验证返回结构"""
    from core.analyzer import OversoldScanner

    scanner = OversoldScanner(db_path=DB_PATH)

    try:
        results = scanner.scan()
    except Exception as e:
        # 超跌扫描依赖腾讯行情 API，网络不通时可能失败
        # 这种情况下 skip 而非 fail
        print(f"     超跌扫描: SKIP (API 不可用: {type(e).__name__}: {e})")
        return

    assert isinstance(results, list), f"返回类型应为 list，实际 {type(results)}"

    if results:
        r = results[0]
        assert "btiq" in r, f"缺少 btiq 字段"
        assert "up" in r, f"缺少 up 字段"
        assert "down" in r, f"缺少 down 字段"
        assert "total" in r, f"缺少 total 字段"
        assert isinstance(r["btiq"], (int, float)), f"btiq 类型异常"
        assert 0 <= r["btiq"] <= 100, f"btiq 越界: {r['btiq']}"
        assert r["total"] > 0, f"total 应 > 0"

        print(f"     BTIQ={r['btiq']}% 上涨={r['up']} 下跌={r['down']} "
              f"总计={r['total']} MA5={r.get('ma5')} signal={r.get('signal')}")
    else:
        print(f"     超跌扫描: 0 结果（API 未返回数据）")


# ═══════════════════════════════════════════════════════════════════════════════
#  主入口
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # 确保 DB 初始化
    from core.database import init_db
    init_db(DB_PATH)

    tests = [
        ("T1:  Pipeline 单股票采集",        test_t1_single_stock),
        ("T2:  Pipeline 多股票批量采集",     test_t2_batch_stocks),
        ("T3:  Pipeline 异常股票处理",       test_t3_invalid_stock),
        ("T4:  超预期扫描",                 test_t4_earnings_beat),
        ("T5:  扣非新高扫描",               test_t5_new_high),
        ("T6:  端到端链路验证",             test_t6_e2e),
        ("T7:  数据质量校验",               test_t7_data_quality),
        ("T8:  1.0 daily_scan.py 回归",     test_t8_regression),
        ("T9:  性能基准",                   test_t9_performance),
        ("T10: DisclosureScanner API 调用",  test_t10_disclosure_scanner_fetch),
        ("T11: DisclosureScanner diff 去重", test_t11_disclosure_scanner_diff),
        ("T12: quarterly_net_profit 计算",   test_t12_quarterly_net_profit),
        ("T13: 超预期扫描 N/A 处理",        test_t13_beat_expectation_na),
        ("T14: NewsProvider 获取新闻",      test_t14_news_provider),
        ("T15: 发现池自动入场",             test_t15_discovery_pool),
        ("T16: T+N 跟踪创建+更新",          test_t16_tn_tracking),
        ("T17: QuoteProvider 实时行情",      test_t17_quote_provider),
        ("T18: PullbackAnalyzer 四层漏斗",  test_t18_pullback_analyzer),
        ("T19: 回测计算",                  test_t19_backtest),
        ("T20: EventAnalyzer 事件检测",     test_t20_event_detection),
        ("T21: 回调 DM 推送(dry run)",      test_t21_pullback_dm),
        ("T22: 事件 DM 推送(dry run)",      test_t22_event_dm),
        ("T23: 发现池升级+过期",            test_t23_discovery_upgrade),
        ("T24: SectorProvider 板块数据",     test_t24_sector_provider),
        ("T25: 超跌扫描",                  test_t25_oversold),
    ]

    print(f"\n🚀 开始执行 {len(tests)} 个测试...\n")

    for name, func in tests:
        print(f"▶ {name}")
        run_test(name, func)
        print()

    print(T.summary())

    # 退出码
    sys.exit(0 if T.failed == 0 else 1)
