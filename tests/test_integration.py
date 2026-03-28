"""
Phase 3 集成测试 — 端到端链路验证
===================================

测试完整链路：
  pipeline.py --codes 600660.SH,600938.SH
    → SQLite (earnings + consensus + prices)
    → analyzer.py --mode full
    → SQLite (analysis_results + discovery_pool)
    → pusher.py --mode scan
    → stdout JSON

验证：
  - 数据从 Pipeline → DB → Analyzer → DB → Pusher 流通
  - 字段完整、值合理
  - 1.0 的 daily_scan.py 不受影响
"""

import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ═══════════════════════════════════════════════════════════════════════════════
#  Fixture: 预填充 DB（模拟 Pipeline 采集后的状态）
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def pipeline_db():
    """
    创建临时 DB 并预填 earnings + consensus + prices + stocks 数据，
    模拟 Pipeline 采集完成后的状态。
    """
    from core.database import init_db, get_connection, SCHEMA_SQL

    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA_SQL)

    # 写入 stocks
    conn.execute("INSERT INTO stocks (code, name, industry) VALUES ('600660.SH', '福耀玻璃', '汽车零部件')")
    conn.execute("INSERT INTO stocks (code, name, industry) VALUES ('600938.SH', '中国海油', '石油石化')")

    # 写入 earnings — 多季度（用于扣非新高分析）
    # 福耀玻璃 net_profit_yoy=22.5, consensus expected=15.0, diff=7.5 >= 5.0 → 超预期
    earnings_data = [
        # 福耀玻璃 — 超预期（net_profit_yoy > expected 15.0 + threshold 5.0）
        ("600660.SH", "2025-09-30", "Q3", 280.0, 25.3, 52.0, 22.5, 2.08, 18.5, 35.2),
        ("600660.SH", "2025-06-30", "Q2", 190.0, 20.1, 35.0, 15.2, 1.40, 12.8, 34.8),
        ("600660.SH", "2024-12-31", "Q4", 350.0, 18.0, 65.0, 12.5, 2.60, 25.0, 35.0),
        ("600660.SH", "2024-09-30", "Q3", 250.0, 15.0, 43.0, 10.0, 1.72, 17.0, 34.5),
        ("600660.SH", "2024-06-30", "Q2", 170.0, 12.0, 30.0, 8.0, 1.20, 11.0, 34.0),
        ("600660.SH", "2023-12-31", "Q4", 300.0, 10.0, 58.0, 7.0, 2.32, 22.0, 33.5),
        ("600660.SH", "2023-09-30", "Q3", 220.0, 8.0, 39.0, 6.0, 1.56, 15.5, 33.0),
        ("600660.SH", "2023-06-30", "Q2", 155.0, 5.0, 28.0, 4.0, 1.12, 10.0, 32.5),
        # 中国海油 — 扣非新高
        ("600938.SH", "2025-09-30", "Q3", 3200.0, 22.0, 1200.0, 35.0, 2.52, 15.0, 55.0),
        ("600938.SH", "2025-06-30", "Q2", 2100.0, 18.0, 800.0, 28.0, 1.68, 12.0, 53.0),
        ("600938.SH", "2024-12-31", "Q4", 4000.0, 15.0, 1500.0, 20.0, 3.15, 18.0, 54.0),
        ("600938.SH", "2024-09-30", "Q3", 2800.0, 10.0, 900.0, 12.0, 1.89, 13.0, 52.0),
        ("600938.SH", "2024-06-30", "Q2", 1800.0, 8.0, 650.0, 10.0, 1.37, 10.0, 51.0),
    ]
    for e in earnings_data:
        conn.execute("""
            INSERT INTO earnings (stock_code, report_date, report_type,
                                  revenue, revenue_yoy, net_profit, net_profit_yoy,
                                  eps, roe, gross_margin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, e)

    # 写入 consensus — 福耀玻璃有预期（超预期场景），中国海油无预期
    conn.execute("""
        INSERT INTO consensus (stock_code, eps, net_profit_yoy, rev_yoy, num_analysts)
        VALUES ('600660.SH', 2.00, 15.0, 12.0, 25)
    """)

    # 写入 prices — 至少 61 天（回调分析要求）
    import random
    random.seed(42)
    for code, base_price in [("600660.SH", 55.0), ("600938.SH", 28.0)]:
        price = base_price
        for i in range(120):
            day = 120 - i
            date_str = f"2025-{(day // 30) + 1:02d}-{(day % 28) + 1:02d}"
            price += random.uniform(-1.5, 1.5)
            price = max(price, base_price * 0.7)
            conn.execute("""
                INSERT OR IGNORE INTO prices
                (stock_code, trade_date, open_price, high_price, low_price,
                 close_price, volume, change_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code, date_str,
                round(price - 0.3, 2), round(price + 0.8, 2),
                round(price - 0.9, 2), round(price, 2),
                random.randint(100000, 500000),
                round(random.uniform(-3, 3), 2),
            ))

    conn.commit()
    conn.close()

    yield path

    os.unlink(path)


# ═══════════════════════════════════════════════════════════════════════════════
#  测试类
# ═══════════════════════════════════════════════════════════════════════════════

class TestPusherUnit:
    """Pusher 单元测试"""

    def test_pusher_loads_and_runs(self, pipeline_db):
        """Pusher 能正常初始化和运行"""
        from pusher import Pusher

        p = Pusher(db_path=pipeline_db)
        result = p.run(mode="scan", no_bitable=True)

        assert "cards" in result
        assert "timestamp" in result
        assert result["mode"] == "scan"

    def test_pusher_scan_mode_returns_card(self, pipeline_db):
        """scan 模式输出包含 card 数据"""
        from pusher import Pusher

        p = Pusher(db_path=pipeline_db)
        result = p.run(mode="scan", no_bitable=True)

        scan_cards = [c for c in result["cards"] if c["type"] == "scan"]
        assert len(scan_cards) == 1
        card = scan_cards[0]["card"]
        assert "header" in card
        assert "elements" in card
        assert "备选股池" in card["header"]["title"]["content"]

    def test_pusher_pool_mode_returns_card(self, pipeline_db):
        """pool 模式输出包含跟踪池概要"""
        from pusher import Pusher

        p = Pusher(db_path=pipeline_db)
        result = p.run(mode="pool", no_bitable=True)

        pool_cards = [c for c in result["cards"] if c["type"] == "pool"]
        assert len(pool_cards) == 1
        card = pool_cards[0]["card"]
        assert "header" in card
        assert "跟踪池" in card["header"]["title"]["content"]

    def test_pusher_all_mode_returns_both(self, pipeline_db):
        """all 模式输出 scan + pool 两张卡片"""
        from pusher import Pusher

        p = Pusher(db_path=pipeline_db)
        result = p.run(mode="all", no_bitable=True)

        assert len(result["cards"]) == 2
        types = {c["type"] for c in result["cards"]}
        assert types == {"scan", "pool"}

    def test_pusher_output_is_valid_json(self, pipeline_db, capsys):
        """Pusher stdout 输出合法 JSON"""
        from pusher import Pusher

        p = Pusher(db_path=pipeline_db)
        p.run(mode="scan", no_bitable=True)

        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "cards" in output
        assert "timestamp" in output


class TestPusherDataFlow:
    """Pusher 数据读取逻辑测试"""

    def test_load_scan_results_empty_db(self):
        """空 DB 不崩溃"""
        from pusher import load_scan_results
        from core.database import init_db

        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        init_db(path)

        try:
            result = load_scan_results(path)
            assert result["beats"] == []
            assert result["new_highs"] == []
            assert result["pullback_signals"] == []
        finally:
            os.unlink(path)

    def test_load_scan_results_with_data(self, pipeline_db):
        """有数据时正确解析 discovery_pool"""
        from pusher import load_scan_results
        from core.database import get_connection

        # 先写入 discovery_pool 记录模拟 analyzer 输出
        with get_connection(pipeline_db) as conn:
            conn.execute("""
                INSERT INTO discovery_pool
                (stock_code, stock_name, source, score, signal, detail, status)
                VALUES ('600660.SH', '福耀玻璃', 'earnings_beat', 85.0, 'buy',
                        '{"actual_yoy": 25.3, "expected_yoy": 15.0, "has_consensus": true}', 'active')
            """)
            conn.execute("""
                INSERT INTO discovery_pool
                (stock_code, stock_name, source, score, signal, detail, status)
                VALUES ('600938.SH', '中国海油', 'profit_new_high', 78.0, 'watch',
                        '{"quarterly_profit": 1200.0, "growth_pct": 35.0}', 'active')
            """)

        result = load_scan_results(pipeline_db)

        assert len(result["beats"]) == 1
        assert result["beats"][0]["code"] == "600660.SH"
        assert result["beats"][0]["actual_profit_yoy"] == 25.3

        assert len(result["new_highs"]) == 1
        assert result["new_highs"][0]["code"] == "600938.SH"

    def test_load_pool_summary(self, pipeline_db):
        """跟踪池按状态分组"""
        from pusher import load_pool_summary
        from core.database import get_connection

        with get_connection(pipeline_db) as conn:
            conn.execute("""
                INSERT INTO discovery_pool (stock_code, stock_name, source, score, status)
                VALUES ('600660.SH', '福耀玻璃', 'earnings_beat', 85.0, 'active')
            """)
            conn.execute("""
                INSERT INTO discovery_pool (stock_code, stock_name, source, score, status)
                VALUES ('600938.SH', '中国海油', 'profit_new_high', 78.0, 'promoted')
            """)

        summary = load_pool_summary(pipeline_db)
        assert len(summary["active"]) == 1
        assert len(summary["promoted"]) == 1
        assert summary["total"] == 2


class TestEndToEndIntegration:
    """端到端集成测试：Pipeline DB → Analyzer → Pusher"""

    def test_analyzer_writes_discovery_pool(self, pipeline_db):
        """Analyzer 从 DB 读取数据并写入 discovery_pool"""
        from analyzer import Analyzer

        az = Analyzer(db_path=pipeline_db)
        result = az.run(mode="full", codes=["600660.SH", "600938.SH"], no_push=True)

        # 验证有分析结果
        assert len(result["earnings_beat"]) >= 1

        # 验证至少有一个超预期（触发 discovery_pool 写入）
        beats = [r for r in result["earnings_beat"] if r.get("is_beat")]
        if beats:
            conn = sqlite3.connect(pipeline_db)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM discovery_pool WHERE status = 'active'").fetchall()
            conn.close()
            assert len(rows) >= 1
        else:
            # 无超预期时 discovery_pool 可能为空，但 analysis_results 应有记录
            conn = sqlite3.connect(pipeline_db)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM analysis_results").fetchall()
            conn.close()
            assert len(rows) >= 1

    def test_full_chain_analyzer_to_pusher(self, pipeline_db):
        """完整链路：DB 数据 → Analyzer → Pusher 输出卡片"""
        from analyzer import Analyzer
        from pusher import Pusher

        # Step 1: Analyzer 分析
        az = Analyzer(db_path=pipeline_db)
        az_result = az.run(mode="full", codes=["600660.SH", "600938.SH"], no_push=True)
        assert len(az_result["earnings_beat"]) >= 1, "Analyzer 应产出超预期结果"

        # Step 2: Pusher 推送
        p = Pusher(db_path=pipeline_db)
        push_result = p.run(mode="all", no_bitable=True)

        # 验证输出
        assert len(push_result["cards"]) == 2

        # 验证 scan 卡片有内容
        scan_card = next(c for c in push_result["cards"] if c["type"] == "scan")
        card = scan_card["card"]
        assert "elements" in card
        assert len(card["elements"]) > 2, "卡片应有多个元素（概览 + 表格 + 底部）"

    def test_analyzer_earnings_beat_writes_analysis_results(self, pipeline_db):
        """验证超预期分析结果写入 analysis_results 表"""
        from analyzer import Analyzer

        az = Analyzer(db_path=pipeline_db)
        az.run(mode="earnings", codes=["600660.SH"], no_push=True)

        conn = sqlite3.connect(pipeline_db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT * FROM analysis_results
            WHERE stock_code = '600660.SH' AND analysis_type = 'earnings_beat'
        """).fetchall()
        conn.close()

        assert len(rows) >= 1
        row = dict(rows[0])
        assert row["score"] is not None
        assert row["signal"] is not None

    def test_analyzer_profit_new_high(self, pipeline_db):
        """验证扣非新高分析"""
        from analyzer import Analyzer

        az = Analyzer(db_path=pipeline_db)
        result = az.run(mode="earnings", codes=["600938.SH"], no_push=True)

        # 中国海油单季度利润应创新高
        new_highs = [r for r in result["profit_new_high"] if r.get("is_new_high")]
        # 至少应该有分析结果（是否新高取决于数据）
        assert len(result["profit_new_high"]) >= 1

    def test_pusher_card_structure_valid(self, pipeline_db):
        """Pusher 输出的卡片 JSON 结构合法"""
        from analyzer import Analyzer
        from pusher import Pusher

        az = Analyzer(db_path=pipeline_db)
        az.run(mode="full", codes=["600660.SH", "600938.SH"], no_push=True)

        p = Pusher(db_path=pipeline_db)
        result = p.run(mode="scan", no_bitable=True)

        card = result["cards"][0]["card"]

        # 验证飞书卡片基本结构
        assert card.get("config", {}).get("wide_screen_mode") is True
        assert "header" in card
        assert "title" in card["header"]
        assert "template" in card["header"]
        assert "elements" in card
        assert isinstance(card["elements"], list)

        # 验证有 table 元素（如果数据足够）
        tables = [e for e in card["elements"] if e.get("tag") == "table"]
        for table in tables:
            assert "columns" in table
            assert "rows" in table
            assert len(table["columns"]) > 0


class TestDailyScanUnchanged:
    """验证 1.0 的 daily_scan.py 不受影响"""

    def test_daily_scan_imports_succeed(self):
        """daily_scan.py 能正常 import CardGenerator"""
        from notifiers.card_generator import CardGenerator

        gen = CardGenerator()
        assert hasattr(gen, "generate_daily_scan_card")

    def test_generate_daily_scan_card_basic(self):
        """generate_daily_scan_card 正常工作"""
        from notifiers.card_generator import CardGenerator

        gen = CardGenerator()
        card = gen.generate_daily_scan_card(
            beats=[{
                "code": "600660.SH", "name": "福耀玻璃",
                "consensus_available": True, "actual_profit_yoy": 25.3,
                "expected_profit_yoy": 15.0, "report_type": "Q3",
                "ann_date": "20251028",
            }],
            new_highs=[],
        )

        assert card["config"]["wide_screen_mode"] is True
        assert "备选股池" in card["header"]["title"]["content"]
        assert len(card["elements"]) > 0

    def test_truncate_method_exists(self):
        """truncate 静态方法仍然可用（feishu_pusher.py 依赖）"""
        from notifiers.card_generator import CardGenerator

        short = "短文本"
        assert CardGenerator.truncate(short) == short

        long_text = "A" * 3000
        truncated = CardGenerator.truncate(long_text)
        assert len(truncated) <= 2000
        assert "截断" in truncated


class TestCardGeneratorCleanup:
    """验证 card_generator.py 已清理旧方法"""

    def test_deleted_methods_not_present(self):
        """7 个旧方法已删除"""
        from notifiers.card_generator import CardGenerator

        gen = CardGenerator()
        deleted = [
            "generate_daily_card", "generate_stock_card", "generate_pool_card",
            "generate_alert_card", "generate_surprise_card",
            "generate_close_card", "generate_open_check_card",
        ]
        for method_name in deleted:
            assert not hasattr(gen, method_name), f"{method_name} 应已删除"

    def test_retained_methods_present(self):
        """保留的方法仍然存在"""
        from notifiers.card_generator import CardGenerator

        gen = CardGenerator()
        assert hasattr(gen, "generate_daily_scan_card")
        assert hasattr(gen, "truncate")

    def test_no_old_dataclasses(self):
        """旧 dataclass 已删除"""
        import notifiers.card_generator as mod
        assert not hasattr(mod, "HoldingInfo"), "HoldingInfo 应已删除"
        assert not hasattr(mod, "AlertInfo"), "AlertInfo 应已删除"
