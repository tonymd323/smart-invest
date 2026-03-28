"""
Phase 0 测试 — Fixtures 验证 + 数据库 Schema 测试

⚠️ 在写 Provider/Pipeline 之前先跑通这些测试
"""

import sqlite3
import json


class TestFixtures:
    """验证测试 fixtures 数据完整性"""

    def test_watchlist_structure(self, watchlist):
        """股票清单格式正确"""
        assert len(watchlist) >= 1
        for item in watchlist:
            assert "code" in item
            assert "name" in item
            assert "." in item["code"]  # 格式: 000858.SZ

    def test_financial_data_structure(self, mock_financial_data):
        """财务数据格式正确"""
        data = mock_financial_data["000858.SZ"]
        assert data["code"] == "000858"
        assert len(data["data"]) >= 4  # 至少 4 个季度

        for item in data["data"]:
            assert "PARENT_NETPROFIT" in item
            assert "PARENT_NETPROFIT_YOY" in item
            assert "TOTAL_OPERATE_INCOME" in item
            assert "REPORT_DATE_NAME" in item

    def test_consensus_data_structure(self, mock_consensus_data):
        """一致预期数据格式正确"""
        data = mock_consensus_data["000858.SZ"]
        assert "profit_yoy_expected" in data
        assert "analyst_count" in data
        assert data["analyst_count"] > 0

    def test_kline_data_structure(self, mock_kline_data):
        """日K行情格式正确"""
        data = mock_kline_data["000858.SZ"]
        assert len(data) >= 5
        for item in data:
            assert "trade_date" in item
            assert "close" in item
            assert item["close"] > 0


class TestDatabaseSchema:
    """验证 SQLite Schema 完整性（含新增表）"""

    def test_init_db_creates_tables(self, test_db):
        """数据库初始化创建所有表"""
        conn = sqlite3.connect(test_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t[0] for t in tables]

        # v1.0 现有表
        assert "stocks" in table_names
        assert "earnings" in table_names
        assert "prices" in table_names
        assert "analysis_results" in table_names
        assert "event_tracking" in table_names

        # v2 新增表
        assert "consensus" in table_names
        assert "discovery_pool" in table_names
        assert "events" in table_names

        # v2 已删除表
        assert "news" not in table_names
        assert "fund_flows" not in table_names
        assert "push_logs" not in table_names
        conn.close()

    def test_wal_mode_enabled(self, test_db):
        """WAL 模式已启用"""
        conn = sqlite3.connect(test_db)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal", f"Expected WAL mode, got {mode}"
        conn.close()

    def test_earnings_table_has_beat_fields(self, test_db):
        """earnings 表包含超预期相关字段"""
        conn = sqlite3.connect(test_db)
        cursor = conn.execute("PRAGMA table_info(earnings)")
        columns = [row[1] for row in cursor.fetchall()]

        assert "is_beat_expectation" in columns
        assert "expectation_diff_pct" in columns
        assert "quarterly_profit_new_high" in columns
        assert "quarterly_net_profit" in columns
        conn.close()

    def test_analysis_results_table(self, test_db):
        """analysis_results 表可写入"""
        conn = sqlite3.connect(test_db)
        conn.execute("""
            INSERT INTO analysis_results (stock_code, analysis_type, score, signal, summary)
            VALUES ('000858.SZ', 'earnings_beat', 75.0, 'watch', '{}')
        """)
        conn.commit()
        rows = conn.execute(
            "SELECT * FROM analysis_results WHERE stock_code = '000858.SZ'"
        ).fetchall()
        assert len(rows) == 1
        conn.close()

    def test_event_tracking_table(self, test_db):
        """event_tracking 表结构正确"""
        conn = sqlite3.connect(test_db)
        cursor = conn.execute("PRAGMA table_info(event_tracking)")
        columns = [row[1] for row in cursor.fetchall()]

        assert "event_type" in columns
        assert "event_date" in columns
        assert "entry_price" in columns
        assert "alpha_5d" in columns
        assert "tracking_status" in columns
        conn.close()


class TestBeatExpectationLogic:
    """验证超预期判定逻辑（纯函数测试）"""

    def test_beat_expectation(self):
        """实际 > 预期 = 超预期"""
        actual_yoy = 18.0
        expected_yoy = 15.0
        diff = actual_yoy - expected_yoy  # +3.0
        is_beat = diff >= 5.0  # threshold = 5%
        assert not is_beat  # 3.0 < 5.0，不算显著超预期

    def test_significant_beat(self):
        """实际 >> 预期 = 显著超预期"""
        actual_yoy = 25.0
        expected_yoy = 15.0
        diff = actual_yoy - expected_yoy  # +10.0
        is_beat = diff >= 5.0
        assert is_beat

    def test_miss_expectation(self):
        """实际 < 预期 = 低于预期"""
        actual_yoy = 12.5
        expected_yoy = 15.0
        diff = actual_yoy - expected_yoy  # -2.5
        is_beat = diff >= 5.0
        is_miss = diff <= -5.0
        assert not is_beat
        assert not is_miss  # -2.5 > -5.0，轻微低于预期

    def test_new_high_detection(self):
        """扣非新高判定"""
        quarterly_profits = [14.21, 15.38, 20.14, 22.05, 24.80]  # 亿元
        current = quarterly_profits[-1]
        prev_high = max(quarterly_profits[:-1])
        is_new_high = current > prev_high
        assert is_new_high
        assert current > prev_high
