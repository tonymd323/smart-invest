"""
Phase 1 测试 — ConsensusProvider / KlineProvider / 数据库新表 / BitableSync 合并
"""

import pytest
import sqlite3
import json
import os
from unittest.mock import patch, MagicMock


# ═══════════════════════════════════════════════════════════════════════════════
#  ConsensusProvider 测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestConsensusProvider:
    """验证一致预期 Provider"""

    def test_fetch_with_mock_data(self, consensus_provider):
        """使用 mock 数据验证 ConsensusProvider 输出"""
        from core.data_provider import ConsensusProvider

        result = consensus_provider.fetch("000858.SZ")

        assert result is not None
        assert result.stock_code == "000858.SZ"
        assert result.eps == 7.50
        assert result.net_profit_yoy == 15.0
        assert result.rev_yoy == 12.0
        assert result.num_analysts == 28
        assert result.source == "eastmoney_f10"
        assert consensus_provider.last_source == "eastmoney"

    def test_fetch_empty_returns_none(self):
        """无数据时返回 None"""
        from core.data_provider import ConsensusProvider

        provider = ConsensusProvider(data={})
        result = provider.fetch("999999.XX")
        assert result is None

    def test_fetch_all_stocks(self, consensus_provider):
        """测试多只股票"""
        from core.data_provider import ConsensusProvider

        for code in ["000858.SZ", "600519.SH", "300750.SZ"]:
            result = consensus_provider.fetch(code)
            assert result is not None
            assert result.num_analysts > 0

    def test_consensus_to_dict(self, consensus_provider):
        """ConsensusData → dict 格式正确"""
        result = consensus_provider.fetch("000858.SZ")
        d = result.to_dict()
        assert isinstance(d, dict)
        assert "stock_code" in d
        assert "eps" in d
        assert "net_profit_yoy" in d
        assert "num_analysts" in d

    def test_last_source_tracking(self):
        """验证 last_source 追踪"""
        from core.data_provider import ConsensusProvider

        provider = ConsensusProvider(data={})
        provider.fetch("999999.XX")
        assert provider.last_source == "none"

        provider2 = ConsensusProvider(data={"000858.SZ": {"eps": 5.0, "profit_yoy_expected": 10.0, "rev_yoy_expected": 8.0, "analyst_count": 5}})
        provider2.fetch("000858.SZ")
        assert provider2.last_source == "eastmoney"


# ═══════════════════════════════════════════════════════════════════════════════
#  KlineProvider 测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestKlineProvider:
    """验证日K行情 Provider"""

    def test_fetch_with_mock_data(self, kline_provider):
        """使用 mock 数据验证 KlineProvider 输出"""
        from core.data_provider import KlineProvider

        results = kline_provider.fetch("000858.SZ")

        assert len(results) >= 5
        for item in results:
            assert hasattr(item, "close_price")
            assert hasattr(item, "volume")
            assert item.close_price > 0
            assert item.source == "tushare"

    def test_fetch_empty_returns_empty_list(self):
        """无数据时返回空列表"""
        from core.data_provider import KlineProvider

        provider = KlineProvider(data={})
        results = provider.fetch("999999.XX")
        assert results == []

    def test_kline_to_dict(self, kline_provider):
        """KlineData → dict 格式正确"""
        results = kline_provider.fetch("000858.SZ")
        d = results[0].to_dict()
        assert isinstance(d, dict)
        assert "stock_code" in d
        assert "trade_date" in d
        assert "close_price" in d
        assert "volume" in d

    def test_kline_ohlcv_fields(self, kline_provider):
        """验证 OHLCV 字段完整性"""
        results = kline_provider.fetch("000858.SZ")
        item = results[0]
        assert item.open_price > 0
        assert item.high_price > 0
        assert item.low_price > 0
        assert item.close_price > 0
        assert item.high_price >= item.low_price
        assert item.volume > 0
        assert item.amount > 0

    def test_kline_data_class(self):
        """验证 KlineData dataclass"""
        from core.data_provider import KlineData

        kd = KlineData(
            stock_code="000858.SZ",
            trade_date="20260327",
            open_price=175.0,
            high_price=176.5,
            low_price=173.8,
            close_price=174.8,
            volume=3210000,
            amount=561000000,
            change_pct=-0.23,
            source="test",
        )
        assert kd.stock_code == "000858.SZ"
        d = kd.to_dict()
        assert d["close_price"] == 174.8


# ═══════════════════════════════════════════════════════════════════════════════
#  FinancialProvider 升级测试（v2 字段映射）
# ═══════════════════════════════════════════════════════════════════════════════

class TestFinancialProviderV2:
    """验证 FinancialProvider v2 字段映射"""

    def test_v2_field_mapping(self):
        """验证东方财富 API v2 字段名映射（无下划线命名）"""
        from core.data_provider import FinancialProvider

        em_data = {
            "600660.SH": {
                "data": [{
                    "SECURITY_CODE": "600660",
                    "REPORT_DATE": "2025-09-30 00:00:00",
                    "PARENTNETPROFIT": 5200000000.0,
                    "TOTALOPERATEREVE": 28000000000.0,
                    "PARENTNETPROFITTZ": 25.3,
                    "DJD_TOI_YOY": 18.7,
                    "ROEJQ": 18.5,
                    "XSMLL": 35.2,
                    "EPSJB": 2.08,
                }]
            }
        }
        provider = FinancialProvider(data=em_data)
        results = provider.fetch("600660.SH")

        assert len(results) == 1
        fd = results[0]
        assert fd.stock_code == "600660.SH"
        assert fd.report_date == "2025-09-30"
        assert fd.net_profit_yoy == 25.3
        assert fd.roe == 18.5
        assert fd.gross_margin == 35.2
        assert fd.eps == 2.08
        assert fd.source == "eastmoney"

    def test_fallback_source_tracking(self):
        """验证降级时 source 追踪"""
        from core.data_provider import FinancialProvider

        # 东财无数据，Tushare 有数据
        ts_data = {
            "000858.SZ": {
                "data": [{
                    "REPORT_DATE_NAME": "2025-09-30",
                    "PARENT_NETPROFIT": 24800000000,
                    "PARENT_NETPROFIT_YOY": 12.5,
                    "TOTAL_OPERATE_INCOME": 75000000000,
                    "TOTAL_OPERATE_INCOME_YOY": 10.2,
                    "WEIGHTAVG_ROE": 22.3,
                    "GROSS_PROFIT_RATIO": 78.5,
                    "EPS-basic": 6.39,
                }]
            }
        }
        provider = FinancialProvider(data={}, tushare_data=ts_data)
        results = provider.fetch("000858.SZ")

        assert len(results) >= 1
        assert provider.last_source == "tushare"
        assert results[0].source == "tushare"


# ═══════════════════════════════════════════════════════════════════════════════
#  数据库新表测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestDatabaseV2:
    """验证 v2 数据库新表"""

    def test_consensus_table_exists(self, test_db):
        """consensus 表已创建"""
        conn = sqlite3.connect(test_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "consensus" in table_names
        conn.close()

    def test_discovery_pool_table_exists(self, test_db):
        """discovery_pool 表已创建"""
        conn = sqlite3.connect(test_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "discovery_pool" in table_names
        conn.close()

    def test_events_table_exists(self, test_db):
        """events 表已创建"""
        conn = sqlite3.connect(test_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "events" in table_names
        conn.close()

    def test_old_tables_removed(self, test_db):
        """旧空表已删除"""
        conn = sqlite3.connect(test_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "news" not in table_names
        assert "fund_flows" not in table_names
        assert "push_logs" not in table_names
        conn.close()

    def test_consensus_table_crud(self, test_db):
        """consensus 表增删改查"""
        conn = sqlite3.connect(test_db)

        # INSERT
        conn.execute("""
            INSERT INTO consensus (stock_code, eps, net_profit_yoy, rev_yoy, num_analysts, source)
            VALUES ('000858.SZ', 7.5, 15.0, 12.0, 28, 'eastmoney')
        """)
        conn.commit()

        # SELECT
        row = conn.execute(
            "SELECT * FROM consensus WHERE stock_code = '000858.SZ'"
        ).fetchone()
        assert row is not None
        assert row[2] == 7.5  # eps
        assert row[5] == 28   # num_analysts

        # UPDATE
        conn.execute("""
            UPDATE consensus SET eps = 8.0, num_analysts = 30
            WHERE stock_code = '000858.SZ'
        """)
        conn.commit()
        row = conn.execute(
            "SELECT * FROM consensus WHERE stock_code = '000858.SZ'"
        ).fetchone()
        assert row[2] == 8.0
        assert row[5] == 30

        conn.close()

    def test_discovery_pool_table_crud(self, test_db):
        """discovery_pool 表增删改查"""
        conn = sqlite3.connect(test_db)

        conn.execute("""
            INSERT INTO discovery_pool (stock_code, stock_name, industry, source, score, signal)
            VALUES ('600660.SH', '福耀玻璃', '汽车', 'earnings_beat', 85.0, 'watch')
        """)
        conn.commit()

        row = conn.execute(
            "SELECT stock_code, stock_name, source, score, signal FROM discovery_pool WHERE stock_code = '600660.SH'"
        ).fetchone()
        assert row is not None
        assert row[0] == '600660.SH'
        assert row[2] == 'earnings_beat'  # source
        assert row[3] == 85.0  # score
        assert row[4] == 'watch'  # signal

        conn.close()

    def test_events_table_crud(self, test_db):
        """events 表增删改查"""
        conn = sqlite3.connect(test_db)

        conn.execute("""
            INSERT INTO events (stock_code, event_type, title, sentiment, severity)
            VALUES ('000858.SZ', 'earnings', '五粮液Q3净利润超预期', 'positive', 'important')
        """)
        conn.commit()

        row = conn.execute(
            "SELECT stock_code, event_type, title, sentiment, severity FROM events WHERE stock_code = '000858.SZ'"
        ).fetchone()
        assert row is not None
        assert row[0] == '000858.SZ'
        assert row[1] == 'earnings'   # event_type
        assert row[3] == 'positive'   # sentiment
        assert row[4] == 'important'  # severity

        conn.close()

    def test_existing_tables_still_work(self, test_db):
        """验证现有表不受影响"""
        conn = sqlite3.connect(test_db)

        # earnings 表仍然正常
        conn.execute("""
            INSERT INTO earnings (stock_code, report_date, net_profit)
            VALUES ('000858.SZ', '2025-09-30', 248.0)
        """)
        conn.commit()

        rows = conn.execute(
            "SELECT * FROM earnings WHERE stock_code = '000858.SZ'"
        ).fetchall()
        assert len(rows) == 1

        # analysis_results 表仍然正常
        conn.execute("""
            INSERT INTO analysis_results (stock_code, analysis_type, score, signal)
            VALUES ('000858.SZ', 'earnings_beat', 75.0, 'watch')
        """)
        conn.commit()

        rows = conn.execute(
            "SELECT * FROM analysis_results WHERE stock_code = '000858.SZ'"
        ).fetchall()
        assert len(rows) == 1

        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  BitableSync 合并测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestBitableSyncMerged:
    """验证合并后的 BitableSync"""

    def test_from_preset(self):
        """from_preset 预设配置"""
        from core.bitable_sync import BitableSync

        sync = BitableSync.from_preset('scan')
        assert sync.app_token == 'CvTRbdVyfa9PnMsnzIXcCSNmnnb'
        assert sync.table_id == 'tbluSQrjOW0tppTP'

    def test_extract_key(self):
        """复合键提取"""
        from core.bitable_sync import BitableSync

        sync = BitableSync(
            app_token="test",
            table_id="test",
            dedup_keys=["股票代码", "报告期"],
        )
        record = {"fields": {"股票代码": "000858.SZ", "报告期": "2025-09-30"}}
        key = sync.extract_key(record)
        assert key == "000858.SZ_2025-09-30"

    def test_generate_scan_records(self):
        """扫描记录生成"""
        from core.bitable_sync import BitableSync

        sync = BitableSync(app_token="test", table_id="test")
        beats = [{
            "code": "000858.SZ",
            "name": "五粮液",
            "actual_profit_yoy": 18.0,
            "expected_profit_yoy": 15.0,
            "profit_diff": 3.0,
            "close": 175.0,
            "pe": 25.5,
            "period": "2025-09-30",
            "ann_date": "2025-10-15",
        }]
        records = sync.generate_scan_records(beats, [])

        assert len(records) == 1
        assert records[0]["fields"]["股票代码"] == "000858.SZ"
        assert records[0]["fields"]["利润增速"] == 18.0

    def test_dedup_records(self):
        """去重逻辑"""
        from core.bitable_sync import BitableSync

        sync = BitableSync(
            app_token="test",
            table_id="test",
            dedup_keys=["股票代码", "报告期"],
        )
        new_records = [
            {"fields": {"股票代码": "000858.SZ", "报告期": "2025-09-30", "利润增速": 18.0}},
            {"fields": {"股票代码": "600519.SH", "报告期": "2025-09-30", "利润增速": 20.0}},
        ]
        existing_keys = {"000858.SZ_2025-09-30"}  # 第一条已存在

        filtered = sync.dedup_records(new_records, existing_keys=existing_keys)
        assert len(filtered) == 1
        assert filtered[0]["fields"]["股票代码"] == "600519.SH"

    def test_backward_compatibility(self):
        """向后兼容：sync_scan_results 仍然可用"""
        from core.bitable_sync import BitableSync

        sync = BitableSync(app_token="test", table_id="test", backtest_table_id="test2")
        beats = [{"code": "000858.SZ", "name": "五粮液"}]
        records = sync.sync_scan_results(beats, [])
        assert len(records) >= 1

    def test_sync_with_cache(self, tmp_path):
        """完整同步流程（含缓存更新）"""
        from core.bitable_sync import BitableSync

        # 使用唯一 table_id 避免缓存污染
        import uuid
        unique_id = f"test_{uuid.uuid4().hex[:8]}"
        sync = BitableSync(
            app_token="test",
            table_id=unique_id,
            dedup_keys=["股票代码"],
        )
        records = [
            {"fields": {"股票代码": "000858.SZ", "利润增速": 18.0}},
        ]

        pending_path = str(tmp_path / "pending.json")
        count = sync.sync(records, pending_path=pending_path)

        assert count == 1
        assert os.path.exists(pending_path)

        # 验证缓存已更新
        cache_path = str(tmp_path / "cache.json")
        sync.save_existing_keys({"000858.SZ"}, filepath=cache_path)
        keys = sync.load_existing_keys(filepath=cache_path)
        assert "000858.SZ" in keys

    def test_generate_backtest_records(self):
        """回测记录生成"""
        from core.bitable_sync import BitableSync

        sync = BitableSync(
            app_token="test",
            table_id="test",
            backtest_table_id="test2",
        )
        bt_results = [{
            "stock_code": "000858.SZ",
            "stock_name": "五粮液",
            "event_date": "2025-10-15",
            "entry_price": 170.0,
            "return_5d": 2.5,
            "alpha": 1.2,
            "is_win": True,
        }]
        records = sync.generate_backtest_records(bt_results)
        assert len(records) == 1
        assert records[0]["fields"]["股票代码"] == "000858.SZ"


# ═══════════════════════════════════════════════════════════════════════════════
#  Provider 降级测试 Helper 验证
# ═══════════════════════════════════════════════════════════════════════════════

class TestProviderFallbackHelpers:
    """验证测试 Helper 本身"""

    def test_mock_provider_fallback_context(self):
        """mock_provider_fallback 上下文管理器"""
        from tests.fixtures.test_helpers import mock_provider_fallback

        with mock_provider_fallback(primary_empty=True, fallback_has_data=True) as (em, ts):
            assert em == {}
            assert "000858.SZ" in ts
            assert len(ts["000858.SZ"]["data"]) >= 1

    def test_assert_provider_fallback(self):
        """assert_provider_fallback 断言 helper"""
        from core.data_provider import FinancialProvider
        from tests.fixtures.test_helpers import assert_provider_fallback

        provider = FinancialProvider(data={}, tushare_data={})
        provider.fetch("999999.XX")
        assert_provider_fallback(provider, "none")

    def test_create_mock_consensus_provider(self):
        """create_mock_consensus_provider helper"""
        from tests.fixtures.test_helpers import create_mock_consensus_provider

        provider = create_mock_consensus_provider()
        result = provider.fetch("000858.SZ")
        assert result is not None
        assert result.eps == 7.50

    def test_create_mock_kline_provider(self):
        """create_mock_kline_provider helper"""
        from tests.fixtures.test_helpers import create_mock_kline_provider

        provider = create_mock_kline_provider()
        results = provider.fetch("000858.SZ")
        assert len(results) >= 5
