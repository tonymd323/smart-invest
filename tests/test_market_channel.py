"""
T26-T28: 超跌监控集成测试

T26: MarketSnapshotProvider — 模型创建 + 信号判断
T27: MarketAnalyzer — 分析 + 保存 + 历史查询
T28: Pipeline.run_market_snapshot — 端到端（Mock Provider）
"""
import pytest
import sqlite3
import json
import os
import sys
from datetime import datetime
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.models import MarketSnapshot
from core.analyzer import MarketAnalyzer


class TestT26MarketSnapshotProvider:
    """T26: MarketSnapshot 模型 + Provider 信号判断"""

    def test_model_creation(self):
        """MarketSnapshot 模型创建"""
        snap = MarketSnapshot(
            up_count=1500,
            down_count=2000,
            flat_count=100,
            total_count=3600,
            btiq=42.86,
        )
        assert snap.up_count == 1500
        assert snap.down_count == 2000
        assert snap.btiq == 42.86
        assert snap.source == "tencent"

    def test_model_to_dict(self):
        """MarketSnapshot 转字典"""
        snap = MarketSnapshot(
            up_count=1000, down_count=2000, flat_count=50,
            total_count=3050, btiq=33.33, signal="buy",
        )
        d = snap.to_dict()
        assert d["btiq"] == 33.33
        assert d["signal"] == "buy"

    def test_judge_signal_buy(self):
        """MA5 < 30 → buy"""
        from core.data_provider import MarketSnapshotProvider
        assert MarketSnapshotProvider.judge_signal(28.0, 25.0) == "buy"

    def test_judge_signal_warn(self):
        """MA5 < 25 → warn"""
        from core.data_provider import MarketSnapshotProvider
        assert MarketSnapshotProvider.judge_signal(20.0, 22.0) == "warn"

    def test_judge_signal_hot(self):
        """MA5 > 80 → hot"""
        from core.data_provider import MarketSnapshotProvider
        assert MarketSnapshotProvider.judge_signal(85.0, 82.0) == "hot"

    def test_judge_signal_none(self):
        """正常范围 → None"""
        from core.data_provider import MarketSnapshotProvider
        assert MarketSnapshotProvider.judge_signal(50.0, 55.0) is None

    def test_calc_btiq(self):
        """BTIQ 计算"""
        from core.data_provider import MarketSnapshotProvider
        stocks = [
            {"code": "1", "change_pct": 1.0},
            {"code": "2", "change_pct": -2.0},
            {"code": "3", "change_pct": 0.5},
            {"code": "4", "change_pct": -1.0},
            {"code": "5", "change_pct": 0},
        ]
        result = MarketSnapshotProvider._calc_btiq(stocks)
        assert result["up"] == 2
        assert result["down"] == 2
        assert result["flat"] == 1
        assert result["total"] == 5
        assert result["btiq"] == 50.0


class TestT27MarketAnalyzer:
    """T27: MarketAnalyzer 分析 + 保存 + 历史"""

    def test_analyze_basic(self, test_db):
        """基本分析流程"""
        snap = MarketSnapshot(
            up_count=1000, down_count=2000, flat_count=50,
            total_count=3050, btiq=33.33,
            snapshot_time=datetime.now().isoformat(),
        )
        analyzer = MarketAnalyzer(db_path=test_db)
        result = analyzer.analyze(snap)

        assert result["btiq"] == 33.33
        assert result["up_count"] == 1000
        assert result["signal"] is None  # MA5 不足

    def test_save_and_query(self, test_db):
        """保存 + 历史查询"""
        analyzer = MarketAnalyzer(db_path=test_db)

        # 保存 5 条快照
        for i in range(5):
            result = {
                "btiq": 25.0 + i * 2,
                "ma5": None,
                "signal": None,
                "up_count": 1000 + i * 100,
                "down_count": 2000 - i * 50,
                "flat_count": 50,
                "total_count": 3050,
                "snapshot_time": f"2026-03-29T{10+i}:00:00",
            }
            analyzer.save(result)

        # 查询历史
        history = analyzer.get_history(days=7)
        assert len(history) == 5

    def test_ma5_calculation(self, test_db):
        """MA5 计算"""
        analyzer = MarketAnalyzer(db_path=test_db)

        # 先保存 4 条历史
        for i, btiq in enumerate([30.0, 28.0, 26.0, 24.0]):
            analyzer.save({
                "btiq": btiq, "ma5": None, "signal": None,
                "up_count": 1000, "down_count": 2000, "flat_count": 50,
                "total_count": 3050, "snapshot_time": f"2026-03-29T{10+i}:00:00",
            })

        # 当前值 22.0，MA5 = (22+24+26+28+30)/5 = 26.0
        ma5 = analyzer._calc_ma5(22.0)
        assert ma5 == 26.0

    def test_signal_with_ma5(self, test_db):
        """MA5 < 30 → buy 信号"""
        analyzer = MarketAnalyzer(db_path=test_db)

        # 填充历史使 MA5 低于阈值
        for i, btiq in enumerate([28.0, 26.0, 24.0, 22.0]):
            analyzer.save({
                "btiq": btiq, "ma5": None, "signal": None,
                "up_count": 1000, "down_count": 2000, "flat_count": 50,
                "total_count": 3050, "snapshot_time": f"2026-03-29T{10+i}:00:00",
            })

        # 当前 20.0 → MA5 = (20+22+24+26+28)/5 = 24.0 → warn
        snap = MarketSnapshot(
            up_count=800, down_count=2200, flat_count=50,
            total_count=3050, btiq=20.0,
            snapshot_time=datetime.now().isoformat(),
        )
        result = analyzer.analyze(snap)
        assert result["ma5"] == 24.0
        assert result["signal"] == "warn"


class TestT28PipelineMarketSnapshot:
    """T28: Pipeline.run_market_snapshot 端到端"""

    def test_run_market_snapshot(self, test_db):
        """Mock Provider 测试 Pipeline 市场通道"""
        from core.pipeline import Pipeline

        mock_snapshot = MarketSnapshot(
            up_count=1200, down_count=1800, flat_count=100,
            total_count=3100, btiq=40.0,
            snapshot_time=datetime.now().isoformat(),
        )

        with patch('core.data_provider.MarketSnapshotProvider') as MockProvider:
            mock_instance = MagicMock()
            mock_instance.fetch_snapshot.return_value = mock_snapshot
            MockProvider.return_value = mock_instance

            pipe = Pipeline(db_path=test_db)
            result = pipe.run_market_snapshot()

            assert result["btiq"] == 40.0
            assert result["up_count"] == 1200
            assert "elapsed_ms" in result
            assert "error" not in result

    def test_run_market_snapshot_error(self, test_db):
        """Provider 异常时优雅降级"""
        from core.pipeline import Pipeline

        with patch('core.data_provider.MarketSnapshotProvider') as MockProvider:
            mock_instance = MagicMock()
            mock_instance.fetch_snapshot.side_effect = ConnectionError("网络超时")
            MockProvider.return_value = mock_instance

            pipe = Pipeline(db_path=test_db)
            result = pipe.run_market_snapshot()

            assert result["btiq"] is None
            assert "error" in result
            assert "网络超时" in result["error"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
