"""
ConsensusProvider 端到端测试
验证 Provider 模式：Pipeline → ConsensusProvider → AkShare
"""

import sys
import pytest
sys.path.insert(0, ".")

from core.data_provider import ConsensusProvider, ConsensusData


class TestConsensusProvider:
    """ConsensusProvider 基础测试"""

    def test_fetch_multi_year(self):
        """测试多年预期获取"""
        provider = ConsensusProvider()
        result = provider.fetch_multi_year("600660.SH")

        # 福耀玻璃应有多年预期
        assert len(result) >= 1, f"expected at least 1 year, got {len(result)}"
        assert "25E" in result, "missing 25E"
        assert isinstance(result["25E"], ConsensusData)
        assert result["25E"].net_profit_yoy != 0

        # 验证数据源
        assert provider.last_source in ("akshare_growth", "eastmoney_f10")

    def test_fetch_single_year(self):
        """测试单年预期获取"""
        provider = ConsensusProvider()
        result = provider.fetch("600660.SH")

        assert result is not None
        assert isinstance(result, ConsensusData)
        assert result.stock_code == "600660.SH"
        assert result.net_profit_yoy != 0

    def test_consensus_values_reasonable(self):
        """测试预期值合理性"""
        provider = ConsensusProvider()
        result = provider.fetch_multi_year("600660.SH")

        for year, data in result.items():
            # 净利润增速应在合理范围
            assert -200 < data.net_profit_yoy < 500, (
                f"{year} profit_yoy={data.net_profit_yoy} out of range"
            )

    def test_nonexistent_stock_returns_empty(self):
        """测试不存在的股票返回空"""
        provider = ConsensusProvider()
        result = provider.fetch_multi_year("999999.ZZ")
        assert result == {}

    def test_presets_overrides_api(self):
        """测试预注入数据覆盖 API"""
        presets = {
            "TEST.SZ": {
                "profit_25e": 10.0,
                "rev_25e": 8.0,
                "profit_26e": 12.0,
                "rev_26e": 9.0,
            }
        }
        provider = ConsensusProvider(data=presets)
        result = provider.fetch_multi_year("TEST.SZ")

        assert provider.last_source == "preloaded"
        assert "25E" in result
        assert result["25E"].net_profit_yoy == 10.0
        assert result["25E"].rev_yoy == 8.0
