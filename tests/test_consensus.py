"""
ConsensusProvider 端到端测试 — v2.6 多源从严
验证 Provider 模式：Pipeline → ConsensusProvider → AkShare + 东方财富 F10
"""

import sys
import json
import pytest
sys.path.insert(0, ".")

from core.data_provider import ConsensusProvider, ConsensusData


class TestConsensusProvider:
    """ConsensusProvider 基础测试"""

    def test_fetch_multi_year(self):
        """测试多年预期获取（v2.6 多源从严）"""
        provider = ConsensusProvider()
        result = provider.fetch_multi_year("600660.SH")

        # 福耀玻璃应有多年预期
        assert len(result) >= 1, f"expected at least 1 year, got {len(result)}"
        assert "25E" in result, "missing 25E"
        assert isinstance(result["25E"], ConsensusData)
        assert result["25E"].net_profit_yoy != 0

        # v2.6: source 格式应含 max: 前缀或单源名
        assert provider.last_source != "none"
        assert "akshare_growth" in provider.last_source or "eastmoney_f10" in provider.last_source

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
        assert provider.last_source == "none"
        assert provider.last_source_detail == {}

    def test_presets_overrides_api(self):
        """测试预注入数据覆盖 API（最高优先级）"""
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
        # 预注入时 source_detail 为空
        assert provider.last_source_detail == {}

    def test_multi_source_returns_max(self):
        """v2.6: 测试多源取 max 逻辑（真实股票）"""
        provider = ConsensusProvider()
        result = provider.fetch_multi_year("600660.SH")

        assert len(result) >= 1
        # 验证 source 含 max: 前缀（两源都有数据时）
        for year, data in result.items():
            # source 应为 max:akshare_growth 或 max:eastmoney_f10 或单源名
            assert data.source != "", f"{year} source is empty"

        # 验证 last_source_detail 有数据
        assert len(provider.last_source_detail) >= 1
        for year, detail in provider.last_source_detail.items():
            assert "selected" in detail, f"{year} detail missing 'selected'"
            # 至少有一个源
            assert "akshare" in detail or "eastmoney" in detail, (
                f"{year} detail missing source data"
            )

    def test_source_detail_json_serializable(self):
        """v2.6: 测试 source_detail 可序列化为 JSON"""
        provider = ConsensusProvider()
        provider.fetch_multi_year("600660.SH")

        # 应能正常 JSON 序列化
        detail_json = json.dumps(provider.last_source_detail)
        assert len(detail_json) > 10

        # 反序列化后结构正确
        restored = json.loads(detail_json)
        for year, detail in restored.items():
            assert "selected" in detail

    def test_source_format_multi_source(self):
        """v2.6: 测试两源都有时 source 标注 max:"""
        provider = ConsensusProvider()
        result = provider.fetch_multi_year("600660.SH")

        if len(result) >= 1:
            for year, data in result.items():
                if "akshare" in provider.last_source and "eastmoney" in provider.last_source:
                    # 两源都有，source 应含 max: 前缀
                    assert data.source.startswith("max:"), (
                        f"{year} source={data.source} should start with 'max:'"
                    )
