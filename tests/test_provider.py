"""
Phase 0 测试 — Provider 抽象 + FinancialProvider Spike

⚠️ 测试在实现之前写（TDD）
"""

import pytest
from unittest.mock import patch, MagicMock


class TestProviderBase:
    """验证 Provider 抽象基类"""

    def test_provider_cannot_instantiate_abstract(self):
        """抽象基类不能直接实例化"""
        from core.data_provider import BaseProvider
        with pytest.raises(TypeError):
            BaseProvider()

    def test_provider_subclass_must_implement_fetch(self):
        """子类必须实现 fetch 方法（类定义时即报错）"""
        from core.data_provider import BaseProvider

        with pytest.raises(TypeError):
            class BadProvider(BaseProvider):
                pass

    def test_provider_subclass_works(self):
        """正确实现的子类可以实例化"""
        from core.data_provider import BaseProvider

        class GoodProvider(BaseProvider):
            def fetch(self, stock_code):
                return {"code": stock_code}

        p = GoodProvider()
        result = p.fetch("000858.SZ")
        assert result["code"] == "000858.SZ"

    def test_provider_has_standard_output(self):
        """Provider 输出为标准 dataclass"""
        from core.data_provider import FinancialData

        fd = FinancialData(
            stock_code="000858.SZ",
            report_date="2025-09-30",
            net_profit=24.8,
            net_profit_yoy=12.5,
            revenue=75.0,
            revenue_yoy=10.2,
            roe=22.3,
            gross_margin=78.5,
            eps=6.39,
            source="test",
        )
        assert fd.net_profit == 24.8
        assert fd.stock_code == "000858.SZ"


class TestFinancialProvider:
    """验证 FinancialProvider（Spike 核心）"""

    def test_fetch_with_mock_data(self, mock_financial_data):
        """使用 mock 数据验证 Provider 输出"""
        from core.data_provider import FinancialProvider

        provider = FinancialProvider(
            data=mock_financial_data,  # 注入 mock
            source="eastmoney",
        )
        results = provider.fetch("000858.SZ")

        assert len(results) >= 1
        for item in results:
            assert hasattr(item, "net_profit")
            assert hasattr(item, "net_profit_yoy")
            assert hasattr(item, "report_date")

    def test_fetch_fallback_to_tushare(self):
        """东财失败时降级到 Tushare（验证 fallback 调用）"""
        from core.data_provider import FinancialProvider

        # 模拟东财返回空，Tushare 返回数据
        mock_em = {}
        mock_ts = {
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
        provider = FinancialProvider(
            data=mock_em,
            tushare_data=mock_ts,
            source="eastmoney",
        )
        results = provider.fetch("000858.SZ")
        assert len(results) >= 1
        assert provider.last_source == "tushare"  # 降级了

    def test_fetch_empty_returns_empty_list(self):
        """无数据时返回空列表，不报错"""
        from core.data_provider import FinancialProvider

        provider = FinancialProvider(data={}, source="eastmoney")
        results = provider.fetch("999999.XX")
        assert results == []


class TestFinancialProviderToDict:
    """验证 FinancialProvider 转换为 dict（供 Pipeline 写入 SQLite）"""

    def test_to_dict_format(self, mock_financial_data):
        """FinancialData → dict 格式正确"""
        from core.data_provider import FinancialProvider

        provider = FinancialProvider(data=mock_financial_data, source="eastmoney")
        results = provider.fetch("000858.SZ")
        d = results[0].to_dict()

        assert isinstance(d, dict)
        assert "stock_code" in d
        assert "net_profit" in d
        assert "source" in d
        assert d["stock_code"] == "000858.SZ"
