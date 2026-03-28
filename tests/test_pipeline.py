"""
Phase 0 测试 — Pipeline + EarningsAnalyzer 端到端

验证链路：Provider → Pipeline → SQLite → Analyzer
"""

import pytest
import sqlite3
import json


class TestPipeline:
    """Pipeline 单 Provider 版本测试"""

    def test_pipeline_run_single_provider(self, test_db, mock_financial_data):
        """Pipeline 调用 FinancialProvider → 写入 SQLite"""
        from core.data_provider import FinancialProvider
        from core.pipeline import Pipeline

        provider = FinancialProvider(data=mock_financial_data)
        pipe = Pipeline(db_path=test_db, providers=[provider])

        results = pipe.run(stock_codes=["000858.SZ"])

        # 验证返回结果
        assert results["000858.SZ"]["status"] == "ok"
        assert results["000858.SZ"]["records_written"] >= 1

        # 验证 SQLite 已写入
        conn = sqlite3.connect(test_db)
        rows = conn.execute(
            "SELECT * FROM earnings WHERE stock_code = '000858.SZ'"
        ).fetchall()
        assert len(rows) >= 1
        conn.close()

    def test_pipeline_data_quality_check(self, test_db, mock_financial_data):
        """Pipeline 数据质量校验（数值范围）"""
        from core.data_provider import FinancialProvider
        from core.pipeline import Pipeline

        provider = FinancialProvider(data=mock_financial_data)
        pipe = Pipeline(db_path=test_db, providers=[provider])

        # 运行 pipeline
        results = pipe.run(stock_codes=["000858.SZ"])

        # 验证质量校验结果
        quality = results["000858.SZ"].get("quality", {})
        assert quality.get("passed", False), f"质量校验失败: {quality}"

    def test_pipeline_handles_missing_stock(self, test_db):
        """Pipeline 处理不存在的股票（不崩溃）"""
        from core.data_provider import FinancialProvider
        from core.pipeline import Pipeline

        provider = FinancialProvider(data={})
        pipe = Pipeline(db_path=test_db, providers=[provider])

        results = pipe.run(stock_codes=["999999.XX"])
        assert results["999999.XX"]["status"] == "empty"

    def test_pipeline_wal_mode(self, test_db, mock_financial_data):
        """Pipeline 运行时数据库保持 WAL 模式"""
        from core.data_provider import FinancialProvider
        from core.pipeline import Pipeline

        provider = FinancialProvider(data=mock_financial_data)
        pipe = Pipeline(db_path=test_db, providers=[provider])
        pipe.run(stock_codes=["000858.SZ"])

        # 验证 WAL 模式仍然启用
        conn = sqlite3.connect(test_db)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        conn.close()


class TestEarningsAnalyzer:
    """EarningsAnalyzer 适配 2.0 Schema 测试"""

    def test_analyze_earnings_beat(self, test_db):
        """分析超预期 —— 给定 earnings 数据，输出扫描结果"""
        # 先写入模拟 earnings 数据
        conn = sqlite3.connect(test_db)
        conn.executescript("""
            INSERT INTO earnings (stock_code, report_date, net_profit_yoy, 
                                  revenue_yoy, is_beat_expectation, expectation_diff_pct)
            VALUES ('000858.SZ', '2025-09-30', 18.0, 12.0, 1, 3.0);
            
            INSERT INTO earnings (stock_code, report_date, net_profit_yoy, 
                                  revenue_yoy, is_beat_expectation, expectation_diff_pct)
            VALUES ('000858.SZ', '2024-09-30', 9.5, 7.2, 0, -5.5);
        """)
        conn.commit()
        conn.close()

        from core.analyzer import EarningsAnalyzer
        analyzer = EarningsAnalyzer(db_path=test_db)
        results = analyzer.scan_beat_expectation(stock_codes=["000858.SZ"])

        assert len(results) >= 1
        for item in results:
            assert "stock_code" in item
            assert "score" in item
            assert "signal" in item

    def test_analyze_profit_new_high(self, test_db):
        """分析扣非新高 —— 给定 earnings 数据，检测是否创新高"""
        conn = sqlite3.connect(test_db)
        conn.executescript("""
            INSERT INTO earnings (stock_code, report_date, quarterly_net_profit)
            VALUES ('000858.SZ', '2025-09-30', 24.8);
            INSERT INTO earnings (stock_code, report_date, quarterly_net_profit)
            VALUES ('000858.SZ', '2025-06-30', 17.2);
            INSERT INTO earnings (stock_code, report_date, quarterly_net_profit)
            VALUES ('000858.SZ', '2024-09-30', 22.05);
            INSERT INTO earnings (stock_code, report_date, quarterly_net_profit)
            VALUES ('000858.SZ', '2024-06-30', 15.38);
        """)
        conn.commit()
        conn.close()

        from core.analyzer import EarningsAnalyzer
        analyzer = EarningsAnalyzer(db_path=test_db)
        results = analyzer.scan_new_high(stock_codes=["000858.SZ"])

        assert len(results) >= 1
        for item in results:
            assert "is_new_high" in item
            assert isinstance(item["is_new_high"], bool)

    def test_analyzer_writes_results_to_db(self, test_db):
        """分析结果写入 analysis_results 表"""
        conn = sqlite3.connect(test_db)
        conn.execute("""
            INSERT INTO earnings (stock_code, report_date, net_profit_yoy,
                                  revenue_yoy, is_beat_expectation, expectation_diff_pct)
            VALUES ('000858.SZ', '2025-09-30', 18.0, 12.0, 1, 3.0);
        """)
        conn.commit()
        conn.close()

        from core.analyzer import EarningsAnalyzer
        analyzer = EarningsAnalyzer(db_path=test_db)
        analyzer.scan_beat_expectation(stock_codes=["000858.SZ"])

        # 验证结果已写入
        conn = sqlite3.connect(test_db)
        rows = conn.execute(
            "SELECT * FROM analysis_results WHERE stock_code = '000858.SZ'"
        ).fetchall()
        assert len(rows) >= 1
        conn.close()
