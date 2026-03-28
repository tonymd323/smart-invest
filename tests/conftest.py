"""
pytest 共享 fixtures
Phase 0 + Phase 1
"""

import pytest
import sqlite3
import os
import tempfile

# 导入项目模块
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.fixture
def test_db():
    """
    创建临时 SQLite 数据库（WAL 模式），初始化 Schema。
    测试结束后自动清理。
    """
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    conn = sqlite3.connect(path)
    # WAL 模式 — 先生指示
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    # 加载 Schema
    from core.database import SCHEMA_SQL
    conn.executescript(SCHEMA_SQL)

    yield path

    conn.close()
    os.unlink(path)


@pytest.fixture
def watchlist():
    """返回 Phase 0 测试用股票清单"""
    from tests.fixtures.watchlist import WATCHLIST
    return WATCHLIST


@pytest.fixture
def mock_financial_data():
    """返回模拟东方财富财务数据"""
    from tests.fixtures.mock_eastmoney import EASTMONEY_FINA_INDICATOR
    return EASTMONEY_FINA_INDICATOR


@pytest.fixture
def mock_consensus_data():
    """返回模拟一致预期数据"""
    from tests.fixtures.mock_eastmoney import EASTMONEY_CONSENSUS
    return EASTMONEY_CONSENSUS


@pytest.fixture
def mock_kline_data():
    """返回模拟日K行情数据"""
    from tests.fixtures.mock_eastmoney import TUSHARE_DAILY
    return TUSHARE_DAILY


@pytest.fixture
def consensus_provider():
    """返回使用 mock 数据的 ConsensusProvider"""
    from tests.fixtures.test_helpers import create_mock_consensus_provider
    return create_mock_consensus_provider()


@pytest.fixture
def kline_provider():
    """返回使用 mock 数据的 KlineProvider"""
    from tests.fixtures.test_helpers import create_mock_kline_provider
    return create_mock_kline_provider()
