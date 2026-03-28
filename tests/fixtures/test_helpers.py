"""
测试 Fixtures — SQLite 内存数据库 helper + Mock 数据
Phase 0 + Phase 1
"""

import sqlite3
import os
import tempfile
from contextlib import contextmanager
from unittest.mock import patch, MagicMock


# ── 测试数据库 ────────────────────────────────────────────────────────────────

@contextmanager
def get_test_db():
    """
    创建临时 SQLite 数据库（WAL 模式），自动清理。
    用于 spike 测试和单元测试。
    """
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS earnings (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code                TEXT    NOT NULL,
            report_date               TEXT    NOT NULL,
            report_type               TEXT    DEFAULT 'Q4',
            revenue                   REAL,
            net_profit                REAL,
            net_profit_yoy            REAL,
            eps                       REAL,
            is_beat_expectation       INTEGER DEFAULT 0,
            expectation_diff_pct      REAL,
            quarterly_profit_new_high INTEGER DEFAULT 0,
            quarterly_net_profit      REAL,
            prev_quarterly_high       REAL,
            roe                       REAL,
            gross_margin              REAL,
            created_at                TEXT    DEFAULT (datetime('now', 'localtime')),
            UNIQUE(stock_code, report_date, report_type)
        );

        CREATE TABLE IF NOT EXISTS consensus (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code      TEXT    NOT NULL,
            eps             REAL,
            net_profit_yoy  REAL,
            rev_yoy         REAL,
            num_analysts    INTEGER DEFAULT 0,
            source          TEXT    DEFAULT 'eastmoney',
            fetched_at      TEXT    DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (stock_code) REFERENCES stocks(code),
            UNIQUE(stock_code)
        );

        CREATE TABLE IF NOT EXISTS discovery_pool (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code      TEXT    NOT NULL UNIQUE,
            stock_name      TEXT,
            industry        TEXT,
            source          TEXT    NOT NULL,
            score           REAL    DEFAULT 0.0,
            signal          TEXT    DEFAULT 'watch',
            detail          TEXT,
            status          TEXT    DEFAULT 'active',
            discovered_at   TEXT    DEFAULT (datetime('now', 'localtime')),
            expires_at      TEXT,
            updated_at      TEXT    DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code      TEXT,
            event_type      TEXT    NOT NULL,
            title           TEXT    NOT NULL,
            content         TEXT,
            source          TEXT,
            url             TEXT,
            sentiment       TEXT    DEFAULT 'neutral',
            sentiment_score REAL    DEFAULT 0.0,
            severity        TEXT    DEFAULT 'normal',
            published_at    TEXT,
            created_at      TEXT    DEFAULT (datetime('now', 'localtime'))
        );
    """)
    conn.commit()

    yield path

    conn.close()
    os.unlink(path)


def insert_earnings(conn: sqlite3.Connection, records: list) -> int:
    """批量插入 earnings 记录，返回插入行数"""
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO earnings
            (stock_code, report_date, report_type, revenue, net_profit,
             net_profit_yoy, eps, roe, gross_margin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r.get('stock_code'),
            r.get('report_date'),
            r.get('report_type', 'Q4'),
            r.get('revenue'),
            r.get('net_profit'),
            r.get('net_profit_yoy'),
            r.get('eps'),
            r.get('roe'),
            r.get('gross_margin'),
        ))
        count += 1
    conn.commit()
    return count


# ── Mock Earnings 数据 ────────────────────────────────────────────────────────

MOCK_EARNINGS = [
    {
        'stock_code': '000858.SZ',
        'report_date': '2025-09-30',
        'report_type': 'Q3',
        'revenue': 750.0,
        'net_profit': 248.0,
        'net_profit_yoy': 12.5,
        'eps': 6.39,
        'roe': 22.3,
        'gross_margin': 78.5,
    },
    {
        'stock_code': '000858.SZ',
        'report_date': '2025-06-30',
        'report_type': 'Q2',
        'revenue': 510.0,
        'net_profit': 172.0,
        'net_profit_yoy': 11.8,
        'eps': 4.43,
        'roe': 15.6,
        'gross_margin': 78.1,
    },
    {
        'stock_code': '600660.SH',
        'report_date': '2025-09-30',
        'report_type': 'Q3',
        'revenue': 280.0,
        'net_profit': 52.0,
        'net_profit_yoy': 25.3,
        'eps': 2.08,
        'roe': 18.5,
        'gross_margin': 35.2,
    },
]

MOCK_EARNINGS_RAW_EM = [
    {
        'SECURITY_CODE': '600660',
        'REPORT_DATE': '2025-09-30 00:00:00',
        'PARENTNETPROFIT': 5200000000.0,
        'TOTALOPERATEREVE': 28000000000.0,
        'PARENTNETPROFITTZ': 25.3,
        'DJD_DPNP_YOY': 25.3,
        'DJD_TOI_YOY': 18.7,
        'ROEJQ': 18.5,
        'XSMLL': 35.2,
        'EPSJB': 2.08,
    },
    {
        'SECURITY_CODE': '600660',
        'REPORT_DATE': '2025-06-30 00:00:00',
        'PARENTNETPROFIT': 3500000000.0,
        'TOTALOPERATEREVE': 19000000000.0,
        'PARENTNETPROFITTZ': 20.1,
        'DJD_DPNP_YOY': 20.1,
        'DJD_TOI_YOY': 15.2,
        'ROEJQ': 12.8,
        'XSMLL': 34.8,
        'EPSJB': 1.40,
    },
]


# ── Mock Consensus 数据 ──────────────────────────────────────────────────────

MOCK_CONSENSUS = {
    "000858.SZ": {
        "profit_25e": 15.0,
        "rev_25e": 12.0,
        "profit_26e": 16.0,
        "rev_26e": 13.0,
        "profit_27e": 17.0,
        "rev_27e": 14.0,
    },
    "600519.SH": {
        "eps": 68.50,
        "profit_yoy_expected": 18.0,
        "rev_yoy_expected": 15.0,
        "analyst_count": 42,
        "source": "eastmoney_f10",
    },
    "300750.SZ": {
        "eps": 12.80,
        "profit_yoy_expected": 30.0,
        "rev_yoy_expected": 25.0,
        "analyst_count": 35,
        "source": "eastmoney_f10",
    },
}


# ── Mock Kline 数据 ──────────────────────────────────────────────────────────

MOCK_KLINE = {
    "000858.SZ": [
        {"trade_date": "20260319", "open": 168.50, "high": 170.20, "low": 167.80,
         "close": 169.60, "vol": 2850000, "amount": 483000000, "pct_chg": 1.2},
        {"trade_date": "20260320", "open": 169.80, "high": 172.50, "low": 169.10,
         "close": 171.80, "vol": 3120000, "amount": 535000000, "pct_chg": 1.3},
        {"trade_date": "20260321", "open": 172.00, "high": 173.80, "low": 170.50,
         "close": 170.90, "vol": 2980000, "amount": 509000000, "pct_chg": -0.5},
        {"trade_date": "20260324", "open": 170.50, "high": 172.10, "low": 169.80,
         "close": 171.50, "vol": 2750000, "amount": 472000000, "pct_chg": 0.35},
        {"trade_date": "20260325", "open": 171.80, "high": 174.20, "low": 171.20,
         "close": 173.60, "vol": 3350000, "amount": 578000000, "pct_chg": 1.22},
        {"trade_date": "20260326", "open": 173.50, "high": 175.80, "low": 172.90,
         "close": 175.20, "vol": 3580000, "amount": 623000000, "pct_chg": 0.92},
        {"trade_date": "20260327", "open": 175.00, "high": 176.50, "low": 173.80,
         "close": 174.80, "vol": 3210000, "amount": 561000000, "pct_chg": -0.23},
    ],
    "600519.SH": [
        {"trade_date": "20260327", "open": 1580.00, "high": 1600.50, "low": 1575.00,
         "close": 1592.00, "vol": 1200000, "amount": 1910000000, "pct_chg": 0.76},
    ],
}


# ── Provider 降级测试 Helper ──────────────────────────────────────────────────

@contextmanager
def mock_provider_fallback(primary_empty=True, fallback_has_data=True):
    """
    模拟 Provider 主源失败 → 降级的场景。

    用法：
        with mock_provider_fallback(primary_empty=True) as (em_data, ts_data):
            provider = FinancialProvider(data=em_data, tushare_data=ts_data)
            results = provider.fetch("000858.SZ")
            assert provider.last_source == "tushare"
    """
    from tests.fixtures.mock_eastmoney import EASTMONEY_FINA_INDICATOR

    if primary_empty:
        em_data = {}
    else:
        em_data = EASTMONEY_FINA_INDICATOR

    if fallback_has_data:
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
    else:
        ts_data = {}

    yield em_data, ts_data


def assert_provider_fallback(provider, expected_source: str):
    """断言 Provider 降级到指定数据源"""
    assert provider.last_source == expected_source, (
        f"期望 last_source={expected_source}, 实际={provider.last_source}"
    )


def create_mock_consensus_provider(data: dict = None):
    """创建一个使用 mock 数据的 ConsensusProvider"""
    from core.data_provider import ConsensusProvider
    return ConsensusProvider(data=data or MOCK_CONSENSUS)


def create_mock_kline_provider(data: dict = None):
    """创建一个使用 mock 数据的 KlineProvider"""
    from core.data_provider import KlineProvider
    return KlineProvider(data=data or MOCK_KLINE)
