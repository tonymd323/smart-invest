"""
数据库 Schema 定义与初始化
=========================
SQLite 数据库，包含核心表：
stocks / earnings / prices / analysis_results / event_tracking / backtest
v2 新增：consensus / discovery_pool / events

已移除（v1 空表）：news / fund_flows / push_logs
"""

import sqlite3
import os
from pathlib import Path
from contextlib import contextmanager
from typing import Optional

DB_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DB_DIR / "smart_invest.db"


# ── Schema DDL ────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
-- 1. 股票清单
CREATE TABLE IF NOT EXISTS stocks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    code            TEXT    NOT NULL UNIQUE,          -- 股票代码，如 000001.SZ
    name            TEXT    NOT NULL,                 -- 股票名称
    market          TEXT    DEFAULT 'A',              -- 市场：A / HK / US
    industry        TEXT,                              -- 行业分类
    sector          TEXT,                              -- 板块
    is_active       INTEGER DEFAULT 1,                -- 是否活跃
    created_at      TEXT    DEFAULT (datetime('now', 'localtime')),
    updated_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 2. 业绩数据（含单季度净利润历史新高字段）
CREATE TABLE IF NOT EXISTS earnings (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code                TEXT    NOT NULL,
    report_date               TEXT    NOT NULL,        -- 报告期，如 2025-12-31
    report_type               TEXT    DEFAULT 'Q4',    -- Q1 / Q2 / Q3 / Q4 / 半年报 / 年报
    revenue                   REAL,                     -- 营业收入（亿元）
    revenue_yoy               REAL,                     -- 营收同比增长率（%）
    net_profit                REAL,                     -- 净利润（亿元）
    net_profit_yoy            REAL,                     -- 净利润同比增长率（%）
    eps                       REAL,                     -- 每股收益
    -- 超预期相关
    is_beat_expectation       INTEGER DEFAULT 0,        -- 是否超预期
    expectation_diff_pct      REAL,                     -- 超预期幅度（%）
    -- 单季度净利润历史新高
    quarterly_profit_new_high INTEGER DEFAULT 0,        -- 单季度净利润是否创历史新高（1=是）
    quarterly_net_profit      REAL,                     -- 单季度净利润（亿元）
    prev_quarterly_high       REAL,                     -- 历史单季度最高净利润（亿元）
    -- 其他
    roe                       REAL,                     -- 净资产收益率（%）
    gross_margin              REAL,                     -- 毛利率（%）
    created_at                TEXT    DEFAULT (datetime('now', 'localtime')),
    UNIQUE(stock_code, report_date, report_type),
    FOREIGN KEY (stock_code) REFERENCES stocks(code)
);

-- 3. 行情数据
CREATE TABLE IF NOT EXISTS prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code      TEXT    NOT NULL,
    trade_date      TEXT    NOT NULL,                  -- 交易日期
    open_price      REAL,
    high_price      REAL,
    low_price       REAL,
    close_price     REAL,
    volume          REAL,                              -- 成交量（手）
    turnover        REAL,                              -- 成交额（亿元）
    change_pct      REAL,                              -- 涨跌幅（%）
    turnover_rate   REAL,                              -- 换手率（%）
    -- 技术指标
    ma5             REAL,                              -- 5日均线
    ma10            REAL,
    ma20            REAL,
    ma60            REAL,
    rsi6            REAL,                              -- RSI(6)
    macd_dif        REAL,
    macd_dea        REAL,
    macd_hist       REAL,
    created_at      TEXT    DEFAULT (datetime('now', 'localtime')),
    UNIQUE(stock_code, trade_date),
    FOREIGN KEY (stock_code) REFERENCES stocks(code)
);

-- 4. 分析结果
CREATE TABLE IF NOT EXISTS analysis_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code      TEXT    NOT NULL,
    analysis_type   TEXT    NOT NULL,                  -- news_sentiment / earnings_beat / technical / risk
    score           REAL,                              -- 综合评分 0-100
    signal          TEXT,                              -- buy / hold / sell / watch
    summary         TEXT,                              -- 分析摘要（JSON 格式）
    detail          TEXT,                              -- 详细分析（JSON 格式）
    confidence      REAL    DEFAULT 0.0,               -- 置信度 0-1
    analyst         TEXT    DEFAULT 'system',           -- 分析来源
    created_at      TEXT    DEFAULT (datetime('now', 'localtime')),
    FOREIGN KEY (stock_code) REFERENCES stocks(code),
    UNIQUE(stock_code, analysis_type, created_at)
);

-- 5. 超预期事件跟踪（T+N 表现追踪）
CREATE TABLE IF NOT EXISTS event_tracking (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code      TEXT    NOT NULL,
    stock_name      TEXT,
    event_type      TEXT    NOT NULL,              -- earnings_beat / profit_new_high
    event_date      TEXT    NOT NULL,              -- 事件日期 (YYYY-MM-DD)
    report_period   TEXT,                           -- 报告期
    actual_yoy      REAL,                           -- 实际利润增速
    expected_yoy    REAL,                           -- 预期利润增速
    profit_diff     REAL,                           -- 超预期幅度
    is_non_recurring INTEGER DEFAULT 0,             -- 是否非经常性损益
    -- 事件日收盘价
    entry_price     REAL,                           -- 事件日收盘价
    entry_pe        REAL,                           -- 事件日 PE
    -- T+N 表现
    return_1d       REAL,                           -- T+1 收益率 (%)
    return_5d       REAL,                           -- T+5 收益率 (%)
    return_10d      REAL,                           -- T+10 收益率 (%)
    return_20d      REAL,                           -- T+20 收益率 (%)
    -- 基准对比
    benchmark_1d    REAL,                           -- 沪深300 T+1 涨幅
    benchmark_5d    REAL,                           -- 沪深300 T+5 涨幅
    benchmark_10d   REAL,
    benchmark_20d   REAL,
    alpha_5d        REAL,                           -- T+5 超额收益
    alpha_20d       REAL,                           -- T+20 超额收益
    -- 状态
    tracking_status TEXT    DEFAULT 'pending',      -- pending / tracking / completed
    last_updated    TEXT    DEFAULT (datetime('now', 'localtime')),
    created_at      TEXT    DEFAULT (datetime('now', 'localtime')),
    UNIQUE(stock_code, event_date, event_type)
);

-- 6. 回测记录（入池后 T+N 收益 vs 沪深300）
CREATE TABLE IF NOT EXISTS backtest (
    stock_code      TEXT    NOT NULL,
    event_date      TEXT    NOT NULL,              -- 入池日期 (YYYY-MM-DD)
    event_type      TEXT,                           -- earnings_beat / profit_new_high
    entry_price     REAL,                           -- 入池价
    return_5d       REAL,                           -- T+5 收益率 (%)
    return_10d      REAL,
    return_20d      REAL,
    return_60d      REAL,
    benchmark_5d    REAL,                           -- 沪深300 T+5 涨幅
    benchmark_10d   REAL,
    benchmark_20d   REAL,
    benchmark_60d   REAL,
    alpha_5d        REAL,                           -- T+5 超额收益
    alpha_10d       REAL,
    alpha_20d       REAL,
    alpha_60d       REAL,
    is_win          INTEGER,                        -- 1=跑赢 / 0=跑输 / NULL=数据不足
    UNIQUE(stock_code, event_date)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- v2 新增表
-- ═══════════════════════════════════════════════════════════════════════════════

-- 7. 一致预期（机构预期数据）
CREATE TABLE IF NOT EXISTS consensus (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code      TEXT    NOT NULL,
    year            TEXT    NOT NULL,                   -- 预期年份，如 25E/26E/27E
    eps             REAL,                              -- 预期每股收益
    net_profit_yoy  REAL,                              -- 预期净利润同比（%）
    rev_yoy         REAL,                              -- 预期营收同比（%）
    num_analysts    INTEGER DEFAULT 0,                  -- 分析师数量
    source          TEXT    DEFAULT 'akshare',          -- 数据来源
    fetched_at      TEXT    DEFAULT (datetime('now', 'localtime')),
    FOREIGN KEY (stock_code) REFERENCES stocks(code),
    UNIQUE(stock_code, year)
);

-- 8. 发现池（系统自动发现的候选股票）
CREATE TABLE IF NOT EXISTS discovery_pool (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code      TEXT    NOT NULL UNIQUE,
    stock_name      TEXT,
    industry        TEXT,
    -- 发现来源
    source          TEXT    NOT NULL,                  -- earnings_beat / profit_new_high / consensus_upgrade
    score           REAL    DEFAULT 0.0,               -- 综合评分
    signal          TEXT    DEFAULT 'watch',           -- watch / buy / avoid
    -- 详细数据
    detail          TEXT,                              -- JSON 格式详细数据
    -- 生命周期
    status          TEXT    DEFAULT 'active',          -- active / promoted / expired / removed
    discovered_at   TEXT    DEFAULT (datetime('now', 'localtime')),
    expires_at      TEXT,                              -- 过期时间（默认30天后）
    updated_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 9. 结构化事件（新闻/公告/异动事件）
CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code      TEXT,                              -- 关联股票（可为空=宏观事件）
    event_type      TEXT    NOT NULL,                  -- news / announcement / earnings / price_alert
    title           TEXT    NOT NULL,
    content         TEXT,
    source          TEXT,                              -- 来源
    url             TEXT,
    sentiment       TEXT    DEFAULT 'neutral',         -- positive / negative / neutral
    sentiment_score REAL    DEFAULT 0.0,               -- -1.0 ~ 1.0
    severity        TEXT    DEFAULT 'normal',          -- normal / important / critical
    published_at    TEXT,
    created_at      TEXT    DEFAULT (datetime('now', 'localtime')),
    FOREIGN KEY (stock_code) REFERENCES stocks(code)
);

-- 10. 全市场快照（超跌监控历史）
CREATE TABLE IF NOT EXISTS market_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_time   TEXT    NOT NULL,                   -- 采集时间 (ISO 格式)
    btiq            REAL,                              -- 涨跌比 = up/(up+down)*100
    up_count        INTEGER,                           -- 上涨家数
    down_count      INTEGER,                           -- 下跌家数
    flat_count      INTEGER,                           -- 平盘家数
    total_count     INTEGER,                           -- 总数
    ma5             REAL,                              -- 5日均值 BTIQ
    signal          TEXT,                              -- buy / warn / hot / none
    source          TEXT    DEFAULT 'tencent',          -- 数据来源
    created_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 11. 五维度评分快照（超预期评分体系 v2.0）
CREATE TABLE IF NOT EXISTS stock_scores (
    stock_code          TEXT    NOT NULL,
    score_date          TEXT    NOT NULL,               -- YYYY-MM-DD
    -- 五维度原始分
    q_score             REAL,                           -- 质量因子 0-100
    g_score             REAL,                           -- 成长因子 0-100
    v_score             REAL,                           -- 估值因子 0-100
    c_score             REAL,                           -- 护城河因子 0-100
    s_score             REAL,                           -- 超预期因子 0-100
    -- 综合
    total_score         REAL,
    grade               TEXT,                           -- S/A/B/C/D
    signal              TEXT,                           -- strong_buy/buy/watch/hold/avoid
    -- 明细数据（JSON）
    detail              TEXT,                           -- 各子指标得分
    -- 估值数据
    pe_ttm              REAL,
    pb                  REAL,
    peg                 REAL,
    reasonable_pe       REAL,
    safety_margin       REAL,
    -- 财务快照
    roe                 REAL,
    gross_margin        REAL,
    revenue_yoy         REAL,
    profit_yoy          REAL,
    cashflow_profit_ratio REAL,
    debt_ratio          REAL,
    -- 否决
    veto_applied        TEXT,                           -- 触发的否决条件（逗号分隔）
    created_at          TEXT    DEFAULT (datetime('now', 'localtime')),
    PRIMARY KEY (stock_code, score_date)
);

-- ── 索引 ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_earnings_stock     ON earnings(stock_code, report_date);
CREATE INDEX IF NOT EXISTS idx_prices_stock       ON prices(stock_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_analysis_stock     ON analysis_results(stock_code, analysis_type);
CREATE INDEX IF NOT EXISTS idx_consensus_stock    ON consensus(stock_code);
CREATE INDEX IF NOT EXISTS idx_discovery_status   ON discovery_pool(status, score);
CREATE INDEX IF NOT EXISTS idx_events_stock       ON events(stock_code, published_at);
CREATE INDEX IF NOT EXISTS idx_events_type        ON events(event_type, created_at);
CREATE INDEX IF NOT EXISTS idx_market_snap_time   ON market_snapshots(snapshot_time);
CREATE INDEX IF NOT EXISTS idx_stock_scores_code  ON stock_scores(stock_code, score_date);
CREATE INDEX IF NOT EXISTS idx_stock_scores_total ON stock_scores(total_score DESC);
"""


# ── 数据库操作 ────────────────────────────────────────────────────────────────

def _migrate_schema(conn: sqlite3.Connection):
    """增量迁移：添加缺失列（ALTER TABLE ADD COLUMN）。"""
    # earnings 表迁移
    existing_cols = {row[1] for row in conn.execute('PRAGMA table_info(earnings)').fetchall()}
    migrations = [
        ('earnings', 'revenue_yoy', 'REAL'),
        # v2.0 评分体系新增字段
        ('earnings', 'debt_ratio', 'REAL'),              # 资产负债率（%）
        ('earnings', 'operating_cashflow', 'REAL'),       # 经营活动现金流净额（亿元）
        ('earnings', 'cashflow_per_share', 'REAL'),       # 每股经营现金流（元）
        ('earnings', 'inventory_turnover', 'REAL'),       # 存货周转率（次/年）
        ('earnings', 'inventory_days', 'REAL'),           # 存货周转天数
        ('earnings', 'bps', 'REAL'),                      # 每股净资产（元）
        ('earnings', 'roic', 'REAL'),                     # 投入资本回报率（%）
        ('earnings', 'current_ratio', 'REAL'),            # 流动比率
        ('earnings', 'cash_to_revenue', 'REAL'),          # 销售现金/营收比
        ('earnings', 'koufei_net_profit', 'REAL'),        # 扣非净利润（亿元）
        ('earnings', 'koufei_yoy', 'REAL'),               # 扣非净利润同比（%）
        ('earnings', 'gross_margin_yoy', 'REAL'),         # 毛利率同比变化（pp）
        ('earnings', 'total_assets', 'REAL'),             # 总资产（亿元）
    ]
    for table, col, col_type in migrations:
        if col not in existing_cols:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN {col} {col_type}')

    # consensus 表迁移 — v2.6 多源从严
    consensus_cols = {row[1] for row in conn.execute('PRAGMA table_info(consensus)').fetchall()}
    consensus_migrations = [
        ('consensus', 'source_detail', 'TEXT'),  # JSON: 两源原始值 + 选择结果
    ]
    for table, col, col_type in consensus_migrations:
        if col not in consensus_cols:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN {col} {col_type}')


def init_db(db_path: Optional[str] = None) -> str:
    """初始化数据库，创建表和索引。返回数据库路径。"""
    path = db_path or str(DB_PATH)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA_SQL)
    _migrate_schema(conn)
    conn.commit()
    conn.close()
    return path


@contextmanager
def get_connection(db_path: Optional[str] = None):
    """获取数据库连接的上下文管理器。"""
    path = db_path or str(DB_PATH)
    if not os.path.exists(path):
        init_db(path)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row  # 返回字典式行
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(sql: str, params: tuple = (), db_path: Optional[str] = None) -> list:
    """执行查询并返回结果列表。"""
    with get_connection(db_path) as conn:
        cursor = conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def execute_update(sql: str, params: tuple = (), db_path: Optional[str] = None) -> int:
    """执行更新/插入/删除，返回影响行数。"""
    with get_connection(db_path) as conn:
        cursor = conn.execute(sql, params)
        return cursor.rowcount


# ── 模块入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    path = init_db()
    print(f"✅ 数据库初始化完成: {path}")
    # 验证表结构
    tables = execute_query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    print(f"📋 已创建 {len(tables)} 张表: {[t['name'] for t in tables]}")
