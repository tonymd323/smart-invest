"""
数据模型定义
============
使用 dataclass 定义核心数据模型，与数据库表一一对应。
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict
from datetime import datetime


@dataclass
class Stock:
    """股票清单"""
    code: str                          # 股票代码，如 000001.SZ
    name: str                          # 股票名称
    market: str = "A"                  # 市场：A / HK / US
    industry: Optional[str] = None     # 行业分类
    sector: Optional[str] = None       # 板块
    is_active: bool = True             # 是否活跃
    id: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class News:
    """新闻数据"""
    title: str                         # 标题
    source: str                        # 来源
    stock_code: Optional[str] = None   # 关联股票
    content: Optional[str] = None      # 正文
    url: Optional[str] = None          # 原文链接
    sentiment: str = "neutral"         # positive / negative / neutral
    sentiment_score: float = 0.0       # -1.0 ~ 1.0
    published_at: Optional[str] = None # 发布时间
    id: Optional[int] = None
    created_at: Optional[str] = None


@dataclass
class Earnings:
    """业绩数据（含单季度净利润历史新高字段）"""
    stock_code: str                          # 股票代码
    report_date: str                         # 报告期
    report_type: str = "Q4"                  # Q1 / Q2 / Q3 / Q4 / 半年报 / 年报
    revenue: Optional[float] = None          # 营业收入（亿元）
    net_profit: Optional[float] = None       # 净利润（亿元）
    net_profit_yoy: Optional[float] = None   # 净利润同比增长率（%）
    eps: Optional[float] = None              # 每股收益
    # 超预期
    is_beat_expectation: bool = False         # 是否超预期
    expectation_diff_pct: Optional[float] = None  # 超预期幅度（%）
    # 单季度净利润历史新高
    quarterly_profit_new_high: bool = False   # ⭐ 单季度净利润是否创历史新高
    quarterly_net_profit: Optional[float] = None  # 单季度净利润（亿元）
    prev_quarterly_high: Optional[float] = None   # 历史单季度最高净利润（亿元）
    # 其他指标
    roe: Optional[float] = None              # 净资产收益率（%）
    gross_margin: Optional[float] = None     # 毛利率（%）
    id: Optional[int] = None
    created_at: Optional[str] = None


@dataclass
class Price:
    """行情数据"""
    stock_code: str                    # 股票代码
    trade_date: str                    # 交易日期
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: Optional[float] = None     # 成交量（手）
    turnover: Optional[float] = None   # 成交额（亿元）
    change_pct: Optional[float] = None # 涨跌幅（%）
    turnover_rate: Optional[float] = None  # 换手率（%）
    # 技术指标
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None
    rsi6: Optional[float] = None
    macd_dif: Optional[float] = None
    macd_dea: Optional[float] = None
    macd_hist: Optional[float] = None
    id: Optional[int] = None
    created_at: Optional[str] = None


@dataclass
class FundFlow:
    """资金流向"""
    stock_code: str
    trade_date: str
    main_inflow: Optional[float] = None      # 主力净流入（亿元）
    main_outflow: Optional[float] = None     # 主力净流出（亿元）
    main_net_flow: Optional[float] = None    # 主力净流入额（亿元）
    retail_inflow: Optional[float] = None    # 散户净流入（亿元）
    super_large_net: Optional[float] = None  # 超大单净额（亿元）
    large_net: Optional[float] = None        # 大单净额（亿元）
    medium_net: Optional[float] = None       # 中单净额（亿元）
    small_net: Optional[float] = None        # 小单净额（亿元）
    id: Optional[int] = None
    created_at: Optional[str] = None


@dataclass
class AnalysisResult:
    """分析结果"""
    stock_code: str                          # 股票代码
    analysis_type: str                       # news_sentiment / earnings_beat / technical / risk
    score: Optional[float] = None            # 综合评分 0-100
    signal: Optional[str] = None             # buy / hold / sell / watch
    summary: Optional[str] = None            # 分析摘要（JSON 字符串）
    detail: Optional[str] = None             # 详细分析（JSON 字符串）
    confidence: float = 0.0                  # 置信度 0-1
    analyst: str = "system"                  # 分析来源
    id: Optional[int] = None
    created_at: Optional[str] = None


@dataclass
class PushLog:
    """推送记录"""
    push_type: str                           # daily_report / alert / earnings / news
    target: str                              # 推送目标
    title: Optional[str] = None
    content: Optional[str] = None            # 推送内容（Markdown / JSON）
    status: str = "pending"                  # pending / sent / failed
    error_msg: Optional[str] = None
    sent_at: Optional[str] = None
    id: Optional[int] = None
    created_at: Optional[str] = None


@dataclass
class QuoteData:
    """实时行情数据（Provider 标准输出）"""
    stock_code: str                          # 股票代码
    stock_name: str                          # 股票名称
    price: float                             # 最新价
    change_pct: float                        # 涨跌幅（%）
    volume: float                            # 成交量（万手）
    amount: float                            # 成交额（亿元）
    high: float                              # 最高价
    low: float                               # 最低价
    open: float                              # 开盘价
    prev_close: float                        # 昨收价
    turnover_rate: float                     # 换手率（%）
    pe: float                                # 市盈率(TTM)
    total_mv: float                          # 总市值（亿元）
    pb: float = 0.0                          # 市净率(MRQ)
    source: str = "tencent"                  # 数据来源

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "price": self.price,
            "change_pct": self.change_pct,
            "volume": self.volume,
            "amount": self.amount,
            "high": self.high,
            "low": self.low,
            "open": self.open,
            "prev_close": self.prev_close,
            "turnover_rate": self.turnover_rate,
            "pe": self.pe,
            "total_mv": self.total_mv,
            "pb": self.pb,
            "source": self.source,
        }


@dataclass
class NewsData:
    """新闻数据（Provider 标准输出）"""
    stock_code: str                          # 股票代码
    title: str                               # 标题
    content: str                             # 正文摘要
    source: str                              # 数据来源（eastmoney/rss）
    pub_date: str                            # 发布时间（ISO 格式）
    url: str                                 # 原文链接
    sentiment: str = "neutral"               # positive / negative / neutral
    event_type: str = ""                     # earnings / announcement / policy / industry
    source_name: str = ""                    # 媒体名称（如 东方财富、证券时报）

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "title": self.title,
            "content": self.content,
            "source": self.source,
            "pub_date": self.pub_date,
            "url": self.url,
            "sentiment": self.sentiment,
            "event_type": self.event_type,
            "source_name": self.source_name,
        }


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def model_to_dict(obj) -> dict:
    """将 dataclass 转为字典，过滤掉 None 值。"""
    return {k: v for k, v in asdict(obj).items() if v is not None}


def model_to_insert(obj, table: str) -> tuple[str, tuple]:
    """生成 INSERT SQL 和参数。"""
    d = model_to_dict(obj)
    # 移除 id 和 created_at（自动生成）
    d.pop("id", None)
    d.pop("created_at", None)
    columns = ", ".join(d.keys())
    placeholders = ", ".join(["?"] * len(d))
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    return sql, tuple(d.values())


@dataclass
class QuoteData:
    """实时行情数据"""
    stock_code: str                          # 股票代码
    stock_name: str                          # 股票名称
    price: float                             # 最新价
    change_pct: float                        # 涨跌幅（%）
    volume: float                            # 成交量（万手）
    amount: float                            # 成交额（亿元）
    high: float                              # 最高价
    low: float                               # 最低价
    open: float                              # 开盘价
    prev_close: float                        # 昨收价
    turnover_rate: float                     # 换手率（%）
    pe: float                                # 市盈率(TTM)
    total_mv: float                          # 总市值（亿元）
    pb: float = 0.0                          # 市净率(MRQ)
    source: str = "tencent"                  # 数据来源

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MarketSnapshot:
    """全市场快照数据（市场通道专用）"""
    up_count: int                            # 上涨家数
    down_count: int                          # 下跌家数
    flat_count: int                          # 平盘家数
    total_count: int                         # 总数
    btiq: float                              # 涨跌比 = up/(up+down)*100
    ma5: Optional[float] = None              # 5日均值
    signal: Optional[str] = None             # buy / warn / hot / none
    snapshot_time: Optional[str] = None      # 采集时间 (ISO 格式)
    source: str = "tencent"                  # 数据来源

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SectorData:
    """板块轮动数据（Provider 标准输出）"""
    sector_name: str                        # 板块名称
    change_pct: float                       # 涨跌幅（%）
    net_inflow: float                       # 主力净流入（亿）
    up_count: int                           # 上涨家数
    down_count: int                         # 下跌家数
    source: str = "eastmoney"               # 数据来源

    def to_dict(self) -> dict:
        return asdict(self)


if __name__ == "__main__":
    # 快速测试
    stock = Stock(code="000001.SZ", name="平安银行", industry="银行")
    print(f"Stock: {stock}")

    earnings = Earnings(
        stock_code="000001.SZ",
        report_date="2025-12-31",
        net_profit=450.0,
        quarterly_profit_new_high=True,
        quarterly_net_profit=120.0,
    )
    print(f"Earnings: {earnings}")

    sql, params = model_to_insert(stock, "stocks")
    print(f"INSERT SQL: {sql}")
    print(f"Params: {params}")
