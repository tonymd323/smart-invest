"""
数据 Provider 抽象层 — Phase 1

架构：
  BaseProvider（抽象基类）
    └─ FinancialProvider（财务数据，东财 API → Tushare SDK）
    └─ ConsensusProvider（一致预期，东财 F10 → AkShare）
    └─ KlineProvider（日K行情，Tushare → 东财 API）

关键约束：
  - Provider 输出标准化 dataclass
  - 自动降级逻辑内置
  - 禁止 except:pass → 所有异常显式记录
  - 东方财富字段名：PARENTNETPROFIT, TOTALOPERATEREVE, PARENTNETPROFITTZ,
    DJD_TOI_YOY, ROEJQ, XSMLL, EPSJB
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import List, Optional, Any, Dict
import logging
import json
import os
from pathlib import Path
from datetime import datetime, timedelta

from core.models import QuoteData, SectorData

logger = logging.getLogger(__name__)


# ── 标准输出格式 ──────────────────────────────────────────────────────────────

@dataclass
class FinancialData:
    """财务数据标准格式"""
    stock_code: str
    report_date: str
    net_profit: float            # 归母净利润（亿元）
    net_profit_yoy: float        # 归母净利润同比（%）
    revenue: float               # 营业收入（亿元）
    revenue_yoy: float           # 营收同比（%）
    roe: float                   # 加权 ROE（%）
    gross_margin: float          # 毛利率（%）
    eps: float                   # 基本每股收益
    source: str                  # 数据来源标识

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class KlineData:
    """日K行情标准格式"""
    stock_code: str
    trade_date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    amount: float
    change_pct: float
    source: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ConsensusData:
    """一致预期标准格式"""
    stock_code: str
    eps: float                   # 预期每股收益
    net_profit_yoy: float        # 预期净利润同比（%）
    rev_yoy: float               # 预期营收同比（%）
    num_analysts: int            # 分析师数量
    source: str

    def to_dict(self) -> dict:
        return asdict(self)


# ── Provider 抽象基类 ─────────────────────────────────────────────────────────

class BaseProvider(ABC):
    """Provider 抽象基类 — 所有数据源必须继承并实现 fetch"""

    @abstractmethod
    def fetch(self, stock_code: str, **kwargs) -> Any:
        """获取数据，返回标准格式列表或对象"""
        ...

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, 'fetch') or cls.fetch is BaseProvider.fetch:
            raise TypeError(f"{cls.__name__} must implement fetch()")


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _ts_code_to_em(code: str) -> str:
    """000858.SZ → 000858"""
    return code.split('.')[0]


def _em_code_to_ts(code: str) -> str:
    """000858 → 000858.SZ (根据开头判断市场)"""
    # P2#12: 北交所后缀修复
    if code.startswith(('43', '83', '87', '920')):
        return f"{code}.BJ"
    if code.startswith('6') or code.startswith('9'):
        return f"{code}.SH"
    return f"{code}.SZ"


# ── FinancialProvider ─────────────────────────────────────────────────────────

class FinancialProvider(BaseProvider):
    """
    财务数据 Provider
    主源：东方财富 datacenter-web API
    降级：Tushare SDK（fina_indicator）

    用法：
        provider = FinancialProvider(tushare_data=tushare_dict)
        results = provider.fetch("000858.SZ")  # List[FinancialData]
    """

    # 东方财富 API 实际字段名（v2 修正版，基于 Phase 0 Spike 验证）
    # 任务要求字段名 → 实际 API 字段名
    _EM_FIELD_MAP = {
        'PARENTNETPROFIT':    'net_profit',       # 归母净利润（元）
        'PARENTNETPROFITTZ':  'net_profit_yoy',   # 归母净利润同比（%）
        'TOTALOPERATEREVE':   'revenue',           # 营业总收入（元）
        'DJD_TOI_YOY':        'rev_yoy_alt',       # 单季度营收同比（%）
        'DJD_DPNP_YOY':       'net_profit_yoy_alt',# 单季度归母净利润同比（%）
        'ROEJQ':              'roe',                # 加权 ROE（%）
        'XSMLL':              'gross_margin',       # 销售毛利率（%）
        'EPSJB':              'eps',                # 基本每股收益
    }

    BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    def __init__(self, data: dict = None, tushare_data: dict = None,
                 source: str = "eastmoney"):
        """
        Args:
            data: 预注入的东方财富数据（dict，key=ts_code）
            tushare_data: 预注入的 Tushare 降级数据
            source: 数据源标识
        """
        self.data = data
        self.tushare_data = tushare_data or {}
        self.source = source
        self.last_source: str = source

    def fetch(self, stock_code: str, period: str = None) -> List[FinancialData]:
        """
        获取单只股票的财务数据列表（按报告期倒序）

        降级逻辑：
        1. 尝试东方财富 API（或预注入数据）
        2. 失败 → 尝试 Tushare fina_indicator
        3. 都失败 → 返回空列表
        """
        # 尝试主数据源
        results = self._fetch_from_em(stock_code, period)
        if results:
            self.last_source = "eastmoney"
            return results

        # 降级到 Tushare
        logger.info(f"[FinancialProvider] {stock_code} 东财无数据，降级到 Tushare")
        results = self._fetch_from_tushare(stock_code)
        if results:
            self.last_source = "tushare"
            return results

        logger.warning(f"[FinancialProvider] {stock_code} 所有数据源均无数据")
        self.last_source = "none"
        return []

    def _fetch_from_em(self, stock_code: str, period: str = None) -> List[FinancialData]:
        """从东方财富获取（优先预注入数据，其次实时 API）"""
        # 优先使用预注入数据
        if self.data is not None:
            if stock_code in self.data:
                raw = self.data[stock_code]
                items = raw.get("data", []) if isinstance(raw, dict) else []
                return self._parse_em_items(items, stock_code)
            return []

        # 实时调用东方财富 API
        try:
            import requests
            code = _ts_code_to_em(stock_code)
            params = {
                'reportName': 'RPT_F10_FINANCE_MAINFINADATA',
                'columns': 'ALL',
                'filter': f'(SECURITY_CODE="{code}")',
                'pageSize': 10,
                'sortColumns': 'REPORT_DATE',
                'sortTypes': -1,
            }
            if period:
                params['filter'] += f'(REPORT_DATE=\'{period}\')'

            resp = requests.get(self.BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data.get('success') and data.get('result', {}).get('data'):
                return self._parse_em_items(data['result']['data'], stock_code)
            return []
        except Exception as e:
            logger.error(f"[FinancialProvider] 东方财富 API 调用失败 {stock_code}: {e}")
            return []

    def _parse_em_items(self, items: list, stock_code: str) -> List[FinancialData]:
        """解析东方财富 API 返回的多条数据"""
        results = []
        for item in items:
            try:
                fd = self._parse_em_item(item, stock_code)
                if fd:
                    results.append(fd)
            except (KeyError, TypeError, ValueError) as e:
                logger.error(f"[FinancialProvider] 解析 {stock_code} 数据失败: {e}")
                continue
        return results

    @staticmethod
    def _get_field(item: dict, *keys, default=None):
        """从 item 中按优先级取字段值（兼容下划线/无下划线命名）"""
        for k in keys:
            if k in item and item[k] is not None:
                return item[k]
        return default

    def _parse_em_item(self, item: dict, stock_code: str) -> Optional[FinancialData]:
        """解析单条东方财富数据（兼容 v1 下划线 + v2 无下划线字段名）"""
        report_date = (
            item.get("REPORT_DATE")
            or item.get("REPORT_DATE_NAME")
            or ""
        )
        report_date = report_date[:10]
        if not report_date:
            return None

        BILLION = 1e9
        net_profit = (self._get_field(item, "PARENTNETPROFIT", "PARENT_NETPROFIT", default=0) or 0) / BILLION
        revenue = (self._get_field(item, "TOTALOPERATEREVE", "TOTAL_OPERATE_INCOME", default=0) or 0) / BILLION

        # 优先用累计同比，降级到单季度同比
        net_profit_yoy = self._get_field(
            item, "PARENTNETPROFITTZ", "PARENT_NETPROFIT_YOY", "DJD_DPNP_YOY",
            default=0,
        )
        revenue_yoy = self._get_field(
            item, "DJD_TOI_YOY", "TOTAL_OPERATE_INCOME_YOY",
            default=0,
        )

        return FinancialData(
            stock_code=stock_code,
            report_date=report_date,
            net_profit=round(net_profit, 4),
            net_profit_yoy=float(net_profit_yoy or 0),
            revenue=round(revenue, 4),
            revenue_yoy=float(revenue_yoy or 0),
            roe=float(self._get_field(item, "ROEJQ", "WEIGHTAVG_ROE", default=0) or 0),
            gross_margin=float(self._get_field(item, "XSMLL", "GROSS_PROFIT_RATIO", default=0) or 0),
            eps=float(self._get_field(item, "EPSJB", "EPS-basic", default=0) or 0),
            source="eastmoney",
        )

    def _fetch_from_tushare(self, stock_code: str) -> List[FinancialData]:
        """从 Tushare 格式数据提取（降级路径）"""
        if stock_code not in self.tushare_data:
            return []

        raw = self.tushare_data[stock_code]
        items = raw.get("data", []) if isinstance(raw, dict) else []
        if not items:
            return []

        results = []
        for item in items:
            try:
                fd = self._parse_tushare_item(item, stock_code)
                if fd:
                    results.append(fd)
            except (KeyError, TypeError, ValueError) as e:
                logger.error(f"[FinancialProvider] Tushare 解析 {stock_code} 失败: {e}")
                continue
        return results

    def _parse_tushare_item(self, item: dict, stock_code: str) -> Optional[FinancialData]:
        """解析单条 Tushare fina_indicator 数据（兼容 mock 和真实 API 字段名）"""
        report_date = item.get("ann_date") or item.get("REPORT_DATE_NAME") or ""
        if not report_date:
            return None

        # 兼容 mock 数据使用东财格式字段名的情况
        BILLION = 1e9
        if "PARENT_NETPROFIT" in item:
            net_profit = (item.get("PARENT_NETPROFIT") or 0) / BILLION
            revenue = (item.get("TOTAL_OPERATE_INCOME") or 0) / BILLION
            net_profit_yoy = float(item.get("PARENT_NETPROFIT_YOY") or 0)
            revenue_yoy = float(item.get("TOTAL_OPERATE_INCOME_YOY") or 0)
            roe = float(item.get("WEIGHTAVG_ROE") or 0)
            gross_margin = float(item.get("GROSS_PROFIT_RATIO") or 0)
            eps = float(item.get("EPS-basic") or 0)
        else:
            net_profit = float(item.get("net_profit") or 0)
            revenue = float(item.get("or") or 0)
            net_profit_yoy = float(item.get("netprofit_yoy") or 0)
            revenue_yoy = float(item.get("or_yoy") or 0)
            roe = float(item.get("roe") or 0)
            gross_margin = float(item.get("grossprofit_margin") or 0)
            eps = float(item.get("eps") or 0)

        return FinancialData(
            stock_code=stock_code,
            report_date=report_date,
            net_profit=round(net_profit, 4),
            net_profit_yoy=net_profit_yoy,
            revenue=round(revenue, 4),
            revenue_yoy=revenue_yoy,
            roe=roe,
            gross_margin=gross_margin,
            eps=eps,
            source="tushare",
        )

    def fetch_price(self, stock_code: str) -> dict:
        """
        获取收盘价/PE/市值（Tushare daily_basic）

        返回: {"close": float, "pe": float, "total_mv": float} 或空 dict
        """
        try:
            import tushare as ts
            token = __import__('os').environ.get('TUSHARE_TOKEN', '')
            if not token:
                logger.warning("[FinancialProvider] TUSHARE_TOKEN 未设置，跳过 fetch_price")
                return {}
            pro = ts.pro_api(token)
            df = pro.daily_basic(
                ts_code=stock_code,
                fields='ts_code,trade_date,close,pe,total_mv',
                limit=1,
            )
            if df is not None and not df.empty:
                row = df.iloc[0]
                return {
                    "close": float(row.get("close") or 0),
                    "pe": float(row.get("pe") or 0),
                    "total_mv": float(row.get("total_mv") or 0),
                }
        except ImportError:
            logger.error("[FinancialProvider] tushare 库未安装")
        except Exception as e:
            logger.error(f"[FinancialProvider] fetch_price 失败 {stock_code}: {e}")
        return {}


# ── ConsensusProvider ─────────────────────────────────────────────────────────

class ConsensusProvider(BaseProvider):
    """
    一致预期 Provider
    主源：AkShare 东方财富增长对比表（stock_zh_growth_comparison_em）
    返回多年预期：净利润增长率-25E/26E/27E
    降级：东方财富 F10 API

    用法：
        provider = ConsensusProvider()
        # 单年预期
        result = provider.fetch("600660.SH")  # ConsensusData (当年) 或 None
        # 多年预期
        results = provider.fetch_multi_year("600660.SH")  # {"25E": ConsensusData, ...}
    """

    def __init__(self, data: dict = None):
        self.data = data or {}
        self.last_source: str = "none"

    def fetch(self, stock_code: str) -> Optional[ConsensusData]:
        """获取当年一致预期（取最近一年可用数据）"""
        multi = self.fetch_multi_year(stock_code)
        if not multi:
            return None
        # 取最新一年
        for year in ['25E', '26E', '27E']:
            if year in multi:
                return multi[year]
        return None

    def fetch_multi_year(self, stock_code: str) -> dict:
        """
        获取多年一致预期

        Returns:
            {"25E": ConsensusData, "26E": ConsensusData, "27E": ConsensusData}
        """
        # 尝试预注入数据
        if stock_code in self.data:
            raw = self.data[stock_code]
            result = {}
            for year in ['25E', '26E', '27E']:
                profit = raw.get(f'profit_{year.lower()}')
                if profit is not None:
                    result[year] = ConsensusData(
                        stock_code=stock_code,
                        eps=0,
                        net_profit_yoy=float(profit),
                        rev_yoy=float(raw.get(f'rev_{year.lower()}') or 0),
                        num_analysts=0,
                        source="preloaded",
                    )
            if result:
                self.last_source = "preloaded"
                return result

        # 主源：AkShare 东方财富增长对比表
        result = self._fetch_from_akshare_growth(stock_code)
        if result:
            self.last_source = "akshare_growth"
            return result

        # 降级：东方财富 F10
        result = self._fetch_from_em_f10(stock_code)
        if result:
            self.last_source = "eastmoney_f10"
            return result

        logger.warning(f"[ConsensusProvider] {stock_code} 所有数据源均无预期数据")
        self.last_source = "none"
        return {}

    def _fetch_from_akshare_growth(self, stock_code: str) -> dict:
        """从 AkShare 东方财富增长对比表获取多年预期"""
        try:
            import akshare as ak
            import math
            # AkShare 需要大写交易所前缀: SH600660
            short = stock_code.split('.')[0]
            prefix = 'SH' if stock_code.endswith('.SH') else 'SZ'
            ak_symbol = f'{prefix}{short}'

            df = ak.stock_zh_growth_comparison_em(symbol=ak_symbol)
            if df is None or df.empty:
                return {}

            short_code = stock_code.split('.')[0]  # 600660.SH → 600660
            row = df[df['代码'] == short_code]
            if row.empty:
                return {}
            r = row.iloc[0]

            result = {}
            for year in ['25E', '26E', '27E']:
                profit_col = f'净利润增长率-{year}'
                rev_col = f'营业收入增长率-{year}'
                profit_val = r.get(profit_col)
                rev_val = r.get(rev_col)

                if profit_val is None or (isinstance(profit_val, float) and math.isnan(profit_val)):
                    continue

                result[year] = ConsensusData(
                    stock_code=stock_code,
                    eps=0,
                    net_profit_yoy=float(profit_val),
                    rev_yoy=float(rev_val) if rev_val is not None and not (isinstance(rev_val, float) and math.isnan(rev_val)) else 0,
                    num_analysts=0,
                    source="akshare_growth",
                )

            return result
        except ImportError:
            logger.error("[ConsensusProvider] akshare 库未安装")
        except Exception as e:
            logger.error(f"[ConsensusProvider] AkShare 增长对比获取失败 {stock_code}: {e}")
        return {}

    def _fetch_from_em_f10(self, stock_code: str) -> dict:
        """从东方财富 F10 获取一致预期（降级）"""
        try:
            import requests
            code = _ts_code_to_em(stock_code)
            resp = requests.get(
                "https://datacenter-web.eastmoney.com/api/data/v1/get",
                params={
                    'reportName': 'RPT_RES_ORGRATINGSTAT',
                    'columns': 'SECURITY_CODE,NETPROFITTHISYEAR,REVENUE_THISYEAR,ANALYST_NUM',
                    'filter': f'(SECURITY_CODE="{code}")',
                    'pageSize': 1,
                    'sortColumns': 'REPORT_DATE',
                    'sortTypes': -1,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get('success') and data.get('result', {}).get('data'):
                item = data['result']['data'][0]
                profit = float(item.get("NETPROFITTHISYEAR") or 0)
                if profit != 0:
                    # F10 只有当年，放到 25E
                    return {"25E": ConsensusData(
                        stock_code=stock_code,
                        eps=0,
                        net_profit_yoy=profit,
                        rev_yoy=float(item.get("REVENUE_THISYEAR") or 0),
                        num_analysts=int(item.get("ANALYST_NUM") or 0),
                        source="eastmoney_f10",
                    )}
        except Exception as e:
            logger.error(f"[ConsensusProvider] 东方财富 F10 获取失败 {stock_code}: {e}")
        return {}


class KlineProvider(BaseProvider):
    """
    日K行情 Provider
    主源：Tushare pro.daily()
    降级：东方财富行情 API

    用法：
        provider = KlineProvider(data=tushare_kline_dict)
        df = provider.fetch("000858.SZ", start_date="20260101", end_date="20260327")
    """

    def __init__(self, data: dict = None, source: str = "tushare"):
        self.data = data or {}
        self.source = source
        self.last_source: str = source

    def fetch(self, stock_code: str, start_date: str = None,
              end_date: str = None, limit: int = 120) -> List[KlineData]:
        """
        获取日K数据

        降级逻辑：
        1. 尝试 Tushare pro.daily()（或预注入数据）
        2. 失败 → 尝试东方财富行情 API
        3. 都失败 → 返回空列表
        """
        # 尝试主数据源
        results = self._fetch_from_tushare(stock_code, start_date, end_date, limit)
        if results:
            self.last_source = "tushare"
            return results

        # 降级到东方财富
        logger.info(f"[KlineProvider] {stock_code} Tushare 无数据，降级到东财")
        results = self._fetch_from_em(stock_code, start_date, end_date, limit)
        if results:
            self.last_source = "eastmoney"
            return results

        logger.warning(f"[KlineProvider] {stock_code} 所有数据源均无K线数据")
        self.last_source = "none"
        return []

    def _fetch_from_tushare(self, stock_code: str, start_date: str = None,
                            end_date: str = None, limit: int = 120) -> List[KlineData]:
        """从 Tushare 获取日K"""
        # 优先使用预注入数据
        if stock_code in self.data:
            raw_items = self.data[stock_code]
            if isinstance(raw_items, list):
                return self._parse_tushare_items(raw_items, stock_code)

        # 实时调用 Tushare API
        try:
            import tushare as ts
            token = __import__('os').environ.get('TUSHARE_TOKEN', '')
            if not token:
                logger.warning("[KlineProvider] TUSHARE_TOKEN 未设置，跳过 Tushare")
                return []
            pro = ts.pro_api(token)
            df = pro.daily(
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date,
            )
            if df is not None and not df.empty:
                df = df.head(limit)
                results = []
                for _, row in df.iterrows():
                    results.append(KlineData(
                        stock_code=stock_code,
                        trade_date=str(row.get("trade_date", "")),
                        open_price=float(row.get("open") or 0),
                        high_price=float(row.get("high") or 0),
                        low_price=float(row.get("low") or 0),
                        close_price=float(row.get("close") or 0),
                        volume=float(row.get("vol") or 0),
                        amount=float(row.get("amount") or 0),
                        change_pct=float(row.get("pct_chg") or 0),
                        source="tushare",
                    ))
                return results
        except ImportError:
            logger.error("[KlineProvider] tushare 库未安装")
        except Exception as e:
            logger.error(f"[KlineProvider] Tushare 日K 获取失败 {stock_code}: {e}")
        return []

    def _parse_tushare_items(self, items: list, stock_code: str) -> List[KlineData]:
        """解析 Tushare 格式的 K 线数据（预注入）"""
        results = []
        for item in items:
            try:
                results.append(KlineData(
                    stock_code=stock_code,
                    trade_date=str(item.get("trade_date", "")),
                    open_price=float(item.get("open") or 0),
                    high_price=float(item.get("high") or 0),
                    low_price=float(item.get("low") or 0),
                    close_price=float(item.get("close") or 0),
                    volume=float(item.get("vol") or 0),
                    amount=float(item.get("amount") or 0),
                    change_pct=float(item.get("pct_chg") or 0),
                    source="tushare",
                ))
            except (KeyError, TypeError, ValueError) as e:
                logger.error(f"[KlineProvider] Tushare K线解析失败 {stock_code}: {e}")
                continue
        return results

    def _fetch_from_em(self, stock_code: str, start_date: str = None,
                       end_date: str = None, limit: int = 120) -> List[KlineData]:
        """从东方财富行情 API 获取（降级路径）"""
        try:
            import requests
            code = _ts_code_to_em(stock_code)
            # 东方财富日K API (1=上证, 0=深证)
            market = "1" if stock_code.endswith(".SH") else "0"
            resp = requests.get(
                f"https://push2his.eastmoney.com/api/qt/stock/kline/get",
                params={
                    "secid": f"{market}.{code}",
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57",
                    "klt": "101",  # 日K
                    "fqt": "1",
                    "end": "20500101",
                    "lmt": limit,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            klines = data.get("data", {}).get("klines", [])
            if not klines:
                return []

            results = []
            for line in klines:
                parts = line.split(",")
                if len(parts) >= 7:
                    results.append(KlineData(
                        stock_code=stock_code,
                        trade_date=parts[0].replace("-", ""),
                        open_price=float(parts[1]),
                        close_price=float(parts[2]),
                        high_price=float(parts[3]),
                        low_price=float(parts[4]),
                        volume=float(parts[5]),
                        amount=float(parts[6]),
                        change_pct=0.0,  # 东财K线不直接提供涨跌幅
                        source="eastmoney",
                    ))
            return results
        except Exception as e:
            logger.error(f"[KlineProvider] 东方财富K线获取失败 {stock_code}: {e}")
            return []


# ── QuoteProvider ─────────────────────────────────────────────────────────────

# 股票代码 → 腾讯行情前缀转换
def _ts_code_to_tencent(code: str) -> str:
    """000858.SZ → sz000858"""
    raw = code.split('.')[0]
    if code.endswith('.SH') or raw.startswith(('6', '9')):
        return f"sh{raw}"
    return f"sz{raw}"


def _ts_code_to_em_secid(code: str) -> str:
    """600660.SH → 1.600660 (东财 secid 格式)"""
    raw = code.split('.')[0]
    if code.endswith('.SH') or raw.startswith(('6', '9')):
        return f"1.{raw}"
    return f"0.{raw}"


class QuoteProvider(BaseProvider):
    """
    实时行情 Provider
    主源：腾讯行情 API（qt.gtimg.cn）
    降级：东方财富 Push2 API

    用法：
        provider = QuoteProvider()
        records = provider.fetch("600660.SH")         # List[QuoteData]（单只）
        quotes = provider.fetch_batch(["600660.SH", "000858.SZ"])  # Dict 批量
    """

    TENCENT_URL = "https://qt.gtimg.cn/q="
    EM_URL = "https://push2.eastmoney.com/api/qt/stock/get"

    def __init__(self, source: str = "tencent"):
        self.source = source
        self.last_source: str = source

    def fetch(self, stock_code: str) -> List[QuoteData]:
        """
        获取单只股票实时行情。

        降级逻辑：
        1. 腾讯行情 API（批量支持，逗号分隔多只股票）
        2. 失败 → 东方财富 Push2
        3. 都失败 → 返回空列表
        """
        # 尝试腾讯行情
        results = self._fetch_from_tencent([stock_code])
        if results:
            self.last_source = "tencent"
            return results

        # 降级到东方财富
        logger.info(f"[QuoteProvider] {stock_code} 腾讯行情失败，降级到东财 Push2")
        result = self._fetch_from_eastmoney(stock_code)
        if result:
            self.last_source = "eastmoney"
            return [result]

        logger.warning(f"[QuoteProvider] {stock_code} 所有行情源均无数据")
        self.last_source = "none"
        return []

    def fetch_batch(self, stock_codes: List[str]) -> Dict[str, QuoteData]:
        """
        批量获取行情数据。

        腾讯行情 API 支持逗号分隔多只股票，单次可传数百只，
        充分利用批量接口提升性能。

        Args:
            stock_codes: 股票代码列表，如 ["600660.SH", "000858.SZ"]

        Returns:
            Dict[str, QuoteData]: key=股票代码, value=行情数据
        """
        if not stock_codes:
            return {}

        # 尝试腾讯行情（批量）
        results = self._fetch_from_tencent(stock_codes)
        result_map = {r.stock_code: r for r in results}

        # 对缺失的股票降级到东财
        missing = [c for c in stock_codes if c not in result_map]
        if missing:
            logger.info(f"[QuoteProvider] {len(missing)} 只降级到东财 Push2")
            for code in missing:
                result = self._fetch_from_eastmoney(code)
                if result:
                    result_map[code] = result

        if results:
            self.last_source = "tencent"
        elif result_map:
            self.last_source = "eastmoney"
        else:
            self.last_source = "none"

        return result_map

    def _fetch_from_tencent(self, stock_codes: List[str]) -> List[QuoteData]:
        """从腾讯行情 API 获取实时行情"""
        try:
            import urllib.request
            # 转换代码格式：600660.SH → sh600660
            tencent_codes = [_ts_code_to_tencent(c) for c in stock_codes]
            # 分批请求，每批 800 只
            batch_size = 800
            results = []

            for start in range(0, len(tencent_codes), batch_size):
                batch = tencent_codes[start:start + batch_size]
                url = self.TENCENT_URL + ",".join(batch)

                req = urllib.request.Request(url)
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(req, timeout=15) as resp:
                    data = resp.read().decode("gbk", errors="ignore")

                for line in data.split(";"):
                    if "~" not in line:
                        continue
                    try:
                        quote = self._parse_tencent_line(line, stock_codes)
                        if quote:
                            results.append(quote)
                    except (ValueError, IndexError) as e:
                        logger.debug(f"[QuoteProvider] 解析腾讯行情行失败: {e}")
                        continue

            return results
        except Exception as e:
            logger.error(f"[QuoteProvider] 腾讯行情 API 调用失败: {e}")
            return []

    @staticmethod
    def _parse_tencent_line(line: str, stock_codes: list) -> Optional[QuoteData]:
        """
        解析腾讯行情单行数据。

        腾讯行情返回格式（~ 分隔）：
        0=市场(1深/2上), 1=名称, 2=代码, 3=最新价, 4=昨收, 5=今开,
        6=成交量(手), 7=外盘, 8=内盘, 9-20=买一~五, 21-32=卖一~五,
        33=涨跌, 34=涨幅%, 35=成交额, 36=最高, 37=最低, 38=换手率%,
        39=市盈率, 40=总市值, 41=流通市值
        """
        parts = line.split("~")
        if len(parts) < 42:
            return None

        code = parts[2].strip()
        name = parts[1].strip()
        price = float(parts[3]) if parts[3] else 0

        if price <= 0 or not code:
            return None

        # 映射回标准代码格式
        market = "SH" if "sh" in parts[0].lower() else "SZ"  # v_sz000858 or v_sh600660
        ts_code = f"{code}.{market}"

        prev_close = float(parts[4]) if parts[4] else 0
        change_pct = float(parts[32]) if parts[32] else 0  # [32] 涨跌幅%
        # 成交量转万手（腾讯返回单位：手）
        volume = float(parts[6]) / 10000 if parts[6] else 0
        # 成交额从复合字段提取（parts[35] = "价格/成交量/成交额元"）
        amount = 0
        if parts[35] and '/' in parts[35]:
            composite = parts[35].split('/')
            if len(composite) >= 3:
                amount = float(composite[2]) / 1e8  # 元→亿
        high = float(parts[33]) if parts[33] else 0  # [33] 最高价
        low = float(parts[34]) if parts[34] else 0  # [34] 最低价
        open_price = float(parts[5]) if parts[5] else 0
        turnover_rate = float(parts[38]) if parts[38] else 0  # [38] 换手率%
        pe = float(parts[39]) if parts[39] else 0  # [39] PE
        # 总市值转亿元（腾讯返回单位：万元）
        total_mv = float(parts[44]) if len(parts) > 44 and parts[44] else 0  # [44] 总市值(亿)

        return QuoteData(
            stock_code=ts_code,
            stock_name=name,
            price=round(price, 2),
            change_pct=round(change_pct, 2),
            volume=round(volume, 2),
            amount=round(amount, 4),
            high=round(high, 2),
            low=round(low, 2),
            open=round(open_price, 2),
            prev_close=round(prev_close, 2),
            turnover_rate=round(turnover_rate, 2),
            pe=round(pe, 2),
            total_mv=round(total_mv, 2),
            source="tencent",
        )

    def _fetch_from_eastmoney(self, stock_code: str) -> Optional[QuoteData]:
        """从东方财富 Push2 获取实时行情（降级路径）"""
        try:
            import requests
            secid = _ts_code_to_em_secid(stock_code)
            resp = requests.get(
                self.EM_URL,
                params={
                    "secid": secid,
                    "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f169,f170,f171,f116,f117",
                    "ut": "fa5fd1943c7b386f172d6893dbbd4065",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            item = data.get("data")
            if not item:
                return None

            # f43=最新价(×100), f44=最高(×100), f45=最低(×100), f46=今开(×100)
            # f47=成交量(手), f48=成交额(元), f57=代码, f58=名称
            # f169=涨跌(×100), f170=涨幅%(×100), f171=换手率%(×100)
            # f116=总市值, f117=流通市值
            DIV = 100.0
            code = item.get("f57", "")
            name = item.get("f58", "")
            price = (item.get("f43") or 0) / DIV

            if price <= 0:
                return None

            market = "SH" if stock_code.endswith(".SH") else "SZ"
            ts_code = f"{code}.{market}"

            return QuoteData(
                stock_code=ts_code,
                stock_name=name,
                price=round(price, 2),
                change_pct=round((item.get("f170") or 0) / DIV, 2),
                volume=round((item.get("f47") or 0) / 10000, 2),
                amount=round((item.get("f48") or 0) / 1e8, 4),
                high=round((item.get("f44") or 0) / DIV, 2),
                low=round((item.get("f45") or 0) / DIV, 2),
                open=round((item.get("f46") or 0) / DIV, 2),
                prev_close=round(price - ((item.get("f169") or 0) / DIV), 2),
                turnover_rate=round((item.get("f171") or 0) / DIV, 2),
                pe=0.0,
                total_mv=round((item.get("f116") or 0) / 1e8, 2),
                source="eastmoney",
            )
        except Exception as e:
            logger.error(f"[QuoteProvider] 东方财富 Push2 获取失败 {stock_code}: {e}")
            return None


# ── NewsProvider ──────────────────────────────────────────────────────────────

# 导入 NewsData 模型（延迟导入避免循环依赖）
def _get_news_data_class():
    """延迟导入 NewsData"""
    from core.models import NewsData
    return NewsData


class NewsProvider(BaseProvider):
    """
    新闻采集 Provider
    主源：东方财富个股新闻 API
    降级：RSS 文章缓存（event_monitor 路径）

    用法：
        provider = NewsProvider()
        news_list = provider.fetch("600660.SH")  # List[NewsData]
    """

    # 东方财富个股新闻 API
    EM_NEWS_URL = "https://search-api-web.eastmoney.com/api/suggest/get"

    # RSS 缓存路径（复用 rss-reader skill 和 event_monitor 的数据）
    RSS_CACHE_PATH = Path.home() / ".openclaw/skills/rss-reader/data/articles.json"

    # 股票关键词映射（从 event_monitor.py 复用）
    STOCK_KEYWORDS = {
        "600660.SH": ["福耀玻璃", "曹德旺"],
        "600938.SH": ["中国海油", "中海油"],
        "600875.SH": ["东方电气"],
        "603308.SH": ["应流股份"],
        "600989.SH": ["宝丰能源"],
        "002545.SZ": ["东方铁塔"],
        "300750.SZ": ["宁德时代", "CATL"],
        "000807.SZ": ["云铝股份"],
    }

    def __init__(self, data: dict = None, source: str = "eastmoney"):
        """
        Args:
            data: 预注入的新闻数据（dict，key=ts_code，value=新闻列表）
            source: 数据源标识
        """
        self.data = data or {}
        self.source = source
        self.last_source: str = source

    def fetch(self, stock_code: str, limit: int = 20) -> List:
        """
        获取指定股票的最新新闻

        降级逻辑：
        1. 尝试东方财富个股新闻 API（或预注入数据）
        2. 失败 → 降级到 RSS 文章缓存关键词匹配
        3. 都失败 → 返回空列表
        """
        NewsData = _get_news_data_class()

        # 优先使用预注入数据
        if stock_code in self.data:
            items = self.data[stock_code]
            results = []
            for item in items[:limit]:
                results.append(NewsData(
                    stock_code=stock_code,
                    title=item.get("title", ""),
                    content=item.get("content", ""),
                    source="eastmoney",
                    pub_date=item.get("pub_date", ""),
                    url=item.get("url", ""),
                    sentiment=item.get("sentiment", "neutral"),
                    event_type=item.get("event_type", ""),
                    source_name=item.get("source_name", ""),
                ))
            if results:
                self.last_source = "eastmoney"
                return results

        # 尝试东方财富个股新闻 API
        results = self._fetch_from_eastmoney(stock_code, limit)
        if results:
            self.last_source = "eastmoney"
            return results

        # 降级到 RSS 匹配
        logger.info(f"[NewsProvider] {stock_code} 东财无新闻，降级到 RSS 匹配")
        results = self._fetch_from_rss(stock_code, limit)
        if results:
            self.last_source = "rss"
            return results

        logger.warning(f"[NewsProvider] {stock_code} 所有数据源均无新闻")
        self.last_source = "none"
        return []

    def _fetch_from_eastmoney(self, stock_code: str, limit: int = 20) -> List:
        """从东方财富搜索 API 获取个股新闻"""
        NewsData = _get_news_data_class()
        try:
            import requests
            code = _ts_code_to_em(stock_code)
            resp = requests.get(
                "https://search-api-web.eastmoney.com/api/suggest/get",
                params={
                    "input": code,
                    "type": "14",
                    "token": "",
                    "count": limit,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            # 东方财富搜索 API 返回的新闻结构
            results = []
            news_list = data.get("data", {}).get("news", [])
            if not news_list:
                # 尝试另一种 API 格式
                news_list = data.get("news", [])

            for item in news_list[:limit]:
                title = item.get("title", "")
                content = item.get("content", "") or item.get("summary", "") or ""
                url = item.get("url", "") or item.get("link", "") or ""
                pub_date = item.get("date", "") or item.get("publishTime", "") or ""
                media = item.get("mediaName", "") or item.get("source", "") or ""

                # 规范化日期格式
                if pub_date and len(pub_date) == 10 and "-" not in pub_date:
                    pub_date = f"{pub_date[:4]}-{pub_date[4:6]}-{pub_date[6:8]}"

                results.append(NewsData(
                    stock_code=stock_code,
                    title=title,
                    content=content[:500],
                    source="eastmoney",
                    pub_date=pub_date,
                    url=url,
                    source_name=media,
                ))

            if results:
                logger.info(f"[NewsProvider] 东方财富返回 {len(results)} 条新闻: {stock_code}")
                return results

        except Exception as e:
            logger.error(f"[NewsProvider] 东方财富新闻 API 失败 {stock_code}: {e}")

        # 尝试东方财富资讯列表 API（备用）
        return self._fetch_from_em_list(stock_code, limit)

    def _fetch_from_em_list(self, stock_code: str, limit: int = 20) -> List:
        """从东方财富资讯列表 API 获取（备用接口）"""
        NewsData = _get_news_data_class()
        try:
            import requests
            code = _ts_code_to_em(stock_code)
            # 东方财富公司公告/资讯 API
            resp = requests.get(
                "https://np-anotice-stock.eastmoney.com/api/security/ann",
                params={
                    "sr": -1,
                    "page_size": limit,
                    "page_index": 1,
                    "ann_type": "A",
                    "client_source": "web",
                    "stock_list": code,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            results = []
            items = data.get("data", {}).get("list", [])
            for item in items[:limit]:
                title = item.get("title", "")
                notice_date = item.get("notice_date", "")
                url = item.get("url", "") or ""
                if not url and item.get("art_code"):
                    url = f"https://data.eastmoney.com/notices/detail/{code}/{item['art_code']}.html"

                results.append(NewsData(
                    stock_code=stock_code,
                    title=title,
                    content="",
                    source="eastmoney",
                    pub_date=notice_date,
                    url=url,
                    event_type="announcement",
                    source_name="东方财富",
                ))

            if results:
                logger.info(f"[NewsProvider] 东财经讯列表返回 {len(results)} 条: {stock_code}")
                return results

        except Exception as e:
            logger.error(f"[NewsProvider] 东财经讯列表 API 失败 {stock_code}: {e}")
        return []

    def _fetch_from_rss(self, stock_code: str, limit: int = 20) -> List:
        """从 RSS 缓存中按关键词匹配股票新闻"""
        NewsData = _get_news_data_class()

        if not self.RSS_CACHE_PATH.exists():
            logger.info(f"[NewsProvider] RSS 缓存文件不存在: {self.RSS_CACHE_PATH}")
            return []

        try:
            data = json.loads(self.RSS_CACHE_PATH.read_text())
            if not isinstance(data, dict):
                return []

            # 获取关键词
            keywords = self.STOCK_KEYWORDS.get(stock_code, [])
            if not keywords:
                # 尝试从 stocks 表获取名称（需要 db_path）
                logger.info(f"[NewsProvider] {stock_code} 无预定义关键词，跳过 RSS 匹配")
                return []

            # 24 小时时间窗口
            cutoff = datetime.now() - timedelta(hours=24)
            results = []

            for url, article in data.items():
                # 检查时间窗口
                read_at = article.get("read_at", "")
                if read_at:
                    try:
                        art_time = datetime.fromisoformat(read_at)
                        if art_time < cutoff:
                            continue
                    except (ValueError, TypeError):
                        pass

                title = article.get("title", "")
                summary = article.get("summary", "")
                source = article.get("source", "")
                link = article.get("link", url)
                content = f"{title} {summary}"

                # 关键词匹配
                matched = False
                for kw in keywords:
                    if kw.lower() in content.lower():
                        matched = True
                        break

                if matched:
                    results.append(NewsData(
                        stock_code=stock_code,
                        title=title,
                        content=summary[:500],
                        source="rss",
                        pub_date=read_at or datetime.now().isoformat(),
                        url=link,
                        source_name=source,
                    ))

                    if len(results) >= limit:
                        break

            if results:
                logger.info(f"[NewsProvider] RSS 匹配 {len(results)} 条: {stock_code}")

            return results

        except json.JSONDecodeError as e:
            logger.error(f"[NewsProvider] RSS 缓存 JSON 解析失败: {e}")
        except Exception as e:
            logger.error(f"[NewsProvider] RSS 匹配失败 {stock_code}: {e}")
        return []


# ── SectorProvider ────────────────────────────────────────────────────────────

class SectorProvider(BaseProvider):
    """
    板块数据 Provider
    主源：东方财富 Push2 行业板块 API
    降级：AkShare 板块资金流向

    用法：
        provider = SectorProvider()
        sectors = provider.fetch()  # List[SectorData]
    """

    # 东方财富板块排行 API
    EM_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    # 字段映射：f14=板块名, f3=涨跌幅(×100), f62=主力净流入(元),
    #           f104=上涨数, f105=下跌数
    EM_PARAMS = {
        "pn": 1,
        "pz": 100,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": "m:90+t:2",
        "fields": "f14,f3,f62,f104,f105",
    }

    def __init__(self, source: str = "eastmoney"):
        self.source = source
        self.last_source: str = source

    def fetch(self, stock_code: str = None) -> List[SectorData]:
        """
        获取板块轮动数据（行业板块涨跌排行）。

        降级逻辑：
        1. 东方财富 Push2 API
        2. 失败 → AkShare 板块资金流向
        3. 都失败 → 返回空列表
        """
        results = self._fetch_from_eastmoney()
        if results:
            self.last_source = "eastmoney"
            return results

        logger.info("[SectorProvider] 东财无数据，降级到 AkShare")
        results = self._fetch_from_akshare()
        if results:
            self.last_source = "akshare"
            return results

        logger.warning("[SectorProvider] 所有数据源均无板块数据")
        self.last_source = "none"
        return []

    def _fetch_from_eastmoney(self) -> List[SectorData]:
        """从东方财富 Push2 获取行业板块数据"""
        try:
            import requests
            resp = requests.get(self.EM_URL, params=self.EM_PARAMS, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("data", {}).get("diff", [])
            if not items:
                return []

            results = []
            for item in items:
                try:
                    name = item.get("f14", "")
                    if not name:
                        continue
                    # f3: 涨跌幅(×100), f62: 主力净流入(元), f104: 上涨数, f105: 下跌数
                    change_pct = (item.get("f3") or 0) / 100.0
                    net_inflow = (item.get("f62") or 0) / 1e8  # 转亿
                    up_count = item.get("f104") or 0
                    down_count = item.get("f105") or 0

                    results.append(SectorData(
                        sector_name=name,
                        change_pct=round(change_pct, 2),
                        net_inflow=round(net_inflow, 2),
                        up_count=int(up_count),
                        down_count=int(down_count),
                        source="eastmoney",
                    ))
                except (KeyError, TypeError, ValueError) as e:
                    logger.debug(f"[SectorProvider] 解析板块数据异常: {e}")
                    continue

            if results:
                logger.info(f"[SectorProvider] 东财返回 {len(results)} 个板块")
                return results

        except Exception as e:
            logger.error(f"[SectorProvider] 东财 API 调用失败: {e}")
        return []

    def _fetch_from_akshare(self) -> List[SectorData]:
        """从 AkShare 获取板块资金流向（降级路径）"""
        try:
            import akshare as ak
            df = ak.stock_sector_fund_flow_rank(indicator="今日")
            if df is None or df.empty:
                return []

            results = []
            for _, row in df.iterrows():
                try:
                    name = str(row.get("名称", ""))
                    if not name:
                        continue
                    change_val = row.get("今日涨跌幅")
                    change_pct = float(change_val) if change_val is not None and str(change_val) != "nan" else 0.0
                    fund_val = row.get("今日主力净流入-净额")
                    net_inflow = round(float(fund_val) / 1e8, 2) if fund_val is not None and str(fund_val) != "nan" else 0.0
                    # AkShare 不直接提供上涨/下跌数，用 0 占位
                    results.append(SectorData(
                        sector_name=name,
                        change_pct=round(change_pct, 2),
                        net_inflow=net_inflow,
                        up_count=0,
                        down_count=0,
                        source="akshare",
                    ))
                except (ValueError, TypeError) as e:
                    logger.debug(f"[SectorProvider] AkShare 解析异常: {e}")
                    continue

            if results:
                logger.info(f"[SectorProvider] AkShare 返回 {len(results)} 个板块")
                return results

        except ImportError:
            logger.error("[SectorProvider] akshare 库未安装")
        except Exception as e:
            logger.error(f"[SectorProvider] AkShare 获取失败: {e}")
        return []
