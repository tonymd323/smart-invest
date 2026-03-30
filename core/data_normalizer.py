"""
数据标准化层 — v2.23

所有数据入库前必须经过此模块。禁止任何模块直接 INSERT。

标准化规则：
  - 日期：统一 YYYY-MM-DD（ISO 8601）
  - 股票代码：统一 canonical_code（如 000001.SZ）
  - 百分比：round(2)，5.23 表示 5.23%
  - 价格：round(2)
  - 金额：亿元，round(4)
"""

import re
from typing import Optional


class DataNormalizer:
    """数据格式标准化"""

    # 交易所后缀映射
    EXCHANGE_MAP = {
        '6': 'SH',   # 上证主板
        '9': 'SH',   # 上证B股
        '0': 'SZ',   # 深证主板
        '3': 'SZ',   # 创业板
        '8': 'BJ',   # 北交所
        '4': 'BJ',   # 北交所
    }

    @staticmethod
    def normalize_date(raw: Optional[str]) -> Optional[str]:
        """
        任意日期格式 → YYYY-MM-DD

        支持：
          - '20260330'      → '2026-03-30'
          - '2026-03-30'    → '2026-03-30'（原样返回）
          - '2026/03/30'    → '2026-03-30'
          - '20260330 10:00:00' → '2026-03-30'
          - None / ''       → None
        """
        if not raw:
            return None

        raw = str(raw).strip()

        # 已经是标准格式
        if re.match(r'^\d{4}-\d{2}-\d{2}$', raw):
            return raw

        # YYYYMMDD（8位纯数字）
        if re.match(r'^\d{8}$', raw):
            return f'{raw[:4]}-{raw[4:6]}-{raw[6:8]}'

        # YYYY/MM/DD
        if re.match(r'^\d{4}/\d{1,2}/\d{1,2}$', raw):
            parts = raw.split('/')
            return f'{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}'

        # YYYYMMDD HH:MM:SS 或 YYYYMMDDHHMMSS
        compact_date = raw.replace(' ', '').replace('-', '').replace(':', '')[:8]
        if re.match(r'^\d{8}$', compact_date):
            return f'{compact_date[:4]}-{compact_date[4:6]}-{compact_date[6:8]}'

        # YYYY-MM-DD HH:MM:SS → 取日期部分
        if len(raw) >= 10 and raw[4] == '-' and raw[7] == '-':
            return raw[:10]

        return raw

    @staticmethod
    def normalize_date_compact(raw: Optional[str]) -> Optional[str]:
        """
        任意日期格式 → YYYYMMDD（Tushare 兼容格式）

        用于跟 prices 表 trade_date 比较。
        """
        iso = DataNormalizer.normalize_date(raw)
        if iso:
            return iso.replace('-', '')
        return None

    @classmethod
    def normalize_code(cls, raw: Optional[str]) -> Optional[str]:
        """
        任意股票代码格式 → canonical_code（如 000001.SZ）

        支持：
          - '000001.SZ'  → '000001.SZ'（原样返回）
          - '000001'     → '000001.SZ'（根据首位判断交易所）
          - 'sz000001'   → '000001.SZ'
          - 'SZ000001'   → '000001.SZ'
          - None / ''    → None
        """
        if not raw:
            return None

        raw = str(raw).strip().upper()

        # 已有交易所后缀
        if re.match(r'^\d{6}\.(SH|SZ|BJ)$', raw):
            return raw

        # 去掉交易所前缀（如 SZ000001）
        raw = re.sub(r'^(SH|SZ|BJ)', '', raw)

        # 纯数字
        digits = re.sub(r'\D', '', raw)
        if len(digits) == 6:
            exchange = cls.EXCHANGE_MAP.get(digits[0], 'SZ')
            return f'{digits}.{exchange}'

        return raw

    @staticmethod
    def normalize_pct(value: Optional[float]) -> Optional[float]:
        """百分比标准化：round(2)"""
        if value is None:
            return None
        try:
            return round(float(value), 2)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_price(value: Optional[float]) -> Optional[float]:
        """价格标准化：round(2)"""
        if value is None:
            return None
        try:
            v = float(value)
            if v <= 0:
                return None
            return round(v, 2)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_amount(value: Optional[float], decimals: int = 4) -> Optional[float]:
        """金额标准化：round(N)"""
        if value is None:
            return None
        try:
            return round(float(value), decimals)
        except (ValueError, TypeError):
            return None

    @classmethod
    def normalize_record(cls, record: dict, date_fields: list = None,
                         code_fields: list = None, pct_fields: list = None,
                         price_fields: list = None) -> dict:
        """
        批量标准化一条记录。

        Args:
            record: 原始记录字典
            date_fields: 需要标准化的日期字段名列表
            code_fields: 需要标准化的代码字段名列表
            pct_fields: 需要标准化的百分比字段名列表
            price_fields: 需要标准化的价格字段名列表

        Returns:
            标准化后的记录副本
        """
        result = record.copy()

        for field in (date_fields or []):
            if field in result:
                result[field] = cls.normalize_date(result[field])

        for field in (code_fields or []):
            if field in result:
                result[field] = cls.normalize_code(result[field])

        for field in (pct_fields or []):
            if field in result:
                result[field] = cls.normalize_pct(result[field])

        for field in (price_fields or []):
            if field in result:
                result[field] = cls.normalize_price(result[field])

        return result


# ── 全局单例 ──────────────────────────────────────────────
normalizer = DataNormalizer()
