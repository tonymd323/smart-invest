"""
披露日扫描器（DisclosureScanner）
================================
基于东方财富 NOTICE_DATE 字段，实时扫描最新披露的财报/业绩预告。

数据源：
  - 财报主表：RPT_F10_FINANCE_MAINFINADATA
  - 业绩预告表：RPT_PUBLIC_OP_NEWPREDICT
  - API：https://datacenter-web.eastmoney.com/api/data/v1/get

用法：
    scanner = DisclosureScanner(db_path="data/smart_invest.db")
    codes = scanner.get_scan_list(since_hours=24)
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set

logger = logging.getLogger(__name__)


class DisclosureScanner:
    """
    披露日扫描器

    职责：
      1. 从东方财富 API 获取最新披露的财报/业绩预告
      2. 与 DB 中已有数据做差量对比，只返回真正新增的股票
    """

    BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    # 财报主表
    FINANCE_TABLE = "RPT_F10_FINANCE_MAINFINADATA"
    FINANCE_COLUMNS = "SECURITY_CODE,SECURITY_NAME_ABBR,REPORT_DATE,NOTICE_DATE,PARENTNETPROFIT,PARENTNETPROFITTZ"

    # 业绩预告表
    PREDICT_TABLE = "RPT_PUBLIC_OP_NEWPREDICT"
    PREDICT_COLUMNS = "SECURITY_CODE,SECURITY_NAME_ABBR,REPORT_DATE,NOTICE_DATE,PREDICT_FINANCE_CODE"

    def __init__(self, db_path: str):
        self.db_path = db_path

    def fetch_new_disclosures(self, since_hours: int = 24) -> List[Dict]:
        """
        从东方财富 API 获取指定时间范围内新披露的股票列表。

        Args:
            since_hours: 回溯小时数，默认 24 小时

        Returns:
            去重后的股票信息列表，每项包含：
            {"stock_code", "stock_name", "report_date", "notice_date", "source"}
        """
        since_dt = datetime.now() - timedelta(hours=since_hours)
        since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S")

        all_items = {}

        # 1. 查询财报主表
        try:
            finance_items = self._fetch_table(
                table=self.FINANCE_TABLE,
                columns=self.FINANCE_COLUMNS,
                notice_date_filter=since_str,
            )
            for item in finance_items:
                code = item.get("SECURITY_CODE", "")
                if not code:
                    continue
                ts_code = self._em_code_to_ts(code)
                report_date = (item.get("REPORT_DATE") or "")[:10]
                notice_date = (item.get("NOTICE_DATE") or "")[:10]

                key = f"{ts_code}_{report_date}"
                if key not in all_items:
                    all_items[key] = {
                        "stock_code": ts_code,
                        "stock_name": item.get("SECURITY_NAME_ABBR", ""),
                        "report_date": report_date,
                        "notice_date": notice_date,
                        "source": "finance_main",
                    }
            logger.info(f"[DisclosureScanner] 财报主表返回 {len(finance_items)} 条")
        except Exception as e:
            logger.error(f"[DisclosureScanner] 财报主表查询失败: {e}")

        # 2. 查询业绩预告表
        try:
            predict_items = self._fetch_table(
                table=self.PREDICT_TABLE,
                columns=self.PREDICT_COLUMNS,
                notice_date_filter=since_str,
            )
            for item in predict_items:
                code = item.get("SECURITY_CODE", "")
                if not code:
                    continue
                ts_code = self._em_code_to_ts(code)
                report_date = (item.get("REPORT_DATE") or "")[:10]
                notice_date = (item.get("NOTICE_DATE") or "")[:10]

                key = f"{ts_code}_{report_date}"
                if key not in all_items:
                    all_items[key] = {
                        "stock_code": ts_code,
                        "stock_name": item.get("SECURITY_NAME_ABBR", ""),
                        "report_date": report_date,
                        "notice_date": notice_date,
                        "source": "forecast",
                    }
            logger.info(f"[DisclosureScanner] 业绩预告表返回 {len(predict_items)} 条")
        except Exception as e:
            logger.error(f"[DisclosureScanner] 业绩预告表查询失败: {e}")

        result = list(all_items.values())
        logger.info(f"[DisclosureScanner] 去重后共 {len(result)} 条新披露")
        return result

    def _fetch_table(self, table: str, columns: str,
                     notice_date_filter: str) -> List[Dict]:
        """
        调用东方财富 datacenter-web API 查询指定表。

        Args:
            table: 报表名
            columns: 查询列名
            notice_date_filter: NOTICE_DATE 过滤条件（如 "2026-03-27 00:00:00"）
        """
        import requests

        # filter 格式: (NOTICE_DATE>'2026-03-27')
        date_part = notice_date_filter[:10]  # 取日期部分
        filter_str = f"(NOTICE_DATE>'{date_part}')"

        params = {
            "reportName": table,
            "columns": columns,
            "filter": filter_str,
            "pageSize": 500,
            "sortColumns": "NOTICE_DATE",
            "sortTypes": -1,
        }

        all_data = []
        page = 1

        while True:
            params["pageNumber"] = page
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("success"):
                logger.warning(f"[DisclosureScanner] API 返回失败: {table} page={page}")
                break

            result = data.get("result", {})
            page_data = result.get("data", [])
            if not page_data:
                break

            all_data.extend(page_data)

            # 分页检查
            total_pages = result.get("pages", 1)
            if page >= total_pages:
                break
            page += 1

        return all_data

    def diff_with_db(self, stock_codes: List[str]) -> List[str]:
        """
        对比 DB 中 earnings 表已有的 (stock_code, report_date) 组合，
        只返回真正新增的股票代码。

        Args:
            stock_codes: 待检查的股票代码列表

        Returns:
            需要采集的新股票代码列表
        """
        if not stock_codes:
            return []

        # 获取东方财富新披露的完整信息（用于精确比对）
        since_dt = datetime.now() - timedelta(hours=48)  # 扩大窗口确保覆盖
        disclosures = self.fetch_new_disclosures(since_hours=48)

        # 构建待检查的 (stock_code, report_date) 集合
        pending_set: Set[str] = set()
        for d in disclosures:
            if d["stock_code"] in stock_codes:
                key = f"{d['stock_code']}_{d['report_date']}"
                pending_set.add(key)

        if not pending_set:
            return stock_codes  # 没有详细披露信息，全部返回

        # 查询 DB 中已有的组合
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        existing_set: Set[str] = set()
        try:
            placeholders = ",".join(["?" for _ in stock_codes])
            rows = conn.execute(f"""
                SELECT DISTINCT stock_code, report_date
                FROM earnings
                WHERE stock_code IN ({placeholders})
            """, stock_codes).fetchall()

            for row in rows:
                key = f"{row['stock_code']}_{row['report_date']}"
                existing_set.add(key)
        except Exception as e:
            logger.error(f"[DisclosureScanner] DB 查询失败: {e}")
        finally:
            conn.close()

        # 差量：pending - existing
        new_keys = pending_set - existing_set
        new_codes = list({k.split("_")[0] for k in new_keys})

        logger.info(
            f"[DisclosureScanner] diff: pending={len(pending_set)}, "
            f"existing={len(existing_set)}, new={len(new_codes)}"
        )
        return new_codes

    def get_scan_list(self, since_hours: int = 24) -> List[str]:
        """
        获取需要扫描的股票代码列表（组合 fetch + diff）。

        Returns:
            需要 Pipeline 采集的股票代码列表
        """
        # Step 1: 获取最新披露
        disclosures = self.fetch_new_disclosures(since_hours=since_hours)
        if not disclosures:
            logger.info("[DisclosureScanner] 无新披露")
            return []

        # 提取去重后的股票代码
        all_codes = list({d["stock_code"] for d in disclosures})

        # Step 2: 与 DB 差量对比
        new_codes = self.diff_with_db(all_codes)

        logger.info(f"[DisclosureScanner] 扫描列表: {len(new_codes)} 只股票")
        return new_codes

    @staticmethod
    def _em_code_to_ts(code: str) -> str:
        """000858 → 000858.SZ（根据开头判断市场）"""
        if code.startswith("6") or code.startswith("9"):
            return f"{code}.SH"
        return f"{code}.SZ"
