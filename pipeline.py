#!/usr/bin/env python3
"""
Pipeline — 统一数据采集入口（Phase 2）
========================================
职责：
  1. 调 FinancialProvider → 批量获取财报 → 写入 earnings 表
  2. 调 ConsensusProvider → 批量获取一致预期 → 写入 consensus 表
  3. 调 KlineProvider → 批量获取日K → 写入 prices 表
  4. 数据质量校验
  5. 采集完成后触发 analyzer.py

设计约束：
  - Pipeline 只写 DB，不分析
  - 并行获取（ThreadPoolExecutor），控制速率
  - 支持 --quiet 模式（只采集，不触发分析）
  - 股票池：stocks.json（跟踪池）+ discovery_pool（发现池）

用法：
  python3 pipeline.py                     # 全量采集 + 触发分析
  python3 pipeline.py --quiet             # 只采集，不触发分析
  python3 pipeline.py --codes 600660.SH,600938.SH  # 指定股票
  python3 pipeline.py --skip-kline        # 跳过 K 线采集
"""

import sys
import os
import json
import time
import logging
import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.insert(0, str(Path(__file__).parent))
from core.database import init_db, get_connection, DB_PATH
from core.stock_config import get_all_codes, get_stock_pool

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
)
logger = logging.getLogger('pipeline')


# ── 数据质量校验 ──────────────────────────────────────────────────────────────

class DataQualityChecker:
    """财务数据质量校验（写入前检查）"""

    RULES = {
        "net_profit": {"min": -1000, "max": 10000},    # 亿元
        "net_profit_yoy": {"min": -500, "max": 1000},   # %
        "revenue": {"min": -100, "max": 50000},          # 亿元
        "revenue_yoy": {"min": -500, "max": 1000},       # %
        "roe": {"min": -100, "max": 200},                # %
        "gross_margin": {"min": -50, "max": 100},        # %
    }

    @classmethod
    def check(cls, records: list) -> dict:
        errors = []
        warnings = []
        for rec in records:
            stock_code = rec.get("stock_code", "unknown")
            for field, limits in cls.RULES.items():
                value = rec.get(field)
                if value is None:
                    continue
                if not isinstance(value, (int, float)):
                    errors.append(f"{stock_code}: {field} 非数值: {value}")
                    continue
                if value < limits["min"] or value > limits["max"]:
                    warnings.append(
                        f"{stock_code}: {field}={value} 超出常规范围 "
                        f"[{limits['min']}, {limits['max']}]"
                    )
            for key in ["stock_code", "report_date"]:
                if not rec.get(key):
                    errors.append(f"缺少关键字段: {key}")

        return {
            "passed": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "records_checked": len(records),
        }


# ── 股票池构建 ────────────────────────────────────────────────────────────────

def build_stock_pool(codes: list = None) -> list:
    """
    构建股票池：
      - 指定 codes → 仅采集这些
      - 否则 → stocks.json（持仓+备选）+ discovery_pool（active）
    """
    if codes:
        return [{"code": c, "name": "", "source": "manual"} for c in codes]

    pool = []
    # stocks.json
    cfg_pool = get_stock_pool()
    for s in cfg_pool:
        pool.append({"code": s["code"], "name": s.get("name", ""), "source": s.get("type", "config")})

    # discovery_pool
    try:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT stock_code, stock_name FROM discovery_pool WHERE status = 'active'"
            ).fetchall()
            existing = {s["code"] for s in pool}
            for r in rows:
                if r["stock_code"] not in existing:
                    pool.append({
                        "code": r["stock_code"],
                        "name": r["stock_name"] or "",
                        "source": "discovery",
                    })
    except Exception as e:
        logger.warning(f"读取 discovery_pool 失败: {e}")

    return pool


# ── 写入操作 ──────────────────────────────────────────────────────────────────

def _write_earnings(conn: sqlite3.Connection, stock_code: str, records: list) -> int:
    """写入 earnings 表（UPSERT），返回写入行数"""
    written = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO earnings
            (stock_code, report_date, report_type, revenue, revenue_yoy,
             net_profit, net_profit_yoy, eps, roe, gross_margin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stock_code,
            rec.get("report_date", ""),
            "Q4",
            rec.get("revenue"),
            rec.get("revenue_yoy"),
            rec.get("net_profit"),
            rec.get("net_profit_yoy"),
            rec.get("eps"),
            rec.get("roe"),
            rec.get("gross_margin"),
        ))
        written += 1
    return written


def _write_consensus(conn: sqlite3.Connection, stock_code: str, data: dict) -> int:
    """写入 consensus 表（UPSERT），返回写入行数"""
    conn.execute("""
        INSERT OR REPLACE INTO consensus
        (stock_code, eps, net_profit_yoy, rev_yoy, num_analysts, source)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        stock_code,
        data.get("eps"),
        data.get("net_profit_yoy"),
        data.get("rev_yoy"),
        data.get("num_analysts", 0),
        data.get("source", "eastmoney"),
    ))
    return 1


def _write_prices(conn: sqlite3.Connection, stock_code: str, records: list) -> int:
    """写入 prices 表（UPSERT），返回写入行数"""
    written = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO prices
            (stock_code, trade_date, open_price, high_price, low_price,
             close_price, volume, turnover, change_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stock_code,
            rec.get("trade_date", ""),
            rec.get("open_price"),
            rec.get("high_price"),
            rec.get("low_price"),
            rec.get("close_price"),
            rec.get("volume"),
            rec.get("amount"),
            rec.get("change_pct"),
        ))
        written += 1
    return written


# ── 单股票采集任务 ────────────────────────────────────────────────────────────

def _fetch_one_stock(
    stock: dict,
    financial_provider,
    consensus_provider,
    kline_provider,
    skip_kline: bool = False,
    db_path: str = None,
) -> dict:
    """
    对单只股票执行全量采集。
    返回: {"code": str, "status": str, "earnings": int, "consensus": int, "prices": int, "errors": list}
    """
    code = stock["code"]
    result = {
        "code": code,
        "name": stock.get("name", ""),
        "status": "ok",
        "earnings": 0,
        "consensus": 0,
        "prices": 0,
        "errors": [],
    }

    _db_path = db_path or str(DB_PATH)

    # 1. 财务数据
    try:
        fd_list = financial_provider.fetch(code)
        if fd_list:
            records = [fd.to_dict() for fd in fd_list]
            quality = DataQualityChecker.check(records)
            if not quality["passed"]:
                result["errors"].extend(quality["errors"])
                logger.warning(f"[Pipeline] {code} 财务质量校验: {quality['errors']}")

            with get_connection(_db_path) as conn:
                result["earnings"] = _write_earnings(conn, code, records)

            if quality["warnings"]:
                logger.info(f"[Pipeline] {code} 财务警告: {len(quality['warnings'])} 条")
    except Exception as e:
        result["errors"].append(f"财务采集失败: {e}")
        logger.error(f"[Pipeline] {code} 财务采集异常: {e}")

    # 2. 一致预期
    try:
        cd = consensus_provider.fetch(code)
        if cd:
            with get_connection(_db_path) as conn:
                result["consensus"] = _write_consensus(conn, code, cd.to_dict())
    except Exception as e:
        result["errors"].append(f"预期采集失败: {e}")
        logger.error(f"[Pipeline] {code} 预期采集异常: {e}")

    # 3. K 线数据（可选）
    if not skip_kline:
        try:
            klines = kline_provider.fetch(code, limit=120)
            if klines:
                records = [k.to_dict() for k in klines]
                with get_connection(_db_path) as conn:
                    result["prices"] = _write_prices(conn, code, records)
        except Exception as e:
            result["errors"].append(f"K线采集失败: {e}")
            logger.error(f"[Pipeline] {code} K线采集异常: {e}")

    if result["errors"]:
        result["status"] = "partial" if (result["earnings"] or result["consensus"] or result["prices"]) else "error"

    return result


# ── Pipeline 主流程 ───────────────────────────────────────────────────────────

class Pipeline:
    """
    统一数据采集 Pipeline

    用法：
        from core.data_provider import FinancialProvider, ConsensusProvider, KlineProvider
        from pipeline import Pipeline

        pipe = Pipeline()
        stats = pipe.run()
    """

    def __init__(self, db_path: str = None, max_workers: int = 6):
        self.db_path = db_path or str(DB_PATH)
        self.max_workers = max_workers

    def run(
        self,
        codes: list = None,
        skip_kline: bool = False,
    ) -> dict:
        """
        执行采集。

        Args:
            codes: 指定股票代码列表，None 则用默认池
            skip_kline: 是否跳过 K 线采集

        Returns:
            {
                "stocks": [{"code": str, "status": str, ...}, ...],
                "summary": {"total": int, "ok": int, "partial": int, "error": int, ...}
            }
        """
        # 确保数据库就绪
        init_db(self.db_path)

        # 构建股票池
        pool = build_stock_pool(codes)
        logger.info(f"📊 Pipeline 采集开始 | 股票池: {len(pool)} 只")

        if not pool:
            logger.warning("股票池为空，跳过采集")
            return {"stocks": [], "summary": {"total": 0, "ok": 0, "partial": 0, "error": 0}}

        # 创建 Providers（实时模式，无预注入数据）
        from core.data_provider import FinancialProvider, ConsensusProvider, KlineProvider
        fp = FinancialProvider()
        cp = ConsensusProvider()
        kp = KlineProvider()

        # 并行采集
        t0 = time.time()
        results = []
        ok_count = 0
        partial_count = 0
        error_count = 0
        total_earnings = 0
        total_consensus = 0
        total_prices = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {
                executor.submit(
                    _fetch_one_stock, stock, fp, cp, kp, skip_kline, self.db_path
                ): stock
                for stock in pool
            }

            for future in as_completed(future_map):
                try:
                    r = future.result()
                    results.append(r)

                    if r["status"] == "ok":
                        ok_count += 1
                    elif r["status"] == "partial":
                        partial_count += 1
                    else:
                        error_count += 1

                    total_earnings += r["earnings"]
                    total_consensus += r["consensus"]
                    total_prices += r["prices"]

                    if r["status"] != "ok":
                        logger.warning(
                            f"[Pipeline] {r['code']} {r['status']}: "
                            f"earn={r['earnings']} cons={r['consensus']} "
                            f"prices={r['prices']} errors={r['errors']}"
                        )
                except Exception as e:
                    code = future_map[future].get("code", "unknown")
                    logger.error(f"[Pipeline] {code} 采集任务异常: {e}")
                    error_count += 1

        elapsed = time.time() - t0
        summary = {
            "total": len(pool),
            "ok": ok_count,
            "partial": partial_count,
            "error": error_count,
            "earnings_written": total_earnings,
            "consensus_written": total_consensus,
            "prices_written": total_prices,
            "elapsed_sec": round(elapsed, 1),
        }

        logger.info(
            f"\n{'='*50}\n"
            f"📊 Pipeline 采集完成\n"
            f"  股票: {summary['total']} 只 "
            f"(成功={ok_count}, 部分={partial_count}, 失败={error_count})\n"
            f"  写入: earnings={total_earnings}, consensus={total_consensus}, "
            f"prices={total_prices}\n"
            f"  耗时: {elapsed:.1f}s\n"
            f"{'='*50}"
        )

        return {"stocks": results, "summary": summary}


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Pipeline — 统一数据采集入口")
    parser.add_argument("--codes", type=str, default=None,
                        help="指定股票代码（逗号分隔），如 600660.SH,600938.SH")
    parser.add_argument("--quiet", action="store_true",
                        help="只采集，不触发 analyzer")
    parser.add_argument("--skip-kline", action="store_true",
                        help="跳过 K 线采集")
    parser.add_argument("--workers", type=int, default=6,
                        help="并行线程数（默认 6）")
    return parser.parse_args()


def main():
    args = parse_args()

    codes = None
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    pipe = Pipeline(max_workers=args.workers)
    result = pipe.run(codes=codes, skip_kline=args.skip_kline)

    # 触发 analyzer（除非 quiet）
    if not args.quiet and result["summary"]["ok"] > 0:
        logger.info("\n🔄 触发 analyzer.py...")
        import subprocess
        analyzer_path = Path(__file__).parent / "analyzer.py"
        cmd = [sys.executable, str(analyzer_path)]
        if codes:
            cmd.extend(["--codes", ",".join(codes)])
        proc = subprocess.run(cmd, capture_output=False)
        if proc.returncode != 0:
            logger.error(f"analyzer.py 执行失败 (exit={proc.returncode})")
    elif args.quiet:
        logger.info("🔇 quiet 模式，跳过 analyzer 触发")


if __name__ == "__main__":
    main()
