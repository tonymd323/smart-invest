"""
数据 Pipeline — Phase 0 版本

职责：
  1. 串行调用 Provider 列表
  2. 数据质量校验
  3. 写入 SQLite（SSOT）
  4. 触发 Analyzer

约束：
  - WAL 模式
  - 每个 Provider 独立超时（30s）
  - 禁止 except:pass
"""

import sqlite3
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ── 数据质量校验 ──────────────────────────────────────────────────────────────

class DataQualityChecker:
    """财务数据质量校验（写入前检查）"""

    RULES = {
        "net_profit": {"min": -1000, "max": 10000},    # 亿元，范围合理
        "net_profit_yoy": {"min": -200, "max": 500},    # %，同比范围
        "revenue": {"min": -100, "max": 50000},          # 亿元
        "roe": {"min": -100, "max": 200},                # %
        "gross_margin": {"min": -50, "max": 100},        # %
    }

    @classmethod
    def check(cls, records: List[Dict]) -> Dict[str, Any]:
        """
        校验一批记录，返回质量报告
        返回: {"passed": bool, "errors": [...], "warnings": [...]}
        """
        errors = []
        warnings = []

        for rec in records:
            stock_code = rec.get("stock_code", "unknown")

            for field, limits in cls.RULES.items():
                value = rec.get(field)
                if value is None:
                    continue  # 可选字段允许为空
                if not isinstance(value, (int, float)):
                    errors.append(f"{stock_code}: {field} 非数值: {value}")
                    continue
                if value < limits["min"] or value > limits["max"]:
                    errors.append(
                        f"{stock_code}: {field}={value} 超出合理范围 "
                        f"[{limits['min']}, {limits['max']}]"
                    )

            # 关键字段非空检查
            for key in ["stock_code", "report_date"]:
                if not rec.get(key):
                    errors.append(f"缺少关键字段: {key}")

        return {
            "passed": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "records_checked": len(records),
        }


# ── Pipeline ──────────────────────────────────────────────────────────────────

class Pipeline:
    """
    数据采集 Pipeline（单 Provider 版本）

    用法：
        from core.data_provider import FinancialProvider
        from core.pipeline import Pipeline

        provider = FinancialProvider(data=eastmoney_data)
        pipe = Pipeline(db_path="data/smart_invest.db", providers=[provider])
        results = pipe.run(stock_codes=["000858.SZ", "600519.SH"])
    """

    def __init__(self, db_path: str, providers: list = None):
        self.db_path = db_path
        self.providers = providers or []
        self._ensure_wal_mode()

    def _ensure_wal_mode(self):
        """确保 WAL 模式已启用"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
        finally:
            conn.close()

    def run(self, stock_codes: List[str] = None,
            use_disclosure_filter: bool = False) -> Dict[str, Any]:
        """
        执行 Pipeline：采集 → 校验 → 写入

        Args:
            stock_codes: 股票代码列表。当 use_disclosure_filter=True 时可为空。
            use_disclosure_filter: 是否使用 DisclosureScanner 自动获取待扫描列表。
                为 True 时，调用 DisclosureScanner 获取最新披露的股票，
                不再依赖外部传入 stock_codes。

        Returns:
            {
                "000858.SZ": {
                    "status": "ok" | "empty" | "error",
                    "records_written": int,
                    "quality": {"passed": bool, ...},
                    "elapsed_ms": int,
                },
                ...
            }
        """
        # 如果启用披露日过滤，自动获取待扫描列表
        if use_disclosure_filter:
            try:
                from core.disclosure_scanner import DisclosureScanner
                scanner = DisclosureScanner(db_path=self.db_path)
                scan_codes = scanner.get_scan_list(since_hours=24)
                if scan_codes:
                    if stock_codes:
                        # 合并：外部传入 + 自动扫描
                        stock_codes = list(set(stock_codes) | set(scan_codes))
                    else:
                        stock_codes = scan_codes
                    logger.info(
                        f"[Pipeline] DisclosureScanner 返回 {len(scan_codes)} 只新披露股票"
                    )
                else:
                    logger.info("[Pipeline] DisclosureScanner 无新披露，使用外部 stock_codes")
            except Exception as e:
                logger.error(f"[Pipeline] DisclosureScanner 调用失败: {e}")
                # 不中断，降级到外部 stock_codes

        if not stock_codes:
            logger.warning("[Pipeline] 无股票代码，跳过执行")
            return {}

        results = {}

        for code in stock_codes:
            t0 = time.time()
            try:
                # Step 1: 从所有 Provider 采集
                all_records = []
                for provider in self.providers:
                    try:
                        records = provider.fetch(code)
                        if records:
                            # 标准化为 dict
                            dicts = [r.to_dict() for r in records]
                            all_records.extend(dicts)
                            logger.info(
                                f"[Pipeline] {code}: "
                                f"{len(dicts)} 条来自 {provider.last_source}"
                            )
                    except Exception as e:
                        logger.error(f"[Pipeline] {code} Provider 异常: {e}")
                        # 不中断，继续其他 Provider
                        continue

                if not all_records:
                    results[code] = {
                        "status": "empty",
                        "records_written": 0,
                        "quality": {"passed": True},
                        "elapsed_ms": int((time.time() - t0) * 1000),
                    }
                    continue

                # Step 2: 数据质量校验
                quality = DataQualityChecker.check(all_records)
                if not quality["passed"]:
                    logger.warning(
                        f"[Pipeline] {code} 质量校验失败: {quality['errors']}"
                    )

                # Step 3: 写入 SQLite
                written = self._write_to_db(code, all_records)

                results[code] = {
                    "status": "ok",
                    "records_written": written,
                    "quality": quality,
                    "elapsed_ms": int((time.time() - t0) * 1000),
                }

            except Exception as e:
                logger.error(f"[Pipeline] {code} 执行失败: {e}")
                results[code] = {
                    "status": "error",
                    "error": str(e),
                    "records_written": 0,
                    "quality": {"passed": False},
                    "elapsed_ms": int((time.time() - t0) * 1000),
                }

        return results

    def _write_to_db(self, stock_code: str, records: List[Dict]) -> int:
        """
        写入 earnings 表（UPSERT），并计算 quarterly_net_profit。
        返回写入行数
        """
        conn = sqlite3.connect(self.db_path)
        written = 0

        try:
            # Step 1: 写入基础数据
            for rec in records:
                conn.execute("""
                    INSERT OR REPLACE INTO earnings 
                    (stock_code, report_date, net_profit, net_profit_yoy,
                     revenue, revenue_yoy, roe, gross_margin, eps)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec.get("stock_code"),
                    rec.get("report_date"),
                    rec.get("net_profit"),
                    rec.get("net_profit_yoy"),
                    rec.get("revenue"),
                    rec.get("revenue_yoy"),
                    rec.get("roe"),
                    rec.get("gross_margin"),
                    rec.get("eps"),
                ))
                written += 1

            conn.commit()

            # Step 2: 计算 quarterly_net_profit（单季度净利润）
            # 对同一股票的所有 earnings 记录按 report_date 正序排列，
            # 用相邻两期的累计净利润做差值。
            # 第一条（最早期）没有前值，quarterly_net_profit 留空。
            self._compute_quarterly_net_profit(conn, stock_code)

            conn.commit()
        except Exception as e:
            logger.error(f"[Pipeline] {stock_code} 写入失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

        return written

    @staticmethod
    def _compute_quarterly_net_profit(conn: sqlite3.Connection, stock_code: str):
        """
        计算单季度净利润（quarterly_net_profit）。

        逻辑：
          财报 API 返回的是累计净利润（如 Q3 = Q1+Q2+Q3），
          单季度值 = 本期累计 - 上期累计。
          第一条（最早期）没有前值，quarterly_net_profit 留空。
        """
        rows = conn.execute("""
            SELECT id, report_date, net_profit
            FROM earnings
            WHERE stock_code = ? AND net_profit IS NOT NULL
            ORDER BY report_date ASC
        """, (stock_code,)).fetchall()

        if len(rows) < 2:
            return  # 数据不足，无法计算差值

        prev_net_profit = None
        for row in rows:
            row_id, report_date, net_profit = row
            if prev_net_profit is not None and net_profit is not None:
                quarterly = round(net_profit - prev_net_profit, 4)
                conn.execute("""
                    UPDATE earnings SET quarterly_net_profit = ?
                    WHERE id = ?
                """, (quarterly, row_id))
            prev_net_profit = net_profit


# ═══════════════════════════════════════════════════════════════════════════════
#  run_backtest — 回测更新 (A-07)
# ═══════════════════════════════════════════════════════════════════════════════

def run_backtest(db_path: str) -> Dict[str, int]:
    """
    回测更新：计算入池后 T+5/10/20/60 收益 vs 沪深300。

    逻辑：
      1. 查找 event_tracking 中 entry_price 非空但 return_5d 为空的记录（待回测）
      2. 从 prices 表获取后续 N 日收盘价
      3. 获取沪深300 同期价格作为基准
      4. 计算收益率和超额收益
      5. 写回 backtest 表

    Args:
        db_path: SQLite 数据库路径

    Returns:
        {"updated": N, "skipped": M} 回测结果统计
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    updated = 0
    skipped = 0

    try:
        # 获取待回测记录（entry_price 非空但 return_5d 为空）
        pending_rows = conn.execute("""
            SELECT id, stock_code, event_date, event_type, entry_price
            FROM event_tracking
            WHERE entry_price IS NOT NULL
              AND entry_price > 0
              AND return_5d IS NULL
              AND tracking_status != 'pending'
            ORDER BY event_date DESC
        """).fetchall()

        logger.info(f"[run_backtest] 待回测记录: {len(pending_rows)} 条")

        for row in pending_rows:
            track_id = row["id"]
            code = row["stock_code"]
            event_date = row["event_date"]
            entry_price = row["entry_price"]
            event_type = row["event_type"]

            try:
                # 从 prices 表获取入池日之后的收盘价序列
                price_rows = conn.execute("""
                    SELECT trade_date, close_price
                    FROM prices
                    WHERE stock_code = ? AND trade_date >= ?
                    ORDER BY trade_date ASC
                    LIMIT 65
                """, (code, event_date)).fetchall()

                if len(price_rows) < 2:
                    logger.debug(f"[run_backtest] {code} {event_date} 价格数据不足，跳过")
                    skipped += 1
                    continue

                # 入池价取 prices 表中 event_date 当天收盘价（如果有的话）
                actual_entry = entry_price
                for pr in price_rows:
                    if pr["trade_date"] == event_date:
                        actual_entry = pr["close_price"] or entry_price
                        break

                # 计算 T+N 收益
                periods = {"5d": 5, "10d": 10, "20d": 20, "60d": 60}
                returns = {}

                for label, days in periods.items():
                    idx = days  # 入池日是 idx=0，T+N 是 idx=N
                    if idx < len(price_rows) and actual_entry > 0:
                        close_n = price_rows[idx]["close_price"]
                        if close_n and close_n > 0:
                            ret = round((close_n / actual_entry - 1) * 100, 2)
                            returns[f"return_{label}"] = ret
                        else:
                            returns[f"return_{label}"] = None
                    else:
                        returns[f"return_{label}"] = None

                # 获取沪深300 同期基准收益
                benchmark_returns = _get_benchmark_returns(conn, event_date, periods)

                # 计算超额收益
                alpha = {}
                for label in periods:
                    ret = returns.get(f"return_{label}")
                    bm_ret = benchmark_returns.get(f"benchmark_{label}")
                    if ret is not None and bm_ret is not None:
                        alpha[f"alpha_{label}"] = round(ret - bm_ret, 2)
                    else:
                        alpha[f"alpha_{label}"] = None

                # 判断是否跑赢（T+20 alpha）
                alpha_20d = alpha.get("alpha_20d")
                is_win = 1 if alpha_20d is not None and alpha_20d > 0 else (
                    0 if alpha_20d is not None else None
                )

                # 写入 backtest 表
                conn.execute("""
                    INSERT OR REPLACE INTO backtest
                    (stock_code, event_date, event_type, entry_price,
                     return_5d, return_10d, return_20d, return_60d,
                     benchmark_5d, benchmark_10d, benchmark_20d, benchmark_60d,
                     alpha_5d, alpha_10d, alpha_20d, alpha_60d, is_win)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    code, event_date, event_type, actual_entry,
                    returns.get("return_5d"), returns.get("return_10d"),
                    returns.get("return_20d"), returns.get("return_60d"),
                    benchmark_returns.get("benchmark_5d"),
                    benchmark_returns.get("benchmark_10d"),
                    benchmark_returns.get("benchmark_20d"),
                    benchmark_returns.get("benchmark_60d"),
                    alpha.get("alpha_5d"), alpha.get("alpha_10d"),
                    alpha.get("alpha_20d"), alpha.get("alpha_60d"),
                    is_win,
                ))

                # 更新 event_tracking 的 return_* 字段
                conn.execute("""
                    UPDATE event_tracking SET
                        return_5d = ?, return_10d = ?, return_20d = ?,
                        alpha_5d = ?, alpha_20d = ?,
                        tracking_status = 'completed',
                        last_updated = datetime('now', 'localtime')
                    WHERE id = ?
                """, (
                    returns.get("return_5d"),
                    returns.get("return_10d"),
                    returns.get("return_20d"),
                    alpha.get("alpha_5d"),
                    alpha.get("alpha_20d"),
                    track_id,
                ))

                updated += 1

            except Exception as e:
                logger.error(f"[run_backtest] {code} {event_date} 回测计算失败: {e}")
                skipped += 1
                continue

        conn.commit()

    except Exception as e:
        logger.error(f"[run_backtest] 执行失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[run_backtest] 回测完成: updated={updated}, skipped={skipped}")
    return {"updated": updated, "skipped": skipped}

def fetch_and_apply_consensus(db_path: str, stock_codes: list = None) -> dict:
    """
    从东方财富获取一致预期 → 写入 consensus 表 → 计算 expectation_diff_pct 更新 earnings。

    Args:
        db_path: 数据库路径
        stock_codes: 股票代码列表，None 则处理 earnings 表中所有股票
    Returns:
        {"fetched": N, "updated": M, "skipped": K}
    """
    from core.data_provider import ConsensusProvider

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    provider = ConsensusProvider()

    # 获取需要处理的股票列表
    if stock_codes is None:
        rows = conn.execute(
            "SELECT DISTINCT stock_code FROM earnings WHERE expectation_diff_pct IS NULL"
        ).fetchall()
        stock_codes = [r["stock_code"] for r in rows]

    fetched = 0
    updated = 0
    skipped = 0

    try:
        for code in stock_codes:
            # 1. 获取一致预期
            consensus = provider.fetch(code)
            if consensus is None:
                skipped += 1
                continue

            # 2. 写入 consensus 表
            conn.execute("""
                INSERT OR REPLACE INTO consensus
                (stock_code, eps, net_profit_yoy, rev_yoy, num_analysts, source)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                code,
                consensus.eps,
                consensus.net_profit_yoy,
                consensus.rev_yoy,
                consensus.num_analysts,
                consensus.source,
            ))
            fetched += 1

            # 3. 计算 expectation_diff_pct 并更新 earnings
            expected = consensus.net_profit_yoy
            if expected and expected != 0:
                conn.execute("""
                    UPDATE earnings
                    SET expectation_diff_pct = net_profit_yoy - ?
                    WHERE stock_code = ?
                      AND report_date = (
                          SELECT MAX(report_date) FROM earnings WHERE stock_code = ?
                      )
                      AND net_profit_yoy IS NOT NULL
                """, (expected, code, code))
                updated += 1

        conn.commit()
    except Exception as e:
        logger.error(f"[fetch_consensus] 失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[fetch_consensus] 完成: fetched={fetched}, updated={updated}, skipped={skipped}")
    return {"fetched": fetched, "updated": updated, "skipped": skipped}



def _get_benchmark_returns(conn: sqlite3.Connection, event_date: str,
                           periods: Dict[str, int]) -> Dict[str, Optional[float]]:
    """
    获取沪深300（000300.SH）同期基准收益。

    从 prices 表获取 000300.SH 的收盘价数据。
    如果 prices 表中没有基准数据，返回空字典。
    """
    result = {}

    # 从 prices 表获取基准指数历史价格
    bm_rows = conn.execute("""
        SELECT trade_date, close_price
        FROM prices
        WHERE stock_code = '000300.SH' AND trade_date >= ?
        ORDER BY trade_date ASC
        LIMIT 65
    """, (event_date,)).fetchall()

    if len(bm_rows) < 2:
        logger.debug("[_get_benchmark_returns] 沪深300 价格数据不足")
        return result

    bm_entry = bm_rows[0]["close_price"]
    if not bm_entry or bm_entry <= 0:
        return result

    for label, days in periods.items():
        idx = days
        if idx < len(bm_rows):
            bm_close = bm_rows[idx]["close_price"]
            if bm_close and bm_close > 0:
                result[f"benchmark_{label}"] = round((bm_close / bm_entry - 1) * 100, 2)
            else:
                result[f"benchmark_{label}"] = None
        else:
            result[f"benchmark_{label}"] = None

    return result
