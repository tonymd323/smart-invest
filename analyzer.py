#!/usr/bin/env python3
"""
Analyzer — 统一分析入口（Phase 2）
=====================================
职责：
  1. 读 SQLite earnings + consensus → 超预期分析
  2. 读 SQLite earnings → 扣非新高分析
  3. 读 SQLite prices → 回调买入评分
  4. 结果统一写入 analysis_results 表
  5. 分析完成后触发 pusher.py

设计约束：
  - Analyzer 只读 DB，不采集
  - 复用现有分析逻辑（scanners/ 目录）
  - 支持 --mode 参数：full / earnings / pullback
  - 不破坏 1.0 的 daily_scan.py

用法：
  python3 analyzer.py                          # 全量分析
  python3 analyzer.py --mode earnings          # 仅财报分析
  python3 analyzer.py --mode pullback          # 仅回调分析
  python3 analyzer.py --codes 600660.SH        # 指定股票
  python3 analyzer.py --no-push                # 不触发推送
"""

import sys
import os
import json
import math
import time
import logging
import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from core.database import init_db, get_connection, DB_PATH
from core.stock_config import get_all_codes, get_stock_pool

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
)
logger = logging.getLogger('analyzer')


# ── 超预期分析（从 DB 读取，纯分析） ──────────────────────────────────────────

def analyze_earnings_beat(db_path: str, codes: list = None, threshold: float = 5.0) -> list:
    """
    超预期分析

    逻辑（复用 scanners/earnings_scanner.py _check_beat 的核心）：
      1. 从 earnings 表获取每只股票最新一期数据
      2. 从 consensus 表获取一致预期
      3. 对比 actual_yoy vs expected_yoy，diff >= threshold → 超预期

    写入 analysis_results，analysis_type='earnings_beat'
    """
    logger.info("📊 超预期分析开始...")

    with get_connection(db_path) as conn:
        # 获取有数据的股票列表
        if codes:
            placeholders = ",".join(["?" for _ in codes])
            stock_filter = f"AND e.stock_code IN ({placeholders})"
            params = codes
        else:
            stock_filter = ""
            params = []

        # 每只股票最新一期 earnings + consensus 联合查询
        rows = conn.execute(f"""
            SELECT
                e.stock_code,
                e.report_date,
                e.net_profit_yoy,
                e.revenue_yoy,
                e.net_profit,
                e.revenue,
                e.roe,
                e.eps,
                c.eps AS consensus_eps,
                c.net_profit_yoy AS expected_yoy,
                c.rev_yoy AS expected_rev_yoy,
                c.num_analysts
            FROM earnings e
            LEFT JOIN consensus c ON e.stock_code = c.stock_code
            INNER JOIN (
                SELECT stock_code AS sc, MAX(report_date) AS max_date
                FROM earnings
                GROUP BY stock_code
            ) latest ON e.stock_code = latest.sc
                    AND e.report_date = latest.max_date
            WHERE e.net_profit_yoy IS NOT NULL {stock_filter}
            ORDER BY e.stock_code
        """, params).fetchall()

    results = []
    now = datetime.now().isoformat()

    with get_connection(db_path) as conn:
        for row in rows:
            rec = dict(row)
            stock_code = rec["stock_code"]
            stock_name = stock_code  # earnings 表暂未存 name，从 stocks 表补充

            # 尝试获取股票名称
            name_row = conn.execute(
                "SELECT name FROM stocks WHERE code = ?", (stock_code,)
            ).fetchone()
            if name_row and name_row["name"]:
                stock_name = name_row["name"]

            actual_yoy = float(rec.get("net_profit_yoy") or 0)
            expected_yoy = rec.get("expected_yoy")
            num_analysts = rec.get("num_analysts", 0) or 0

            has_consensus = expected_yoy is not None
            if has_consensus:
                expected_yoy = float(expected_yoy)
                beat_diff = actual_yoy - expected_yoy
            else:
                expected_yoy = None
                beat_diff = 0

            is_beat = has_consensus and beat_diff >= threshold
            is_miss = has_consensus and beat_diff <= -threshold

            # 评分
            if not has_consensus:
                score = 55  # 无预期数据，给一个中性偏积极分
                signal = "watch"
            elif is_beat:
                score = min(100, 60 + beat_diff * 2)
                signal = "buy" if score >= 75 else "watch"
            elif is_miss:
                score = max(0, 40 + beat_diff * 2)
                signal = "avoid"
            else:
                score = 50 + beat_diff
                signal = "hold"

            result = {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "analysis_type": "earnings_beat",
                "report_date": rec.get("report_date"),
                "actual_yoy": round(actual_yoy, 2),
                "expected_yoy": round(expected_yoy, 2) if expected_yoy is not None else None,
                "beat_diff": round(beat_diff, 2) if has_consensus else None,
                "is_beat": is_beat,
                "is_miss": is_miss,
                "has_consensus": has_consensus,
                "num_analysts": num_analysts,
                "score": round(score, 1),
                "signal": signal,
                "analyzed_at": now,
            }
            results.append(result)

            # 写入 analysis_results
            detail_json = json.dumps(result, ensure_ascii=False, default=str)
            conn.execute("""
                INSERT OR REPLACE INTO analysis_results
                (stock_code, analysis_type, score, signal, summary, detail, created_at)
                VALUES (?, 'earnings_beat', ?, ?, ?, ?, datetime('now', 'localtime'))
            """, (
                stock_code,
                round(score, 1),
                signal,
                detail_json,
                detail_json,
            ))

    logger.info(f"  ✅ 超预期分析完成: {len(results)} 只股票")
    return results


# ── 扣非新高分析（从 DB 读取，纯分析） ────────────────────────────────────────

def analyze_profit_new_high(db_path: str, codes: list = None) -> list:
    """
    扣非新高分析

    逻辑（复用 scanners/new_high_scanner.py 的核心思路）：
      1. 从 earnings 表获取最近 8 个季度的 net_profit
      2. 累计转单季度（利用 net_profit 字段近似扣非）
      3. 判断最新单季度是否创历史新高

    写入 analysis_results，analysis_type='profit_new_high'
    """
    logger.info("💎 扣非新高分析开始...")

    with get_connection(db_path) as conn:
        if codes:
            placeholders = ",".join(["?" for _ in codes])
            stock_filter = f"WHERE stock_code IN ({placeholders})"
            params = codes
        else:
            stock_filter = ""
            params = []

        stock_rows = conn.execute(
            f"SELECT DISTINCT stock_code FROM earnings {stock_filter}", params
        ).fetchall()

    results = []
    now = datetime.now().isoformat()

    with get_connection(db_path) as conn:
        for srow in stock_rows:
            stock_code = srow["stock_code"]

            # 获取最近 8 个季度数据
            quarters = conn.execute("""
                SELECT report_date, net_profit, net_profit_yoy
                FROM earnings
                WHERE stock_code = ?
                  AND net_profit IS NOT NULL
                ORDER BY report_date DESC
                LIMIT 8
            """, (stock_code,)).fetchall()

            if len(quarters) < 5:
                continue

            # 累计转单季度
            # quarters 按 report_date DESC 排序（最新在前）
            # 转为正序后：Q季度 = Q累计 - (Q-1)累计，Q1 直接取累计值
            # 注意：最早一条数据无法计算单季度（缺前值），跳过
            quarters_asc = list(reversed(quarters))
            quarterly_profits = []
            for i in range(1, len(quarters_asc)):  # 从第2条开始（有前值）
                rec = dict(quarters_asc[i])
                date = rec["report_date"]
                cumulative = float(rec["net_profit"] or 0)
                month = date[5:7] if len(date) >= 7 else ""

                if month == "03" or month == "01":
                    # Q1 = 累计值即单季度
                    quarterly_profits.append({"date": date, "value": cumulative})
                elif month in ("06", "07", "09", "10", "12"):
                    # Q2/Q3/Q4 = 当期累计 - 上期累计
                    prev_cum = float(dict(quarters_asc[i - 1]).get("net_profit") or 0)
                    quarterly_profits.append({"date": date, "value": cumulative - prev_cum})

            if len(quarterly_profits) < 2:
                continue

            latest_val = quarterly_profits[-1]["value"]
            historical_max = max(q["value"] for q in quarterly_profits[:-1])

            is_new_high = latest_val > historical_max and latest_val > 0

            if is_new_high:
                growth = ((latest_val - historical_max) / historical_max * 100) if historical_max > 0 else 0
                score = min(100, 60 + growth * 2)
                signal = "watch"
            else:
                score = 40
                signal = "hold"

            # 获取股票名称
            stock_name = stock_code
            name_row = conn.execute(
                "SELECT name FROM stocks WHERE code = ?", (stock_code,)
            ).fetchone()
            if name_row and name_row["name"]:
                stock_name = name_row["name"]

            result = {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "analysis_type": "profit_new_high",
                "report_date": quarterly_profits[-1]["date"],
                "quarterly_profit": round(latest_val, 4),
                "prev_quarterly_high": round(historical_max, 4),
                "is_new_high": is_new_high,
                "growth_pct": round(((latest_val / historical_max - 1) * 100), 2) if historical_max > 0 else 0,
                "score": round(score, 1),
                "signal": signal,
                "analyzed_at": now,
            }
            results.append(result)

            # 写入 analysis_results
            detail_json = json.dumps(result, ensure_ascii=False, default=str)
            conn.execute("""
                INSERT OR REPLACE INTO analysis_results
                (stock_code, analysis_type, score, signal, summary, detail, created_at)
                VALUES (?, 'profit_new_high', ?, ?, ?, ?, datetime('now', 'localtime'))
            """, (
                stock_code,
                round(score, 1),
                signal,
                detail_json,
                detail_json,
            ))

    logger.info(f"  ✅ 扣非新高分析完成: {len(results)} 只股票")
    return results


# ── 回调买入分析（从 DB 读取 K 线 → 用现有 calc_pullback_score） ─────────────

def analyze_pullback(db_path: str, codes: list = None, min_score: int = 40) -> list:
    """
    回调买入分析

    逻辑：
      1. 从 prices 表读取 K 线数据，转为 DataFrame
      2. 调用 scanners/pullback_scanner.py 的 calc_pullback_score
      3. 写入 analysis_results

    复用：scanners.pullback_scanner.calc_pullback_score（纯分析函数）
    """
    from scanners.pullback_scanner import calc_pullback_score

    logger.info("📐 回调买入分析开始...")

    with get_connection(db_path) as conn:
        if codes:
            placeholders = ",".join(["?" for _ in codes])
            stock_filter = f"WHERE stock_code IN ({placeholders})"
            params = codes
        else:
            stock_filter = ""
            params = []

        stock_rows = conn.execute(
            f"SELECT DISTINCT stock_code FROM prices {stock_filter}", params
        ).fetchall()

    results = []
    now = datetime.now().isoformat()
    beat_codes = set()  # 超预期股票集合

    # 获取超预期股票集合（用于加分）
    with get_connection(db_path) as conn:
        beat_rows = conn.execute("""
            SELECT DISTINCT stock_code FROM analysis_results
            WHERE analysis_type = 'earnings_beat'
            AND score >= 70
        """).fetchall()
        beat_codes = {r["stock_code"] for r in beat_rows}

    with get_connection(db_path) as conn:
        for srow in stock_rows:
            stock_code = srow["stock_code"]

            # 获取最近 120 天 K 线
            klines = conn.execute("""
                SELECT trade_date, open_price, high_price, low_price,
                       close_price, volume
                FROM prices
                WHERE stock_code = ?
                ORDER BY trade_date DESC
                LIMIT 120
            """, (stock_code,)).fetchall()

            if len(klines) < 61:
                continue

            # 构建 DataFrame（升序）
            rows = [dict(r) for r in reversed(klines)]
            df = pd.DataFrame(rows)
            df = df.rename(columns={
                "open_price": "open",
                "high_price": "high",
                "low_price": "low",
                "close_price": "close",
            })

            # 获取股票名称
            stock_name = stock_code
            name_row = conn.execute(
                "SELECT name FROM stocks WHERE code = ?", (stock_code,)
            ).fetchone()
            if name_row and name_row["name"]:
                stock_name = name_row["name"]

            try:
                score_result = calc_pullback_score(
                    df, stock_name,
                    is_earnings_beat=(stock_code in beat_codes),
                    market_env_good=True,
                )

                if score_result.get("passed") and score_result.get("score", 0) >= min_score:
                    score_result["code"] = stock_code
                    score_result["name"] = stock_name
                    score_result["close"] = round(float(df["close"].iloc[-1]), 2)
                    score_result["trade_date"] = str(df["trade_date"].iloc[-1])
                    score_result["analyzed_at"] = now
                    results.append(score_result)

                    # 写入 analysis_results
                    detail_json = json.dumps({
                        "name": stock_name,
                        "score": score_result.get("score"),
                        "grade": score_result.get("grade"),
                        "reason": score_result.get("reason", ""),
                        "close": score_result.get("close"),
                        "path_a": score_result.get("path_a", False),
                        "path_b": score_result.get("path_b", False),
                        "trend": score_result.get("trend", {}),
                        "volume": score_result.get("volume", {}),
                        "support": {"count": score_result.get("support", {}).get("count", 0)},
                        "momentum": {"passed": score_result.get("momentum", {}).get("passed", False)},
                    }, ensure_ascii=False, default=str)

                    conn.execute("""
                        INSERT OR REPLACE INTO analysis_results
                        (stock_code, analysis_type, score, signal, summary, detail, created_at)
                        VALUES (?, 'pullback_buy', ?, ?, ?, ?, datetime('now', 'localtime'))
                    """, (
                        stock_code,
                        score_result["score"],
                        score_result.get("grade", "B"),
                        score_result.get("reason", ""),
                        detail_json,
                    ))

            except Exception as e:
                logger.warning(f"  {stock_code} 回调分析失败: {e}")
                continue

    # 按评分排序
    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    logger.info(f"  ✅ 回调买入分析完成: {len(results)} 只信号")
    return results


# ── 发现池管理 ─────────────────────────────────────────────────────────────────

def update_discovery_pool(db_path: str, beat_results: list, new_high_results: list,
                          pullback_results: list) -> int:
    """
    将分析发现的优质股票写入 discovery_pool。
    返回新增/更新的条数。
    """
    now = datetime.now().isoformat()
    expires = (datetime.now() + timedelta(days=30)).isoformat()
    count = 0

    with get_connection(db_path) as conn:
        for r in beat_results:
            if not r.get("is_beat"):
                continue
            code = r["stock_code"]
            try:
                conn.execute("""
                    INSERT INTO discovery_pool (stock_code, stock_name, source, score, signal, detail, expires_at)
                    VALUES (?, ?, 'earnings_beat', ?, ?, ?, ?)
                    ON CONFLICT(stock_code) DO UPDATE SET
                        score = MAX(score, excluded.score),
                        signal = excluded.signal,
                        detail = excluded.detail,
                        updated_at = datetime('now', 'localtime')
                """, (
                    code, r.get("stock_name", ""),
                    r["score"], r["signal"],
                    json.dumps(r, ensure_ascii=False, default=str),
                    expires,
                ))
                count += 1
            except Exception as e:
                logger.warning(f"  discovery_pool 更新失败 {code}: {e}")

        for r in new_high_results:
            if not r.get("is_new_high"):
                continue
            code = r["stock_code"]
            try:
                conn.execute("""
                    INSERT INTO discovery_pool (stock_code, stock_name, source, score, signal, detail, expires_at)
                    VALUES (?, ?, 'profit_new_high', ?, ?, ?, ?)
                    ON CONFLICT(stock_code) DO UPDATE SET
                        score = MAX(score, excluded.score),
                        signal = excluded.signal,
                        detail = excluded.detail,
                        updated_at = datetime('now', 'localtime')
                """, (
                    code, r.get("stock_name", ""),
                    r["score"], r["signal"],
                    json.dumps(r, ensure_ascii=False, default=str),
                    expires,
                ))
                count += 1
            except Exception as e:
                logger.warning(f"  discovery_pool 更新失败 {code}: {e}")

        for r in pullback_results:
            code = r.get("code")
            if not code:
                continue
            try:
                conn.execute("""
                    INSERT INTO discovery_pool (stock_code, stock_name, source, score, signal, detail, expires_at)
                    VALUES (?, ?, 'pullback_buy', ?, ?, ?, ?)
                    ON CONFLICT(stock_code) DO UPDATE SET
                        score = MAX(score, excluded.score),
                        signal = excluded.signal,
                        detail = excluded.detail,
                        updated_at = datetime('now', 'localtime')
                """, (
                    code, r.get("name", ""),
                    r.get("score", 0), r.get("grade", "B"),
                    json.dumps(r, ensure_ascii=False, default=str),
                    expires,
                ))
                count += 1
            except Exception as e:
                logger.warning(f"  discovery_pool 更新失败 {code}: {e}")

    if count:
        logger.info(f"  🏊 discovery_pool 更新: {count} 条")
    return count


# ── Analyzer 主流程 ───────────────────────────────────────────────────────────

class Analyzer:
    """
    统一分析引擎

    用法：
        from analyzer import Analyzer
        az = Analyzer()
        result = az.run(mode="full")
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DB_PATH)

    def run(self, mode: str = "full", codes: list = None,
            min_score: int = 40, no_push: bool = False) -> dict:
        """
        执行分析。

        Args:
            mode: full / earnings / pullback
            codes: 指定股票代码列表
            min_score: 回调买入最低分
            no_push: 不触发推送

        Returns:
            {"earnings_beat": [...], "profit_new_high": [...], "pullback": [...]}
        """
        init_db(self.db_path)
        t0 = time.time()

        beat_results = []
        new_high_results = []
        pullback_results = []

        # 1. 超预期分析
        if mode in ("full", "earnings"):
            beat_results = analyze_earnings_beat(self.db_path, codes)

        # 2. 扣非新高分析
        if mode in ("full", "earnings"):
            new_high_results = analyze_profit_new_high(self.db_path, codes)

        # 3. 回调买入分析
        if mode in ("full", "pullback"):
            pullback_results = analyze_pullback(self.db_path, codes, min_score)

        # 4. 更新发现池
        update_discovery_pool(self.db_path, beat_results, new_high_results, pullback_results)

        elapsed = time.time() - t0
        logger.info(
            f"\n{'='*50}\n"
            f"📊 Analyzer 分析完成\n"
            f"  超预期: {len(beat_results)} 只\n"
            f"  扣非新高: {len(new_high_results)} 只\n"
            f"  回调买入: {len(pullback_results)} 只\n"
            f"  耗时: {elapsed:.1f}s\n"
            f"{'='*50}"
        )

        # 5. 触发 pusher（除非 no_push）
        if not no_push and (beat_results or new_high_results or pullback_results):
            self._trigger_pusher()

        return {
            "earnings_beat": beat_results,
            "profit_new_high": new_high_results,
            "pullback": pullback_results,
        }

    def _trigger_pusher(self):
        """触发推送（如果有 pusher.py）"""
        pusher_path = Path(__file__).parent / "pusher.py"
        if pusher_path.exists():
            logger.info("🔄 触发 pusher.py...")
            import subprocess
            proc = subprocess.run(
                [sys.executable, str(pusher_path)],
                capture_output=True, text=True,
            )
            if proc.returncode != 0:
                logger.error(f"pusher.py 执行失败 (exit={proc.returncode}): {proc.stderr}")
        else:
            logger.info("ℹ️  pusher.py 不存在，跳过推送")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Analyzer — 统一分析入口")
    parser.add_argument("--mode", type=str, default="full",
                        choices=["full", "earnings", "pullback"],
                        help="分析模式: full/earnings/pullback")
    parser.add_argument("--codes", type=str, default=None,
                        help="指定股票代码（逗号分隔）")
    parser.add_argument("--min-score", type=int, default=40,
                        help="回调买入最低分（默认 40）")
    parser.add_argument("--no-push", action="store_true",
                        help="不触发推送")
    return parser.parse_args()


def main():
    args = parse_args()

    codes = None
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    az = Analyzer()
    az.run(mode=args.mode, codes=codes, min_score=args.min_score, no_push=args.no_push)


if __name__ == "__main__":
    main()
