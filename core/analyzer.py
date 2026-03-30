"""
分析器 - Phase 0 版本(超预期 + 扣非新高)

架构:
  EarningsAnalyzer(财报分析)
    ├─ scan_beat_expectation() - 超预期扫描
    ├─ scan_new_high() - 扣非新高扫描
    ├─ auto_discover_pool() - 双池自动入场 (I-05)
    ├─ create_tn_tracking() - T+N 跟踪创建 (A-06)
    └─ update_tn_tracking() - T+N 跟踪更新 (A-06)
  PullbackAnalyzer(回调买入评分)
    └─ scan() - 四层漏斗评分扫描 (A-04)

数据流:SQLite earnings → 分析 → analysis_results / discovery_pool / event_tracking
"""

import sqlite3
import logging
import json
import urllib.request
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from core.data_normalizer import normalizer

logger = logging.getLogger(__name__)


class EarningsAnalyzer:
    """
    财报分析器

    用法:
        analyzer = EarningsAnalyzer(db_path="data/smart_invest.db")
        beat_results = analyzer.scan_beat_expectation(stock_codes=["000858.SZ"])
        high_results = analyzer.scan_new_high(stock_codes=["000858.SZ"])
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def scan_beat_expectation(
        self, stock_codes: List[str] = None, threshold_pct: float = 5.0
    ) -> List[Dict]:
        """
        超预期扫描

        逻辑:
          从 earnings 表读取最新报告期数据,
          对比 net_profit_yoy(实际)与 expectation_diff_pct(预期差),
          判断是否显著超预期。

        关键修复：
          如果 consensus 表中该股票没有预期数据，signal 设为 N/A，
          score 设为 None，不参与评分排名。

        返回:
            [{"stock_code": "000858.SZ", "score": 75.0, "signal": "buy", ...}, ...]
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        results = []
        stock_filter = ""
        params = []

        if stock_codes:
            placeholders = ",".join(["?" for _ in stock_codes])
            stock_filter = f"AND stock_code IN ({placeholders})"
            params = stock_codes

        # 获取每只股票最新一期数据（优先实际财报，其次预告）
        rows = conn.execute(f"""
            SELECT e.*
            FROM earnings e
            INNER JOIN (
                SELECT stock_code AS sc, MAX(report_date) as max_date
                FROM earnings
                GROUP BY stock_code
            ) latest ON e.stock_code = latest.sc
                     AND e.report_date = latest.max_date
            WHERE 1=1 {stock_filter}
            ORDER BY e.is_forecast ASC, e.stock_code
        """, params).fetchall()

        for row in rows:
            rec = dict(row)
            stock_code = rec["stock_code"]
            stock_name = rec.get("stock_name", stock_code)

            # 实际利润增速（预告用上限，实际财报用中值）
            is_forecast = rec.get("is_forecast", 0)
            actual_yoy = rec.get("net_profit_yoy", 0) or 0
            yoy_lower = rec.get("net_profit_yoy_lower")
            yoy_upper = rec.get("net_profit_yoy_upper")
            forecast_type = rec.get("forecast_type", "")
            
            # 对于预告，用上限做超预期判断（更敏感）
            if is_forecast and yoy_upper is not None:
                actual_yoy = yoy_upper
            
            report_date = rec.get("report_date", "")

            # 动态选年份: 2025-12-31 → 25E, 2026-03-31 → 26E
            year = self._pick_consensus_year(report_date)

            # 查 consensus 表获取对应年份预期
            cr = conn.execute(
                "SELECT net_profit_yoy FROM consensus WHERE stock_code = ? AND year = ?",
                (stock_code, year)
            ).fetchone()

            if cr is None or cr["net_profit_yoy"] is None or cr["net_profit_yoy"] == 0:
                result = {
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "analysis_type": "earnings_beat",
                    "report_period": report_date,
                    "actual_profit_yoy": actual_yoy,
                    "consensus_year": year,
                    "beat_diff_pct": None,
                    "is_beat": False,
                    "is_miss": False,
                    "score": None,
                    "signal": "N/A",
                    "analyzed_at": datetime.now().isoformat(),
                }
                results.append(result)
                self._write_result(conn, result)
                logger.info(f"[EarningsAnalyzer] {stock_code} {year} 无一致预期，跳过")
                continue

            # 计算超预期差值
            expected_yoy = cr["net_profit_yoy"]
            beat_diff = actual_yoy - expected_yoy

            is_beat = beat_diff >= threshold_pct
            is_miss = beat_diff <= -threshold_pct

            # 评分:超预期越多分越高
            if is_beat:
                score = min(100, 60 + beat_diff * 2)  # 5% 超预期 → 70 分
                signal = "buy" if score >= 75 else "watch"
            elif is_miss:
                score = max(0, 40 + beat_diff * 2)     # -5% 低于预期 → 30 分
                signal = "avoid"
            else:
                score = 50 + beat_diff                 # ±5% 内 → 中性
                signal = "hold"

            result = {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "analysis_type": "earnings_beat",
                "report_period": rec.get("report_date"),
                "actual_profit_yoy": actual_yoy,
                "yoy_lower": yoy_lower,
                "yoy_upper": yoy_upper,
                "is_forecast": is_forecast,
                "forecast_type": forecast_type,
                "beat_diff_pct": beat_diff,
                "is_beat": is_beat,
                "is_miss": is_miss,
                "score": round(score, 1),
                "signal": signal,
                "analyzed_at": datetime.now().isoformat(),
            }
            results.append(result)

            # 写入 analysis_results
            self._write_result(conn, result)

        try:
            conn.commit()
        finally:
            conn.close()

        logger.info(f"[EarningsAnalyzer] 超预期扫描完成: {len(results)} 只股票")

        # 自动入场发现池（I-05）
        try:
            beats_only = [r for r in results if r.get("signal") == "buy"]
            if beats_only:
                self.auto_discover_pool(beats=beats_only, new_highs=[])
        except Exception as e:
            logger.error(f"[EarningsAnalyzer] 自动入场失败: {e}")

        return results

    @staticmethod
    def _has_consensus_data(conn: sqlite3.Connection, stock_code: str, year: str = None) -> bool:
        """检查 consensus 表中该股票是否有有效的一致预期数据"""
        if year:
            row = conn.execute(
                "SELECT net_profit_yoy FROM consensus WHERE stock_code = ? AND year = ?",
                (stock_code, year)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT net_profit_yoy FROM consensus WHERE stock_code = ? AND net_profit_yoy != 0",
                (stock_code,)
            ).fetchone()
        return row is not None and row[0] is not None

    @staticmethod
    def _pick_consensus_year(end_date: str) -> str:
        """根据报告期选择一致预期年份: 2025-12-31 → 25E, 2026-03-31 → 26E"""
        if not end_date:
            return '25E'
        try:
            year = int(end_date[:4])
            return f"{year % 100}E"
        except (ValueError, IndexError):
            return '25E'

    def scan_new_high(self, stock_codes: List[str] = None) -> List[Dict]:
        """
        扣非新高扫描

        逻辑:
          检查每只股票最近 4 个季度的 quarterly_net_profit,
          最新一期是否创历史新高。

        返回:
            [{"stock_code": "000858.SZ", "is_new_high": True, "score": 75.0, ...}, ...]
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        results = []
        stock_filter = ""
        params = []

        if stock_codes:
            placeholders = ",".join(["?" for _ in stock_codes])
            stock_filter = f"AND e.stock_code IN ({placeholders})"
            params = stock_codes

        # 获取所有股票代码(去重)
        stock_rows = conn.execute(f"""
            SELECT DISTINCT stock_code FROM earnings e WHERE 1=1 {stock_filter}
        """, params).fetchall()

        for srow in stock_rows:
            stock_code = srow["stock_code"]

            # 获取该股票最近 8 个季度数据(按报告期倒序)
            quarters = conn.execute("""
                SELECT report_date, quarterly_net_profit, net_profit_yoy
                FROM earnings
                WHERE stock_code = ?
                  AND quarterly_net_profit IS NOT NULL
                ORDER BY report_date DESC
                LIMIT 8
            """, (stock_code,)).fetchall()

            if len(quarters) < 4:
                continue  # 数据不足,跳过

            latest = dict(quarters[0])
            latest_profit = latest.get("quarterly_net_profit", 0)

            # P0#2: 亏损股直接跳过
            if latest_profit <= 0:
                continue

            prev_quarters = [dict(q).get("quarterly_net_profit", 0) for q in quarters[1:]]
            prev_high = max(prev_quarters) if prev_quarters else 0

            is_new_high = latest_profit > prev_high

            # P0#1: 仅新高才评分，非新高标记 N/A 不入池
            if is_new_high:
                growth = ((latest_profit - prev_high) / prev_high * 100) if prev_high > 0 else 0
                score = min(100, 60 + growth * 2)
                signal = "watch"
            else:
                # 非新高：标记 N/A，不入发现池
                result = {
                    "stock_code": stock_code,
                    "analysis_type": "profit_new_high",
                    "report_period": latest.get("report_date"),
                    "quarterly_net_profit": latest_profit,
                    "is_new_high": False,
                    "score": 0,
                    "signal": "N/A",
                    "analyzed_at": datetime.now().isoformat(),
                }
                self._write_result(conn, result)
                continue  # 不返回非新高结果

            result = {
                "stock_code": stock_code,
                "analysis_type": "profit_new_high",
                "report_period": latest.get("report_date"),
                "quarterly_net_profit": latest_profit,
                "prev_quarterly_high": prev_high,
                "is_new_high": is_new_high,
                "growth_pct": round(((latest_profit / prev_high - 1) * 100), 2) if prev_high > 0 else 0,
                "score": round(score, 1),
                "signal": signal,
                "analyzed_at": datetime.now().isoformat(),
            }
            results.append(result)

            # 写入 analysis_results
            self._write_result(conn, result)

        try:
            conn.commit()
        finally:
            conn.close()

        logger.info(f"[EarningsAnalyzer] 扣非新高扫描完成: {len(results)} 只股票")
        return results

    def _write_result(self, conn, result: Dict):
        """写入 analysis_results 表（每只股票每类型只保留最新1条）"""
        import json as _json

        # 跳过 N/A 信号（无一致预期的股票不写入）
        if result.get("signal") == "N/A":
            return

        # 先删旧记录，避免重复（替代 INSERT OR REPLACE 的唯一约束失效问题）
        conn.execute(
            "DELETE FROM analysis_results WHERE stock_code = ? AND analysis_type = ?",
            (result["stock_code"], result["analysis_type"])
        )

        conn.execute("""
            INSERT INTO analysis_results
            (stock_code, analysis_type, score, signal, summary, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now', 'localtime'))
        """, (
            normalizer.normalize_code(result["stock_code"]),
            result["analysis_type"],
            result.get("score"),
            result.get("signal"),
            _json.dumps(result, ensure_ascii=False),
        ))

    # ══════════════════════════════════════════════════════════════════════════
    #  I-05: 双池自动入场
    # ══════════════════════════════════════════════════════════════════════════

    def auto_discover_pool(
        self, beats: List[Dict] = None, new_highs: List[Dict] = None
    ) -> List[Dict]:
        """
        自动发现池入场

        规则：
          - 超预期 signal=buy → 自动入池，source=earnings_beat
          - 扣非新高 signal=watch → 自动入池，source=profit_new_high
          - 已在池中且 active 的不重复入池
          - 入场后 7 天自动 expire

        Args:
            beats: scan_beat_expectation 返回的结果列表
            new_highs: scan_new_high 返回的结果列表

        Returns:
            新入场的记录列表
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        new_entries = []
        beats = beats or []
        new_highs = new_highs or []

        try:
            # 获取当前 active 的股票集合（避免重复入池）
            active_rows = conn.execute(
                "SELECT stock_code FROM discovery_pool WHERE status = 'active'"
            ).fetchall()
            active_codes = {row["stock_code"] for row in active_rows}

            # Step 1: 超预期 buy → 入池
            for beat in beats:
                if beat.get("signal") != "buy":
                    continue
                code = beat["stock_code"]
                if code in active_codes:
                    logger.info(f"[DiscoverPool] {code} 已在池中（active），跳过")
                    continue

                entry = self._insert_discovery(
                    conn, code, beat,
                    source="earnings_beat",
                    signal="buy",
                    stock_name=beat.get("stock_name"),
                )
                if entry:
                    new_entries.append(entry)

            # Step 2: 扣非新高 watch → 入池
            for high in new_highs:
                if high.get("signal") != "watch":
                    continue
                if not high.get("is_new_high"):
                    continue
                code = high["stock_code"]
                if code in active_codes:
                    logger.info(f"[DiscoverPool] {code} 已在池中（active），跳过")
                    continue

                entry = self._insert_discovery(
                    conn, code, high,
                    source="profit_new_high",
                    signal="watch",
                )
                if entry:
                    new_entries.append(entry)

            # Step 3: 过期处理 — 超过 7 天的 active → expired
            expire_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                UPDATE discovery_pool
                SET status = 'expired', updated_at = datetime('now', 'localtime')
                WHERE status = 'active' AND discovered_at < ?
            """, (expire_date,))

            conn.commit()

        except Exception as e:
            logger.error(f"[DiscoverPool] 自动入场失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

        if new_entries:
            logger.info(f"[DiscoverPool] 新入池 {len(new_entries)} 只")
        return new_entries

    def _insert_discovery(
        self, conn, stock_code: str, data: Dict,
        source: str, signal: str, stock_name: str = None,
    ) -> Optional[Dict]:
        """将单只股票写入 discovery_pool 表"""
        import json as _json

        # 过滤 B 股和新股
        code = stock_code.split('.')[0]
        if code.startswith(('900', '200')):
            logger.debug(f"[DiscoverPool] 跳过 B 股: {stock_code}")
            return None
        if code.startswith('A25') or (len(code) > 6 and not code.isdigit()):
            logger.debug(f"[DiscoverPool] 跳过新股/异常: {stock_code}")
            return None

        # P1#6: 从 stocks 表查询公司名称 + 行业
        industry = None
        if not stock_name:
            row = conn.execute(
                "SELECT name, industry FROM stocks WHERE code = ?", (stock_code,)
            ).fetchone()
            if row:
                stock_name = row["name"]
                industry = row["industry"]
        else:
            row = conn.execute(
                "SELECT industry FROM stocks WHERE code = ?", (stock_code,)
            ).fetchone()
            if row:
                industry = row["industry"]

        # P1#9: 从 detail data 获取市值，补充到入池数据
        total_mv = data.get("total_mv") or data.get("prev_quarterly_high", 0)
        detail_data = dict(data)
        if industry and "industry" not in detail_data:
            detail_data["industry"] = industry

        # 过期时间：7 天后
        expires_at = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

        # 过滤旧报告：只保留最近2个季度的报告
        report_date = detail_data.get("report_date", "")
        if report_date and report_date < "2025-10-01":
            logger.debug(f"[DiscoverPool] 跳过旧报告: {stock_code} report_date={report_date}")
            return None

        try:
            dp_code = normalizer.normalize_code(stock_code)
            conn.execute("""
                INSERT OR REPLACE INTO discovery_pool
                (stock_code, stock_name, industry, source, score, signal, detail, status, discovered_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'active', datetime('now', 'localtime'), ?)
            """, (
                dp_code,
                stock_name or dp_code,
                industry,
                source,
                data.get("score", 0),
                signal,
                _json.dumps(detail_data, ensure_ascii=False),
                expires_at,
            ))
            logger.info(f"[DiscoverPool] 入池: {stock_code} ({stock_name or '?'}) source={source} signal={signal}")
            return {
                "stock_code": stock_code,
                "stock_name": stock_name or stock_code,
                "industry": industry,
                "source": source,
                "signal": signal,
                "score": data.get("score", 0),
                "expires_at": expires_at,
            }
        except Exception as e:
            logger.error(f"[DiscoverPool] 写入失败 {stock_code}: {e}")
            return None

    # ══════════════════════════════════════════════════════════════════════════
    #  A-06: T+N 跟踪
    # ══════════════════════════════════════════════════════════════════════════

    def create_tn_tracking(
        self, stock_codes: List[str], event_type: str
    ) -> None:
        """
        为新入池股票创建 T+N 跟踪记录

        写入 event_tracking 表：
          - event_date = today
          - entry_price = 当前收盘价（从 prices 表取最新）

        Args:
            stock_codes: 股票代码列表
            event_type: 事件类型（earnings_beat / profit_new_high）
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            today = datetime.now().strftime("%Y-%m-%d")

            for code in stock_codes:
                # 检查是否已存在同类型的跟踪记录（避免多日扫描产生重复）
                existing = conn.execute("""
                    SELECT id FROM event_tracking
                    WHERE stock_code = ? AND event_type = ?
                    ORDER BY id DESC LIMIT 1
                """, (code, event_type)).fetchone()

                if existing:
                    logger.info(f"[T+N] {code} {event_type} 已存在(id={existing['id']})，跳过")
                    continue

                # 从 prices 表取最新收盘价
                price_row = conn.execute("""
                    SELECT close_price FROM prices
                    WHERE stock_code = ?
                    ORDER BY trade_date DESC
                    LIMIT 1
                """, (code,)).fetchone()

                entry_price = price_row["close_price"] if price_row else None

                # 查询公司名称
                name_row = conn.execute(
                    "SELECT name FROM stocks WHERE code = ?", (code,)
                ).fetchone()
                stock_name = name_row["name"] if name_row else None

                # 从 discovery_pool 读取报告期和财务数据
                pool_row = conn.execute("""
                    SELECT detail FROM discovery_pool
                    WHERE stock_code = ? AND status = 'active'
                    ORDER BY discovered_at DESC LIMIT 1
                """, (code,)).fetchone()
                
                report_period = None
                actual_yoy = None
                expected_yoy = None
                profit_diff = None
                if pool_row and pool_row["detail"]:
                    import json as _json
                    detail = _json.loads(pool_row["detail"])
                    # profit_new_high 用 report_period，earnings_beat 用 report_date
                    report_date = detail.get("report_date") or detail.get("report_period", "")
                    if report_date:
                        report_period = report_date.replace("-", "")
                    actual_yoy = detail.get("actual_yoy")
                    expected_yoy = detail.get("expected_yoy")
                    profit_diff = detail.get("beat_diff")

                et_code = normalizer.normalize_code(code)
                et_date = normalizer.normalize_date(today)
                conn.execute("""
                    INSERT INTO event_tracking
                    (stock_code, stock_name, event_type, event_date, entry_price, 
                     report_period, actual_yoy, expected_yoy, profit_diff, tracking_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """, (et_code, stock_name, event_type, et_date, entry_price,
                      report_period, actual_yoy, expected_yoy, profit_diff))

                logger.info(
                    f"[T+N] 创建跟踪: {code} event={event_type} "
                    f"date={today} price={entry_price} period={report_period}"
                )

            conn.commit()

        except Exception as e:
            logger.error(f"[T+N] 创建跟踪失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_tn_tracking(self) -> List[Dict]:
        """
        更新所有未完成的 T+N 跟踪记录

        逻辑：
          - 查 event_tracking 表中 return_5d/10d/20d 为空的记录
          - 从 prices 表计算 T+N 收益率
          - 写回 event_tracking

        Returns:
            有更新的记录列表
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        updated = []

        try:
            # 获取所有未完成的跟踪记录
            rows = conn.execute("""
                SELECT id, stock_code, event_date, entry_price, event_type,
                       return_1d, return_5d, return_10d, return_20d
                FROM event_tracking
                WHERE entry_price IS NOT NULL
                  AND (return_5d IS NULL OR return_10d IS NULL OR return_20d IS NULL)
                ORDER BY event_date ASC
            """).fetchall()

            for row in rows:
                track_id = row["id"]
                stock_code = row["stock_code"]
                event_date = row["event_date"]
                entry_price = row["entry_price"]

                if entry_price is None or entry_price == 0:
                    continue

                # 获取事件日后的交易日数据
                # 注意：prices.trade_date 格式为 20260327，event_date 格式为 2026-03-29
                # 需要统一格式比较，否则字符串比较会出错
                event_date_compact = event_date.replace('-', '') if event_date else ''
                prices_after = conn.execute("""
                    SELECT trade_date, close_price
                    FROM prices
                    WHERE stock_code = ? AND trade_date > ?
                    ORDER BY trade_date ASC
                    LIMIT 25
                """, (stock_code, event_date_compact)).fetchall()

                if not prices_after:
                    continue

                # 计算 T+N 收益率
                updates = {}
                for day_offset, field in [(1, "return_1d"), (5, "return_5d"),
                                          (10, "return_10d"), (20, "return_20d")]:
                    if row[field] is not None:
                        continue  # 已有值，跳过
                    if len(prices_after) >= day_offset:
                        target_price = prices_after[day_offset - 1]["close_price"]
                        if target_price and target_price > 0:
                            ret = round((target_price / entry_price - 1) * 100, 2)
                            updates[field] = ret

                if not updates:
                    continue

                # 构建 UPDATE SQL
                set_clauses = []
                params = []
                for field, value in updates.items():
                    set_clauses.append(f"{field} = ?")
                    params.append(value)

                # 更新 tracking_status 和 last_updated
                if row["return_5d"] is not None or updates.get("return_5d") is not None:
                    if row["return_10d"] is not None or updates.get("return_10d") is not None:
                        if row["return_20d"] is not None or updates.get("return_20d") is not None:
                            set_clauses.append("tracking_status = 'completed'")
                        else:
                            set_clauses.append("tracking_status = 'tracking'")
                    else:
                        set_clauses.append("tracking_status = 'tracking'")

                set_clauses.append("last_updated = datetime('now', 'localtime')")
                params.append(track_id)

                sql = f"UPDATE event_tracking SET {', '.join(set_clauses)} WHERE id = ?"
                conn.execute(sql, params)

                updated.append({
                    "stock_code": stock_code,
                    "event_date": event_date,
                    "event_type": row["event_type"],
                    **updates,
                })

                logger.info(f"[T+N] 更新 {stock_code} {event_date}: {updates}")

            conn.commit()

        except Exception as e:
            logger.error(f"[T+N] 更新跟踪失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

        if updated:
            logger.info(f"[T+N] 更新了 {len(updated)} 条跟踪记录")
        return updated


# ═══════════════════════════════════════════════════════════════════════════════
#  PullbackAnalyzer — 回调买入评分 (A-04)
# ═══════════════════════════════════════════════════════════════════════════════

def _to_native(obj):
    """递归转换 numpy 类型为 Python 原生类型（用于 JSON 序列化）"""
    import numpy as np
    if isinstance(obj, (np.bool_, np.integer)):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_native(v) for v in obj]
    return obj

class PullbackAnalyzer:
    """
    回调买入评分 Analyzer

    复用 scanners/pullback_scanner.py 的四层漏斗评分逻辑：
      第一层：趋势确认（MA20>MA60, 收盘>MA20, 近20日涨幅>0）
      第二层：回调识别（烧香拜佛路径A / 备买路径B）
      第三层：多重共振确认（缩量、支撑位、动量、K线形态）
      第四层：风险过滤（放量破MA60、连续暴跌、ST）

    用法：
        analyzer = PullbackAnalyzer(db_path="data/smart_invest.db")
        results = analyzer.scan(stock_codes=["600660.SH"])
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def scan(self, stock_codes: List[str] = None) -> List[Dict]:
        """
        扫描回调买入信号。

        从 DB prices 表读取 K 线数据，
        复用 pullback_scanner 的四层漏斗评分，
        结果写入 analysis_results（analysis_type=pullback_score）。

        Args:
            stock_codes: 指定股票列表。为 None 时扫描 DB 中所有有 K 线数据的股票。

        Returns:
            [{"stock_code": "600660.SH", "score": 75, "signal": "buy", ...}, ...]
        """
        import pandas as pd
        import numpy as np
        from scanners.pullback_scanner import calc_pullback_score
        from datetime import datetime as dt

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")

        # 确定扫描范围
        if stock_codes:
            placeholders = ",".join(["?" for _ in stock_codes])
            rows = conn.execute(f"""
                SELECT DISTINCT stock_code FROM prices
                WHERE stock_code IN ({placeholders})
            """, stock_codes).fetchall()
        else:
            rows = conn.execute("SELECT DISTINCT stock_code FROM prices").fetchall()

        scan_codes = [r["stock_code"] for r in rows]

        # 获取 stocks 表名称映射
        stock_names = {}
        name_rows = conn.execute("SELECT code, name FROM stocks").fetchall()
        for nr in name_rows:
            stock_names[nr["code"]] = nr["name"]

        results = []

        for code in scan_codes:
            try:
                # 从 DB 读取 K 线数据（最近 120 个交易日）
                kline_rows = conn.execute("""
                    SELECT trade_date, open_price, high_price, low_price,
                           close_price, volume
                    FROM prices
                    WHERE stock_code = ?
                    ORDER BY trade_date ASC
                    LIMIT 120
                """, (code,)).fetchall()

                if len(kline_rows) < 61:
                    logger.debug(f"[PullbackAnalyzer] {code} K 线数据不足 61 条，跳过")
                    continue

                # 转换为 DataFrame（pullback_scanner 期望的格式）
                df = pd.DataFrame([{
                    "trade_date": r["trade_date"],
                    "open": r["open_price"],
                    "high": r["high_price"],
                    "low": r["low_price"],
                    "close": r["close_price"],
                    "volume": r["volume"] or 0,
                } for r in kline_rows])

                stock_name = stock_names.get(code, "")

                # 复用 pullback_scanner 的四层漏斗评分
                score_result = calc_pullback_score(df, stock_name)

                # 构造标准结果
                result = {
                    "stock_code": code,
                    "stock_name": stock_name,
                    "analysis_type": "pullback_score",
                    "score": score_result["score"],
                    "grade": score_result["grade"],
                    "signal": self._score_to_signal(score_result),
                    "passed": bool(score_result["passed"]),
                    "reason": score_result.get("reason", ""),
                    "analyzed_at": dt.now().isoformat(),
                }

                # 保留详细评分信息到 summary
                detail = {
                    "grade": score_result.get("grade"),
                    "path_a": score_result.get("path_a"),
                    "path_b": score_result.get("path_b"),
                    "trend": score_result.get("trend"),
                    "volume": score_result.get("volume"),
                    "support": {
                        "count": score_result.get("support", {}).get("count", 0),
                        "near_support": score_result.get("support", {}).get("near_support", False),
                    } if "support" in score_result else {},
                    "momentum": {
                        "confirmed": score_result.get("momentum", {}).get("confirmed", 0),
                        "passed": score_result.get("momentum", {}).get("passed", False),
                    } if "momentum" in score_result else {},
                    "kline": score_result.get("kline", {}),
                    "risk": score_result.get("risk", {}),
                }

                results.append(result)

                # 只写入 buy 信号（S/A 级回调买入）
                if result["signal"] != "buy":
                    continue

                # 写入 analysis_results
                pb_code = normalizer.normalize_code(code)
                conn.execute(
                    "DELETE FROM analysis_results WHERE stock_code = ? AND analysis_type = ?",
                    (pb_code, "pullback_score")
                )
                conn.execute("""
                    INSERT INTO analysis_results
                    (stock_code, analysis_type, score, signal, summary, detail, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                """, (
                    pb_code,
                    "pullback_score",
                    score_result["score"],
                    result["signal"],
                    result["reason"],
                    json.dumps(detail, default=_to_native, ensure_ascii=False),
                ))

            except Exception as e:
                logger.error(f"[PullbackAnalyzer] {code} 扫描失败: {e}")
                continue

        try:
            conn.commit()
        finally:
            conn.close()

        logger.info(f"[PullbackAnalyzer] 回调评分扫描完成: {len(scan_codes)} 只 → {len(results)} 只信号")
        return results

    @staticmethod
    def _score_to_signal(score_result: Dict) -> str:
        """
        将 pullback_scanner 的评分结果映射为标准化信号。

        评分等级：S≥80 → buy | A≥60 → buy | B≥40 → watch | C<40 → hold
        风险否决 → avoid
        """
        if not score_result.get("passed", False):
            risk = score_result.get("risk", {})
            if risk.get("blocked", False):
                return "avoid"
            return "hold"

        grade = score_result.get("grade", "C")
        if grade in ("S", "A"):
            return "buy"
        elif grade == "B":
            return "watch"
        return "hold"


# ═══════════════════════════════════════════════════════════════════════════════
#  EventAnalyzer — 新闻事件检测 + 分类 (A-08)
# ═══════════════════════════════════════════════════════════════════════════════

class EventAnalyzer:
    """
    新闻事件检测 + 分类 Analyzer

    事件类型：
      earnings_beat    — 财报超预期
      profit_new_high  — 利润新高
      policy利好       — 政策利好
      policy利空       — 政策利空
      industry_up      — 行业景气
      industry_down    — 行业下行
      major_contract   — 重大合同
      risk_warning     — 风险警示

    用法：
        analyzer = EventAnalyzer(db_path="data/smart_invest.db")
        events = analyzer.detect_from_news(news_list)
    """

    EVENT_TYPES = [
        'earnings_beat',
        'profit_new_high',
        'policy利好',
        'policy利空',
        'industry_up',
        'industry_down',
        'major_contract',
        'risk_warning',
        'capital_buy',
        'capital_sell',
        'ops_production',
        'ops_restructure',
        'finance_report',
        'finance_dividend',
    ]

    # 关键词字典（内置）
    KEYWORDS = {
        # 政策类
        'policy利好': ['政策', '补贴', '扶持', '鼓励', '减税', '利好', '国务院', '鼓励', '专项资金', '产业政策'],
        'policy利空': ['监管', '处罚', '限制', '禁止', '罚款', '收紧', '严查', '约谈', '立案调查'],
        # 合同类
        'major_contract': ['中标', '合同', '签约', '订单', '合作协议', '框架协议', '重大合同', '重大协议'],
        # 风险类
        'risk_warning': ['ST', '退市', '风险警示', '违规', '立案', '诉讼', '仲裁', '处罚', '谴责'],
        # 行业类
        'industry_up': ['景气', '涨价', '供不应求', '扩产', '需求旺盛', '量价齐升'],
        'industry_down': ['产能过剩', '降价', '需求萎缩', '亏损', '裁员', '减产'],
        # 资本类（新增 — 公告高频）
        'capital_buy': ['增持', '回购', '员工持股', '股权激励', '回购股份'],
        'capital_sell': ['减持', '减持计划', '减持股份'],
        # 运营类（新增 — 公告高频）
        'ops_production': ['投产', '扩产', '项目投产', '生产基地', '产线', '产能释放'],
        'ops_restructure': ['重组', '并购', '收购', '合并', '分拆', '资产注入', '股权转让'],
        # 财务类（新增）
        'finance_report': ['年报', '季报', '业绩预告', '业绩快报', '经营数据', '营收'],
        'finance_dividend': ['分红', '派息', '送股', '转增'],
    }

    # 事件类型 → 情感映射
    SENTIMENT_MAP = {
        'policy利好': 'positive',
        'policy利空': 'negative',
        'major_contract': 'positive',
        'risk_warning': 'negative',
        'industry_up': 'positive',
        'industry_down': 'negative',
        'capital_buy': 'positive',
        'capital_sell': 'negative',
        'ops_production': 'positive',
        'ops_restructure': 'neutral',
        'finance_report': 'neutral',
        'finance_dividend': 'positive',
    }

    # 事件类型 → 严重程度
    SEVERITY_MAP = {
        'policy利好': 'medium',
        'policy利空': 'high',
        'major_contract': 'medium',
        'risk_warning': 'high',
        'industry_up': 'medium',
        'industry_down': 'medium',
        'capital_buy': 'medium',
        'capital_sell': 'medium',
        'ops_production': 'medium',
        'ops_restructure': 'high',
        'finance_report': 'low',
        'finance_dividend': 'low',
    }

    def __init__(self, db_path: str):
        self.db_path = db_path

    def detect_from_news(self, news_list: list) -> List[Dict]:
        """
        从新闻数据检测事件

        输入: NewsProvider 返回的新闻列表（含 title/content 字段）
        逻辑:
          - 关键词匹配分类（政策/行业/合同/风险）
          - 情感判定（positive/negative/neutral）
          - 严重程度（high/medium/low）
        输出: 写入 events 表 + 返回事件列表
        """
        events = []

        for news in news_list:
            # 提取标题和内容（兼容 NewsData 对象和 dict）
            if hasattr(news, 'title'):
                title = news.title or ""
                content = news.content or ""
                stock_code = getattr(news, 'stock_code', None)
                url = getattr(news, 'url', "")
                pub_date = getattr(news, 'pub_date', "")
                source_name = getattr(news, 'source_name', "")
            elif isinstance(news, dict):
                title = news.get("title", "")
                content = news.get("content", "")
                stock_code = news.get("stock_code")
                url = news.get("url", "")
                pub_date = news.get("pub_date", "")
                source_name = news.get("source_name", "")
            else:
                continue

            text = f"{title} {content}"

            # 关键词匹配
            for event_type, keywords in self.KEYWORDS.items():
                matched_kw = [kw for kw in keywords if kw in text]
                if not matched_kw:
                    continue

                sentiment = self.SENTIMENT_MAP.get(event_type, 'neutral')
                severity = self.SEVERITY_MAP.get(event_type, 'normal')

                event = {
                    "stock_code": stock_code,
                    "event_type": event_type,
                    "title": title,
                    "content": content[:500],
                    "source": source_name or "news",
                    "url": url,
                    "sentiment": sentiment,
                    "sentiment_score": 0.8 if sentiment == 'positive' else (-0.8 if sentiment == 'negative' else 0.0),
                    "severity": severity,
                    "published_at": pub_date,
                    "matched_keywords": matched_kw,
                }
                events.append(event)
                # 每条新闻只匹配第一个事件类型
                break

        # 写入 events 表
        if events:
            self._write_events(events)

        logger.info(f"[EventAnalyzer] 新闻事件检测: {len(news_list)} 条新闻 → {len(events)} 个事件")
        return events

    def detect_from_codes(self, stock_codes: list, limit: int = 5) -> List[Dict]:
        """
        从股票代码列表获取新闻并检测事件（P2#11 集成 NewsProvider）

        Args:
            stock_codes: 股票代码列表
            limit: 每只股票最多获取新闻数
        Returns:
            事件列表
        """
        from core.data_provider import NewsProvider

        all_news = []
        provider = NewsProvider()
        for code in stock_codes:
            try:
                news = provider.fetch(code, limit=limit)
                if news:
                    all_news.extend(news)
            except Exception as e:
                logger.warning(f"[EventAnalyzer] NewsProvider 获取失败 {code}: {e}")

        if not all_news:
            return []

        return self.detect_from_news(all_news)

    def detect_from_pipeline(self, beats: list = None, new_highs: list = None) -> List[Dict]:
        """
        从 Pipeline 分析结果检测事件

        - 超预期 buy → events (earnings_beat, positive, high)
        - 扣非新高 watch → events (profit_new_high, positive, medium)
        """
        events = []

        # 预加载合法股票代码白名单（过滤北交所误标.SZ、新股申购A2xxxx等无效代码）
        valid_codes = set()
        try:
            _conn = sqlite3.connect(self.db_path)
            for row in _conn.execute("SELECT code FROM stocks").fetchall():
                valid_codes.add(row[0])
            _conn.close()
        except Exception:
            pass

        def _is_valid(code):
            if not code:
                return False
            if "." not in code:
                return any(c.startswith(code + ".") for c in valid_codes)
            return code in valid_codes

        # 股票代码过滤：丢弃不在 stocks 白名单中的代码
        beats = [b for b in (beats or []) if _is_valid(b.get("stock_code"))]
        new_highs = [h for h in (new_highs or []) if _is_valid(h.get("stock_code"))]

# 预加载名称映射（多源查询）
        conn = sqlite3.connect(self.db_path)
        try:
            name_map = {}
            # 来源1: stocks 表
            for row in conn.execute("SELECT code, name FROM stocks").fetchall():
                name_map[row["code"]] = row["name"]
            # 来源2: event_tracking 表
            for row in conn.execute("SELECT DISTINCT stock_code, stock_name FROM event_tracking WHERE stock_name IS NOT NULL AND stock_name != ''").fetchall():
                if row["stock_code"] not in name_map:
                    name_map[row["stock_code"]] = row["stock_name"]
            # 来源3: discovery_pool 表
            for row in conn.execute("SELECT DISTINCT stock_code, stock_name FROM discovery_pool WHERE stock_name IS NOT NULL AND stock_name != ''").fetchall():
                if row["stock_code"] not in name_map:
                    name_map[row["stock_code"]] = row["stock_name"]
        except Exception:
            name_map = {}
        finally:
            conn.close()

        # 来源4: beats/new_highs 参数中的 stock_name
        for item in (beats or []) + (new_highs or []):
            code = item.get("stock_code")
            name = item.get("stock_name")
            if code and name and code not in name_map:
                name_map[code] = name

        # 超预期 buy 事件
        for beat in (beats or []):
            if beat.get("signal") != "buy":
                continue

            stock_code = beat.get("stock_code", "")
            stock_name = beat.get("stock_name") or name_map.get(stock_code, stock_code)
            beat_diff = beat.get("beat_diff_pct", 0)
            actual = beat.get("actual_growth_pct", 0)
            expected = beat.get("expected_growth_pct", 0)

            # P1#8: 格式化 content
            content_lines = [
                f"公司: {stock_name} ({stock_code})",
                f"实际增速: {actual:.1f}%",
                f"一致预期: {expected:.1f}%",
                f"超预期幅度: {beat_diff:.1f}%",
                f"评分: {beat.get('score', 0)}",
            ]
            if beat.get("report_period"):
                content_lines.append(f"报告期: {beat['report_period']}")

            event = {
                "stock_code": stock_code,
                "event_type": "earnings_beat",
                "title": f"{stock_name} 财报超预期 {beat_diff:.1f}%",
                "content": "\n".join(content_lines),
                "source": "pipeline",
                "sentiment": "positive",
                "sentiment_score": 0.9,
                "severity": "high",
                "published_at": datetime.now().isoformat(),
            }
            events.append(event)

        # 扣非新高 watch 事件
        for high in (new_highs or []):
            if high.get("signal") != "watch" or not high.get("is_new_high"):
                continue

            stock_code = high.get("stock_code", "")
            stock_name = name_map.get(stock_code, stock_code)
            profit = high.get("quarterly_net_profit", 0)
            growth = high.get("growth_pct", 0)
            report = high.get("report_period", "")

            # P1#7: title 用公司名；P1#8: content 格式化
            content_lines = [
                f"公司: {stock_name} ({stock_code})",
                f"单季净利润: {profit:.2f}亿",
                f"超前高幅度: {growth:.1f}%",
            ]
            if report:
                content_lines.append(f"报告期: {report}")

            event = {
                "stock_code": stock_code,
                "event_type": "profit_new_high",
                "title": f"{stock_name} 单季度净利润新高 {profit:.2f}亿",
                "content": "\n".join(content_lines),
                "source": "pipeline",
                "sentiment": "positive",
                "sentiment_score": 0.7,
                "severity": "medium",
                "published_at": datetime.now().isoformat(),
            }
            events.append(event)

        # 写入 events 表
        if events:
            self._write_events(events)

        logger.info(f"[EventAnalyzer] Pipeline 事件检测: beats={len(beats or [])}, new_highs={len(new_highs or [])} → {len(events)} 个事件")
        return events

    def _write_events(self, events: List[Dict]):
        """将事件写入 events 表（按 title+stock_code 去重）"""
        conn = sqlite3.connect(self.db_path)
        try:
            written = 0
            for event in events:
                title = event.get("title", "")
                stock_code = event.get("stock_code", "")
                # 去重：相同标题+股票不重复写入
                existing = conn.execute(
                    "SELECT id FROM events WHERE title = ? AND stock_code = ?",
                    (title, stock_code)
                ).fetchone()
                if existing:
                    continue

                ev_code = normalizer.normalize_code(stock_code) if stock_code else stock_code
                conn.execute("""
                    INSERT INTO events
                    (stock_code, event_type, title, content, source, url,
                     sentiment, sentiment_score, severity, published_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ev_code,
                    event.get("event_type"),
                    title,
                    event.get("content", ""),
                    event.get("source", ""),
                    event.get("url", ""),
                    event.get("sentiment", "neutral"),
                    event.get("sentiment_score", 0.0),
                    event.get("severity", "normal"),
                    event.get("published_at", ""),
                ))
                written += 1
            conn.commit()
            if written:
                logger.info(f"[EventAnalyzer] 写入 {written}/{len(events)} 条事件（去重后）")
        except Exception as e:
            logger.error(f"[EventAnalyzer] 写入 events 失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  DiscoveryPool 升级操作 (U-07)
# ═══════════════════════════════════════════════════════════════════════════════

class DiscoveryPoolManager:
    """
    发现池管理器

    用法：
        manager = DiscoveryPoolManager(db_path="data/smart_invest.db")
        manager.promote_to_watchlist("600660.SH")
        expired = manager.expire_old_entries()
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def promote_to_watchlist(self, stock_code: str) -> bool:
        """
        将发现池中的股票升级到跟踪池

        逻辑:
          - 更新 discovery_pool.status = 'promoted'
          - 更新 discovery_pool.promoted_at = now
          - 返回是否成功
        """
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("""
                UPDATE discovery_pool
                SET status = 'promoted', updated_at = datetime('now', 'localtime')
                WHERE stock_code = ? AND status = 'active'
            """, (stock_code,))
            conn.commit()
            success = cursor.rowcount > 0
            if success:
                logger.info(f"[DiscoveryPool] 升级: {stock_code} → promoted")
            else:
                logger.warning(f"[DiscoveryPool] 升级失败: {stock_code} 不在 active 发现池中")
            return success
        except Exception as e:
            logger.error(f"[DiscoveryPool] 升级异常 {stock_code}: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def expire_old_entries(self) -> int:
        """
        清理过期的发现池记录

        逻辑: discovered_at > 7天 且 status=active → status=expired
        返回: 过期条数
        """
        conn = sqlite3.connect(self.db_path)
        try:
            expire_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            cursor = conn.execute("""
                UPDATE discovery_pool
                SET status = 'expired', updated_at = datetime('now', 'localtime')
                WHERE status = 'active' AND discovered_at < ?
            """, (expire_date,))
            conn.commit()
            expired_count = cursor.rowcount
            if expired_count > 0:
                logger.info(f"[DiscoveryPool] 过期清理: {expired_count} 条")
            return expired_count
        except Exception as e:
            logger.error(f"[DiscoveryPool] 过期清理异常: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  MarketAnalyzer — 市场级分析（v2.12 市场通道专用）
# ═══════════════════════════════════════════════════════════════════════════════

class MarketAnalyzer:
    """
    市场级分析器 — 与 EarningsAnalyzer / PullbackAnalyzer 并列

    职责：分析全市场快照数据，计算 BTIQ 涨跌比 + MA5 趋势 + 超跌信号
    数据流：MarketSnapshotProvider.fetch_snapshot() → MarketAnalyzer.analyze() → SQLite

    逻辑从 OversoldScanner 迁移，接口升级为接收 MarketSnapshot 对象。
    """

    ALERT_THRESHOLD = 30   # 超跌信号阈值（MA5 < 30）
    WARN_THRESHOLD = 25    # 冰点警告（MA5 < 25）
    HOT_THRESHOLD = 80     # 过热警告（MA5 > 80）

    def __init__(self, db_path: str):
        self.db_path = db_path

    def analyze(self, snapshot) -> Dict:
        """
        分析全市场快照，计算 MA5 和信号。

        Args:
            snapshot: MarketSnapshot 对象（需有 btiq 属性）

        Returns:
            完善后的 snapshot 数据 dict，含 ma5 + signal
        """
        btiq = snapshot.btiq if hasattr(snapshot, 'btiq') else snapshot.get('btiq')

        # 从 DB 计算 MA5
        ma5 = self._calc_ma5(btiq)

        # 信号判断
        signal = self._judge_signal(btiq, ma5)

        result = {
            "btiq": btiq,
            "ma5": ma5,
            "signal": signal,
            "up_count": snapshot.up_count if hasattr(snapshot, 'up_count') else snapshot.get('up_count'),
            "down_count": snapshot.down_count if hasattr(snapshot, 'down_count') else snapshot.get('down_count'),
            "flat_count": snapshot.flat_count if hasattr(snapshot, 'flat_count') else snapshot.get('flat_count'),
            "total_count": snapshot.total_count if hasattr(snapshot, 'total_count') else snapshot.get('total_count'),
            "snapshot_time": snapshot.snapshot_time if hasattr(snapshot, 'snapshot_time') else snapshot.get('snapshot_time'),
        }

        logger.info(f"[MarketAnalyzer] BTIQ={btiq}% MA5={ma5} signal={signal}")
        return result

    def save(self, result: Dict):
        """保存分析结果到 market_snapshots 表"""
        try:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute("""
                    INSERT INTO market_snapshots
                    (snapshot_time, btiq, up_count, down_count, flat_count, total_count, ma5, signal, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'tencent')
                """, (
                    result.get("snapshot_time"),
                    result.get("btiq"),
                    result.get("up_count"),
                    result.get("down_count"),
                    result.get("flat_count"),
                    result.get("total_count"),
                    result.get("ma5"),
                    result.get("signal"),
                ))
                conn.commit()
            finally:
                conn.close()
            logger.info(f"[MarketAnalyzer] 快照已保存: BTIQ={result.get('btiq')}%")
        except Exception as e:
            logger.error(f"[MarketAnalyzer] 保存失败: {e}")

    def _calc_ma5(self, current_btiq: float) -> Optional[float]:
        """从 market_snapshots 表取最近 4 条 + 当前值，计算 MA5"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT btiq FROM market_snapshots
                ORDER BY created_at DESC
                LIMIT 4
            """).fetchall()
            conn.close()

            values = [current_btiq]
            for row in rows:
                val = row["btiq"]
                if val is not None:
                    values.append(float(val))

            if len(values) < 2:
                return None

            return round(sum(values) / len(values), 2)
        except Exception as e:
            logger.debug(f"[MarketAnalyzer] MA5 计算失败: {e}")
            return None

    @staticmethod
    def _judge_signal(btiq: float, ma5: Optional[float] = None) -> Optional[str]:
        """信号判断"""
        if ma5 is not None:
            if ma5 < MarketAnalyzer.WARN_THRESHOLD:
                return "warn"  # 冰点
            if ma5 < MarketAnalyzer.ALERT_THRESHOLD:
                return "buy"   # 超跌
        if ma5 is not None and ma5 > MarketAnalyzer.HOT_THRESHOLD:
            return "hot"       # 过热
        return None

    def get_history(self, days: int = 30) -> List[Dict]:
        """获取历史快照数据（供前端趋势图使用）"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT snapshot_time, btiq, ma5, signal, up_count, down_count
                FROM market_snapshots
                ORDER BY snapshot_time DESC
                LIMIT ?
            """, (days * 48,)).fetchall()  # 每30分钟一次 ≈ 48次/天
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"[MarketAnalyzer] 历史查询失败: {e}")
            return []


# ═══════════════════════════════════════════════════════════════════════════════
#  超跌全市场扫描 (A-05) [已废弃 — 由 MarketAnalyzer 替代]
# ═══════════════════════════════════════════════════════════════════════════════

class OversoldScanner:
    """
    超跌全市场扫描器

    复用 scripts/btiq_monitor.py 的逻辑：
      - 腾讯行情 API 获取全市场数据
      - 计算 BTIQ = 上涨家数 / (上涨+下跌) × 100
      - 计算 MA5(BTIQ) = 5日均值
      - 买入信号 = MA5 < 30

    用法：
        scanner = OversoldScanner()
        signals = scanner.scan()
    """

    ALERT_THRESHOLD = 30   # 买入信号阈值
    WARN_THRESHOLD = 25    # 冰点警告
    HOT_THRESHOLD = 80     # 过热警告

    def __init__(self, db_path: str = None):
        self.db_path = db_path

    def scan(self) -> List[Dict]:
        """
        超跌全市场扫描

        - 腾讯行情 API 获取全市场数据
        - 计算上涨/下跌/涨跌比
        - 计算 MA5（如果有历史数据）
        - 返回超跌信号列表

        返回: [{"signal": "buy"|"warn"|"hot"|None, "btiq": float, "ma5": float, ...}]
        """
        try:
            stocks = self._fetch_all_stocks()
        except Exception as e:
            logger.error(f"[OversoldScanner] 获取全市场数据失败: {e}")
            return []

        if not stocks:
            logger.warning("[OversoldScanner] 无股票数据")
            return []

        result = self._calc_btiq(stocks)
        if not result:
            return []

        # 尝试从 DB 加载历史计算 MA5
        ma5 = None
        if self.db_path:
            ma5 = self._calc_ma5_from_db(result["btiq"])

        # 信号判断
        signal = self._judge_signal(result["btiq"], ma5)

        result["ma5"] = ma5
        result["signal"] = signal

        # 保存到 DB 历史
        if self.db_path:
            self._save_to_history(result)

        logger.info(f"[OversoldScanner] BTIQ={result['btiq']}% MA5={ma5} signal={signal}")
        return [result]

    def _fetch_all_stocks(self) -> List[Dict]:
        """通过腾讯行情 API 获取全市场数据"""
        # 先获取 A 股列表（通过腾讯行情批量查询前缀）
        # 使用常见的沪深主板代码段
        codes = []
        # 沪市主板 600xxx, 601xxx, 603xxx, 605xxx
        for prefix in ("600", "601", "603", "605"):
            for i in range(0, 1000):
                code = f"{prefix}{i:03d}"
                codes.append(f"sh{code}")
        # 深市主板 000xxx, 001xxx
        for prefix in ("000", "001"):
            for i in range(0, 1000):
                code = f"{prefix}{i:03d}"
                codes.append(f"sz{code}")
        # 创业板 300xxx, 301xxx
        for prefix in ("300", "301"):
            for i in range(0, 1000):
                code = f"{prefix}{i:03d}"
                codes.append(f"sz{code}")

        stocks = []
        batch_size = 800

        for start in range(0, len(codes), batch_size):
            batch = codes[start:start + batch_size]
            url = f"https://qt.gtimg.cn/q={','.join(batch)}"
            try:
                req = urllib.request.Request(url)
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(req, timeout=15) as resp:
                    data = resp.read().decode("gbk", errors="ignore")

                for line in data.split(";"):
                    if "~" not in line:
                        continue
                    parts = line.split("~")
                    if len(parts) < 40:
                        continue
                    try:
                        code = parts[2].strip()
                        name = parts[1].strip()
                        price = float(parts[3]) if parts[3] else 0
                        change_pct = float(parts[32]) if parts[32] else 0
                        if price > 0 and code:
                            stocks.append({
                                "code": code,
                                "name": name,
                                "price": price,
                                "change_pct": change_pct,
                            })
                    except (ValueError, IndexError):
                        continue
            except Exception as e:
                logger.debug(f"[OversoldScanner] batch {start} error: {e}")
                continue

        logger.info(f"[OversoldScanner] 获取 {len(stocks)} 只股票")
        return stocks

    @staticmethod
    def _calc_btiq(stocks: List[Dict]) -> Optional[Dict]:
        """计算涨跌比指标 BTIQ"""
        up = sum(1 for s in stocks if s["change_pct"] > 0)
        down = sum(1 for s in stocks if s["change_pct"] < 0)
        flat = sum(1 for s in stocks if s["change_pct"] == 0)
        total = up + down

        if total == 0:
            return None

        btiq = up / total * 100

        return {
            "up": up,
            "down": down,
            "flat": flat,
            "total": len(stocks),
            "btiq": round(btiq, 2),
            "analyzed_at": datetime.now().isoformat(),
        }

    def _calc_ma5_from_db(self, current_btiq: float) -> Optional[float]:
        """从 DB 历史数据计算 MA5"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            # 从 analysis_results 表查最近 5 条 btiq 记录
            rows = conn.execute("""
                SELECT summary FROM analysis_results
                WHERE analysis_type = 'oversold_btiq'
                ORDER BY created_at DESC
                LIMIT 4
            """).fetchall()
            conn.close()

            if len(rows) < 1:
                return None

            values = [current_btiq]
            for row in rows:
                try:
                    data = json.loads(row["summary"])
                    btiq_val = data.get("btiq")
                    if btiq_val is not None:
                        values.append(float(btiq_val))
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue

            if len(values) < 2:
                return None

            return round(sum(values) / len(values), 2)
        except Exception as e:
            logger.debug(f"[OversoldScanner] MA5 计算失败: {e}")
            return None

    def _save_to_history(self, result: Dict):
        """保存结果到 DB history"""
        if not self.db_path:
            return
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "DELETE FROM analysis_results WHERE stock_code = 'MARKET' AND analysis_type = 'oversold_btiq'"
            )
            conn.execute("""
                INSERT INTO analysis_results
                (stock_code, analysis_type, score, signal, summary, created_at)
                VALUES ('MARKET', 'oversold_btiq', ?, ?, ?, datetime('now', 'localtime'))
            """, (
                result.get("btiq"),
                result.get("signal", "none"),
                json.dumps(result, ensure_ascii=False),
            ))
            try:
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.debug(f"[OversoldScanner] 保存历史失败: {e}")

    @staticmethod
    def _judge_signal(btiq: float, ma5: Optional[float]) -> Optional[str]:
        """信号判断"""
        if ma5 is not None and ma5 < OversoldScanner.ALERT_THRESHOLD:
            return "buy"
        elif btiq < OversoldScanner.WARN_THRESHOLD:
            return "warn"
        elif btiq > OversoldScanner.HOT_THRESHOLD:
            return "hot"
        elif ma5 is not None and ma5 < 40:
            return "weak"
        return None

