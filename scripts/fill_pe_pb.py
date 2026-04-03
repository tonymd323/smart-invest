#!/usr/bin/env python3
"""
fill_pe_pb.py — 从 Tushare Pro daily_basic 拉取 PE/PB 数据写入 stock_scores

用法:
    python3 scripts/fill_pe_pb.py              # 拉今日数据（交易日）
    python3 scripts/fill_pe_pb.py --date 20260331  # 指定日期
    python3 scripts/fill_pe_pb.py --dry-run        # 只看覆盖情况，不写入

定时任务（建议每个交易日 16:00）:
    0 16 * * 1-5 cd /app && python3 scripts/fill_pe_pb.py >> data/logs/fill_pe_pb.log 2>&1
"""

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ── 日志 ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fill_pe_pb")

# ── 路径 ────────────────────────────────────────────
APP_DIR = Path(__file__).parent.parent
DB_PATH = os.getenv("DB_PATH", "/data/smart_invest.db")
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")

# ── 依赖导入 ────────────────────────────────────────
try:
    import tushare as ts
except ImportError:
    logger.error("tushare 未安装，请先: pip install tushare")
    sys.exit(1)


def get_latest_trade_date(pro_api) -> str:
    """获取最新交易日（向前推算）"""
    today = datetime.now()
    for days_ago in range(15):
        date = today - timedelta(days=days_ago)
        if date.weekday() < 5:  # 排除周末
            d = date.strftime("%Y%m%d")
            try:
                # 测试这个日期是否有数据
                df = pro_api.daily_basic(ts_code="000001.SZ", trade_date=d)
                if not df.empty:
                    logger.info(f"最新交易日: {d}")
                    return d
            except Exception:
                continue
    raise RuntimeError("无法确定最新交易日")


def fetch_daily_basic(trade_date: str) -> dict:
    """
    从 Tushare 拉 daily_basic，返回 dict {stock_code: {pe_ttm, pb}}
    股票代码格式统一为: XXXXXX.SH / XXXXXX.SZ
    """
    pro = ts.pro_api(TUSHARE_TOKEN)

    # 拉全市场（ts_code='' 表示全部）
    logger.info(f"正在拉取 Tushare daily_basic: {trade_date}")
    df = pro.daily_basic(trade_date=trade_date, fields="ts_code,close,pe_ttm,pb")
    logger.info(f"拉取到 {len(df)} 条记录")

    result = {}
    for _, row in df.iterrows():
        ts_code = row["ts_code"]
        if ts_code and (row["pe_ttm"] > 0 or row["pb"] > 0):
            result[ts_code] = {
                "pe_ttm": row["pe_ttm"] if row["pe_ttm"] and row["pe_ttm"] > 0 else None,
                "pb": row["pb"] if row["pb"] and row["pb"] > 0 else None,
            }
    return result


def fetch_daily_basic_all() -> dict:
    """拉所有有交易的股票（一次性拉，取最新有数据的交易日）"""
    pro = ts.pro_api(TUSHARE_TOKEN)
    trade_date = get_latest_trade_date(pro)

    all_data = {}
    offset = 0
    page_size = 5000

    while True:
        try:
            df = pro.daily_basic(
                trade_date=trade_date,
                fields="ts_code,pe_ttm,pb",
                offset=offset,
                limit=page_size,
            )
            if df.empty:
                break
            for _, row in df.iterrows():
                ts_code = row["ts_code"]
                if ts_code:
                    pe = row["pe_ttm"] if row["pe_ttm"] and row["pe_ttm"] > 0 else None
                    pb = row["pb"] if row["pb"] and row["pb"] > 0 else None
                    if pe or pb:
                        all_data[ts_code] = {"pe_ttm": pe, "pb": pb}
            if len(df) < page_size:
                break
            offset += page_size
        except Exception as e:
            logger.warning(f"拉取 offset={offset} 失败: {e}")
            break

    logger.info(f"全市场拉取完成: {len(all_data)} 只股票有 PE/PB 数据")
    return all_data


def fill_pe_pb_multi_source() -> dict:
    """
    多数据源修复 PE/PB：
    主源：腾讯实时行情（PE[39] + PB[46]），批量 800 只/次
    降级：东财 Push2（单股兜底）
    """
    from core.data_provider import QuoteProvider
    import sqlite3

    conn = sqlite3.connect(DB_PATH)
    try:
        # 读取当前缺 PE/PB 的股票
        rows = conn.execute("""
            SELECT stock_code FROM stock_scores
            WHERE pe_ttm IS NULL OR pe_ttm = 0 OR pb IS NULL OR pb = 0
        """).fetchall()
        missing_codes = [r[0] for r in rows]
        logger.info(f"缺 PE/PB 股票: {len(missing_codes)} 只")

        if not missing_codes:
            return {"updated": 0, "source": "none"}

        # 腾讯批量获取 PE+PB（PE[39], PB[46]）
        provider = QuoteProvider()
        quotes = provider.fetch_batch(missing_codes)  # {code: QuoteData}

        # 写入
        updated = 0
        for code, q in quotes.items():
            pe = q.pe if q and q.pe and q.pe > 0 else None
            pb = q.pb if q and q.pb and q.pb > 0 else None
            if pe or pb:
                conn.execute(
                    "UPDATE stock_scores SET pe_ttm=COALESCE(?,pe_ttm), pb=COALESCE(?,pb) WHERE stock_code=?",
                    (pe, pb, code),
                )
                updated += 1

        conn.commit()

        # 统计
        cur = conn.execute("SELECT COUNT(*) total, SUM(CASE WHEN pe_ttm>0 THEN 1 ELSE 0 END) has_pe, SUM(CASE WHEN pb>0 THEN 1 ELSE 0 END) has_pb FROM stock_scores")
        r = cur.fetchone()

        logger.info(f"多源修复完成: 更新 {updated} 只 | PE覆盖: {r[1]}/{r[0]} | PB覆盖: {r[2]}/{r[0]}")
        return {
            "updated": updated,
            "total": r[0],
            "pe_count": r[1],
            "pb_count": r[2],
        }
    finally:
        conn.close()


def get_current_pe_pb(conn) -> dict:
    """读取 stock_scores 当前有 PE/PB 的股票"""
    rows = conn.execute("SELECT stock_code, pe_ttm, pb FROM stock_scores").fetchall()
    return {r[0]: {"pe_ttm": r[1], "pb": r[2]} for r in rows}


def fill_pe_pb(data: dict, dry_run: bool = False) -> dict:
    """
    将 PE/PB 数据写入 stock_scores
    返回统计 dict
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        current = get_current_pe_pb(conn)
        updated = 0
        inserted = 0
        unchanged = 0
        no_match = []

        for ts_code, vals in data.items():
            pe = vals["pe_ttm"]
            pb = vals["pb"]

            if ts_code not in current:
                no_match.append(ts_code)
                continue

            old_pe = current[ts_code]["pe_ttm"]
            old_pb = current[ts_code]["pb"]

            # 判断是否需要更新
            needs_update = (
                (pe is not None and (old_pe is None or old_pe == 0)) or
                (pb is not None and (old_pb is None or old_pb == 0))
            )

            if needs_update:
                if dry_run:
                    logger.info(f"[dry-run] UPDATE {ts_code}: PE {old_pe}→{pe}, PB {old_pb}→{pb}")
                else:
                    conn.execute(
                        "UPDATE stock_scores SET pe_ttm=?, pb=? WHERE stock_code=?",
                        (pe, pb, ts_code),
                    )
                    updated += 1
            else:
                unchanged += 1

        if not dry_run:
            conn.commit()

        # 统计覆盖变化
        has_pe_before = sum(1 for v in current.values() if v["pe_ttm"] and v["pe_ttm"] > 0)
        has_pb_before = sum(1 for v in current.values() if v["pb"] and v["pb"] > 0)

        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT COUNT(*) total, SUM(CASE WHEN pe_ttm>0 THEN 1 ELSE 0 END) has_pe, SUM(CASE WHEN pb>0 THEN 1 ELSE 0 END) has_pb FROM stock_scores")
        r = cur.fetchone()
        total = r[0]; has_pe_after = r[1] or 0; has_pb_after = r[2] or 0

        result = {
            "dry_run": dry_run,
            "total_stocks": total,
            "pe_before": has_pe_before,
            "pe_after": has_pe_after,
            "pb_before": has_pb_before,
            "pb_after": has_pb_after,
            "updated": updated,
            "unchanged": unchanged,
            "no_match_count": len(no_match),
            "no_match_samples": no_match[:5],
        }

        if dry_run:
            logger.info(f"[dry-run 摘要] 将更新 {updated} 只，保留 {unchanged} 只，{len(no_match)} 只无对应股票")
        else:
            logger.info(f"[填充完成] PE覆盖率: {has_pe_before}→{has_pe_after}(+{has_pe_after-has_pe_before}), PB覆盖率: {has_pb_before}→{has_pb_after}(+{has_pb_after-has_pb_before}), 更新 {updated} 只")

        return result

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="从 Tushare 拉取 PE/PB 写入 stock_scores")
    parser.add_argument("--date", type=str, help="指定交易日（YYYYMMDD），默认今日")
    parser.add_argument("--dry-run", action="store_true", help="只查询，不写入")
    args = parser.parse_args()

    if not TUSHARE_TOKEN:
        logger.error("未设置 TUSHARE_TOKEN 环境变量")
        sys.exit(1)

    if args.date:
        trade_date = args.date
        data = fetch_daily_basic(trade_date)
    else:
        data = fetch_daily_basic_all()

    if not data:
        logger.error("未获取到任何 PE/PB 数据")
        sys.exit(1)

    result = fill_pe_pb(data, dry_run=args.dry_run)
    print(f"\n=== 覆盖报告 ===")
    print(f"  总股票: {result['total_stocks']}")
    print(f"  PE覆盖: {result['pe_before']} → {result['pe_after']} (+{result['pe_after']-result['pe_before']})")
    print(f"  PB覆盖: {result['pb_before']} → {result['pb_after']} (+{result['pb_after']-result['pb_before']})")
    if result['no_match_count'] > 0:
        print(f"  无匹配股票: {result['no_match_count']} 只，示例: {result['no_match_samples']}")
