#!/usr/bin/env python3
"""
Pusher — 统一推送入口（Phase 3）
=================================
职责：
  1. 读 analysis_results 表 → 生成飞书卡片 JSON
  2. 读 discovery_pool 表 → 生成发现池日报
  3. 调 BitableSync 同步到 Bitable
  4. 输出卡片 JSON 到 stdout（供 cron agent 读取推送）

设计约束：
  - Pusher 只读 DB，不调 Provider
  - 复用 card_generator.py 的 generate_daily_scan_card
  - Bitable 去重由 BitableSync 处理
  - 支持 --mode: scan / pool / all

用法：
  python3 pusher.py                    # 默认 scan 模式
  python3 pusher.py --mode scan        # 发现池日报
  python3 pusher.py --mode pool        # 跟踪池日报
  python3 pusher.py --mode all         # 全部
  python3 pusher.py --no-bitable       # 不同步 Bitable
"""

import sys
import os
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent))
from core.database import init_db, get_connection, DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
)
logger = logging.getLogger('pusher')


# ── 从 DB 读取分析结果 ────────────────────────────────────────────────────────

def load_scan_results(db_path: str) -> dict:
    """
    从 analysis_results + discovery_pool 读取扫描结果，组装成
    generate_daily_scan_card 所需的 beats / new_highs / pullback_signals 格式。

    Returns:
        {"beats": [...], "new_highs": [...], "pullback_signals": [...], "industry_map": {}}
    """
    beats = []
    new_highs = []
    pullback_signals = []
    industry_map = {}

    with get_connection(db_path) as conn:
        # 1. 读 discovery_pool 中 active 的记录
        pool_rows = conn.execute("""
            SELECT stock_code, stock_name, source, score, signal, detail, industry
            FROM discovery_pool
            WHERE status = 'active'
            ORDER BY score DESC
        """).fetchall()

        for row in pool_rows:
            rec = dict(row)
            code = rec["stock_code"]
            name = rec.get("stock_name", "") or ""
            source = rec.get("source", "")
            score = rec.get("score", 0)
            detail = {}

            # 解析 detail JSON
            if rec.get("detail"):
                try:
                    detail = json.loads(rec["detail"])
                except (json.JSONDecodeError, TypeError):
                    detail = {}

            # 行业映射
            if rec.get("industry"):
                industry_map[code] = rec["industry"]

            if source == "earnings_beat":
                beats.append({
                    "code": code,
                    "name": name,
                    "consensus_available": detail.get("has_consensus", True),
                    "is_non_recurring": False,
                    "actual_profit_yoy": detail.get("actual_yoy"),
                    "expected_profit_yoy": detail.get("expected_yoy"),
                    "actual_rev_yoy": detail.get("actual_rev_yoy"),
                    "expected_rev_yoy": detail.get("expected_rev_yoy"),
                    "profit_diff": detail.get("beat_diff"),
                    "report_type": detail.get("report_type", "财报"),
                    "ann_date": detail.get("ann_date", ""),
                })
            elif source == "profit_new_high":
                new_highs.append({
                    "code": code,
                    "name": name,
                    "quarterly_profit": detail.get("quarterly_profit"),
                    "growth_vs_high": detail.get("growth_pct"),
                    "pe": detail.get("pe"),
                    "close": detail.get("close"),
                    "report_type": detail.get("report_type", "财报"),
                    "ann_date": detail.get("ann_date", ""),
                })
            elif source == "pullback_buy":
                pullback_signals.append({
                    "code": code,
                    "name": name,
                    "grade": detail.get("grade", "C"),
                    "score": score,
                    "close": detail.get("close"),
                    "reason": detail.get("reason", ""),
                })

    return {
        "beats": beats,
        "new_highs": new_highs,
        "pullback_signals": pullback_signals,
        "industry_map": industry_map,
    }


def load_pool_summary(db_path: str) -> dict:
    """
    读取 discovery_pool 生成跟踪池概要。
    用于 --mode pool 输出。
    """
    with get_connection(db_path) as conn:
        rows = conn.execute("""
            SELECT stock_code, stock_name, source, score, signal, status
            FROM discovery_pool
            ORDER BY score DESC
        """).fetchall()

    active = []
    promoted = []
    expired = []

    for row in rows:
        rec = dict(row)
        entry = {
            "code": rec["stock_code"],
            "name": rec.get("stock_name", ""),
            "source": rec.get("source", ""),
            "score": rec.get("score", 0),
            "signal": rec.get("signal", "watch"),
        }
        status = rec.get("status", "active")
        if status == "active":
            active.append(entry)
        elif status == "promoted":
            promoted.append(entry)
        else:
            expired.append(entry)

    return {
        "active": active,
        "promoted": promoted,
        "expired": expired,
        "total": len(rows),
    }


# ── 卡片生成 ──────────────────────────────────────────────────────────────────

def generate_scan_card(db_path: str) -> dict:
    """生成发现池日报卡片 JSON"""
    from notifiers.card_generator import CardGenerator

    scan = load_scan_results(db_path)
    gen = CardGenerator()

    card = gen.generate_daily_scan_card(
        beats=scan["beats"],
        new_highs=scan["new_highs"],
        industry_map=scan["industry_map"],
        pullback_signals=scan["pullback_signals"],
    )
    return card


def generate_pool_card(db_path: str) -> dict:
    """生成跟踪池概要卡片 JSON"""
    summary = load_pool_summary(db_path)
    date_str = datetime.now().strftime("%Y-%m-%d")
    scan_time = datetime.now().strftime("%H:%M")

    elements = []

    # 概览
    elements.append({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": (
                f"📊 共 **{summary['total']}** 只 | "
                f"活跃 **{len(summary['active'])}** | "
                f"已晋升 **{len(summary['promoted'])}** | "
                f"已过期 **{len(summary['expired'])}**"
            ),
        },
    })

    # 活跃列表
    if summary["active"]:
        rows = []
        for s in summary["active"][:20]:
            code = s["code"].replace(".SH", "").replace(".SZ", "")
            name = s.get("name", "") or code
            label = f"{name}({code})" if name != code else code
            source_emoji = {
                "earnings_beat": "🏆",
                "profit_new_high": "💎",
                "pullback_buy": "📐",
            }.get(s["source"], "📋")
            rows.append({
                "stock": label,
                "source": f"{source_emoji} {s['source']}",
                "score": f"**{s['score']:.0f}**",
                "signal": s.get("signal", "watch"),
            })

        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "🏊 **活跃发现池**"},
        })
        elements.append({
            "tag": "table",
            "page_size": 20,
            "row_height": "low",
            "header_style": {
                "text_align": "center",
                "background_style": "grey",
                "bold": True,
            },
            "columns": [
                {"name": "stock", "display_name": "股票", "data_type": "text"},
                {"name": "source", "display_name": "来源", "data_type": "text"},
                {"name": "score", "display_name": "评分", "data_type": "lark_md"},
                {"name": "signal", "display_name": "信号", "data_type": "text"},
            ],
            "rows": rows,
        })

    # 底部
    elements.append({"tag": "hr"})
    elements.append({
        "tag": "note",
        "elements": [
            {"tag": "plain_text", "content": f"⏰ 生成时间 {scan_time} | 数据源：SQLite analysis_results"},
        ],
    })

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"🏊 跟踪池日报 {date_str}"},
            "template": "green",
        },
        "elements": elements,
    }
    return card


# ── Bitable 同步 ──────────────────────────────────────────────────────────────

def sync_to_bitable(db_path: str) -> int:
    """
    将发现池数据同步到 Bitable。
    返回新增记录数。
    """
    from core.bitable_sync import BitableSync

    scan = load_scan_results(db_path)
    if not scan["beats"] and not scan["new_highs"]:
        logger.info("  无可同步数据，跳过 Bitable")
        return 0

    sync = BitableSync.from_preset("scan")
    records = sync.generate_scan_records(
        beats=scan["beats"],
        new_highs=scan["new_highs"],
        industry_map=scan["industry_map"],
    )

    if not records:
        return 0

    new_count = sync.sync(records)
    logger.info(f"  📤 Bitable 同步完成: {new_count} 条新记录")
    return new_count


# ── Pusher 主流程 ─────────────────────────────────────────────────────────────

class Pusher:
    """
    统一推送引擎

    用法：
        from pusher import Pusher
        p = Pusher()
        result = p.run(mode="scan")
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DB_PATH)

    def run(self, mode: str = "scan", no_bitable: bool = False) -> dict:
        """
        执行推送。

        Args:
            mode: scan / pool / all
            no_bitable: 不同步 Bitable

        Returns:
            {"cards": [...], "bitable_synced": int}
        """
        init_db(self.db_path)
        cards = []

        # scan 模式：发现池日报
        if mode in ("scan", "all"):
            card = generate_scan_card(self.db_path)
            cards.append({"type": "scan", "card": card})

        # pool 模式：跟踪池概要
        if mode in ("pool", "all"):
            card = generate_pool_card(self.db_path)
            cards.append({"type": "pool", "card": card})

        # Bitable 同步
        bitable_synced = 0
        if not no_bitable and mode in ("scan", "all"):
            try:
                bitable_synced = sync_to_bitable(self.db_path)
            except Exception as e:
                logger.warning(f"Bitable 同步失败（非致命）: {e}")

        # 输出到 stdout
        output = {
            "timestamp": datetime.now().isoformat(),
            "mode": mode,
            "cards": cards,
            "bitable_synced": bitable_synced,
        }
        print(json.dumps(output, ensure_ascii=False, default=str))

        return output


# ── DM 即时推送 ────────────────────────────────────────────────────────────────

def push_pullback_dm(signals: list, dry_run: bool = False) -> int:
    """
    回调买入信号即时推送到 Tony DM

    输入: PullbackAnalyzer.scan() 返回的信号列表（signal=buy/watch 且 score>=60）
    格式: 简洁文本消息，包含股票代码、名称、评分、信号
    推送: 通过 message 工具 action=send
    返回: 推送条数
    """
    # 筛选高分信号
    high_signals = [
        s for s in signals
        if s.get("signal") in ("buy", "watch") and (s.get("score") or 0) >= 60
    ]

    if not high_signals:
        logger.info("[push_pullback_dm] 无高分回调信号，跳过推送")
        return 0

    pushed = 0
    for sig in high_signals:
        code = sig.get("stock_code", "")
        name = sig.get("stock_name", code)
        score = sig.get("score", 0)
        signal = sig.get("signal", "")
        grade = sig.get("grade", "")
        reason = sig.get("reason", "")

        # 构造简洁消息
        emoji = "🟢" if signal == "buy" else "👀"
        msg = (
            f"{emoji} **回调信号**\n"
            f"股票：{name}（{code}）\n"
            f"评分：{score:.0f} 分 | 等级：{grade}\n"
            f"信号：{signal}\n"
            f"原因：{reason[:100]}"
        )

        if dry_run:
            logger.info(f"[push_pullback_dm] DRY RUN: {msg[:50]}...")
            pushed += 1
            continue

        # 实际推送（通过 print 输出 JSON，供 cron agent 读取）
        payload = {
            "type": "pullback_dm",
            "stock_code": code,
            "message": msg,
            "timestamp": datetime.now().isoformat(),
        }
        print(json.dumps(payload, ensure_ascii=False))
        pushed += 1

    logger.info(f"[push_pullback_dm] 推送 {pushed}/{len(high_signals)} 条信号")
    return pushed


def push_event_dm(events: list, dry_run: bool = False) -> int:
    """
    重大事件即时推送到 Tony DM

    输入: EventAnalyzer 输出的事件列表（severity=high）
    格式: 事件类型 + 股票 + 标题 + 情感
    推送: 通过 message 工具 action=send
    返回: 推送条数
    """
    # 筛选高严重程度事件
    high_events = [e for e in events if e.get("severity") == "high"]

    if not high_events:
        logger.info("[push_event_dm] 无高严重程度事件，跳过推送")
        return 0

    pushed = 0
    for event in high_events:
        stock_code = event.get("stock_code", "")
        event_type = event.get("event_type", "")
        title = event.get("title", "")
        sentiment = event.get("sentiment", "neutral")

        # 情感 → emoji
        sentiment_emoji = {"positive": "📈", "negative": "📉", "neutral": "📊"}.get(sentiment, "📊")

        # 事件类型中文标签
        type_labels = {
            "earnings_beat": "财报超预期",
            "profit_new_high": "利润新高",
            "policy利好": "政策利好",
            "policy利空": "政策利空",
            "major_contract": "重大合同",
            "risk_warning": "风险警示",
        }
        type_label = type_labels.get(event_type, event_type)

        msg = (
            f"{sentiment_emoji} **重大事件**\n"
            f"类型：{type_label}\n"
            f"股票：{stock_code or '宏观'}\n"
            f"标题：{title[:200]}\n"
            f"情感：{sentiment}"
        )

        if dry_run:
            logger.info(f"[push_event_dm] DRY RUN: {msg[:50]}...")
            pushed += 1
            continue

        # 实际推送（通过 print 输出 JSON，供 cron agent 读取）
        payload = {
            "type": "event_dm",
            "stock_code": stock_code,
            "event_type": event_type,
            "message": msg,
            "timestamp": datetime.now().isoformat(),
        }
        print(json.dumps(payload, ensure_ascii=False))
        pushed += 1

    logger.info(f"[push_event_dm] 推送 {pushed}/{len(high_events)} 条事件")
    return pushed


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Pusher — 统一推送入口")
    parser.add_argument("--mode", type=str, default="scan",
                        choices=["scan", "pool", "all"],
                        help="推送模式: scan/pool/all")
    parser.add_argument("--no-bitable", action="store_true",
                        help="不同步 Bitable")
    parser.add_argument("--db-path", type=str, default=None,
                        help="指定数据库路径（测试用）")
    return parser.parse_args()


def main():
    args = parse_args()
    p = Pusher(db_path=args.db_path)
    p.run(mode=args.mode, no_bitable=args.no_bitable)


if __name__ == "__main__":
    main()
