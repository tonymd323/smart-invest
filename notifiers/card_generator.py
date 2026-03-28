"""
飞书卡片消息生成器
==================
生成飞书交互式卡片 JSON，供 pusher.py 通过 OpenClaw message 工具发送。

v2 精简版：仅保留 generate_daily_scan_card（含原生 table 组件）。
"""

from collections import Counter
from datetime import datetime


class CardGenerator:
    """飞书消息卡片生成器（v2 精简版 — 仅保留 generate_daily_scan_card）"""

    # ── 工具方法 ──────────────────────────────────────────────────────────────

    @staticmethod
    def truncate(text: str, max_len: int = 2000) -> str:
        """截断超长消息"""
        if len(text) <= max_len:
            return text
        return text[:max_len - 20] + "\n\n... (消息已截断)"

    # ── 飞书交互式卡片 ────────────────────────────────────────────────────────

    @staticmethod
    def generate_daily_scan_card(beats: list, new_highs: list, industry_map: dict = None,
                                  pullback_signals: list = None) -> dict:
        """
        生成备选股池日报的飞书交互式卡片 JSON（含原生 table 组件）。
        """
        date_str = datetime.now().strftime('%Y-%m-%d')
        total = len(beats) + len(new_highs)
        pullback_signals = pullback_signals or []
        scan_time = datetime.now().strftime('%H:%M')

        elements = []

        # 概览行
        true_beats = [b for b in beats if b.get('consensus_available', False)]
        no_consensus = [b for b in beats if not b.get('consensus_available', False)]
        non_recurring = [b for b in true_beats if b.get('is_non_recurring')]
        overview_parts = [f"📊 共发现 **{total}** 只"]
        if true_beats:
            overview_parts.append(f"超预期 **{len(true_beats)}** 只")
        if non_recurring:
            overview_parts.append(f"⚠️ 非经常性 **{len(non_recurring)}** 只")
        if no_consensus:
            overview_parts.append(f"首次覆盖 **{len(no_consensus)}** 只")
        if new_highs:
            overview_parts.append(f"扣非新高 **{len(new_highs)}** 只")
        if pullback_signals:
            overview_parts.append(f"📐 回调买入 **{len(pullback_signals)}** 只")
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": " | ".join(overview_parts)}
        })

        # 板块分布统计（超预期 TOP5）
        if industry_map and true_beats:
            ind_count = Counter()
            for b in true_beats:
                ind = industry_map.get(b['code'], '未分类')
                ind_count[ind] += 1
            top5 = ind_count.most_common(5)
            dist_parts = [f"{ind}({c})" for ind, c in top5]
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"🏭 超预期板块：{' > '.join(dist_parts)}"}
            })

        # 业绩超预期表格（仅显示有共识预期的）
        if true_beats:
            beat_rows = []
            for b in true_beats[:10]:
                code = b['code'].replace('.SH', '').replace('.SZ', '')
                name = b.get('name', '')
                nr_flag = " ⚠️" if b.get('is_non_recurring') else ""
                label = f"{name}({code}){nr_flag}" if name else code
                ap = b.get('actual_profit_yoy')
                ep = b.get('expected_profit_yoy')
                ar = b.get('actual_rev_yoy')
                er = b.get('expected_rev_yoy')
                ap_str = f"**{ap:+.0f}%**" if ap is not None else "-"
                ep_str = f"{ep:+.0f}%" if ep is not None else "-"
                ar_str = f"**{ar:+.0f}%**" if ar is not None else "-"
                er_str = f"{er:+.0f}%" if er is not None else "-"
                rtype = b.get('report_type', '财报')
                ann = b.get('ann_date', '')
                ann_fmt = f"{ann[:4]}-{ann[4:6]}-{ann[6:]}" if len(ann) == 8 else ann

                beat_rows.append({
                    "stock": label,
                    "profit_growth": ap_str,
                    "profit_expected": ep_str,
                    "rev_growth": ar_str,
                    "rev_expected": er_str,
                    "rtype": rtype,
                    "ann_date": ann_fmt
                })

            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "🏆 **业绩超预期**（实际 vs 券商一致预期）"}
            })
            elements.append({
                "tag": "table",
                "page_size": 10,
                "row_height": "low",
                "header_style": {
                    "text_align": "center",
                    "background_style": "grey",
                    "bold": True
                },
                "columns": [
                    {"name": "stock", "display_name": "股票", "data_type": "text"},
                    {"name": "profit_growth", "display_name": "利润增速", "data_type": "lark_md"},
                    {"name": "profit_expected", "display_name": "预期增速", "data_type": "text"},
                    {"name": "rev_growth", "display_name": "营收增速", "data_type": "lark_md"},
                    {"name": "rev_expected", "display_name": "预期增速", "data_type": "text"},
                    {"name": "rtype", "display_name": "类型", "data_type": "text"},
                    {"name": "ann_date", "display_name": "披露日", "data_type": "text"},
                ],
                "rows": beat_rows
            })

        # 首次覆盖/无一致预期表格
        if no_consensus:
            nc_rows = []
            for b in no_consensus[:10]:
                code = b['code'].replace('.SH', '').replace('.SZ', '')
                name = b.get('name', '')
                label = f"{name}({code})" if name else code
                ap = b.get('actual_profit_yoy')
                ar = b.get('actual_rev_yoy')
                ap_str = f"**{ap:+.0f}%**" if ap is not None else "-"
                ar_str = f"**{ar:+.0f}%**" if ar is not None else "-"
                rtype = b.get('report_type', '财报')
                ann = b.get('ann_date', '')
                ann_fmt = f"{ann[:4]}-{ann[4:6]}-{ann[6:]}" if len(ann) == 8 else ann

                nc_rows.append({
                    "stock": label,
                    "profit_growth": ap_str,
                    "rev_growth": ar_str,
                    "rtype": rtype,
                    "ann_date": ann_fmt
                })

            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "📋 **首次覆盖**（有实际数据，暂无券商一致预期）"}
            })
            elements.append({
                "tag": "table",
                "page_size": 10,
                "row_height": "low",
                "header_style": {
                    "text_align": "center",
                    "background_style": "grey",
                    "bold": True
                },
                "columns": [
                    {"name": "stock", "display_name": "股票", "data_type": "text"},
                    {"name": "profit_growth", "display_name": "利润增速", "data_type": "lark_md"},
                    {"name": "rev_growth", "display_name": "营收增速", "data_type": "lark_md"},
                    {"name": "rtype", "display_name": "类型", "data_type": "text"},
                    {"name": "ann_date", "display_name": "披露日", "data_type": "text"},
                ],
                "rows": nc_rows
            })

        # 扣非净利润新高表格
        if new_highs:
            high_rows = []
            for h in new_highs[:10]:
                profit = h.get('quarterly_profit', 0)
                growth = h.get('growth_vs_high', 0)
                pe = h.get('pe', 0)
                code = h['code'].replace('.SH', '').replace('.SZ', '')
                name = h.get('name', '')
                label = f"{name}({code})" if name else code
                rtype = h.get('report_type', '财报')
                ann = h.get('ann_date', '')
                ann_fmt = f"{ann[:4]}-{ann[4:6]}-{ann[6:]}" if len(ann) == 8 else ann

                high_rows.append({
                    "stock": label,
                    "profit": f"**{profit:.2f}亿**",
                    "growth": f"**{growth:.0f}%**",
                    "pe": f"{pe:.1f}" if pe is not None else "-",
                    "rtype": rtype,
                    "ann_date": ann_fmt
                })

            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "💎 **单季度扣非净利润历史新高**"}
            })
            elements.append({
                "tag": "table",
                "page_size": 10,
                "row_height": "low",
                "header_style": {
                    "text_align": "center",
                    "background_style": "grey",
                    "bold": True
                },
                "columns": [
                    {"name": "stock", "display_name": "股票", "data_type": "text"},
                    {"name": "profit", "display_name": "单季扣非", "data_type": "lark_md"},
                    {"name": "growth", "display_name": "超前高", "data_type": "lark_md"},
                    {"name": "pe", "display_name": "PE", "data_type": "text"},
                    {"name": "rtype", "display_name": "类型", "data_type": "text"},
                    {"name": "ann_date", "display_name": "披露日", "data_type": "text"},
                ],
                "rows": high_rows
            })

        # 回调买入信号表格
        if pullback_signals:
            pb_rows = []
            for s in pullback_signals[:10]:
                code = s['code'].replace('.SH', '').replace('.SZ', '')
                name = s.get('name', '') or code
                label = f"{name}({code})" if name != code else code
                grade = s.get('grade', 'C')
                score = s.get('score', 0)
                close = s.get('close', 0)
                reason = s.get('reason', '')[:30]

                grade_emoji = {'S': '🟢', 'A': '🟡', 'B': '🟠', 'C': '🔴'}.get(grade, '⚪')

                pb_rows.append({
                    "stock": label,
                    "grade": f"{grade_emoji} {grade}级",
                    "score": f"**{score}**",
                    "close": f"{close:.2f}" if close else "-",
                    "reason": reason
                })

            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "📐 **回调买入信号** (烧香拜佛+备买融合)"}
            })
            elements.append({
                "tag": "table",
                "page_size": 10,
                "row_height": "low",
                "header_style": {
                    "text_align": "center",
                    "background_style": "blue",
                    "bold": True
                },
                "columns": [
                    {"name": "stock", "display_name": "股票", "data_type": "text"},
                    {"name": "grade", "display_name": "评级", "data_type": "lark_md"},
                    {"name": "score", "display_name": "评分", "data_type": "lark_md"},
                    {"name": "close", "display_name": "现价", "data_type": "text"},
                    {"name": "reason", "display_name": "信号说明", "data_type": "text"},
                ],
                "rows": pb_rows
            })

        # 底部
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "note",
            "elements": [
                {"tag": "plain_text", "content": f"⏰ 扫描时间 {scan_time} | 数据源：Tushare + AkShare"}
            ]
        })

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"🏆 备选股池日报 {date_str}"},
                "template": "blue"
            },
            "elements": elements
        }
        return card


if __name__ == "__main__":
    # 快速测试
    gen = CardGenerator()

    # 测试 generate_daily_scan_card
    beats = [
        {"code": "600660.SH", "name": "福耀玻璃", "consensus_available": True,
         "actual_profit_yoy": 25.3, "expected_profit_yoy": 15.0,
         "actual_rev_yoy": 18.7, "expected_rev_yoy": 12.0,
         "report_type": "Q3", "ann_date": "20251028"},
    ]
    new_highs = [
        {"code": "600938.SH", "name": "中国海油", "quarterly_profit": 42.5,
         "growth_vs_high": 12.0, "pe": 8.5, "close": 28.50,
         "report_type": "Q3", "ann_date": "20251030"},
    ]
    card = gen.generate_daily_scan_card(beats, new_highs, industry_map={"600660.SH": "汽车零部件"})
    import json
    print(json.dumps(card, ensure_ascii=False, indent=2))
