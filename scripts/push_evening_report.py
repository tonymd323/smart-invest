#!/usr/bin/env python3
"""
A股晚报推送 — 板块轮动 + 指数概览
从 Docker crontab 调用，采集数据并推送飞书卡片。
"""

import json
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def get_tenant_token():
    """获取飞书 tenant_access_token"""
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
    if not app_id or not app_secret:
        print("❌ 飞书凭证未配置")
        return None

    req = urllib.request.Request(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        data=json.dumps({"app_id": app_id, "app_secret": app_secret}).encode(),
        headers={"Content-Type": "application/json"},
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=10).read())
    return resp.get("tenant_access_token")


def fetch_index_overview():
    """获取主要指数行情"""
    try:
        import akshare as ak
        df = ak.stock_zh_index_spot_em()
        if df is None or df.empty:
            return []

        targets = {
            "上证指数": "sh000001",
            "深证成指": "sz399001",
            "创业板指": "sz399006",
            "科创50": "sh000688",
            "沪深300": "sh000300",
            "中证500": "sh000905",
        }

        results = []
        for _, row in df.iterrows():
            name = str(row.get("名称", ""))
            if name in targets:
                close = row.get("最新价", 0)
                change_pct = row.get("涨跌幅", 0)
                results.append({
                    "name": name,
                    "close": float(close) if close else 0,
                    "change_pct": float(change_pct) if change_pct else 0,
                })
        return results
    except Exception as e:
        print(f"指数获取失败: {e}")
        return []


def build_card(indices, sector_data):
    """构建飞书卡片 JSON"""
    today = datetime.now().strftime("%Y-%m-%d")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    elements = []

    # 指数概览
    if indices:
        idx_lines = []
        for idx in sorted(indices, key=lambda x: x["name"]):
            emoji = "🔴" if idx["change_pct"] > 0 else ("🟢" if idx["change_pct"] < 0 else "⚪")
            sign = "+" if idx["change_pct"] > 0 else ""
            idx_lines.append(f"{emoji} {idx['name']}: {idx['close']:.2f} ({sign}{idx['change_pct']:.2f}%)")
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "\n".join(idx_lines)},
        })
        elements.append({"tag": "hr"})

    # 板块涨幅 TOP10
    today_top = sector_data.get("today", [])
    if today_top and not today_top[0].get("error"):
        lines = ["**📈 板块涨幅 TOP10**"]
        for i, s in enumerate(today_top[:10], 1):
            lead = f" | 龙头: {s['lead_stock']}" if s.get("lead_stock") else ""
            sign = "+" if s.get("change_pct", 0) > 0 else ""
            lines.append(f"{i}. {s['name']} {sign}{s['change_pct']:.2f}%{lead}")
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "\n".join(lines)},
        })

    # 板块跌幅 TOP5
    today_bottom = sector_data.get("today_bottom", [])
    if today_bottom:
        lines = ["**📉 板块跌幅 TOP5**"]
        for i, s in enumerate(today_bottom[:5], 1):
            lines.append(f"{i}. {s['name']} {s['change_pct']:.2f}%")
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "\n".join(lines)},
        })

    # 5日资金流向
    day5 = sector_data.get("5day", [])
    if day5 and not day5[0].get("error"):
        lines = ["**💰 5日主力资金流入 TOP5**"]
        for i, s in enumerate(day5[:5], 1):
            fund = f"{s['fund_flow_yi']:.1f}亿" if s.get("fund_flow_yi") else ""
            lines.append(f"{i}. {s['name']} {fund}")
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "\n".join(lines)},
        })

    # 检查是否所有数据都是错误
    has_real_data = (
        indices or
        (today_top and not today_top[0].get("error")) or
        today_bottom or
        (day5 and not day5[0].get("error"))
    )
    if not has_real_data:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "⚠️ 今日数据采集异常（数据源连接失败），请稍后重试"},
        })

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"🌆 A股晚报 — {today} {weekday}"},
            "template": "indigo",
        },
        "elements": elements,
    }
    return card


def send_card(token, target, card):
    """发送飞书卡片"""
    payload = {
        "receive_id": target,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
    }
    req = urllib.request.Request(
        "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
    return resp.get("code", -1) == 0


def main():
    from scripts.sector_rotation import fetch_sector_flow, fetch_bottom

    print("📊 A股晚报采集...")

    # 1. 获取指数
    indices = fetch_index_overview()
    print(f"  指数: {len(indices)} 个")

    # 2. 获取板块数据
    sector_data = {
        "today": fetch_sector_flow("今日"),
        "today_bottom": fetch_bottom("今日"),
        "5day": fetch_sector_flow("5日"),
        "10day": fetch_sector_flow("10日"),
    }
    print(f"  板块: {len(sector_data['today'])} 个")

    # 3. 构建卡片
    card = build_card(indices, sector_data)

    # 4. 推送
    target = os.getenv("SI_FEISHU_DAILY_TARGET", "")
    if not target:
        print("❌ 未配置推送目标")
        sys.exit(1)

    token = get_tenant_token()
    if not token:
        sys.exit(1)

    success = send_card(token, target, card)
    if success:
        print("✅ A股晚报推送成功")
    else:
        print("❌ A股晚报推送失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
