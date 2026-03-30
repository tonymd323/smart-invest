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
    """获取主要指数行情（腾讯 API）"""
    indices_map = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000300": "沪深300",
        "sh000905": "中证500",
        "sh000688": "科创50",
    }
    try:
        codes = ",".join(indices_map.keys())
        url = f"https://qt.gtimg.cn/q={codes}"
        req = urllib.request.Request(url)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=10) as resp:
            data = resp.read().decode('gbk', errors='ignore')
        results = []
        for line in data.split(';'):
            if '~' not in line:
                continue
            parts = line.split('~')
            if len(parts) < 40:
                continue
            code = parts[2]
            name = indices_map.get(code, parts[1])
            close = float(parts[3]) if parts[3] else 0
            change_pct = float(parts[32]) if parts[32] else 0
            if close > 0:
                results.append({"name": name, "close": close, "change_pct": change_pct})
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
    if today_top:
        lines = ["**📈 板块涨幅 TOP10**"]
        for i, s in enumerate(today_top[:10], 1):
            lead = f" | 龙头: {s.get('lead_stock','')}" if s.get("lead_stock") else ""
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
    if day5:
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
        bool(today_top) or
        bool(today_bottom) or
        bool(day5)
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
    print("📊 A股晚报采集...")

    # 1. 获取指数（腾讯 API）
    indices = fetch_index_overview()
    print(f"  指数: {len(indices)} 个")

    # 2. 获取板块数据（SectorProvider，自动降级到腾讯+Tushare）
    from core.data_provider import SectorProvider
    sp = SectorProvider()
    top_sectors = sp.fetch()  # 全部，已按涨跌幅排序
    bottom_sectors = top_sectors[::-1]  # 反转 = 跌幅排行
    print(f"  板块: {len(top_sectors)} 个 (来源: {sp.last_source})")

    sector_data = {
        "today": [{"name": s.sector_name, "change_pct": s.change_pct,
                    "lead_stock": ""} for s in top_sectors],
        "today_bottom": [{"name": s.sector_name, "change_pct": s.change_pct}
                         for s in bottom_sectors],
        "5day": [],
    }

    # 3. 检查是否有真实数据（指数+板块至少有一项）
    has_real_indices = len(indices) > 0
    has_real_sectors = len(sector_data.get("today", [])) > 0
    has_real_bottom = len(sector_data.get("today_bottom", [])) > 0
    has_real_5day = len(sector_data.get("5day", [])) > 0

    if not has_real_indices and not has_real_sectors and not has_real_bottom:
        print("❌ 指数和板块数据全部失败，发送失败通知")
        target = os.getenv("SI_FEISHU_DAILY_TARGET", "")
        token = get_tenant_token()
        if target and token:
            fail_card = {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": f"⚠️ A股晚报推送失败 — {datetime.now().strftime('%Y-%m-%d')}"},
                    "template": "red",
                },
                "elements": [{
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": "今日数据采集异常（API 连接失败），请稍后重试"},
                }],
            }
            send_card(token, target, fail_card)
        sys.exit(1)

    # 清理错误数据，只保留有效板块
    if not has_real_sectors:
        sector_data["today"] = []
    if not has_real_bottom:
        sector_data["today_bottom"] = []
    if not has_real_5day:
        sector_data["5day"] = []

    # 4. 构建卡片
    card = build_card(indices, sector_data)

    # 5. 推送
    target = os.getenv("SI_FEISHU_DAILY_TARGET", "")
    if not target:
        print("❌ 未配置推送目标")
        sys.exit(1)

    token = get_tenant_token()
    if not token:
        sys.exit(1)

    success = send_card(token, target, card)
    if success:
        print(f"✅ A股晚报推送成功 (指数{len(indices)} 板块{len(sector_data.get('today',[]))}条)")
    else:
        print("❌ A股晚报推送失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
