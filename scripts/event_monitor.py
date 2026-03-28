#!/usr/bin/env python3
"""
股票池事件驱动监控器 v3
- 股价异动报警（涨跌幅超阈值）
- 个股新闻事件检测（RSS关键词匹配）
- 备选股大跌关注买入机会

数据源：
- 股价：腾讯财经 API（qt.gtimg.cn）
- 新闻：RSS Reader 文章缓存
"""

import json
import os
import re
import sys
import requests
from datetime import datetime
from pathlib import Path

# ========== 股票池配置 ==========

STOCK_POOL = {
    # 持仓关注
    "sh600938": {
        "name": "中国海油",
        "code": "600938",
        "role": "held",
        "keywords": ["中国海油", "中海油", "海上油气", "海上石油"],
        "macro_keywords": ["WTI", "布伦特", "国际油价", "原油期货", "OPEC", "油价"],
    },
    "sh600660": {
        "name": "福耀玻璃",
        "code": "600660",
        "role": "held",
        "keywords": ["福耀玻璃", "曹德旺", "汽车玻璃"],
        "macro_keywords": ["汽车销量", "新能源车", "汽车行业"],
    },
    # 持仓关注
    "sh600875": {
        "name": "东方电气",
        "code": "600875",
        "role": "held",
        "keywords": ["东方电气", "风电", "水电", "核电设备"],
    },
    "sh603308": {
        "name": "应流股份",
        "code": "603308",
        "role": "watch",
        "keywords": ["应流股份", "高端铸件", "航空发动机"],
    },
    "sh600989": {
        "name": "宝丰能源",
        "code": "600989",
        "role": "watch",
        "keywords": ["宝丰能源", "煤化工", "煤制烯烃"],
    },
    "sz002545": {
        "name": "东方铁塔",
        "code": "002545",
        "role": "watch",
        "keywords": ["东方铁塔", "电力铁塔", "输电线路"],
    },
    "sz300750": {
        "name": "宁德时代",
        "code": "300750",
        "role": "watch",
        "keywords": ["宁德时代", "CATL", "动力电池", "储能"],
        "macro_keywords": ["新能源车销量", "锂电池", "碳酸锂"],
    },
    "sz000807": {
        "name": "云铝股份",
        "code": "000807",
        "role": "watch",
        "keywords": ["云铝股份", "电解铝", "铝加工"],
        "macro_keywords": ["铝价", "电解铝产能"],
    },
}

# 涨跌幅报警阈值
ALERT_THRESHOLD_PCT = 3.0
WATCH_DROP_THRESHOLD_PCT = 5.0

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
STATE_FILE = DATA_DIR / "event_state.json"
REPORT_DIR = Path(__file__).parent.parent.parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except:
            pass
    return {"last_prices": {}, "last_check": None}


def save_state(state):
    state["last_check"] = datetime.now().isoformat()
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


def get_stock_prices(codes):
    """通过腾讯财经 API 获取实时价格"""
    results = {}
    try:
        session = requests.Session()
        session.trust_env = False
        url = f"https://qt.gtimg.cn/q={','.join(codes)}"
        resp = session.get(url, timeout=10)
        resp.encoding = "gbk"

        for line in resp.text.strip().split(";"):
            line = line.strip()
            if not line or "=" not in line:
                continue
            match = re.match(r'v_(\w+)="(.+)"', line)
            if not match:
                continue
            symbol = match.group(1)
            fields = match.group(2).split("~")
            if len(fields) < 50:
                continue

            price = float(fields[3]) if fields[3] else 0
            change_pct = float(fields[32]) if fields[32] else 0
            high = float(fields[33]) if fields[33] else 0
            low = float(fields[34]) if fields[34] else 0
            name = fields[1]

            results[symbol] = {
                "name": name,
                "price": price,
                "change_pct": change_pct,
                "high": high,
                "low": low,
            }
    except Exception as e:
        print(f"  ⚠️ 股价获取异常: {e}")
    return results


def check_price_events(code, info, price_data, state):
    """检查价格异动事件"""
    events = []
    price = price_data["price"]
    change_pct = price_data["change_pct"]
    name = info["name"]
    role = info.get("role", "")

    if price <= 0:
        return events

    last_prices = state.get("last_prices", {})
    last_price = last_prices.get(code)

    # 涨跌幅超阈值
    if change_pct >= ALERT_THRESHOLD_PCT:
        events.append(f"📈 {name} 大涨 +{change_pct:.1f}%，现价 ¥{price:.2f}")
    elif change_pct <= -ALERT_THRESHOLD_PCT:
        events.append(f"📉 {name} 大跌 {change_pct:.1f}%，现价 ¥{price:.2f}")

    # 备选股大跌 → 关注买入机会
    if role == "watch" and change_pct <= -WATCH_DROP_THRESHOLD_PCT:
        events.append(f"💡 备选股 {name} 跌 {change_pct:.1f}%，现价 ¥{price:.2f}，可关注买入窗口")

    # 与上次检查价格大幅偏离（>5%）
    if last_price and last_price > 0:
        period_change = (price - last_price) / last_price * 100
        if abs(period_change) >= 5:
            direction = "+" if period_change > 0 else ""
            events.append(f"📊 {name} 区间波动 {direction}{period_change:.1f}%（¥{last_price:.2f} → ¥{price:.2f}）")

    last_prices[code] = price
    state["last_prices"] = last_prices
    return events


def check_news_events():
    """通过 RSS 缓存检测个股相关新闻（仅检查24小时内文章）"""
    events = []
    articles_file = Path.home() / ".openclaw/skills/rss-reader/data/articles.json"

    if not articles_file.exists():
        return events

    try:
        data = json.loads(articles_file.read_text())
        if not isinstance(data, dict):
            return events

        # 24小时时间窗口
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=24)

        matched = {}

        for url, article in data.items():
            # 跳过超过24小时的旧文章
            read_at = article.get("read_at", "")
            if read_at:
                try:
                    art_time = datetime.fromisoformat(read_at)
                    if art_time < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass

            title = article.get("title", "")
            summary = article.get("summary", "")
            source = article.get("source", "")
            link = article.get("link", url)
            content = f"{title} {summary}".lower()

            for code, info in STOCK_POOL.items():
                for kw in info.get("keywords", []):
                    if kw.lower() in content:
                        if code not in matched:
                            matched[code] = []
                        if not any(m["title"] == title for m in matched[code]):
                            matched[code].append({"title": title, "link": link, "source": source})
                        break

                for kw in info.get("macro_keywords", []):
                    if kw.lower() in content:
                        if code not in matched:
                            matched[code] = []
                        if not any(m["title"] == title for m in matched[code]):
                            matched[code].append({"title": title, "link": link, "source": source, "is_macro": True})
                        break

        for code, news_list in matched.items():
            name = STOCK_POOL[code]["name"]
            for news in news_list[:2]:
                prefix = "🌐" if news.get("is_macro") else "📰"
                events.append(f"{prefix} {name} 相关：{news['title']}")
                if news.get("link"):
                    events.append(f"   🔗 {news['link']}")

    except Exception as e:
        print(f"  ⚠️ 新闻检测异常: {e}")

    return events


def generate_report(all_events, price_data):
    """生成报告"""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    lines = []
    lines.append("🔔 股票池事件监控")
    lines.append(f"📅 {date_str} {time_str}")
    lines.append("=" * 30)
    lines.append("")

    # 价格概览
    lines.append("📊 股票池行情")
    for code, info in STOCK_POOL.items():
        if code in price_data:
            pd = price_data[code]
            role_tag = "【持仓】" if info.get("role") == "held" else "【备选】"
            sign = "+" if pd["change_pct"] > 0 else ""
            lines.append(f"  {role_tag} {info['name']}: ¥{pd['price']:.2f} ({sign}{pd['change_pct']:.1f}%)")

    lines.append("")

    if all_events:
        lines.append("⚡ 触发事件")
        for event in all_events:
            lines.append(f"  {event}")
    else:
        lines.append("✅ 无异常事件，一切平稳")

    lines.append("")
    lines.append("=" * 30)

    return "\n".join(lines)


def main():
    print(f"🔍 股票池监控启动 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    state = load_state()
    all_events = []

    # 获取股价
    all_codes = list(STOCK_POOL.keys())
    price_data = get_stock_prices(all_codes)

    for code, info in STOCK_POOL.items():
        if code in price_data:
            pd = price_data[code]
            print(f"  {info['name']}: ¥{pd['price']:.2f} ({'+' if pd['change_pct'] > 0 else ''}{pd['change_pct']:.1f}%)")
            events = check_price_events(code, info, pd, state)
            all_events.extend(events)
        else:
            print(f"  {info['name']}: 数据获取失败")

    # 新闻检测
    news_events = check_news_events()
    all_events.extend(news_events)

    # 生成报告
    report = generate_report(all_events, price_data)
    print(f"\n{'='*50}")
    print(report)

    # 保存
    report_file = REPORT_DIR / f"pool_alert_{datetime.now().strftime('%Y-%m-%d_%H%M')}.md"
    report_file.write_text(report)

    save_state(state)

    if all_events:
        print("\n--- OUTPUT_START ---")
        print(report)
        print("--- OUTPUT_END ---")


if __name__ == "__main__":
    main()
