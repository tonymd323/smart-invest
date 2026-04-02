#!/usr/bin/env python3
"""
A股晚报推送（飞书卡片 JSON 2.0 · 真表格版）
=============================================
数据源：腾讯财经 API + SectorProvider
输出：飞书交互式卡片消息（schema 2.0 + table 组件）

从 Docker crontab 调用，工作日 18:05 执行。
"""

import json
import os
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

INDEX_CODES = {
    "sh000001": "上证", "sz399001": "深证", "sz399006": "创业板",
    "sh000300": "沪深300", "sh000905": "中证500", "sh000688": "科创50",
}
INDEX_FULL = {
    "sh000001": "上证指数", "sz399001": "深证成指", "sz399006": "创业板指",
    "sh000300": "沪深300", "sh000905": "中证500", "sh000688": "科创50",
}


def _safe_float(val, default=0.0):
    try: return float(val)
    except: return default

def _fmt_pct(val):
    try:
        v = float(val)
        return f'{"+" if v > 0 else ""}{v:.2f}%'
    except: return 'N/A'

def _get_tenant_token():
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
    if not app_id or not app_secret: return None
    req = urllib.request.Request(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        data=json.dumps({"app_id": app_id, "app_secret": app_secret}).encode(),
        headers={"Content-Type": "application/json"},
    )
    return json.loads(urllib.request.urlopen(req, timeout=10).read()).get("tenant_access_token")

def _send_card_v2(token, target, card):
    rt = "chat_id" if target.startswith("oc_") else "open_id"
    payload = {"receive_id": target, "msg_type": "interactive",
               "content": json.dumps(card, ensure_ascii=False)}
    req = urllib.request.Request(
        f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={rt}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    return json.loads(urllib.request.urlopen(req, timeout=15).read()).get("code", -1) == 0


def fetch_indices():
    codes = ",".join(INDEX_CODES.keys())
    url = f"https://qt.gtimg.cn/q={codes}"
    try:
        req = urllib.request.Request(url)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=10) as resp:
            data = resp.read().decode('gbk', errors='ignore')
        results = []
        for line in data.split(';'):
            if '~' not in line: continue
            parts = line.split('~')
            if len(parts) < 40: continue
            code = parts[2]
            name = INDEX_CODES.get(code, parts[1])
            name_full = INDEX_FULL.get(code, parts[1])
            close = _safe_float(parts[3])
            pct = _safe_float(parts[32])
            if close > 0: results.append({"name": name, "name_full": name_full, "close": close, "change_pct": pct})
        return results
    except Exception as e:
        print(f"  ⚠️ 指数: {e}", file=sys.stderr)
        return []


def llm_analysis(indices, sectors_top, sectors_bottom):
    api_key = os.getenv('XIAOMI_API_KEY', '')
    if not api_key: return {'summary': '', 'events': ''}

    idx_s = ', '.join(f"{i['name_full']} {_fmt_pct(i['change_pct'])}" for i in indices[:7]) or '无数据'
    top_s = ', '.join(f"{s['name']} {_fmt_pct(s['change_pct'])}" for s in sectors_top[:5]) or '无数据'
    bot_s = ', '.join(f"{s['name']} {_fmt_pct(s['change_pct'])}" for s in sectors_bottom[:5]) or '无数据'

    prompt = f"""你是A股收盘复盘分析师。根据数据输出两部分。

## 数据
指数：{idx_s}
涨幅前5：{top_s}
跌幅前5：{bot_s}

## 输出格式（严格遵循）

[摘要]
一句话（25字以内）

[事件]
📌 xxx → xxx
📌 xxx → xxx
（2-3条，每条不超35字，📌开头，无明显事件则写"今日正常轮动"）"""

    try:
        payload = json.dumps({'model': 'mimo-v2-flash', 'messages': [{'role': 'user', 'content': prompt}], 'max_tokens': 300}).encode()
        req = urllib.request.Request('https://api.xiaomimimo.com/v1/chat/completions', data=payload,
            headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {api_key}'})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=20) as resp:
            data = json.loads(resp.read())
        content = data['choices'][0]['message']['content']
        usage = data.get('usage', {})
        print(f'  LLM: {usage.get("prompt_tokens",0)}+{usage.get("completion_tokens",0)}')

        summary, events = '', ''
        if '[事件]' in content:
            parts = content.split('[事件]')
            summary = parts[0].replace('[摘要]', '').strip()
            events = parts[1].strip()
        else: events = content.strip()
        return {'summary': summary, 'events': events}
    except Exception as e:
        print(f'  ⚠️ LLM: {e}', file=sys.stderr)
        return {'summary': '', 'events': ''}


def build_card(indices, sectors_top, sectors_bottom, analysis):
    today = datetime.now().strftime('%m-%d')
    weekday = ['周一','周二','周三','周四','周五','周六','周日'][datetime.now().weekday()]

    sh = next((i for i in indices if i['name'] == '上证'), None)
    if sh:
        if sh['change_pct'] > 0.3: template = 'green'
        elif sh['change_pct'] < -0.3: template = 'red'
        else: template = 'blue'
    else: template = 'indigo'

    def md(text):
        return {"tag": "markdown", "content": text}

    elements = []

    if analysis.get('summary'):
        elements.append(md(analysis['summary']))

    # 指数表格
    if indices:
        elements.append({
            "tag": "table",
            "columns": [
                {"name": "idx", "display_name": "指数", "horizontal_align": "left"},
                {"name": "pct", "display_name": "涨跌", "horizontal_align": "right"},
            ],
            "rows": [
                {"idx": f"{i['name']} {i['close']:,.2f}", "pct": _fmt_pct(i['change_pct'])}
                for i in sorted(indices, key=lambda x: x['name'])
            ]
        })

    # 板块双栏
    if sectors_top:
        medals = ['🥇','🥈','🥉','4️⃣','5️⃣']
        top5 = sectors_top[:5]
        bot5 = sectors_bottom[:5]
        left = "**涨 TOP5**\n" + "\n".join(f"{medals[j]}{s['name']} {_fmt_pct(s['change_pct'])}" for j, s in enumerate(top5))
        right = "**跌 TOP5**\n" + "\n".join(f"🔻{s['name']} {_fmt_pct(s['change_pct'])}" for s in bot5)
        elements.append({
            "tag": "column_set", "flex_mode": "flow",
            "columns": [
                {"tag": "column", "width": "weighted", "weight": 1, "elements": [md(left)]},
                {"tag": "column", "width": "weighted", "weight": 1, "elements": [md(right)]},
            ]
        })

    # 事件驱动
    if analysis.get('events'):
        elements.append({"tag": "hr"})
        elements.append(md(f"🧠 {analysis['events']}"))

    return {
        "schema": "2.0",
        "config": {"width_mode": "compact"},
        "header": {
            "title": {"tag": "plain_text", "content": f"🌆 A股晚报 {today} {weekday}"},
            "template": template,
        },
        "body": {"elements": elements},
    }


def main():
    print("📊 A股晚报采集...")

    print("  [1/3] 指数行情...")
    indices = fetch_indices()
    print(f"    ✅ {len(indices)} 个指数")

    print("  [2/3] 行业板块...")
    from core.data_provider import SectorProvider
    sp = SectorProvider()
    top_sectors = sp.fetch()
    bottom_sectors = top_sectors[::-1]
    sectors_top = [{"name": s.sector_name, "change_pct": s.change_pct} for s in top_sectors]
    sectors_bottom = [{"name": s.sector_name, "change_pct": s.change_pct} for s in bottom_sectors]
    print(f"    ✅ {len(sectors_top)} 个板块 (来源: {sp.last_source})")

    if not indices and not sectors_top:
        print("❌ 数据全部失败")
        sys.exit(1)

    print("  [3/3] LLM 分析...")
    analysis = llm_analysis(indices, sectors_top, sectors_bottom)

    card = build_card(indices, sectors_top, sectors_bottom, analysis)

    # 保存
    os.makedirs('/tmp/morning_report', exist_ok=True)
    outpath = f'/tmp/morning_report/A股晚报-{datetime.now().strftime("%Y%m%d")}.json'
    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump(card, f, ensure_ascii=False, indent=2)
    print(f"  卡片已保存: {outpath}")

    # 推送
    target = os.getenv("SI_FEISHU_DAILY_TARGET", "")
    if not target:
        print("❌ 未配置推送目标")
        sys.exit(1)
    token = _get_tenant_token()
    if not token: sys.exit(1)
    if _send_card_v2(token, target, card):
        print(f"✅ 推送成功 → 指数{len(indices)} 板块{len(sectors_top)}")
    else:
        print("❌ 推送失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
