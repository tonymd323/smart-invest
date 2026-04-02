#!/usr/bin/env python3
"""
A股早报推送（飞书卡片 JSON 2.0 · 真表格版）
=============================================
数据源：腾讯财经 API + SectorProvider + RSS 缓存
输出：飞书交互式卡片消息（schema 2.0 + table 组件）

从 Docker crontab 调用，工作日 07:03 执行。
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

# ───────────────────── 常量 ─────────────────────

INDEX_CODES = {
    'sh000001': '上证', 'sz399001': '深证', 'sz399006': '创业板',
    'sh000688': '科创50', 'sh000016': '上证50', 'sh000300': '沪深300',
    'sh000905': '中证500',
}
INDEX_FULL = {
    'sh000001': '上证指数', 'sz399001': '深证成指', 'sz399006': '创业板指',
    'sh000688': '科创50', 'sh000016': '上证50', 'sh000300': '沪深300',
    'sh000905': '中证500',
}
GLOBAL_ASSET_CODES = {
    'hf_GC': '💰黄金', 'hf_CL': '🛢️原油', 'hf_SI': '白银',
    'hkHSI': '🇭🇰恒生', 'usNDX': '🇺🇸纳指', 'usINX': '🇺🇸标普', 'usDJI': '🇺🇸道琼斯',
}
RSS_CACHE_PATH = os.path.expanduser('~/.openclaw/skills/rss-reader/data/articles.json')


# ───────────────────── 工具函数 ─────────────────────

def _fetch(url, timeout=10, encoding='utf-8'):
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=timeout) as resp:
            return resp.read().decode(encoding, errors='ignore')
    except Exception as e:
        print(f'  ⚠️ {url[:50]}... → {e}', file=sys.stderr)
        return ''

def _safe_float(val, default=0.0):
    try: return float(val)
    except: return default

def _fmt_pct(val):
    try:
        v = float(val)
        return f'{"+" if v > 0 else ""}{v:.2f}%'
    except: return 'N/A'

def _fmt_price(val, dec=2):
    try: return f'{float(val):,.{dec}f}'
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
    if target.startswith("oc_"): rt = "chat_id"
    elif target.startswith("ou_"): rt = "open_id"
    else: return False
    payload = {"receive_id": target, "msg_type": "interactive",
               "content": json.dumps(card, ensure_ascii=False)}
    req = urllib.request.Request(
        f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={rt}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    return json.loads(urllib.request.urlopen(req, timeout=15).read()).get("code", -1) == 0


# ───────────────────── 数据采集 ─────────────────────

def fetch_indices():
    codes = ','.join(INDEX_CODES.keys())
    text = _fetch(f'https://qt.gtimg.cn/q={codes}')
    if not text: return []
    results = []
    for line in text.strip().split(';'):
        if not line.strip() or '=' not in line: continue
        match = re.match(r'v_(\w+)="(.+)"', line)
        if not match: continue
        fields = match.group(2).split('~')
        if len(fields) < 35: continue
        code_key = match.group(1)
        name = INDEX_CODES.get(code_key, fields[1])
        name_full = INDEX_FULL.get(code_key, fields[1])
        price = _safe_float(fields[3])
        pct = _safe_float(fields[32])
        if price > 0: results.append({'name': name, 'name_full': name_full, 'price': price, 'change_pct': pct})
    return results

def fetch_sectors():
    try:
        from core.data_provider import SectorProvider
        sp = SectorProvider()
        sectors = sp.fetch()
        print(f'  板块: {len(sectors)} 个 (来源: {sp.last_source})')
        return [{'name': s.sector_name, 'change_pct': s.change_pct} for s in sectors]
    except Exception as e:
        print(f'  ⚠️ SectorProvider: {e}', file=sys.stderr)
        return []

def fetch_global_assets():
    codes = ','.join(GLOBAL_ASSET_CODES.keys())
    text = _fetch(f'https://qt.gtimg.cn/q={codes}')
    if not text: return []
    results = []
    for line in text.strip().split(';'):
        if not line.strip() or '=' not in line or 'none_match' in line: continue
        match = re.search(r'v_\w+="(.+)"', line)
        if not match: continue
        content = match.group(1)
        code_match = re.search(r'v_(\w+)=', line)
        if not code_match: continue
        code = code_match.group(1)
        if '~' in content and len(content.split('~')) > 30:
            fields = content.split('~')
            name = GLOBAL_ASSET_CODES.get(code, fields[1])
            price, pct = _safe_float(fields[3]), _safe_float(fields[32])
        else:
            fields = content.split(',')
            if len(fields) < 6: continue
            name = GLOBAL_ASSET_CODES.get(code, fields[-1])
            price, pct = _safe_float(fields[0]), _safe_float(fields[1])
        if price > 0: results.append({'name': name, 'price': price, 'change_pct': pct})
    return results

def fetch_rss_news(max_count=5):
    try:
        if not os.path.exists(RSS_CACHE_PATH): return []
        with open(RSS_CACHE_PATH, 'r', encoding='utf-8') as f: data = json.load(f)
        if not isinstance(data, dict): return []
        ALLOWED = {'36氪','虎嗅','量子位','InfoQ 中文','IT之家','第一财经','财新',
                    '界面新闻','澎湃新闻','证券时报','上海证券报','华尔街见闻'}
        articles = []
        for url, info in data.items():
            if not isinstance(info, dict): continue
            source = info.get('source', '')
            if source in {'Product Hunt','Hacker News'}: continue
            title = info.get('title', '')
            if source not in ALLOWED and title and not any('\u4e00' <= c <= '\u9fff' for c in title): continue
            articles.append({'title': title, 'source': source, 'read_at': info.get('read_at','')})
        articles.sort(key=lambda x: x.get('read_at',''), reverse=True)
        return articles[:max_count]
    except: return []


# ───────────────────── LLM 分析 ─────────────────────

def llm_analysis(indices, sectors, global_assets, news):
    api_key = os.getenv('XIAOMI_API_KEY', '')
    if not api_key: return {'summary': '', 'events': ''}

    idx_s = ', '.join(f"{i['name_full']} {_fmt_pct(i['change_pct'])}" for i in indices[:7])
    top_s = ', '.join(f"{s['name']} {_fmt_pct(s['change_pct'])}" for s in sectors[:5])
    bot_s = ', '.join(f"{s['name']} {_fmt_pct(s['change_pct'])}" for s in sectors[-5:][::-1])
    ga_s = ', '.join(f"{a['name']} {_fmt_pct(a['change_pct'])}" for a in global_assets)
    news_s = '\n'.join(f"- {n['title']}" for n in news[:5]) if news else '（暂无）'

    prompt = f"""你是A股早报分析师。根据数据输出两部分。

## 数据
指数：{idx_s}
涨幅前5：{top_s}
跌幅前5：{bot_s}
全球资产：{ga_s}
新闻：
{news_s}

## 输出格式（严格遵循）

[摘要]
一句话（25字以内）

[事件]
📌 xxx → xxx
📌 xxx → xxx
（2-3条，每条不超35字，📌开头，无明显事件则写"今日无明显事件驱动"）"""

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


# ───────────────────── 卡片构建（JSON 2.0） ─────────────────────

def build_card(indices, sectors, global_assets, news, analysis):
    today = datetime.now().strftime('%m-%d')
    weekday = ['周一','周二','周三','周四','周五','周六','周日'][datetime.now().weekday()]

    sh = next((i for i in indices if i['name'] == '上证'), None)
    if sh:
        if sh['change_pct'] > 0.3: template = 'green'
        elif sh['change_pct'] < -0.3: template = 'red'
        else: template = 'blue'
    else: template = 'blue'

    def md(text):
        return {"tag": "markdown", "content": text}

    elements = []

    # ① 摘要
    if analysis.get('summary'):
        elements.append(md(analysis['summary']))

    # ② 指数表格（两列：名称+点位 | 涨跌）
    if indices:
        elements.append({
            "tag": "table",
            "columns": [
                {"name": "idx", "display_name": "指数", "horizontal_align": "left"},
                {"name": "pct", "display_name": "涨跌", "horizontal_align": "right"},
            ],
            "rows": [
                {"idx": f"{i['name']} {i['price']:,.2f}", "pct": _fmt_pct(i['change_pct'])}
                for i in indices
            ]
        })

    # ③ 板块双栏
    if sectors:
        medals = ['🥇','🥈','🥉','4️⃣','5️⃣']
        top5 = sectors[:5]
        bot5 = sectors[-5:][::-1]
        left = "**涨 TOP5**\n" + "\n".join(f"{medals[j]}{s['name']} {_fmt_pct(s['change_pct'])}" for j, s in enumerate(top5))
        right = "**跌 TOP5**\n" + "\n".join(f"🔻{s['name']} {_fmt_pct(s['change_pct'])}" for s in bot5)
        elements.append({
            "tag": "column_set", "flex_mode": "flow",
            "columns": [
                {"tag": "column", "width": "weighted", "weight": 1, "elements": [md(left)]},
                {"tag": "column", "width": "weighted", "weight": 1, "elements": [md(right)]},
            ]
        })

    elements.append({"tag": "hr"})

    # ④ 全球资产（单行紧凑）
    if global_assets:
        ga_line = " | ".join(f"{a['name']} {_fmt_pct(a['change_pct'])}" for a in global_assets)
        elements.append(md(ga_line))

    # ⑤ 事件驱动
    if analysis.get('events'):
        elements.append({"tag": "hr"})
        elements.append(md(f"🧠 {analysis['events']}"))

    # ⑥ 热点（一行）
    if news:
        elements.append({"tag": "hr"})
        elements.append(md("📰 " + " | ".join(n['title'][:20] for n in news[:3])))

    return {
        "schema": "2.0",
        "config": {"width_mode": "compact"},
        "header": {
            "title": {"tag": "plain_text", "content": f"📊 A股早报 {today} {weekday}"},
            "template": template,
        },
        "body": {"elements": elements},
    }


# ───────────────────── 主入口 ─────────────────────

def main():
    print('📊 A股早报采集...')

    print('  [1/4] 指数行情...')
    indices = fetch_indices()
    print(f'    ✅ {len(indices)} 个指数')

    print('  [2/4] 行业板块...')
    sectors = fetch_sectors()
    print(f'    ✅ {len(sectors)} 个板块')

    print('  [3/4] 全球资产...')
    global_assets = fetch_global_assets()
    print(f'    ✅ {len(global_assets)} 项')

    print('  [4/4] 热点新闻...')
    news = fetch_rss_news(5)
    print(f'    ✅ {len(news)} 条')

    if not indices and not sectors:
        print('❌ 数据全部失败')
        sys.exit(1)

    print('  [LLM] 事件分析...')
    analysis = llm_analysis(indices, sectors, global_assets, news)

    card = build_card(indices, sectors, global_assets, news, analysis)

    # 保存
    os.makedirs('/tmp/morning_report', exist_ok=True)
    date_str = datetime.now().strftime('%Y%m%d')
    outpath = f'/tmp/morning_report/A股早报-{date_str}.json'
    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump(card, f, ensure_ascii=False, indent=2)
    print(f'  卡片已保存: {outpath}')

    # 推送
    target = os.getenv("SI_FEISHU_DAILY_TARGET", "")
    if not target:
        print("⚠️ 未配置推送目标")
        return
    token = _get_tenant_token()
    if not token:
        print("❌ 飞书 token 失败")
        sys.exit(1)
    if _send_card_v2(token, target, card):
        print(f'✅ 推送成功 → {target}')
    else:
        print('❌ 推送失败')
        sys.exit(1)


if __name__ == "__main__":
    main()
