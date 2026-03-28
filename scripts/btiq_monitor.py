#!/usr/bin/env python3
"""
超跌全市场监控 - 基于通达信涨跌比公式
数据源：腾讯行情 API（qt.gtimg.cn）
公式：BTIQ = 上涨家数 / (上涨+下跌) × 100
      判明大盘 = MA(BTIQ, 5)
      买入信号 = 判明大盘 < 30
"""

import urllib.request
import json
import time
import os
import sys
from datetime import datetime, timedelta

# 国内 API 直连，不走代理
os.environ['no_proxy'] = os.environ.get('no_proxy', '') + ',qt.gtimg.cn,*.qq.com'

# ============ 配置 ============
DATA_DIR = os.path.expanduser("~/.openclaw/workspace/smart-invest/data/oversold")
HISTORY_FILE = os.path.join(DATA_DIR, "btiq_history.json")
ALERT_THRESHOLD = 30  # 买入信号阈值
WARN_THRESHOLD = 25   # 冰点警告
HOT_THRESHOLD = 80    # 过热警告
# ==============================

os.makedirs(DATA_DIR, exist_ok=True)


def get_stock_list():
    """通过 akshare 获取标准 A 股代码列表（含名称）"""
    import akshare as ak
    # akshare 内部用 requests，绕过代理
    import requests
    original_request = requests.Session.request
    def no_proxy_request(self, *args, **kwargs):
        kwargs.setdefault('proxies', {'http': None, 'https': None})
        return original_request(self, *args, **kwargs)
    requests.Session.request = no_proxy_request
    df = ak.stock_info_a_code_name()
    requests.Session.request = original_request
    # 返回 {code: name} 字典，code 不含 sh/sz 前缀
    return dict(zip(df['code'], df['name']))


def fetch_all_stocks():
    """获取全部A股实时行情（基于 akshare 标准列表）"""
    stock_list = get_stock_list()
    print(f"akshare 标准 A 股: {len(stock_list)} 只", file=sys.stderr)

    # 构造腾讯行情代码（加 sh/sz 前缀）
    codes = []
    for code in stock_list:
        if code.startswith(('6', '9')):
            codes.append(f'sh{code}')
        else:
            codes.append(f'sz{code}')

    stocks = []
    batch_size = 800

    for start in range(0, len(codes), batch_size):
        batch = codes[start:start + batch_size]
        url = f"https://qt.gtimg.cn/q={','.join(batch)}"
        try:
            req = urllib.request.Request(url)
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(req, timeout=15) as resp:
                data = resp.read().decode('gbk', errors='ignore')
                for line in data.split(';'):
                    if '~' not in line:
                        continue
                    parts = line.split('~')
                    if len(parts) < 40:
                        continue
                    try:
                        code = parts[2]
                        name = parts[1]
                        price = float(parts[3]) if parts[3] else 0
                        change_pct = float(parts[32]) if parts[32] else 0
                        if price > 0:
                            stocks.append({
                                'code': code,
                                'name': name,
                                'price': price,
                                'change_pct': change_pct
                            })
                    except (ValueError, IndexError):
                        continue
        except Exception as e:
            print(f"Batch {start} error: {e}", file=sys.stderr)

    return stocks


def calc_btiq(stocks):
    """计算涨跌比指标"""
    up = sum(1 for s in stocks if s['change_pct'] > 0)
    down = sum(1 for s in stocks if s['change_pct'] < 0)
    flat = sum(1 for s in stocks if s['change_pct'] == 0)
    total = up + down

    if total == 0:
        return None

    btiq = up / total * 100

    return {
        'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'up': up,
        'down': down,
        'flat': flat,
        'total': len(stocks),
        'btiq': round(btiq, 2),
        'top_gainers': sorted(
            [s for s in stocks if s['change_pct'] > 0],
            key=lambda x: x['change_pct'], reverse=True
        )[:10],
        'top_losers': sorted(
            [s for s in stocks if s['change_pct'] < 0],
            key=lambda x: x['change_pct']
        )[:10]
    }


def load_history():
    """加载历史涨跌比数据"""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            return json.load(f)
    return []


def save_history(history):
    """保存历史数据"""
    # 只保留最近5天的数据
    cutoff = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    history = [h for h in history if h['time'][:10] >= cutoff]
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def calc_ma5(history):
    """计算5日均线（判明大盘）"""
    today = datetime.now().strftime('%Y-%m-%d')
    # 获取最近5个交易日的数据（每天取最后一次）
    daily_last = {}
    for h in history:
        day = h['time'][:10]
        daily_last[day] = h['btiq']

    days = sorted(daily_last.keys(), reverse=True)[:5]
    if len(days) < 2:
        return None  # 数据不足

    values = [daily_last[d] for d in days]
    return round(sum(values) / len(values), 2)


def generate_report(result, ma5):
    """生成监控报告"""
    lines = []
    lines.append(f"📊 超跌全市场监控")
    lines.append(f"⏰ {result['time']}")
    lines.append("")
    lines.append(f"📈 上涨：{result['up']}家")
    lines.append(f"📉 下跌：{result['down']}家")
    lines.append(f"➖ 平盘：{result['flat']}家")
    lines.append(f"📊 涨跌比(BTIQ)：{result['btiq']}%")

    if ma5:
        lines.append(f"📐 5日均线(MA5)：{ma5}%")

    lines.append("")

    # 信号判断
    btiq = result['btiq']
    signal = "无信号"
    if ma5 and ma5 < ALERT_THRESHOLD:
        signal = "🔴 买入信号！判明大盘<30，市场极度超跌"
    elif btiq < WARN_THRESHOLD:
        signal = "🟡 冰点警告！上涨占比<25%"
    elif btiq > HOT_THRESHOLD:
        signal = "🔴 过热警告！上涨占比>80%"
    elif ma5 and ma5 < 40:
        signal = "🟠 偏弱，关注是否继续走低"

    lines.append(f"🎯 信号：{signal}")

    # 参考线
    lines.append("")
    lines.append("📏 参考：25=冰点 | 30=买入线 | 80=过热")

    # 涨跌幅TOP5
    if result['top_gainers']:
        lines.append("")
        lines.append("🏆 涨幅前5：")
        for s in result['top_gainers'][:5]:
            lines.append(f"  {s['code']} {s['name']} +{s['change_pct']}%")

    if result['top_losers']:
        lines.append("")
        lines.append("💀 跌幅前5：")
        for s in result['top_losers'][:5]:
            lines.append(f"  {s['code']} {s['name']} {s['change_pct']}%")

    return '\n'.join(lines)


def run_once():
    """执行一次监控"""
    print("正在拉取全市场数据...", file=sys.stderr)
    stocks = fetch_all_stocks()
    print(f"获取 {len(stocks)} 只股票", file=sys.stderr)

    result = calc_btiq(stocks)
    if not result:
        print("计算失败", file=sys.stderr)
        return None

    # 保存历史
    history = load_history()
    history.append(result)
    save_history(history)

    # 计算MA5
    ma5 = calc_ma5(history)

    # 生成报告
    report = generate_report(result, ma5)
    return report


if __name__ == '__main__':
    report = run_once()
    if report:
        print(report)
