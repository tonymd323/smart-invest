#!/usr/bin/env python3
"""鼎龙股份 300054.SZ 盘中监控（飞书卡片版）"""
import urllib.request, time, json, os
from datetime import datetime

STOCK = "sz300054"
CHECK_INTERVAL = 300  # 5分钟

LEVELS = {
    "prev_close":   48.70,
    "today_low_watch": 47.76,
    "strong_support":  46.60,
    "break_up":       50.00,
    "recent_high":    50.64,
}

FEISHU_APP_ID     = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
FEISHU_TARGET     = os.getenv("SI_FEISHU_ALERT_TARGET", "")  # open_id

# ── 飞书卡片格式（JSON 2.0 标准结构） ─────────────
def _feishu_token():
    try:
        data = json.dumps({"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}).encode()
        req = urllib.request.Request(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            data=data, headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req, timeout=10).read()).get("tenant_access_token")
    except Exception as e:
        print(f"  ⚠️ token失败: {e}"); return None

def _send_card(card):
    if not FEISHU_TARGET: return
    token = _feishu_token()
    if not token: return
    try:
        rt = "open_id" if FEISHU_TARGET.startswith("ou_") else "chat_id"
        payload = {
            "receive_id": FEISHU_TARGET,
            "msg_type": "interactive",
            "content": json.dumps(card, ensure_ascii=False)
        }
        req = urllib.request.Request(
            f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={rt}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"})
        resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
        print(f"  ✅ 卡片已发 code={resp.get('code')}" if resp.get('code')==0 else f"  ⚠️ 卡片失败: {resp.get('msg')}")
    except Exception as e:
        print(f"  ⚠️ 卡片异常: {e}")

def _build_card(price, change_pct, high, low, alert_text, now):
    trend = "📈" if change_pct > 0 else "📉" if change_pct < 0 else "➡️"
    color = "red" if change_pct < 0 else "green" if change_pct > 0 else "grey"
    return {
        "schema": "2.0",
        "config": {"width_mode": "compact"},
        "header": {
            "title": {"tag": "plain_text", "content": f"🚨 鼎龙股份 预警 {now.strftime('%H:%M')}"},
            "template": color,
        },
        "body": {
            "elements": [
                {"tag": "markdown", "content": f"**当前价**: `{price:.2f}` {trend} {change_pct:+.2f}%\n**今日高低**: 高 {high:.2f} / 低 {low:.2f}"},
                {"tag": "hr"},
                {"tag": "markdown", "content": alert_text},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"触发时间: {now.strftime('%Y-%m-%d %H:%M')} | 300054.SZ"},
            ]
        }
    }

# ── 行情 ─────────────────────────────────────
def fetch_quote():
    url = f"https://qt.gtimg.cn/q={STOCK}"
    req = urllib.request.Request(url)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=10) as resp:
        data = resp.read().decode("gbk", errors="ignore")
    parts = data.split("~")
    if len(parts) < 40: return None
    return {
        "price":      float(parts[3])  if parts[3]  else 0,
        "prev_close": float(parts[4])  if parts[4]  else 0,
        "open":       float(parts[5])  if parts[5]  else 0,
        "high":       float(parts[33]) if parts[33] else 0,
        "low":        float(parts[34]) if parts[34] else 0,
        "change_pct": float(parts[32]) if parts[32] else 0,
        "volume":     int(parts[6])   if parts[6]   else 0,
    }

def check_alerts(q, prev_q):
    alerts, price = [], q["price"]
    if price <= LEVELS["today_low_watch"] and (not prev_q or prev_q["price"] > LEVELS["today_low_watch"]):
        alerts.append(f"⚠️ **跌破今日低点 {LEVELS['today_low_watch']}！当前 {price}**")
    if price <= LEVELS["strong_support"] and (not prev_q or prev_q["price"] > LEVELS["strong_support"]):
        alerts.append(f"🚨 **跌破强支撑 {LEVELS['strong_support']}！可能继续下行**")
    if price >= LEVELS["break_up"] and (not prev_q or prev_q["price"] < LEVELS["break_up"]):
        alerts.append(f"🟢 **突破 {LEVELS['break_up']}！短线转强**")
    if price >= LEVELS["recent_high"] and (not prev_q or prev_q["price"] < LEVELS["recent_high"]):
        alerts.append(f"🚀 **突破今日最高 {LEVELS['recent_high']}！强势延续**")
    if prev_q and abs(q["change_pct"] - prev_q.get("change_pct", q["change_pct"])) > 1.5:
        alerts.append(f"📊 **快速波动**: {prev_q.get('change_pct',0):.2f}% → {q['change_pct']:.2f}%")
    return alerts

# ── 主循环 ────────────────────────────────────
def main():
    print(f"=== 鼎龙股份 监控启动 ===")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"关键价位: 支撑 {LEVELS['today_low_watch']}/{LEVELS['strong_support']} | 阻力 {LEVELS['break_up']}/{LEVELS['recent_high']}")
    print(f"检查间隔: {CHECK_INTERVAL}秒 | 飞书通知: {'✅' if FEISHU_TARGET else '❌ 未配置'}")
    print()
    prev_q, alert_count = None, 0
    while True:
        now = datetime.now()
        h, m = now.hour, now.minute
        is_trading = (h == 9 and m >= 25) or (h == 10) or (h == 11 and m <= 30) or (h == 13) or (h == 14) or (h == 15 and m <= 5)
        if not is_trading:
            if h >= 15 and m > 10:
                print(f"[{now.strftime('%H:%M')}] 收盘，监控结束"); break
            time.sleep(60); continue
        try:
            q = fetch_quote()
            if q and q["price"] > 0:
                alerts = check_alerts(q, prev_q)
                trend = "↑" if q["change_pct"] > 0 else "↓" if q["change_pct"] < 0 else "→"
                print(f"[{now.strftime('%H:%M')}] {q['price']:.2f} {trend}{q['change_pct']:+.2f}% 高:{q['high']:.2f} 低:{q['low']:.2f}")
                for a in alerts:
                    print(f"  >>> {a}")
                    alert_count += 1
                    card = _build_card(q["price"], q["change_pct"], q["high"], q["low"], a, now)
                    _send_card(card)
                prev_q = q
        except Exception as e:
            print(f"[{now.strftime('%H:%M')}] 获取行情失败: {e}")
        time.sleep(CHECK_INTERVAL)
    print(f"\n监控总结: 共触发 {alert_count} 次预警")

if __name__ == "__main__":
    main()
