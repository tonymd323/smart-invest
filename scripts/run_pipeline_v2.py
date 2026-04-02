#!/usr/bin/env python3
"""
投资系统 v2.3 — 智能调度编排器 (T3: 脚本联动机制)

功能：
  1. 依赖检查 — 运行前验证必要数据是否存在
  2. 缺失自愈 — 发现缺数据自动触发上游脚本补数
  3. 结果校验 — 运行后验证输出完整性，输出健康报告
  4. 飞书通知 — 异常时推送告警到飞书群

用法（替换原有 cron，直接替换掉原来的 15:35 pipeline）：
  15 15 * * 1-5 cd /app && /usr/local/bin/python3 scripts/run_pipeline_v2.py >> data/logs/pipeline_v2.log 2>&1

也支持手动触发：
  python3 scripts/run_pipeline_v2.py --steps all
  python3 scripts/run_pipeline_v2.py --steps prices,scorer,signals
"""
import os
os.environ['TZ'] = 'Asia/Shanghai'

import sys
import json
import time
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import timedelta
import json as _json

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
DB_PATH = PROJECT_ROOT / "data" / "smart_invest.db"

# ─────────────────────────────────────────────────────────────────────────────
# 1. 依赖检查函数
# ─────────────────────────────────────────────────────────────────────────────

def check_prices_coverage(conn: sqlite3.Connection) -> Dict:
    """prices 表覆盖率：有多少只股票有近期价格数据"""
    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - __import__('datetime').timedelta(days=1)).strftime('%Y%m%d')
    
    row = conn.execute("""
        SELECT 
            COUNT(DISTINCT stock_code) as total,
            SUM(CASE WHEN close_price > 0 THEN 1 ELSE 0 END) as has_price,
            SUM(CASE WHEN trade_date >= ? THEN 1 ELSE 0 END) as has_recent
        FROM prices
    """, (yesterday,)).fetchone()
    
    total = row[0] or 0
    has_price = row[1] or 0
    has_recent = row[2] or 0
    
    return {
        "total_stocks_in_prices": total,
        "has_price": has_price,
        "has_recent": has_recent,
        "coverage_pct": _pct(has_price, total) if total else 0,
        "recent_coverage_pct": _pct(has_recent, total) if total else 0,
        "status": "ok" if has_recent >= 3000 else "critical" if has_recent < 1000 else "warning",
    }


def check_pe_pb_coverage(conn: sqlite3.Connection) -> Dict:
    """PE/PB 覆盖率检查"""
    total = conn.execute("SELECT COUNT(*) FROM stock_scores").fetchone()[0]
    has_pe = conn.execute("SELECT COUNT(*) FROM stock_scores WHERE pe_ttm > 0").fetchone()[0]
    has_pb = conn.execute("SELECT COUNT(*) FROM stock_scores WHERE pb > 0").fetchone()[0]
    
    return {
        "total": total,
        "has_pe": has_pe,
        "has_pb": has_pb,
        "pe_pct": _pct(has_pe, total),
        "pb_pct": _pct(has_pb, total),
        "pe_status": "ok" if has_pe / total >= 0.8 else "critical" if has_pe / total < 0.5 else "warning",
        "pb_status": "ok" if has_pb / total >= 0.8 else "critical" if has_pb / total < 0.5 else "warning",
    }


def check_earnings_coverage(conn: sqlite3.Connection) -> Dict:
    """earnings 表覆盖率"""
    total = conn.execute("SELECT COUNT(*) FROM stock_scores").fetchone()[0]
    has_earnings = conn.execute("""
        SELECT COUNT(DISTINCT e.stock_code)
        FROM earnings e
        INNER JOIN stock_scores s ON e.stock_code = s.stock_code
    """).fetchone()[0]
    
    return {
        "total": total,
        "has_earnings": has_earnings,
        "coverage_pct": _pct(has_earnings, total),
        "status": "ok" if has_earnings / total >= 0.7 else "critical" if has_earnings / total < 0.4 else "warning",
    }


def check_discovery_pool(conn: sqlite3.Connection) -> Dict:
    """发现池状态"""
    active = conn.execute("SELECT COUNT(*) FROM discovery_pool WHERE status='active'").fetchone()[0]
    return {
        "active": active,
        "status": "ok" if active >= 50 else "warning" if active >= 20 else "critical",
    }


def _pct(n: int, d: int) -> int:
    if d == 0:
        return 0
    return round(n / d * 100)


def run_dependency_check(conn: sqlite3.Connection) -> Dict:
    """运行所有依赖检查，返回报告"""
    prices = check_prices_coverage(conn)
    pe_pb = check_pe_pb_coverage(conn)
    earnings = check_earnings_coverage(conn)
    pool = check_discovery_pool(conn)
    
    # 综合评分
    score = 0
    score += 20 if prices["recent_coverage_pct"] >= 90 else 10 if prices["recent_coverage_pct"] >= 70 else 0
    score += 25 if pe_pb["pe_pct"] >= 80 else 15 if pe_pb["pe_pct"] >= 60 else 0
    score += 25 if pe_pb["pb_pct"] >= 80 else 15 if pe_pb["pb_pct"] >= 60 else 0
    score += 15 if earnings["coverage_pct"] >= 70 else 8 if earnings["coverage_pct"] >= 50 else 0
    score += 15 if pool["active"] >= 50 else 8 if pool["active"] >= 20 else 0
    
    return {
        "prices": prices,
        "pe_pb": pe_pb,
        "earnings": earnings,
        "pool": pool,
        "overall_score": score,
        "overall_grade": "A" if score >= 80 else "B" if score >= 60 else "C" if score >= 40 else "D",
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. 缺失自愈 — 自动补数
# ─────────────────────────────────────────────────────────────────────────────

def auto_fill_prices() -> Dict:
    """自动补全市场价格数据：腾讯行情批量拉取"""
    import urllib.request
    
    conn = sqlite3.connect(str(DB_PATH))
    try:
        # 找出没有近期价格的股票
        yesterday = (datetime.now() - __import__('datetime').timedelta(days=1)).strftime('%Y%m%d')
        codes = [r[0] for r in conn.execute("""
            SELECT DISTINCT stock_code FROM prices
            WHERE trade_date < ? OR trade_date IS NULL
        """, (yesterday,)).fetchall()]
        
        if len(codes) < 10:
            return {"action": "skip", "reason": "价格数据已够新", "filled": 0}
        
        # 限制每次最多补 500 只
        codes = codes[:500]
        print(f"   自动补 prices: {len(codes)} 只")
        
        def to_tx_code(c):
            c = c.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')
            return f"sh{c}" if c.startswith(('6', '9')) else f"sz{c}"
        
        today_str = datetime.now().strftime('%Y%m%d')
        tx_data = {}
        batch_size = 800
        
        for start in range(0, len(codes), batch_size):
            batch = codes[start:start + batch_size]
            url = f"https://qt.gtimg.cn/q={','.join(to_tx_code(c) for c in batch)}"
            try:
                req = urllib.request.Request(url)
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(req, timeout=15) as resp:
                    raw = resp.read().decode('gbk', errors='ignore')
                for line in raw.split(';'):
                    if '~' not in line or len(line.split('~')) < 40:
                        continue
                    parts = line.split('~')
                    try:
                        code_raw = parts[2]
                        sc = f"{code_raw}.SH" if code_raw.startswith(('6', '9')) else f"{code_raw}.SZ"
                        close = float(parts[3]) if parts[3] else 0
                        if close > 0:
                            tx_data[sc] = {
                                'close': close,
                                'open': float(parts[5]) if parts[5] else close,
                                'high': float(parts[33]) if parts[33] else close,
                                'low': float(parts[34]) if parts[34] else close,
                                'change_pct': float(parts[32]) if parts[32] else 0,
                                'volume': float(parts[37]) if parts[37] else 0,
                                'turnover': round(float(parts[38]) if parts[38] else 0 * 1e8, 2),
                            }
                    except (ValueError, IndexError):
                        continue
            except Exception as e:
                print(f"   ⚠️ 腾讯批量补价格失败: {e}")
        
        filled = 0
        for code in codes:
            if code not in tx_data:
                continue
            d = tx_data[code]
            conn.execute("""
                INSERT OR REPLACE INTO prices
                (stock_code, trade_date, open_price, high_price, low_price, close_price,
                 volume, turnover, change_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (code, today_str, d['open'], d['high'], d['low'], d['close'],
                  d['volume'], d['turnover'], d['change_pct']))
            filled += 1
        conn.commit()
        return {"action": "filled", "stocks": filled}
    finally:
        conn.close()


def auto_fill_pe_pb() -> Dict:
    """自动补全 PE/PB：通过 subprocess 调用 fill_pe_pb.py"""
    import subprocess
    
    tushare_token = os.getenv("TUSHARE_TOKEN")
    if not tushare_token:
        return {"action": "skip", "reason": "TUSHARE_TOKEN 未设置"}
    
    try:
        print("   自动补 PE/PB（subprocess）...")
        result = subprocess.run(
            ["python3", "scripts/fill_pe_pb.py"],
            cwd=str(PROJECT_ROOT),
            capture_output=True, text=True, timeout=120,
            env={**os.environ, "TUSHARE_TOKEN": tushare_token, "DB_PATH": str(PROJECT_ROOT / "data" / "smart_invest.db")}
        )
        
        # 解析输出获取覆盖情况
        pe_before = pe_after = pb_before = pb_after = None
        for line in result.stdout.splitlines():
            if "PE覆盖:" in line or "PE:" in line:
                parts = line.replace("PE覆盖:", "").replace("PE:", "").split("→")
                if len(parts) == 2:
                    pe_before = int(parts[0].split()[-1].strip())
                    pe_after = int(parts[1].split("(")[0].strip())
            elif "PB覆盖:" in line or "PB:" in line:
                parts = line.replace("PB覆盖:", "").replace("PB:", "").split("→")
                if len(parts) == 2:
                    pb_before = int(parts[0].split()[-1].strip())
                    pb_after = int(parts[1].split("(")[0].strip())
        
        if pe_after and pb_after:
            return {
                "action": "filled",
                "pe_pct_after": pe_after,
                "pb_pct_after": pb_after,
            }
        else:
            return {"action": "error", "error": result.stdout[-200:] if result.stdout else result.stderr[-200:]}
    except Exception as e:
        print(f"   ⚠️ fill_pe_pb 失败: {e}")
        return {"action": "error", "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# 辅助函数
# ─────────────────────────────────────────────────────────────────────────────

def write_to_discovery_pool(conn: sqlite3.Connection, result: Dict, source: str) -> bool:
    """将结果写入 discovery_pool（INSERT OR REPLACE）"""
    try:
        code = result.get("stock_code") or result.get("code")
        if not code:
            return False
        
        # 获取股票名称
        name_row = conn.execute(
            "SELECT name FROM stocks WHERE code = ?", (code,)
        ).fetchone()
        stock_name = name_row[0] if name_row else code
        
        # 获取行业
        ind_row = conn.execute(
            "SELECT industry FROM stocks WHERE code = ?", (code,)
        ).fetchone()
        industry = ind_row[0] if ind_row else None
        
        # 信号
        sig = result.get("signal", "watch")
        score = result.get("composite_score") or result.get("score", 0) or 0
        
        # 过期时间：7天后
        expires_at = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        
        # detail JSON（去掉大字段保持简洁）
        detail_keys = ["stock_code", "signal", "score", "report_period", "industry",
                       "beat_diff_pct", "actual_profit_yoy", "growth_pct", "reason",
                       "composite_score", "composite_grade", "grade", "reason_detail"]
        detail = {k: result[k] for k in detail_keys if k in result and result[k] is not None}
        
        conn.execute("""
            INSERT OR REPLACE INTO discovery_pool
            (stock_code, stock_name, industry, source, score, signal, detail, status, discovered_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'active', datetime('now', 'localtime'), ?)
        """, (code, stock_name, industry, source, score, sig,
              _json.dumps(detail, ensure_ascii=False), expires_at))
        return True
    except Exception as e:
        print(f"   ⚠️ 入池失败 {code}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 3. 各 Pipeline 步骤
# ─────────────────────────────────────────────────────────────────────────────

def step_update_prices(conn: sqlite3.Connection, dry: bool = False) -> Dict:
    """Step 0: 批量更新跟踪股行情"""
    from core.database import init_db
    init_db(str(DB_PATH))
    
    today_str = datetime.now().strftime('%Y%m%d')
    track_codes = set()
    
    # 收集跟踪股
    for r in conn.execute("SELECT DISTINCT stock_code FROM discovery_pool WHERE status='active'"):
        track_codes.add(r[0])
    for r in conn.execute("SELECT DISTINCT stock_code FROM event_tracking WHERE tracking_status IN ('tracking','active')"):
        track_codes.add(r[0])
    
    config_path = PROJECT_ROOT / "config" / "stocks.json"
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)
        for s in config.get('stocks', []):
            if s.get('holding') or s.get('tracking'):
                track_codes.add(s['code'])
    
    track_codes = sorted(track_codes)
    
    has_history = []
    no_history = []
    for code in track_codes:
        row = conn.execute("SELECT MAX(trade_date) FROM prices WHERE stock_code=?", (code,)).fetchone()
        if not row[0]:
            no_history.append(code)
        elif row[0] < today_str:
            has_history.append(code)
    
    print(f"   需更新: {len(has_history)} | 需补历史: {len(no_history)}")
    
    if dry:
        return {"action": "skip_dry", "has_history": len(has_history), "no_history": len(no_history)}
    
    import urllib.request
    
    def to_tx_code(c):
        c = c.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')
        return f"sh{c}" if c.startswith(('6', '9')) else f"sz{c}"
    
    updated = 0
    if has_history:
        tx_data = {}
        batch_size = 800
        for start in range(0, len(has_history), batch_size):
            batch = has_history[start:start + batch_size]
            url = f"https://qt.gtimg.cn/q={','.join(to_tx_code(c) for c in batch)}"
            try:
                req = urllib.request.Request(url)
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(req, timeout=15) as resp:
                    raw = resp.read().decode('gbk', errors='ignore')
                for line in raw.split(';'):
                    if '~' not in line or len(line.split('~')) < 40:
                        continue
                    parts = line.split('~')
                    try:
                        code_raw = parts[2]
                        sc = f"{code_raw}.SH" if code_raw.startswith(('6', '9')) else f"{code_raw}.SZ"
                        close = float(parts[3]) if parts[3] else 0
                        if close > 0:
                            tx_data[sc] = {
                                'close': close,
                                'open': float(parts[5]) if parts[5] else close,
                                'high': float(parts[33]) if parts[33] else close,
                                'low': float(parts[34]) if parts[34] else close,
                                'change_pct': float(parts[32]) if parts[32] else 0,
                                'volume': float(parts[37]) if parts[37] else 0,
                                'turnover': round(float(parts[38]) if parts[38] else 0 * 1e8, 2),
                            }
                    except (ValueError, IndexError):
                        continue
            except Exception as e:
                print(f"   ⚠️ 腾讯批次失败: {e}")
        
        for code in has_history:
            if code not in tx_data:
                continue
            d = tx_data[code]
            conn.execute("""
                INSERT OR REPLACE INTO prices
                (stock_code, trade_date, open_price, high_price, low_price, close_price,
                 volume, turnover, change_pct)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (code, today_str, d['open'], d['high'], d['low'], d['close'],
                  d['volume'], d['turnover'], d['change_pct']))
            updated += 1
        conn.commit()
    
    # KlineProvider 补历史
    if no_history:
        try:
            from core.data_provider import KlineProvider
            kline_provider = KlineProvider()
            ok = 0
            for code in no_history:
                try:
                    klines = kline_provider.fetch(code, limit=120)
                    if klines:
                        for k in klines:
                            conn.execute("""
                                INSERT OR REPLACE INTO prices
                                (stock_code, trade_date, open_price, high_price, low_price, close_price,
                                 volume, turnover, change_pct)
                                VALUES (?,?,?,?,?,?,?,?,?)
                            """, (code, k.trade_date, k.open_price, k.high_price, k.low_price,
                                  k.close_price, k.volume, k.amount, k.change_pct))
                        conn.commit()
                        ok += 1
                except Exception:
                    pass
            print(f"   KlineProvider 补历史: {ok}/{len(no_history)}")
        except Exception as e:
            print(f"   ⚠️ KlineProvider 失败: {e}")
    
    return {"action": "done", "updated": updated, "kline_filled": len(no_history)}


def step_scorer(conn: sqlite3.Connection, dry: bool = False) -> Dict:
    """Step 1: 五维度评分"""
    if dry:
        pe_pb = check_pe_pb_coverage(conn)
        return {"action": "dry", "pe_pct": pe_pb["pe_pct"], "will_run": pe_pb["pe_pct"] >= 50}
    
    # 前置检查：PE覆盖率 < 50% → 先补 PE/PB
    pe_pb = check_pe_pb_coverage(conn)
    auto_filled_pe = False
    if pe_pb["pe_pct"] < 50:
        print("   ⚠️ PE覆盖率不足，自动补数...")
        auto_fill_pe_pb()
        auto_filled_pe = True
        # 重新检查
        pe_pb = check_pe_pb_coverage(conn)
    
    try:
        from core.scorer import CompositeScorer
        scorer = CompositeScorer(str(DB_PATH))
        results = scorer.evaluate_batch()
        # _sync_discovery_pool() 在 evaluate_batch() 内部已调用
        return {
            "action": "done",
            "scored": len(results),
            "auto_filled_pe": auto_filled_pe,
            "pe_pct_after": pe_pb["pe_pct"],
            "pb_pct_after": pe_pb["pb_pct"],
        }
    except Exception as e:
        print(f"   ⚠️ Scorer 失败: {e}")
        return {"action": "error", "error": str(e)}


def step_enhanced_scan(conn: sqlite3.Connection, dry: bool = False) -> Dict:
    """Step 2: 增强版超预期扫描"""
    if dry:
        return {"action": "dry"}
    
    try:
        from core.scorer import enhanced_beat_scan
        results = enhanced_beat_scan(str(DB_PATH), min_score=55.0)
        
        # 写发现池
        added = 0
        for r in results:
            sig = r.get("signal", "watch")
            if sig in ("buy", "watch"):
                if write_to_discovery_pool(conn, r, "enhanced_beat"):
                    added += 1
        conn.commit()
        
        return {"action": "done", "qualified": len(results), "added": added}
    except Exception as e:
        print(f"   ⚠️ Enhanced scan 失败: {e}")
        return {"action": "error", "error": str(e)}


def step_pullback_scan(conn: sqlite3.Connection, dry: bool = False) -> Dict:
    """Step 3: 回调买入扫描"""
    if dry:
        return {"action": "dry"}
    
    try:
        from core.analyzer import PullbackAnalyzer
        analyzer = PullbackAnalyzer(db_path=str(DB_PATH))
        results = analyzer.scan()
        
        # 写发现池（信号为 buy 或 watch）
        added = 0
        for r in results:
            sig = r.get("signal", "watch")
            if sig in ("buy", "watch"):
                if write_to_discovery_pool(conn, r, "pullback_score"):
                    added += 1
        conn.commit()
        
        buy_count = sum(1 for x in results if x.get("signal") == "buy")
        watch_count = sum(1 for x in results if x.get("signal") == "watch")
        
        return {
            "action": "done",
            "total": len(results),
            "buy": buy_count,
            "watch": watch_count,
            "added": added,
        }
    except Exception as e:
        print(f"   ⚠️ Pullback scan 失败: {e}")
        return {"action": "error", "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# 4. 结果校验
# ─────────────────────────────────────────────────────────────────────────────

def validate_output(conn: sqlite3.Connection) -> Dict:
    """运行后验证输出完整性"""
    before = run_dependency_check(conn)
    
    alerts = []
    if before["prices"]["recent_coverage_pct"] < 70:
        alerts.append(f"⚠️ prices 近期覆盖率仅 {before['prices']['recent_coverage_pct']}%（{before['prices']['has_recent']}只）")
    if before["pe_pb"]["pe_pct"] < 60:
        alerts.append(f"⚠️ PE覆盖率 {before['pe_pb']['pe_pct']}%，V维度失真")
    if before["pe_pb"]["pb_pct"] < 60:
        alerts.append(f"⚠️ PB覆盖率 {before['pe_pb']['pb_pct']}%，V维度失真")
    if before["earnings"]["coverage_pct"] < 50:
        alerts.append(f"⚠️ earnings覆盖率 {before['earnings']['coverage_pct']}%")
    if before["pool"]["active"] < 20:
        alerts.append(f"⚠️ 发现池仅 {before['pool']['active']} 只，信号源不足")
    
    return {
        "overall_score": before["overall_score"],
        "overall_grade": before["overall_grade"],
        "alerts": alerts,
        "prices": before["prices"],
        "pe_pb": before["pe_pb"],
        "earnings": before["earnings"],
        "pool": before["pool"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5. 飞书通知
# ─────────────────────────────────────────────────────────────────────────────

def send_feishu_alert(report: Dict, steps_results: Dict):
    """发送飞书告警（仅异常时）"""
    feishu_app_id = os.getenv("FEISHU_APP_ID")
    feishu_app_secret = os.getenv("FEISHU_APP_SECRET")
    target = os.getenv("SI_FEISHU_DAILY_TARGET")
    
    if not feishu_app_id or not feishu_app_secret:
        return
    
    try:
        # 获取 tenant_access_token
        import urllib.request
        import urllib.parse
        
        token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        data = json.dumps({"app_id": feishu_app_id, "app_secret": feishu_app_secret}).encode()
        req = urllib.request.Request(token_url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            token_data = json.load(resp)
        token = token_data.get("tenant_access_token")
        if not token:
            return
        
        # 构造消息
        grade = report["overall_grade"]
        score = report["overall_score"]
        alerts = report.get("alerts", [])
        
        if not alerts and grade in ("A", "B"):
            # 一切正常，不发消息
            return
        
        msg = f"🐵 **APEX 数据链路健康报告**\n"
        msg += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        msg += f"综合评分: **{score}分 ({grade}级)**\n\n"
        
        if alerts:
            msg += "**⚠️ 异常项:**\n" + "\n".join(f"• {a}" for a in alerts) + "\n\n"
        
        msg += "**数据状态:**\n"
        msg += f"• prices: {report['prices']['has_recent']}只近期覆盖 ({report['prices']['recent_coverage_pct']}%)\n"
        msg += f"• PE覆盖率: {report['pe_pb']['pe_pct']}%\n"
        msg += f"• PB覆盖率: {report['pe_pb']['pb_pct']}%\n"
        msg += f"• 发现池: {report['pool']['active']}只\n\n"
        
        msg += "**执行结果:**\n"
        for step, res in steps_results.items():
            if res.get("action") == "done":
                msg += f"• {step}: ✅\n"
            elif res.get("action") == "error":
                msg += f"• {step}: ❌ {res.get('error', 'unknown')}\n"
            elif res.get("action") == "skip":
                msg += f"• {step}: ⏭️ {res.get('reason', '')}\n"
        
        # 发送到群
        send_url = "https://open.feishu.cn/open-apis/im/v1/messages"
        payload = {
            "receive_id": target,
            "msg_type": "text",
            "content": json.dumps({"text": msg})
        }
        
        if target.startswith("oc_"):
            payload["receive_id_type"] = "chat_id"
        elif target.startswith("ou_"):
            payload["receive_id_type"] = "open_id"
        
        data = json.dumps(payload).encode()
        req = urllib.request.Request(send_url, data=data,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            pass
        
        print(f"   📲 飞书通知已发送")
    except Exception as e:
        print(f"   ⚠️ 飞书通知失败: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 6. 主编排逻辑
# ─────────────────────────────────────────────────────────────────────────────

def run(args):
    print(f"🐵 APEX 智能调度编排器 v2.3")
    print(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (Asia/Shanghai)")
    print(f"   步骤: {args.steps}")
    print()
    
    t_start = time.time()
    conn = sqlite3.connect(str(DB_PATH))
    
    # 前置检查
    print("📋 前置依赖检查...")
    dep_report = run_dependency_check(conn)
    print(f"   综合评分: {dep_report['overall_score']}分 ({dep_report['overall_grade']}级)")
    print(f"   prices近期覆盖: {dep_report['prices']['has_recent']}只 ({dep_report['prices']['recent_coverage_pct']}%)")
    print(f"   PE覆盖率: {dep_report['pe_pb']['pe_pct']}% | PB覆盖率: {dep_report['pe_pb']['pb_pct']}%")
    print(f"   earnings覆盖: {dep_report['earnings']['coverage_pct']}% | 发现池: {dep_report['pool']['active']}只")
    print()
    
    # 自动补数决策
    if dep_report["prices"]["recent_coverage_pct"] < 70 and "prices" in args.steps:
        print("⚠️ prices 覆盖不足，自动补数...")
        fill_result = auto_fill_prices()
        print(f"   补数结果: {fill_result}")
        print()
    
    if dep_report["pe_pb"]["pe_pct"] < 50 and "scorer" in args.steps:
        print("⚠️ PE覆盖率不足，自动补数...")
        fill_result = auto_fill_pe_pb()
        print(f"   补数结果: {fill_result}")
        print()
    
    # 执行各步骤
    steps_results = {}
    all_steps = ["prices", "scorer", "enhanced", "pullback"]
    
    for step in all_steps:
        if step not in args.steps:
            continue
        
        print(f"▶️  执行 {step}...")
        t_step = time.time()
        
        if step == "prices":
            result = step_update_prices(conn, dry=args.dry)
        elif step == "scorer":
            result = step_scorer(conn, dry=args.dry)
        elif step == "enhanced":
            result = step_enhanced_scan(conn, dry=args.dry)
        elif step == "pullback":
            result = step_pullback_scan(conn, dry=args.dry)
        
        elapsed = time.time() - t_step
        print(f"   完成: {result.get('action')} ({elapsed:.1f}s)")
        for k, v in result.items():
            if k != "action":
                print(f"     {k}={v}")
        print()
        steps_results[step] = result
    
    # 结果校验
    print("📊 结果校验...")
    validation = validate_output(conn)
    print(f"   综合评分: {validation['overall_score']}分 ({validation['overall_grade']}级)")
    for alert in validation.get("alerts", []):
        print(f"   {alert}")
    print()
    
    conn.close()
    
    # 飞书通知
    if not args.dry:
        send_feishu_alert(validation, steps_results)
    
    total_elapsed = time.time() - t_start
    print(f"✅ 编排完成，总耗时 {total_elapsed:.1f}s")
    
    return {
        "overall_score": validation["overall_score"],
        "overall_grade": validation["overall_grade"],
        "steps": steps_results,
        "alerts": validation.get("alerts", []),
        "elapsed_seconds": round(total_elapsed, 1),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="APEX 智能调度编排器")
    parser.add_argument("--steps", default="all",
        help="执行哪些步骤，逗号分隔: prices,scorer,enhanced,pullback 或 'all'")
    parser.add_argument("--dry", action="store_true",
        help="dry run：只检查不执行")
    args = parser.parse_args()
    
    if args.steps == "all":
        args.steps = ["prices", "scorer", "enhanced", "pullback"]
    else:
        args.steps = [s.strip() for s in args.steps.split(",")]
    
    result = run(args)
    sys.exit(0)
