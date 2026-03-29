#!/bin/bash
# 投资系统 v2.2 — Crontab 配置
# 安装: bash scripts/install_crontab.sh

cat > /tmp/smart_invest_cron << 'EOF'
# ═══════════════════════════════════════════════════════════════
# 投资系统 v2.2 — 定时任务配置
# ═══════════════════════════════════════════════════════════════
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# ── 每日 Pipeline（收盘后 20:30，含 disclosures + 扫描 + 分析）──
30 20 * * 1-5 cd /root/.openclaw/workspace/smart-invest && /usr/bin/python3 scripts/run_pipeline.py --window 6h >> /root/.openclaw/workspace/smart-invest/data/logs/pipeline_cron.log 2>&1

# ── 每日收盘后数据采集（15:30，快速模式）──
35 15 * * 1-5 cd /root/.openclaw/workspace/smart-invest && /usr/bin/python3 scripts/run_pipeline.py --window 6h --max-stocks 300 >> /root/.openclaw/workspace/smart-invest/data/logs/pipeline_eod.log 2>&1

# ── 回调买入扫描（盘后 15:15）──
# name:smart-invest-pullback
15 15 * * 1-5 cd /root/.openclaw/workspace/smart-invest && /usr/bin/python3 -c "from core.analyzer import PullbackAnalyzer; pa = PullbackAnalyzer(db_path='data/smart_invest.db'); pa.scan()" >> /root/.openclaw/workspace/smart-invest/data/logs/pullback_cron.log 2>&1

# ── Web 服务器每日重启（06:00）──
0 6 * * * kill $(lsof -t -i:8080) 2>/dev/null; sleep 2; cd /root/.openclaw/workspace/smart-invest && nohup python3 web/main.py >> /root/.openclaw/workspace/smart-invest/data/logs/web.log 2>&1 &

# ── 周日数据维护 ──
# 回测更新（周日 10:00）
0 10 * * 0 cd /root/.openclaw/workspace/smart-invest && /usr/bin/python3 scripts/btiq_backfill.py >> data/logs/backtest_cron.log 2>&1

# 周报统计（周日 21:00）
0 21 * * 0 /usr/bin/python3 /root/.openclaw/workspace/skills/weekly-stats/scripts/collect_stats.py >> /root/.openclaw/workspace/logs/weekly-stats.log 2>&1

# ── 磁盘空间检查（每日 08:00）──
0 8 * * * df -h / | awk 'NR==2 && int($5)>85 {print "磁盘使用" $5 "，需清理"}' | mail -s "磁盘警告" root 2>/dev/null || true
EOF

# 安装
crontab /tmp/smart_invest_cron
rm /tmp/smart_invest_cron

echo "✅ Crontab 安装完成"
echo ""
echo "当前 crontab："
crontab -l
