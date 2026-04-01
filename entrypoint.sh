#!/bin/bash
set -e

# 加载 crontab
CRONTAB_FILE="/app/data/crontab.txt"
if [ -f "$CRONTAB_FILE" ]; then
    crontab "$CRONTAB_FILE"
    echo "✅ Crontab loaded from $CRONTAB_FILE"
    crontab -l
else
    echo "⚠️ No crontab file found at $CRONTAB_FILE"
fi

# 启动 cron daemon
cron
echo "✅ Cron daemon started"

# Cron watchdog — 每60秒检查一次，掉了自动重启
(
  while true; do
    sleep 60
    if ! pidof cron > /dev/null 2>&1; then
      echo "[$(date)] ⚠️ Cron died, restarting..."
      cron
      echo "[$(date)] ✅ Cron restarted"
    fi
  done
) &
echo "✅ Cron watchdog started"

# 启动 web 服务
exec python3 web/main.py
