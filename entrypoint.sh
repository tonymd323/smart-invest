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

# 启动 web 服务
exec python3 web/main.py
