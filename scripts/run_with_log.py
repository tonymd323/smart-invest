#!/usr/bin/env python3
"""
日志包装器 — 为任意脚本添加系统日志记录

用法：
    python3 scripts/run_with_log.py <module_name> <description> -- <command...>
    
示例：
    python3 scripts/run_with_log.py "超跌监控" "BTIQ全市场扫描" -- python3 scripts/btiq_monitor.py
"""

import os
import sys
import time
import subprocess

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from core.system_logger import SystemLogger

DB_PATH = os.path.join(PROJECT_ROOT, "data", "smart_invest.db")


def main():
    if "--" not in sys.argv:
        print("用法: run_with_log.py <module> <description> -- <command...>")
        sys.exit(1)

    split_idx = sys.argv.index("--")
    meta = sys.argv[1:split_idx]
    cmd = sys.argv[split_idx + 1:]

    if len(meta) < 2:
        module = meta[0] if meta else "unknown"
        description = " ".join(cmd[:3])
    else:
        module = meta[0]
        description = meta[1]

    logger = SystemLogger(db_path=str(DB_PATH))
    start = time.time()

    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=False,
            text=True,
        )
        duration_ms = int((time.time() - start) * 1000)

        if result.returncode == 0:
            logger.success(module, description, duration_ms=duration_ms)
        else:
            logger.error(module, description, error=f"退出码 {result.returncode}", duration_ms=duration_ms)

        sys.exit(result.returncode)

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        logger.error(module, description, error=str(e), duration_ms=duration_ms)
        sys.exit(1)


if __name__ == "__main__":
    main()
