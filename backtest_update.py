#!/usr/bin/env python3
"""
智能投资系统 v1.5 — 回测更新入口
=================================
任务D：计算入池后 5/10/20/60 日收益 vs 沪深300
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from core.database import init_db
from backtest.engine import run_pending_backtests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
)
logger = logging.getLogger('backtest_update')


def main():
    init_db()
    logger.info("="*60)
    logger.info("📈 智能投资系统 v1.5 — 回测更新")
    logger.info("="*60)

    count = run_pending_backtests()

    if count > 0:
        logger.info(f"✅ 回测更新完成: {count} 条记录")
    else:
        logger.info("📭 无待回测记录")


if __name__ == '__main__':
    main()
