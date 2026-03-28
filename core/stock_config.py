"""
股票配置加载器
==============
从 stocks.json 单一数据源加载持仓和备选股列表。
所有 agent、所有脚本统一引用此模块。
"""

import json
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent / "config" / "stocks.json"


def load_stocks() -> dict:
    """加载完整配置"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_all_codes() -> list:
    """获取全部股票代码（持仓+备选）"""
    cfg = load_stocks()
    codes = []
    for s in cfg.get('holdings', []):
        codes.append(s['code'])
    for s in cfg.get('watchlist', []):
        codes.append(s['code'])
    return codes


def get_stock_pool() -> list:
    """获取扫描池 [{code, name}, ...]"""
    cfg = load_stocks()
    pool = []
    for s in cfg.get('holdings', []):
        pool.append({'code': s['code'], 'name': s.get('name', ''),
                     'type': 'holding', 'sector': s.get('sector', '')})
    for s in cfg.get('watchlist', []):
        pool.append({'code': s['code'], 'name': s.get('name', ''),
                     'type': 'watchlist', 'sector': s.get('sector', '')})
    return pool


def get_holdings() -> list:
    """仅获取持仓股"""
    return load_stocks().get('holdings', [])


def get_watchlist() -> list:
    """仅获取备选股"""
    return load_stocks().get('watchlist', [])


if __name__ == '__main__':
    cfg = load_stocks()
    print(f"配置版本: {cfg.get('version')}")
    print(f"更新时间: {cfg.get('updated_at')}")
    print(f"\n持仓 ({len(cfg['holdings'])}只):")
    for h in cfg['holdings']:
        print(f"  {h['code']} {h['name']} {h.get('shares',0)}股 成本¥{h.get('cost',0)}")
    print(f"\n备选 ({len(cfg['watchlist'])}只):")
    for w in cfg['watchlist']:
        print(f"  {w['code']} {w['name']} ({w.get('sector','')})")
    print(f"\n全部代码: {get_all_codes()}")
