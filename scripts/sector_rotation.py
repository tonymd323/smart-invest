#!/usr/bin/env python3
"""
板块轮动数据采集
================
输出今日/5日/10日行业资金流向 TOP10，JSON 格式到 stdout。
供晚报 cron agent 读取并生成卡片。
"""

import json
import sys
import akshare as ak
import pandas as pd


def fetch_sector_flow(indicator: str, top_n: int = 10) -> list:
    """获取板块资金流向排行"""
    try:
        df = ak.stock_sector_fund_flow_rank(indicator=indicator)
        if df is None or df.empty:
            return []

        prefix = indicator  # "今日" or "5日" or "10日"
        name_col = "名称"
        change_col = f"{prefix}涨跌幅"
        fund_col = f"{prefix}主力净流入-净额"
        fund_pct_col = f"{prefix}主力净流入-净占比"
        lead_stock_col = f"{prefix}主力净流入最大股"

        top = df.head(top_n)
        results = []
        for _, row in top.iterrows():
            name = str(row.get(name_col, ""))
            change = _safe_float(row.get(change_col))
            fund = _safe_float(row.get(fund_col))
            fund_pct = _safe_float(row.get(fund_pct_col))
            lead = str(row.get(lead_stock_col, ""))

            results.append({
                "name": name,
                "change_pct": change,
                "fund_flow_yi": round(fund / 1e8, 2) if fund else None,  # 转亿元
                "fund_flow_pct": fund_pct,
                "lead_stock": lead,
            })
        return results
    except Exception as e:
        return [{"error": str(e)}]


def _safe_float(val) -> float:
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN
            return None
        return f
    except (ValueError, TypeError):
        return None


def fetch_bottom(indicator: str, top_n: int = 5) -> list:
    """获取板块跌幅 TOP5（按涨跌幅升序）"""
    try:
        df = ak.stock_sector_fund_flow_rank(indicator=indicator)
        if df is None or df.empty:
            return []

        prefix = indicator
        change_col = f"{prefix}涨跌幅"
        fund_col = f"{prefix}主力净流入-净额"

        # 按涨跌幅升序取底部
        df_sorted = df.sort_values(change_col, ascending=True)
        bottom = df_sorted.head(top_n)
        results = []
        for _, row in bottom.iterrows():
            name = str(row.get("名称", ""))
            change = _safe_float(row.get(change_col))
            fund = _safe_float(row.get(fund_col))
            results.append({
                "name": name,
                "change_pct": change,
                "fund_flow_yi": round(fund / 1e8, 2) if fund else None,
            })
        return results
    except Exception as e:
        return []


def main():
    output = {
        "today": fetch_sector_flow("今日"),
        "today_bottom": fetch_bottom("今日"),
        "5day": fetch_sector_flow("5日"),
        "10day": fetch_sector_flow("10日"),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
