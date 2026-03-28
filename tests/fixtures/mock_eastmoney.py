"""
Mock 东方财富财务数据 — Phase 0 Spike 用

数据结构参考东方财富 datacenter-web API 返回格式：
  - fina_indicator (财务指标)
  - income (利润表)

用法：在 test 中 monkeypatch FinancialProvider.fetch_* 方法返回这些数据。
"""

# ── 东方财富 fina_indicator 返回格式（单只股票示例） ──────────────────────────

EASTMONEY_FINA_INDICATOR = {
    "000858.SZ": {
        "code": "000858",
        "name": "五粮液",
        "data": [
            {
                "SECURITY_CODE": "000858",
                "REPORT_DATE_NAME": "2025-09-30",
                "PARENT_NETPROFIT": 24800000000,      # 归母净利润（元）
                "PARENT_NETPROFIT_YOY": 12.5,          # 归母净利润同比（%）
                "TOTAL_OPERATE_INCOME": 75000000000,   # 营业总收入（元）
                "TOTAL_OPERATE_INCOME_YOY": 10.2,      # 营收同比（%）
                "WEIGHTAVG_ROE": 22.3,                 # 加权 ROE（%）
                "GROSS_PROFIT_RATIO": 78.5,            # 毛利率（%）
                "EPS-basic": 6.39,                     # 基本每股收益
            },
            {
                "SECURITY_CODE": "000858",
                "REPORT_DATE_NAME": "2025-06-30",
                "PARENT_NETPROFIT": 17200000000,
                "PARENT_NETPROFIT_YOY": 11.8,
                "TOTAL_OPERATE_INCOME": 51000000000,
                "TOTAL_OPERATE_INCOME_YOY": 9.5,
                "WEIGHTAVG_ROE": 15.6,
                "GROSS_PROFIT_RATIO": 78.1,
                "EPS-basic": 4.43,
            },
            {
                "SECURITY_CODE": "000858",
                "REPORT_DATE_NAME": "2024-12-31",
                "PARENT_NETPROFIT": 31500000000,
                "PARENT_NETPROFIT_YOY": 10.1,
                "TOTAL_OPERATE_INCOME": 93000000000,
                "TOTAL_OPERATE_INCOME_YOY": 8.8,
                "WEIGHTAVG_ROE": 28.5,
                "GROSS_PROFIT_RATIO": 77.9,
                "EPS-basic": 8.12,
            },
            {
                "SECURITY_CODE": "000858",
                "REPORT_DATE_NAME": "2024-09-30",
                "PARENT_NETPROFIT": 22050000000,
                "PARENT_NETPROFIT_YOY": 9.5,
                "TOTAL_OPERATE_INCOME": 68000000000,
                "TOTAL_OPERATE_INCOME_YOY": 7.2,
                "WEIGHTAVG_ROE": 20.1,
                "GROSS_PROFIT_RATIO": 77.5,
                "EPS-basic": 5.68,
            },
            {
                "SECURITY_CODE": "000858",
                "REPORT_DATE_NAME": "2024-06-30",
                "PARENT_NETPROFIT": 15380000000,
                "PARENT_NETPROFIT_YOY": 8.2,
                "TOTAL_OPERATE_INCOME": 46580000000,
                "TOTAL_OPERATE_INCOME_YOY": 6.5,
                "WEIGHTAVG_ROE": 14.2,
                "GROSS_PROFIT_RATIO": 77.2,
                "EPS-basic": 3.97,
            },
            {
                "SECURITY_CODE": "000858",
                "REPORT_DATE_NAME": "2023-12-31",
                "PARENT_NETPROFIT": 28610000000,
                "PARENT_NETPROFIT_YOY": 7.8,
                "TOTAL_OPERATE_INCOME": 85430000000,
                "TOTAL_OPERATE_INCOME_YOY": 6.1,
                "WEIGHTAVG_ROE": 26.1,
                "GROSS_PROFIT_RATIO": 76.8,
                "EPS-basic": 7.39,
            },
            {
                "SECURITY_CODE": "000858",
                "REPORT_DATE_NAME": "2023-09-30",
                "PARENT_NETPROFIT": 20140000000,
                "PARENT_NETPROFIT_YOY": 6.5,
                "TOTAL_OPERATE_INCOME": 63400000000,
                "TOTAL_OPERATE_INCOME_YOY": 5.2,
                "WEIGHTAVG_ROE": 18.8,
                "GROSS_PROFIT_RATIO": 76.5,
                "EPS-basic": 5.20,
            },
            {
                "SECURITY_CODE": "000858",
                "REPORT_DATE_NAME": "2023-06-30",
                "PARENT_NETPROFIT": 14210000000,
                "PARENT_NETPROFIT_YOY": 5.8,
                "TOTAL_OPERATE_INCOME": 43740000000,
                "TOTAL_OPERATE_INCOME_YOY": 4.5,
                "WEIGHTAVG_ROE": 13.5,
                "GROSS_PROFIT_RATIO": 76.2,
                "EPS-basic": 3.66,
            },
        ],
    },
    # 其他股票留空（Phase 0 只用 000858 做 Spike）
    "600519.SH": {"code": "600519", "name": "贵州茅台", "data": []},
    "300750.SZ": {"code": "300750", "name": "宁德时代", "data": []},
    "000001.SZ": {"code": "000001", "name": "平安银行", "data": []},
    "600036.SH": {"code": "600036", "name": "招商银行", "data": []},
}


# ── 一致预期（东方财富 F10 格式）────────────────────────────────────────────

EASTMONEY_CONSENSUS = {
    "000858.SZ": {
        "code": "000858",
        "profit_yoy_expected": 15.0,       # 机构预期利润同比（%）
        "rev_yoy_expected": 12.0,          # 机构预期营收同比（%）
        "report_date": "2025-12-31",
        "analyst_count": 28,
        "source": "eastmoney_f10",
    },
    "600519.SH": {
        "code": "600519",
        "profit_yoy_expected": 18.0,
        "rev_yoy_expected": 15.0,
        "report_date": "2025-12-31",
        "analyst_count": 42,
        "source": "eastmoney_f10",
    },
}


# ── 日K行情（Tushare daily 格式）─────────────────────────────────────────────

TUSHARE_DAILY = {
    "000858.SZ": [
        {"trade_date": "20260319", "open": 168.50, "high": 170.20, "low": 167.80,
         "close": 169.60, "vol": 2850000, "amount": 483000000, "pct_chg": 1.2},
        {"trade_date": "20260320", "open": 169.80, "high": 172.50, "low": 169.10,
         "close": 171.80, "vol": 3120000, "amount": 535000000, "pct_chg": 1.3},
        {"trade_date": "20260321", "open": 172.00, "high": 173.80, "low": 170.50,
         "close": 170.90, "vol": 2980000, "amount": 509000000, "pct_chg": -0.5},
        {"trade_date": "20260324", "open": 170.50, "high": 172.10, "low": 169.80,
         "close": 171.50, "vol": 2750000, "amount": 472000000, "pct_chg": 0.35},
        {"trade_date": "20260325", "open": 171.80, "high": 174.20, "low": 171.20,
         "close": 173.60, "vol": 3350000, "amount": 578000000, "pct_chg": 1.22},
        {"trade_date": "20260326", "open": 173.50, "high": 175.80, "low": 172.90,
         "close": 175.20, "vol": 3580000, "amount": 623000000, "pct_chg": 0.92},
        {"trade_date": "20260327", "open": 175.00, "high": 176.50, "low": 173.80,
         "close": 174.80, "vol": 3210000, "amount": 561000000, "pct_chg": -0.23},
    ],
}


# ── 期望输出：超预期扫描结果 ──────────────────────────────────────────────────

EXPECTED_EARNINGS_BEAT = {
    "000858.SZ": {
        "stock_code": "000858.SZ",
        "stock_name": "五粮液",
        "analysis_type": "earnings_beat",
        "report_period": "2025-09-30",
        "actual_profit_yoy": 12.5,          # 实际：12.5%
        "expected_profit_yoy": 15.0,        # 预期：15%
        "is_beat": False,                    # 低于预期！
        "beat_diff_pct": -2.5,              # 低于 2.5%
        "score": 45.0,                       # 低于预期 → 低分
        "signal": "watch",
    },
}


# ── 期望输出：扣非新高扫描结果 ─────────────────────────────────────────────────

EXPECTED_NEW_HIGH = {
    "000858.SZ": {
        "stock_code": "000858.SZ",
        "stock_name": "五粮液",
        "analysis_type": "profit_new_high",
        "report_period": "2025-09-30",
        "quarterly_net_profit": 24800000000,  # 2025Q3 单季
        "prev_quarterly_high": 22050000000,   # 2024Q3
        "is_new_high": True,                   # 创新高
        "growth_pct": 12.5,
        "score": 75.0,
        "signal": "watch",
    },
}
