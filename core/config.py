"""
统一配置文件
============
所有配置集中管理，支持环境变量覆盖。
支持从 YAML 文件加载配置（可选，向后兼容）。
"""

import os
from pathlib import Path

# ── YAML 配置加载 ─────────────────────────────────────────────────────────────

def load_yaml_config(filepath):
    """加载 YAML 配置文件，如果不存在返回空字典。"""
    try:
        import yaml
    except ImportError:
        return {}
    path = Path(filepath)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


PROJECT_ROOT = Path(__file__).parent.parent
_yaml_config_dir = PROJECT_ROOT / "config"

_stocks_yaml = load_yaml_config(_yaml_config_dir / "stocks.yaml")
_analysis_yaml = load_yaml_config(_yaml_config_dir / "analysis_knowledge.yaml")

# ── 项目路径 ──────────────────────────────────────────────────────────────────

DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

# ── 数据库配置 ────────────────────────────────────────────────────────────────

DATABASE = {
    "path": os.getenv("SI_DB_PATH", str(DATA_DIR / "smart_invest.db")),
    "backup_dir": str(DATA_DIR / "backups"),
}

# ── 数据源配置 ────────────────────────────────────────────────────────────────

# 默认值
_DATA_SOURCES_DEFAULT = {
    "akshare": {
        "enabled": True,
        "timeout": 30,
        "retry": 3,
    },
    "tushare": {
        "enabled": True,
        "token": os.getenv("TUSHARE_TOKEN", ""),
        "timeout": 30,
    },
    "eastmoney": {
        "enabled": True,
        "api_base": "https://datacenter-web.eastmoney.com",
        "timeout": 15,
    },
}

# YAML 覆盖：stocks.yaml 中的 data_sources 仅提供部分映射，深度合并到默认值上
_yaml_ds = _stocks_yaml.get("data_sources", {})
if _yaml_ds:
    _tushare_token_env = _yaml_ds.get("earnings", {}).get("token_env", "")
    if _tushare_token_env:
        _DATA_SOURCES_DEFAULT["tushare"]["token"] = os.getenv(_tushare_token_env, "")

DATA_SOURCES = _DATA_SOURCES_DEFAULT

# ── 监控股票列表 ──────────────────────────────────────────────────────────────

# 从 stocks.yaml 的 watchlist 自动生成 code 列表
_yaml_watchlist = _stocks_yaml.get("watchlist", [])
if _yaml_watchlist:
    WATCHLIST = [item["code"] for item in _yaml_watchlist if isinstance(item, dict) and "code" in item]
else:
    WATCHLIST = []  # YAML 不存在时为空列表（向后兼容）

# ── 分析参数配置 ──────────────────────────────────────────────────────────────

# 默认关键词
_DEFAULT_NEGATIVE_KEYWORDS = [
    "暴雷", "亏损", "退市", "调查", "处罚", "造假",
    "减持", "质押", "诉讼", "仲裁", "冻结",
]
_DEFAULT_POSITIVE_KEYWORDS = [
    "超预期", "增长", "利好", "突破", "签约", "中标",
    "回购", "增持", "分红", "业绩", "创新高",
]

# 从 analysis_knowledge.yaml 覆盖关键词
_yaml_event_kw = _analysis_yaml.get("event_keywords", {})
_negative_keywords = _yaml_event_kw.get("negative", _DEFAULT_NEGATIVE_KEYWORDS)
_positive_keywords = _yaml_event_kw.get("positive", _DEFAULT_POSITIVE_KEYWORDS)

# 从 analysis_knowledge.yaml 覆盖风险阈值
_yaml_risk = _analysis_yaml.get("risk_thresholds", {})

ANALYSIS = {
    # 超预期判定
    "beat_expectation_threshold": _yaml_risk.get("beat_expectation_threshold", 5.0),
    "new_high_lookback_quarters": 8,

    # 技术分析参数
    "ma_periods": [5, 10, 20, 60],
    "rsi_period": 6,
    "macd_fast": 12, "macd_slow": 26, "macd_signal": 9,

    # 风险预警阈值
    "max_drawdown_alert": _yaml_risk.get("max_drawdown_alert", -8.0),
    "volume_surge_ratio": _yaml_risk.get("volume_surge_ratio", 3.0),
    "consecutive_decline_days": _yaml_risk.get("consecutive_decline_days", 5),

    # 情感分析
    "sentiment_model": "simple",
    "negative_keywords": _negative_keywords,
    "positive_keywords": _positive_keywords,
}

# ── 推送配置 ──────────────────────────────────────────────────────────────────

# 默认值
_daily_report_time = "08:30"
_scan_time = "15:30"

_yaml_notif = _stocks_yaml.get("notification", {})
_yaml_feishu = _yaml_notif.get("feishu", {})
if _yaml_notif:
    _daily_report_time = _yaml_notif.get("daily_report_time", _daily_report_time)
    _scan_time = _yaml_notif.get("scan_time", _scan_time)

NOTIFICATION = {
    "feishu": {
        "enabled": True,
        "daily_report_target": os.getenv("SI_FEISHU_DAILY_TARGET", "") or _yaml_feishu.get("daily_report_target", ""),
        "alert_target": os.getenv("SI_FEISHU_ALERT_TARGET", "") or _yaml_feishu.get("alert_target", ""),
    },
    "daily_report_time": _daily_report_time,
    "market_close_report_time": _scan_time,
    "alert_cooldown_minutes": 30,
}

# ── 调度配置 ──────────────────────────────────────────────────────────────────

SCHEDULER = {
    "news_collect_interval_minutes": 15,
    "price_collect_interval_minutes": 5,
    "earnings_check_interval_hours": 6,
    "analysis_interval_minutes": 30,
}

SCANNER_CONFIG = {
    "quarterly_profit_new_high": {
        "enabled": True,
        "min_quarters": 8,
        "min_profit": 100000000,
    },
    "earnings_surprise": {
        "enabled": True,
        "min_surprise_pct": 10,
    },
    "technical_breakout": {
        "enabled": True,
        "breakout_days": 20,
    },
    "industry_rotation": {
        "enabled": True,
        "lookback_days": 20,
        "top_n": 5,
        "bottom_n": 5,
    },
    "value_stocks": {
        "enabled": True,
        "max_pe": 30,
        "min_pe": 0,
        "max_pb": 5,
        "min_roe": 10,
        "min_dividend_yield": 2,
        "min_market_cap_yi": 50,  # 亿元
        "top_n": 50,
    },
    "multi_factor_ranking": {
        "enabled": True,
        "top_n": 50,
        "industry_neutral": False,
        "weight_value": 0.25,
        "weight_growth": 0.25,
        "weight_quality": 0.25,
        "weight_momentum": 0.15,
        "weight_sentiment": 0.10,
    },
    "rating_changes": {
        "enabled": True,
        "lookback_days": 30,
        "top_n": 50,
        "max_stocks_to_scan": 200,
    },
}

# ── 日志配置 ──────────────────────────────────────────────────────────────────

LOGGING = {
    "level": os.getenv("SI_LOG_LEVEL", "INFO"),
    "file": str(LOGS_DIR / "smart_invest.log"),
    "max_size_mb": 10,
    "backup_count": 5,
}
