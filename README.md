# Smart Invest - 备选股池每日扫描系统

## 功能

每日自动扫描全 A 股，筛选两类股票加入备选池：

1. **业绩超预期**：实际净利润/营收 YoY vs 券商一致预期
2. **扣非净利润历史新高**：单季度扣非净利润创历史新高

扫描完成后自动推送飞书消息。

## 定时任务

```
0 21 * * 1-5  python3 daily_scan.py   # 每周一至周五 21:00
```

## 项目结构

```
smart-invest/
├── daily_scan.py      # 主扫描脚本
├── core/
│   ├── config.py      # 配置
│   └── database.py    # SQLite 数据库
├── notifiers/
│   ├── feishu_pusher.py   # 飞书推送
│   ├── card_generator.py  # 消息卡片
│   └── __init__.py
├── data/
│   ├── smart_invest.db    # 数据库
│   └── logs/              # 运行日志
└── requirements.txt
```

## 数据源

- **Tushare**：A 股列表、财务指标、业绩预告
- **AkShare**：东方财富增速数据

## 旧系统

历史版本（pipeline 全市场扫描、事件驱动采集等）已归档至 `archive/20260322/smart-invest-old/`。
