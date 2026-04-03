# Smart Invest（归档）

> ⚠️ **状态**：已废弃。2026-04-03 起，功能已整合迁移至 [APEX 天衍](https://github.com/tonymd323/apex-tianyan)。

## 废弃说明

2026-04-03 完成 APEX + SI 整合后，`smart-invest` 容器已停止删除。

**SI 现在是 APEX 的依赖模块**：
- `smart-invest/` 作为 Python 模块（PYTHONPATH）被 APEX 挂载使用
- 核心分析逻辑（analyzer、scorer、pipeline、data_provider）直接在 APEX 容器内 import
- 所有定时任务由 APEX cron_service 统一调度
- 数据库 `/data/smart_invest.db` 由 APEX 容器共享访问

## 原 SI 功能对照

| SI 功能 | APEX 接管方式 |
|---------|--------------|
| `run_pipeline.py` | `apex-engine/scripts/run_pipeline.py` |
| 早报/晚报 | `apex-engine/scripts/push_morning/evening_report.py` |
| 超预期扫描 | APEX `/api/v1/scanners/earnings` |
| 回调买入 | APEX `/api/v1/scanners/pullback` |
| 五维度评分 | APEX cron preset `composite_score` |
| BTIQ 监控 | APEX cron preset `btiq_monitor` |
| SI Web UI（端口8080）| **已废弃**（APEX 端口 3000 为唯一入口）|

## 项目结构（归档参考）

```
smart-invest/          # ⚠️ 不再独立运行，作为 APEX 依赖模块
├── core/              # 分析逻辑模块（analyzer / scorer / pipeline / data_provider）
├── scanners/          # Scanner 实现（earnings / pullback / new_high / predictor）
├── scripts/           # 任务脚本（已迁入 APEX，不再独立执行）
├── notifiers/         # 飞书推送（由 APEX import 使用）
├── data/              # SQLite 数据库（由 APEX 容器共享）
└── config/            # 配置文件
```

## 保留原因

保留 SI 源码目录原因：
1. `core/` 和 `scanners/` 作为 APEX 的 PYTHONPATH 模块
2. `data/smart_invest.db` 是生产数据库，不能丢失
3. `config/` 中有 API keys 等配置
│   ├── smart_invest.db    # 数据库
│   └── logs/              # 运行日志
└── requirements.txt
```

## 数据源

- **Tushare**：A 股列表、财务指标、业绩预告
- **AkShare**：东方财富增速数据

## 旧系统

历史版本（pipeline 全市场扫描、事件驱动采集等）已归档至 `archive/20260322/smart-invest-old/`。
