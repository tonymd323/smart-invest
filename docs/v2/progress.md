# JARVIS 投资系统 — 进度

_版本：v2.22 | 更新：2026-03-29 20:46 | 筛选维度重构+业绩预告支持+批量行情修复_

---

## 进度

```
P0+P1 功能  ████████████████████ 100%
数据质量    ████████████████████ 100%
前端 10 页面 ████████████████████ 100%
事件采集    ████████████████████ 100%
决策流转    ████████████████████ 100%
持仓行情    ████████████████████ 100%
Plotly 图表 ████████████████████ 100%
超跌监控    ████████████████████ 100%
Docker 化   ████████████████████ 100%
前端 UX 重构 ████████████████████ 100%
回调买入集成 ████████████████████ 100%
筛选多选    ████████████████████ 100%
Cron 前端管理 ████████████████████ 100%
业绩预告采集 ████████████████████ 100%
筛选维度重构 ████████████████████ 100%
盈利质量检测 ████████████████████ 100%
批量行情修复 ████████████████████ 100%
测试 24/24  ████████████████████ 100%
```

## 里程碑（最近 10 条）

| # | 里程碑 | 时间 |
|---|--------|------|
| M60 | v2.21 筛选修复+浏览器实测 | 3/29 18:22 |
| M61 | T+N跟踪去重+报告期补填 | 3/29 18:36 |
| M62 | T+N筛选+状态中文映射 | 3/29 19:05 |
| M63 | 今日行动批量行情修复 | 3/29 19:19 |
| M64 | Pipeline业绩预告采集+Analyzer范围值 | 3/29 15:30 |
| M65 | 盈利质量风险检测(归母vs扣非>20%) | 3/29 16:00 |
| M66 | 信号看板筛选维度重构(分析+披露+报告期) | 3/29 20:10 |
| M67 | 披露类型互斥修复(LIKE summary) | 3/29 20:24 |
| M68 | 业绩预告筛选修复(is_forecast=1) | 3/29 20:15 |
| M69 | v2.22 全量改动梳理 | 3/29 20:46 |

## Bug 修复记录（本轮）

| # | 问题 | 状态 |
|---|------|------|
| 1 | 发现池 `report_period` 列不存在 → SQL 500 | ✅ 改用 report_type |
| 2 | 事件流 511KB → 37KB（分组遍历 all_events） | ✅ 只遍历分页 events |
| 3 | Cron API 返回空（Docker 无 crontab） | ✅ Dockerfile 加 cron |
| 4 | Cron API 名称解析（注释格式不匹配） | ✅ 重写解析逻辑 |
| 5 | Docker 挂载覆盖 host crontab 成目录 | ✅ 改用 data/crontab.txt |
| 6 | 信号看板分析类型选项值不匹配 DB | ✅ earnings_beat→earnings_beat_daily |
| 7 | 信号看板报告期选项值不匹配 DB | ✅ 年报→Q4, 季报→Q1/Q2/Q3 |
| 8 | 事件流报告期选项值不匹配 DB | ✅ 2025Q4→20251231 |
| 9 | 跟踪页状态选项值不匹配 DB | ✅ tracking/pending/active/completed |
| 10 | 跟踪页硬编码 entry_price IS NOT NULL | ✅ 移除 WHERE 条件 |
| 11 | stocks表30条脏数据+搜索失败 | ✅ 清洗+添加镜像条目 |
| 12 | 信号看板筛选不可用（Alpine.js三层兼容性bug） | ✅ Alpine.js→原生select+onchange |
| 13 | T+N跟踪重复数据(286→235条) | ✅ 按stock_code+event_type去重 |
| 14 | T+N跟踪报告期缺失(54条NULL) | ✅ 从discovery_pool补填 |
| 15 | T+N跟踪状态英文显示 | ✅ tracking→跟踪中, active→已买入 |
| 16 | 今日行动只有第一只股票有行情 | ✅ fetch_batch批量查询+去掉20只限制 |
| 17 | 公告类型筛选Q1/Q2/Q3永远0条 | ✅ 拆成披露类型+报告期两个维度 |
| 18 | 业绩预告筛选无效(report_type全是Q4) | ✅ 改用is_forecast=1 |
| 19 | 披露类型财报/预告不互斥(EXISTS按stock_code) | ✅ LIKE匹配summary中disclosure_type |

## v2.22 新增功能

### 1. 信号看板筛选维度重构
- **原**：分析类型 + 公告类型(Q4/Q1/Q2/Q3/业绩预告/业绩快报)
- **新**：分析类型 + 披露类型(财报/业绩预告/业绩快报) + 报告期(2025年报/2025Q3/...)
- 披露类型从 analysis_results.summary JSON 的 disclosure_type 字段提取（LIKE匹配）
- 报告期从 earnings.report_date 关联

### 2. Pipeline 业绩预告采集
- `core/data_provider.py` 新增 `ForecastProvider`：从业绩预告接口采集
- `core/pipeline.py` 业绩预告处理：范围值解析（如 "+100%~+150%"），只取归母净利润
- `scripts/run_pipeline.py` Step 3c 业绩预告采集

### 3. 盈利质量风险检测
- `core/pipeline.py`：归母净利润增速 vs 扣非净利润增速 差异 >20% 标记 ⚠️
- 前端显示：发现池/信号看板/T+N跟踪 红色⚠️标记

### 4. T+N跟踪增强
- 去重：286→235条（按stock_code+event_type）
- 筛选：按事件类型(超预期/扣非新高) + 报告期(2025年报/2026Q1...)
- 状态中文：tracking→跟踪中, active→已买入, pending→待处理
- 扣非+风险标记：从earnings表JOIN获取

### 5. 今日行动行情修复
- `_get_current_prices`：逐只查询→`fetch_batch`批量查询
- 去掉all_codes[:20]限制，全部股票都查行情

### 6. 筛选组件重写
- 放弃Alpine.js多选组件（label_text不可见+var不支持+tojson双引号冲突）
- 改用原生`<select>`+`onchange`内联JS
- 支持清除按钮(✕)

## 数据库（Docker 容器内）

| 表 | 行数 |
|---|------|
| stocks | 6,060 |
| earnings | 4,873 |
| analysis_results | 364 |
| discovery_pool | 72 |
| event_tracking | 235 |
| events | 29 |
| decision_log | 15 |
| backtest | 160 |
| market_snapshots | 41 |

## 文件参考

- 历史完整记录 → `archive/progress-full-2026-03-29.md`
- v2.13 开发方案 → `v2.13-dev-plan.md`
- PRD → `PRD.md`
- 架构 → `architecture.md`
