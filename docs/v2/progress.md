# JARVIS 投资系统 — 进度

_版本：v2.23 | 更新：2026-03-30 13:00 | 数据标准化层 + Bug修复_

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
─────────────────────────────────────
数据标准化层 ██████████████░░░░░░  70%  ← v2.23 进行中
存量数据清洗 ████████████████████ 100%
```

## 里程碑（最近 10 条）

| # | 里程碑 | 时间 |
|---|--------|------|
| M71 | T+N回测日期格式Bug修复(140条数据重置) | 3/30 12:55 |
| M72 | /logs页面datetime修复+Docker重建 | 3/30 11:23 |
| M73 | OpenClaw配置崩溃恢复(lossless-claw清理) | 3/30 10:47 |
| M74 | v2.23 数据标准化方案设计 | 3/30 13:00 |
| M70 | v2.22 全量改动梳理 | 3/29 20:46 |
| M69 | Cron管理重构(删除修复+中文UI) | 3/29 21:10 |
| M68 | 业绩预告筛选修复(is_forecast=1) | 3/29 20:15 |
| M67 | 披露类型互斥修复(LIKE summary) | 3/29 20:24 |
| M66 | 信号看板筛选维度重构(分析+披露+报告期) | 3/29 20:10 |
| M65 | 盈利质量风险检测(归母vs扣非>20%) | 3/29 16:00 |

## Bug 修复记录（v2.23 新增）

| # | 问题 | 状态 |
|---|------|------|
| 21 | T+N回测日期格式不匹配：`trade_date(20260327)` vs `event_date(2026-03-29)`，SQLite字符串比较 `'2'>'-'` 导致返回全历史数据 | ✅ event_date去连字符后比较 |
| 22 | /logs页面 NameError: datetime not defined | ✅ 添加import |
| 23 | /logs页面 TemplateNotFound: logs.html 未打入Docker镜像 | ✅ 重建镜像 |
| 140条错误收益数据已重置为NULL，待Pipeline重新计算 |

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
| 20 | Cron删除任务显示"任务不存在" | ✅ 改为按注释行匹配+跳过下一行cron行 |

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

## v2.23 数据标准化层（进行中）

### 背景
日期格式混乱导致T+N回测Bug：`prices.trade_date=20260327` vs `event_tracking.event_date=2026-03-29`，SQLite字符串比较静默出错。**这不是个例**——系统缺少统一的数据标准化层，每次数据源格式变化都可能触发类似Bug。

### 目标
建立 **数据标准化层**，确保：
1. 所有日期统一 `YYYY-MM-DD`
2. 所有股票代码统一 `canonical_code`（如 `000001.SZ`）
3. 入库前必须经过标准化，数据库层面拒绝脏数据

### 开发计划

| 步骤 | 内容 | 状态 |
|------|------|------|
| 1 | `core/data_normalizer.py` — 标准化函数（日期+代码+数值） | ✅ |
| 2 | `core/stock_resolver.py` — 股票身份解析器（任意格式→canonical_code） | ✅ |
| 3 | 存量数据清洗脚本 `scripts/clean_normalize.py` + 已执行 | ✅ |
| 4 | `pipeline.py` + `analyzer.py` 入库路径改造（所有INSERT过normalizer） | ✅ |
| 5 | `database.py` 添加 UNIQUE 约束 + 格式校验 | ⬜ |
| 6 | 前端/Analyzer 所有SQL比较改为 canonical_code + ISO日期 | ⬜ |
| 7 | 测试验证 | ⬜ |

### 设计原则
- **写入时归一化**：所有数据入库前经过 `DataNormalizer.normalize()`
- **StockResolver 身份解析**：`000001` / `000001.SZ` / `平安银行` → `000001.SZ`
- **数据库层兜底**：UNIQUE 约束 + CHECK 约束拒绝格式不一致
- **单一入口**：不再允许任何模块直接 `INSERT INTO`（统一走 `database.py`）

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
