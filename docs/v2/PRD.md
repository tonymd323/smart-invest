# JARVIS 投资系统 2.0 — PRD

_版本：v2.7 | 日期：2026-03-29 | v2.11 全部完成, 新增超跌监控集成 + 大函数拆分 + Docker化_

---

## 产品概述

**一句话：** 统一数据管道 + 双池监控 + 实时事件驱动的智能投资系统

**三层分离：** 采集 → 分析 → 推送
**双池分离：** 跟踪池（手动）+ 发现池（自动）

## 首版 P0（12项）— ✅ 全部完成

| ID | 需求 | 模块 | 状态 |
|----|------|------|------|
| P-01 | 财务数据采集（东方财富→Tushare） | Pipeline | ✅ |
| P-02 | 一致预期采集（东方财富F10→AkShare） | Pipeline | ✅ |
| P-03 | 日K行情采集（Tushare） | Pipeline | ✅ |
| P-06 | 数据质量校验 | Pipeline | ✅ |
| I-01 | SSOT stocks.json | 基础设施 | ✅ |
| I-02 | Provider 抽象层（3个） | 基础设施 | ✅ |
| I-04 | 错误处理+日志 | 基础设施 | ✅ |
| A-01 | 超预期扫描 | Analyzer | ✅ |
| A-02 | 扣非新高扫描 | Analyzer | ✅ |
| A-03 | 回调买入评分 | Analyzer | ✅ |
| U-01 | 发现池日报卡片 | Pusher | ✅ |
| U-03 | Bitable 同步 | Pusher | ✅ |

## v2.1 修复（3项）— ✅ 全部完成

| 修复 | 说明 | 状态 |
|------|------|------|
| quarterly_net_profit | 累计→单季度差值计算 | ✅ |
| 超预期 N/A 处理 | 无 consensus 股票标 N/A | ✅ |
| NOTICE_DATE 扫描 | DisclosureScanner 替代 Tushare | ✅ |

## 迭代 P1（13项）— ✅ 全部完成

| ID | 需求 | 模块 | 1.0 状态 | 2.0 状态 | 优先级 | 测试 |
|----|------|------|---------|---------|--------|------|
| P-07 | 新闻采集 Provider | Pipeline | ✅ event_monitor | ✅ NewsProvider | 🔴 P0 | T14 |
| I-05 | 双池完整（自动入场） | Pipeline+DB | 🟡 表有逻辑无 | ✅ auto_discover_pool | 🔴 P0 | T15 |
| A-06 | T+N 跟踪 | Analyzer | 🟡 表有逻辑无 | ✅ create/update_tn | 🔴 P0 | T16 |
| A-04 | 回调预测集成 | Analyzer | ✅ pullback_predictor | ✅ PullbackAnalyzer | 🟡 P1 | T18 |
| A-07 | 回测集成 | Pipeline | ✅ backtest_update | ✅ run_backtest | 🟡 P1 | T19 |
| P-04 | 实时行情 Provider | Pipeline | ✅ 腾讯行情API | ✅ QuoteProvider | 🟡 P1 | T17 |
| A-08 | 事件检测 Analyzer | Analyzer | ❌ 未实现 | ✅ EventAnalyzer | 🟢 P2 | T20 |
| U-02 | 回调 DM 推送 | Pusher | ✅ Cron 在跑 | ✅ push_pullback_dm | 🟢 P2 | T21 |
| U-05 | 事件 DM 推送 | Pusher | ❌ 未实现 | ✅ push_event_dm | 🟢 P2 | T22 |
| U-07 | 升级操作（发现池→跟踪池） | DB | 🟡 表结构有 | ✅ DiscoveryPoolManager | 🟢 P2 | T23 |
| P-05 | 板块数据 | Pipeline | ✅ sector_rotation | ✅ SectorProvider | 🟢 P2 | T24 |
| I-03 | Cron 调度 | 基础设施 | ✅ | ✅ v2.1 上线 | ✅ Done | — |
| A-05 | 超跌监控集成 | Analyzer | ✅ btiq_monitor | ✅ OversoldScanner | 🟢 P2 | T25 |

**P1 总工时：~16h（含测试）** — ✅ 全部完成

## 关键约束

- 先建测试 Fixture，再写业务代码
- SQLite WAL 模式
- 禁止 `except: pass`
- 1.0 脚本优先封装进 2.0，不重复造轮子
- Provider 降级链必须完整

---

_PRD v2.4 | 2026-03-28 P1 全部完成, T1-T25 25/25 通过, 超预期 v2.5 重构, 前端脚手架完成_

---

## 数据质量问题（14 项，2026-03-28 回填后发现）

### P0 — 数据正确性（5 项）

| ID | 问题 | 说明 |
|----|------|------|
| D-01 | scan_new_high 扫入亏损股 | quarterly_net_profit ≤ 0 的公司被扫入，占 35% |
| D-02 | score=40 占 82% | is_new_high=False 时默认 40 分，大量无意义记录 |
| D-03 | T+N 收益全 None | update_tn_tracking() 未执行 |
| D-04 | BitableSync 不调 API | sync() 只写 pending 文件 |
| D-05 | Bitable 去重失效 | 跨批次写入无去重 |

### P1 — 数据完整性（5 项）

| ID | 问题 | 说明 |
|----|------|------|
| D-06 | 公司名称=代码 | SQLite 不存名称 |
| D-07 | 事件 title 含代码 | EventAnalyzer 用 stock_code 做标题 |
| D-08 | 事件详情=JSON | dump dict 未格式化 |
| D-09 | 市值/行业缺失 | Pipeline 不采集 |
| D-10 | 回测 50% 无收益 | 入池太新，等日期推移 |

### P2 — 数据丰富度（4 项）

| ID | 问题 | 说明 |
|----|------|------|
| D-11 | 事件只有扣非新高 | 未集成 NewsProvider |
| D-12 | 北交所后缀 .SZ vs .BJ | 东财搜索 API 需适配 |
| D-13 | buy 只有 2 只 | 无 consensus → 超预期少（非 bug） |
| D-14 | 子代理写 Bitable 超时 | 10 分钟限制 |

### 修复约束

- P0 必须在下次 Cron 执行前修复（否则继续产生噪音数据）
- P1 应在本周内修复
- P2 可在下个迭代处理

### v2.5 修复状态（2026-03-29）

| ID | 问题 | 状态 | 说明 |
|----|------|------|------|
| D-01 | scan_new_high 扫入亏损股 | ✅ 已修代码 | 旧数据待清理 |
| D-02 | score=40 占 82% | ✅ 已修代码 | 旧 N/A 数据待 DELETE |
| D-03 | T+N 收益全 None | ✅ Cron 已调用 | — |
| D-04 | BitableSync 不调 API | ✅ Cron Step2 | — |
| D-05 | Bitable 去重失效 | ✅ 缓存+pending | — |
| D-06 | 公司名称=代码 | ✅ 已修代码 | 前端查询层待补 JOIN |
| D-07 | 事件 title 含代码 | ✅ 已修代码 | — |
| D-08 | 事件详情=JSON | ✅ 已修代码 | 前端展示层待格式化 |
| D-09 | 市值/行业缺失 | ⚠️ 部分 | industry 已入池，市值待 QuoteProvider |
| D-10 | 回测 50% 无收益 | ⏳ 等时间推移 | — |

---

## v2.6 需求变更 — ConsensusProvider 多源从严

### 背景
一致预期数据是超预期检测的核心阈值。当前降级策略（AkShare → 东方财富 F10）存在单源风险：任一数据源异常直接影响超预期判断。

### 需求
从"降级保底"升级为"多源从严"：**同时获取两个数据源的一致预期数据，以净利润增速为锚，取更高预期值（更严格门槛），写入数据库并标注来源。**

### 设计原则
- **更高预期 = 更严格筛选 = 减少假阳性**
- 以 `net_profit_yoy` 为锚选源，`rev_yoy` 取同一源（不交叉拼接）
- 预注入数据仍为最高优先级（手动覆盖）

### 数据源优先级（变更后）

| 优先级 | 数据源 | 标识 | 说明 |
|--------|--------|------|------|
| 1️⃣ | 预注入数据 | `preloaded` | 手动 presets，最高优先级（不变） |
| 2️⃣ | **双源取 max** | `max(akshare,eastmoney)` | 同时获取两源，取净利润增速更高的 |

### 改动范围

| 文件 | 改动 | 复杂度 |
|------|------|--------|
| `core/data_provider.py` | ConsensusProvider 重构：并行取两源 + 取 max | 中 |
| `core/database.py` | consensus 表新增 source_detail 字段 | 小 |
| `core/pipeline.py` | fetch_and_apply_consensus 写入 source_detail | 小 |
| `tests/test_consensus.py` | 新增多源对比测试用例 | 小 |

### DB Schema 变更
```sql
ALTER TABLE consensus ADD COLUMN source_detail TEXT;  -- JSON: {"akshare": {...}, "eastmoney": {...}, "selected": "..."}
```

### 验收标准
- [ ] 两源都有数据时，取净利润增速更高的值
- [ ] 只有一源时，降级使用
- [ ] source_detail 记录两源原始值 + 选择结果
- [ ] 预注入仍为最高优先级
- [ ] 新测试用例全通过
- [ ] 既有测试回归通过

---

## v2.7 需求变更 — 持仓管理 + 事件流重构

### 背景
1. **持仓管理缺失** — 当前持仓配置在 `config/stocks.json` 手动修改，无 Web 界面。用户无法在线增删跟踪池股票、编辑成本价/目标价/止损价。
2. **事件流页面冗余** — 当前事件流只展示财报类事件（超预期/扣非新高），与信号看板内容重复。NewsProvider 已实现但未接入。页面独立价值不足。

### 需求

#### A. 持仓管理页面（新页面 📒）
- **功能：** 在线管理跟踪池（stocks.json），支持增删改查
- **字段：** 股票代码、名称、持仓股数、成本价、目标价、止损价、备注
- **交互：** HTMX 表单提交，实时刷新，无需页面跳转
- **SSOT 同步：** 修改后写入 `config/stocks.json`，与现有系统兼容
- **发现池升级：** 支持从发现池一键"升级到跟踪池"

#### B. 今日行动重构（现有页面 📌）
- **问题：** 当前与信号看板功能重复，只做了信号列表+优先级排序
- **重构方向：** 从"信号列表"变为"操作建议"
  - 持仓决策：持有股票的最新信号 → 加仓/减仓/持有/卖出建议
  - 买入建议：发现池候选 → 目标价+止损价+评分
  - 关注提醒：值得关注但未到买入时机的信号
- **数据源：** 持仓 + 发现池 + 今日信号 + 实时行情

#### C. 事件流页面（现有页面 📰）
- **方案：** 暂不删除，后续接入新闻/重大事件后恢复价值
- **短期优化：** 过滤财报类事件（可合并到信号看板），保留页面框架
- **中期计划：** 接入 NewsProvider → events 表，增加"新闻事件""重大公告"类型

### 验收标准
- [ ] 持仓管理页面可用：增删改查 + 从发现池升级
- [ ] 今日行动页展示操作建议（非信号列表）
- [ ] 修改持仓后 stocks.json 同步更新
- [ ] 事件流页面标记财报类为可过滤

---

## v2.8 需求变更 — 全局列表增强（搜索+筛选+排序+分页）

### 背景
所有列表页面（信号看板、发现池、持仓管理、事件流、T+N跟踪、回测）缺乏搜索和分页功能，数据量大时用户体验差。持仓管理发现池候选74只只显示前20只，无法完整浏览。

### 需求
所有列表页面统一支持：**搜索、筛选、排序、分页**。

### 设计原则
- **通用组件化：** Jinja2 宏封装 list_controls，所有页面复用
- **HTMX 无刷新：** 分页+排序通过 hx-get 局部刷新，不整页重载
- **后端统一：** services.py 封装通用分页查询函数

### 功能清单

| 功能 | 说明 | 技术实现 |
|------|------|---------|
| **搜索** | 按股票代码/名称过滤 | SQL LIKE + 输入框 |
| **筛选** | 按类型/信号/来源等 | query params + 下拉框 |
| **排序** | 点击表头切换升/降序 | ?sort=&order= 参数 |
| **分页** | 每页20条，前后翻页 | ?page=&page_size= + HTMX 局部刷新 |

### 页面改造范围

| 页面 | 路由 | 搜索 | 排序 | 分页 |
|------|------|------|------|------|
| 📒 持仓管理-发现池 | `/portfolio` | ✅ 代码/名称 | ✅ 评分/名称 | ✅ 20/页 |
| 📋 信号看板 | `/signals` | ✅ 代码/名称 | ✅ 评分/日期 | ✅ 20/页 |
| 🔍 发现池 | `/discovery` | ✅ 代码/名称 | ✅ 评分/日期 | ✅ 20/页 |
| 📰 事件流 | `/events` | ✅ 代码/名称 | ✅ 日期 | ✅ 20/页 |
| 📈 T+N 跟踪 | `/tracking` | ✅ 代码/名称 | ✅ 收益/日期 | ✅ 20/页 |
| 📊 回测 | `/backtest` | ✅ 代码/名称 | ✅ 收益/日期 | ✅ 20/页 |

### 验收标准
- [ ] 通用 list_controls 模板宏可用
- [ ] 所有列表页支持搜索（代码/名称）
- [ ] 所有列表页支持排序（表头点击切换）
- [ ] 所有列表页支持分页（HTMX 局部刷新）
- [ ] 持仓管理发现池完整展示 74 只候选

---

## v2.11 需求 — 图表可视化 + 持仓行情 + 卖出决策

### A. Plotly 图表集成（~2h）
- T+N 跟踪页：收益曲线图（1d/5d/10d/20d）
- 回测页：柱状图（按信号类型胜率 + 平均收益）
- 总览页：信号趋势图（近30天信号数量）

### B. 持仓实时行情+盈亏（~1h）
- 持仓管理页：实时价格 + 持仓市值 + 盈亏金额/比例
- 总览页：持仓卡片显示实时行情
- 数据源：QuoteProvider

### C. 卖出决策逻辑（~1.5h）
- 今日行动增加"减仓/清仓"建议
- 触发条件：低于预期、跌破止损价、获利了结
- 决策按钮：已卖出 → 从持仓移除 + 记录收益

### D. 新闻事件（✅ 已完成）
- JARVIS 已实现 events + event_tracking 合并显示
- NewsProvider 已集成

---

## v2.12 需求 — 超跌监控集成 + 技术债收尾

### 背景
超跌全市场监控（BTIQ 涨跌比指标）当前是独立脚本 `scripts/btiq_monitor.py`，通过 Cron 每30分钟运行，直接推送飞书但**不入库 SQLite**。存在三个问题：
1. **前端无展示** — 9个页面中没有超跌数据，信号看板/今日行动均未纳入
2. **无历史可查** — 结果不入数据库，无法画趋势图、回溯信号准确性
3. **架构孤岛** — 不经过 Pipeline/Analyzer，与系统其他模块割裂

同时 `core/analyzer.py` 中已有 `OversoldScanner` 类但从未被 Pipeline 调用，代码已写好但未接入。

### 需求

#### A. 数据采集层改造 — 双通道架构

**核心原则：** 采集粒度与分析粒度对齐。单股采集→单股分析，市场采集→市场分析。

| 改动 | 说明 |
|------|------|
| 新增 `MarketSnapshotProvider` | 全市场快照采集，接口为 `fetch_snapshot()`（非 `fetch(stock_code)`） |
| 新增 `market_snapshots` 表 | 存储 BTIQ 历史：btiq, up/down/total, ma5, signal, timestamp |
| Pipeline 新增 `run_market_snapshot()` | 市场通道入口，与单股通道 `run()` 并列 |

**数据流：**
```
MarketSnapshotProvider.fetch_snapshot()
    → 腾讯行情批量 API（4秒扫完 4913 只）
    → MarketSnapshot(up, down, total, btiq, timestamp)
    → MarketAnalyzer.analyze(snapshot)
    → BTIQ + MA5 + signal
    → 写入 market_snapshots 表
    → 前端可查 / 飞书推送
```

#### B. 分析层改造 — MarketAnalyzer

| 改动 | 说明 |
|------|------|
| 新增 `MarketAnalyzer` 类 | 与 EarningsAnalyzer / PullbackAnalyzer 并列 |
| 迁移 OversoldScanner 逻辑 | calc_btiq / calc_ma5 / judge_signal 从 analyzer.py 底部迁移到 MarketAnalyzer |
| Pipeline.run_market_snapshot 调用 | 采集完自动分析，结果入库 |

#### C. 前端新增超跌页面

| 页面 | 路由 | 功能 |
|------|------|------|
| 📉 超跌监控 | `/oversold` | BTIQ 实时值 + MA5 趋势图 + 历史信号时间线 |

**页面内容：**
- 信号卡片：当前 BTIQ 值 + MA5 + 状态（正常/超跌/冰点/过热）
- Plotly 折线图：BTIQ 历史曲线 + 30/25 阈值参考线
- 信号历史：超跌/冰点触发时间线
- 总览仪表板新增市场情绪指标卡片

#### D. Cron 改造

| 旧 | 新 |
|----|-----|
| `btiq_monitor.py` 独立脚本 | `Pipeline.run_market_snapshot()` |
| 直接推飞书 | SQLite 入库 → 前端可查 → 飞书推送 |

#### E. 验收标准
- [ ] MarketSnapshotProvider 可获取全市场数据并返回 MarketSnapshot
- [ ] market_snapshots 表正确存储 BTIQ 历史
- [ ] Pipeline.run_market_snapshot() 端到端可用
- [ ] MarketAnalyzer 正确计算 BTIQ/MA5/signal
- [ ] 前端 `/oversold` 页面展示 BTIQ 趋势图 + 信号时间线
- [ ] 总览仪表板显示市场情绪指标
- [ ] 测试用例 T26-T28 全部通过
- [ ] Cron 从 btiq_monitor.py 切换到新通道
- [ ] 既有 T1-T25 回归通过

### 工时估算

| 模块 | 改动 | 预估 |
|------|------|------|
| `core/data_provider.py` | 新增 MarketSnapshotProvider + MarketSnapshot 模型 | 1h |
| `core/database.py` | 新增 market_snapshots 表 + 索引 | 0.5h |
| `core/analyzer.py` | 新增 MarketAnalyzer（逻辑从 OversoldScanner 迁移） | 1h |
| `core/pipeline.py` | 新增 run_market_snapshot() | 0.5h |
| `web/routes/` | 新增 oversold.py 路由 | 0.5h |
| `web/templates/` | 新增 oversold.html（Plotly 图表） | 1.5h |
| `web/services.py` | 新增 get_oversold_data() | 0.5h |
| 测试 | T26-T28 | 1h |
| **超跌集成小计** | | **~6.5h** |

---

### F. 大函数拆分

| 函数 | 当前行数 | 目标行数 | 拆分方式 |
|------|---------|---------|---------|
| `get_today_actions()` | ~277 | ~60 | 拆为 _calc_portfolio_actions + _calc_buy_candidates + _calc_watch_alerts |
| `format_summary()` | ~121 | ~28 | 拆为 _format_earnings_section + _format_pullback_section + _format_event_section |
| 预估 | | | 2h |

### G. Docker 化部署

| 任务 | 预估 |
|------|------|
| Dockerfile（Python 3.11 + 依赖） | 0.5h |
| docker-compose.yml（FastAPI + SQLite volume） | 0.5h |
| systemd → Docker 迁移 | 0.5h |
| 预估 | 1.5h |

### v2.12 总工时

| 模块 | 预估 |
|------|------|
| 超跌监控集成 | 6.5h |
| 大函数拆分 | 2h |
| Docker 化 | 1.5h |
| **总计** | **~10h** |

---

## 前端 v1.0 — 已确认
