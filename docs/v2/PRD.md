# JARVIS 投资系统 2.0 — PRD

_版本：v2.3 | 日期：2026-03-28 | P1 完成, T1-T24 全量通过, 前端 PRD 已确认_

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

_PRD v2.2 | 2026-03-28 P1 全部完成, T1-T25 25/25 通过_

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

---

## 前端 v1.0 — 已确认

**技术栈：** FastAPI + Jinja2 + HTMX + SSE + Plotly + Tailwind CDN  
**原则：** 零构建工具、零 npm、全栈 Python

**页面：**
1. 🏠 总览 — 一页看全局（信号 + 持仓 + 系统健康）
2. 📌 今日行动 — 核心决策界面（系统直接告诉用户该做什么）
3. 🔍 发现池 — 筛选排序 + 决策记录
4. 📋 信号看板 — 超预期/扣非新高/回调 + 决策记录
5. 📰 事件流 — 时间线 + 表格双视图
6. 📈 T+N 跟踪 — 收益曲线图
7. 💼 持仓快照 — 实时行情 + 风险预警
8. 📊 策略胜率 — 按信号类型拆分统计
9. 🎯 回测 — 历史信号收益 + CSV 导出
10. ⚙️ 系统控制 — 手动触发 Pipeline + 实时日志 + Cron 管理

**核心创新：** 决策记录功能 — 用户可标记每条信号的采纳/未采纳/观望状态，系统计算采纳胜率。

**详细 PRD：** → `docs/v2/frontend-prd.md`
