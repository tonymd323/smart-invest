# JARVIS 投资系统 2.0 — 架构设计

_版本：v2.6 | 日期：2026-03-29 | ConsensusProvider 多源从严：并行双源取 max + source_detail_

---

## 架构原则

三层分离 | 数据驱动 | 自动降级 | 零重复 | 双池分离 | 采集分析职责分离

## 系统分层

### Layer 0: Provider（6 个，5 个在线）

| Provider | 主源 | 降级源 | Pipeline 使用 | 状态 |
|----------|------|--------|-------------|------|
| FinancialProvider | 东方财富 datacenter | Tushare | ✅ | ✅ 生产 |
| ConsensusProvider | 双源取 max（AkShare + 东方财富 F10） | — | ✅ fetch_and_apply_consensus | ✅ 生产 |
| KlineProvider | Tushare pro.daily | 东方财富 push2his | ❌ 待接入 | ⏸️ 备用 |
| QuoteProvider | 腾讯行情 API | 东方财富 Push2 | ❌ 单独调用 | ✅ 生产（pullback_predictor） |
| NewsProvider | RSS + 东方财富个股新闻 | — | ❌ 单独调用 | ✅ 生产（EventAnalyzer.detect_from_codes） |
| SectorProvider | 东方财富板块数据 | — | ❌ | ⏸️ 备用 |

### Layer 0.5: DisclosureScanner

- 基于东方财富 `NOTICE_DATE` 实时扫描财报/业绩预告披露日
- API: `RPT_F10_FINANCE_MAINFINADATA` + `RPT_PUBLIC_OP_NEWPREDICT`
- filter: `(NOTICE_DATE>'{datetime}')`，SQL 单引号格式
- 输出：新披露股票代码列表，跟 DB diff 后只扫新增的

### Layer 1: Pipeline（pipeline.py）

- DisclosureScanner 获取新披露列表（use_disclosure_filter=True）
- 串行调 FinancialProvider → 写入 SQLite earnings 表
- 自动计算 quarterly_net_profit（累计净利润差值法）
- 数据质量校验
- `fetch_and_apply_consensus()` → ConsensusProvider 并行获取 AkShare + 东方财富 F10 多年一致预期（25E/26E/27E）→ 取净利润增速更高值 → 写 consensus 表（含 source_detail）→ 按报告期匹配预期年份 → 计算 expectation_diff_pct

### Layer 2: Analyzer（analyzer.py）

| Analyzer | 功能 | 状态 |
|----------|------|------|
| EarningsAnalyzer | 超预期（actual - expected ≥ 5%）+ 扣非新高 | ✅ 生产 |
| PullbackAnalyzer | 回调买入四层漏斗评分 | ✅ 生产 |
| EventAnalyzer | Pipeline 事件 + 新闻事件检测 | ✅ 生产 |
| DiscoveryPoolManager | 自动发现池入场/过期（7天） | ✅ 生产 |
| EarningsAnalyzer.update_tn | T+N 收益跟踪 | ✅ 生产 |
| OversoldScanner | BTIQ 全市场超跌扫描 | ✅ 生产 |

### Layer 3: 同步（BitableSync）

- 生成记录 → 去重（同批内合并 + 缓存文件）→ 分批导出 pending JSON（max 200 条/批）
- Agent 读取 pending → feishu_bitable_app_table_record batch_create 写入飞书
- 5 张表：数据表 / 发现池 / 事件 / T+N 跟踪 / 回测

### Layer 4: 推送（Pusher）

| 推送 | 时机 | 状态 |
|------|------|------|
| A股早报 | 07:03 | ✅ |
| 跟踪池收盘总结 | 15:30 | ✅ |
| A股晚报 | 18:05 | ✅ |
| 晚间扫描卡片 | 21:00 | ✅ |
| 回调买入即时 DM | 盘中触发 | ✅ |
| 超跌信号即时 DM | 盘中触发 | ✅ |

## 双池设计

| | 跟踪池 | 发现池 |
|---|---|---|
| 来源 | 手动（stocks.json） | 自动（discovery_pool 表） |
| 入场 | 手动添加 | 超预期/扣非新高/回调自动入池 |
| 监控 | 全维度 | 基本面+技术面 |
| 卡片 | 15:30 收盘总结 | 21:00 晚间扫描 |
| 生命周期 | 持续 | 7天自动 expire |

## 数据模型

### 核心表

| 表 | 行数 | 说明 |
|----|------|------|
| stocks | 350 | 股票清单 |
| earnings | 4686 | 财务数据 + quarterly_net_profit + expectation_diff_pct |
| consensus | 增长中 | 多年一致预期（双源取 max）UNIQUE(stock_code, year)，含 source_detail |
| prices | 480 | 日K行情 |
| analysis_results | 1656 | 分析结果 |

### v2 新增表

| 表 | 行数 | 说明 |
|----|------|------|
| discovery_pool | 76 | 自动发现池 |
| events | 2+ | 结构化事件 |
| event_tracking | 236 | T+N 收益跟踪 |
| backtest | 160 | 回测记录 |

## 飞书多维表格（5 张）

| 表 | table_id | 职责 |
|----|----------|------|
| 数据表 | tbluSQrjOW0tppTP | 主看板：buy/watch/hold 信号 |
| 发现池 | tblPKXYUsow2Pd6A | 自动发现候选股 |
| 事件 | tblUgPIXejUOggWx | 结构化事件 |
| T+N 跟踪 | tblNZIrovX0WRmW3 | 入池后收益跟踪 |
| 回测 | tblP6OwkzGQns8Uc | 历史信号收益 |

## Cron 时间线（v2.4 上线版）

```
07:03  A股早报（巴菲特群飞书卡片）
07:05  早盘新披露扫描（18h 窗口）→ Pipeline → DB
09-14  盘中轻检（每30分钟 pullback_predictor）+ 超跌监控（每30分钟 btiq_monitor）
15:15  回调买入预测（全市场扫描）
15:30  盘后新披露扫描（4h 窗口）+ pool-monitor 收盘总结
18:05  A股晚报（指数 + 板块轮动）
18:30  回测更新（backtest_update + Bitable 写入）
21:00  晚间全量扫描（12h 窗口）
       → Pipeline → fetch_and_apply_consensus
       → scan_beat_expectation + scan_new_high
       → auto_discover_pool + update_tn_tracking
       → EventAnalyzer（pipeline + news）
       → Bitable 3 张表同步
       → 飞书卡片推送
```

## 前端 (FastAPI + Jinja2 + HTMX)

```
用户浏览器 ←→ FastAPI (port 8080) ←→ SQLite
                    ↓
              Jinja2 模板渲染
                    ↓
              HTMX 动态交互
                    ↓
              SSE 实时日志流
                    ↓
              subprocess 调用 Pipeline
```

**技术栈：** FastAPI + Jinja2 + HTMX + SSE + Plotly + Tailwind CDN  
**数据源：** SQLite 直读（/data/smart_invest.db）  
**Pipeline 触发：** subprocess.run() + SSE 实时输出  
**部署：** Docker Compose（与主系统同一容器）  
**详细 PRD：** → `docs/v2/frontend-prd.md`

## 测试

25 个真实环境测试（T1-T25）全部通过。

| 阶段 | 测试数 | 结果 |
|------|--------|------|
| T1-T9 | 9 | ✅ |
| T10-T13 | 4 | ✅ |
| T14-T16 | 3 | ✅ |
| T17-T19 | 3 | ✅ |
| T20-T25 | 6 | ✅ |

---

_架构 v2.6 | 2026-03-29 | ConsensusProvider 多源从严：并行双源取 max + source_detail_
