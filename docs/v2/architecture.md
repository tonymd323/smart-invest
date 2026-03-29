# JARVIS 投资系统 2.0 — 架构设计

_版本：v2.15 | 日期：2026-03-29 | Docker 化部署 + 跟踪页增强_

---

## 架构原则

三层分离 | 数据驱动 | 自动降级 | 零重复 | 双池分离 | 采集分析职责分离 | **采集粒度与分析粒度对齐**

## 系统分层

### Layer 0: Provider（7 个，6 个在线）

**单股通道 Provider（逐只采集）：**

| Provider | 主源 | 降级源 | Pipeline 使用 | 状态 |
|----------|------|--------|-------------|------|
| FinancialProvider | 东方财富 datacenter | Tushare | ✅ | ✅ 生产 |
| ConsensusProvider | 双源取 max（AkShare + 东方财富 F10） | — | ✅ fetch_and_apply_consensus | ✅ 生产 |
| KlineProvider | Tushare pro.daily | 东方财富 push2his | ❌ 待接入 | ⏸️ 备用 |
| QuoteProvider | 腾讯行情 API | 东方财富 Push2 | ❌ 单独调用 | ✅ 生产（pullback_predictor） |
| NewsProvider | RSS + 东方财富个股新闻 | — | ❌ 单独调用 | ✅ 生产（EventAnalyzer.detect_from_codes） |
| SectorProvider | 东方财富板块数据 | — | ❌ | ⏸️ 备用 |

**市场通道 Provider（全市场快照）：**

| Provider | 主源 | Pipeline 使用 | 状态 |
|----------|------|-------------|------|
| MarketSnapshotProvider | 腾讯行情批量 API | ✅ run_market_snapshot() | 🔨 v2.12 开发中 |

> **设计原则：** 单股 Provider 接口为 `fetch(stock_code)`，市场 Provider 接口为 `fetch_snapshot()`。不强行统一粒度。

### Layer 0.5: DisclosureScanner

- 基于东方财富 `NOTICE_DATE` 实时扫描财报/业绩预告披露日
- API: `RPT_F10_FINANCE_MAINFINADATA` + `RPT_PUBLIC_OP_NEWPREDICT`
- filter: `(NOTICE_DATE>'{datetime}')`，SQL 单引号格式
- 输出：新披露股票代码列表，跟 DB diff 后只扫新增的

### Layer 1: Pipeline（pipeline.py）— 双通道

**单股通道** `Pipeline.run(stock_codes)`：
- DisclosureScanner 获取新披露列表（use_disclosure_filter=True）
- 串行调 FinancialProvider → 写入 SQLite earnings 表
- 自动计算 quarterly_net_profit（累计净利润差值法）
- 数据质量校验
- `fetch_and_apply_consensus()` → ConsensusProvider 并行获取 AkShare + 东方财富 F10 多年一致预期（25E/26E/27E）→ 取净利润增速更高值 → 写 consensus 表（含 source_detail）→ 按报告期匹配预期年份 → 计算 expectation_diff_pct

**市场通道** `Pipeline.run_market_snapshot()` — 🔨 v2.12：
- MarketSnapshotProvider.fetch_snapshot() → 全市场 4913 只股票实时行情
- MarketAnalyzer.analyze(snapshot) → BTIQ + MA5 + signal
- 写入 market_snapshots 表
- 返回市场情绪信号（buy/warn/hot/none）

> **两个通道独立运行，互不干扰。** 单股通道由 Cron 21:00 晚间扫描触发，市场通道由 Cron 每30分钟触发。

### Layer 2: Analyzer（analyzer.py）— 单股 + 市场双层

**单股分析器（由 Pipeline.run 调用）：**

| Analyzer | 功能 | 状态 |
|----------|------|------|
| EarningsAnalyzer | 超预期（actual - expected ≥ 5%）+ 扣非新高 | ✅ 生产 |
| PullbackAnalyzer | 回调买入四层漏斗评分 | ✅ 生产 |
| EventAnalyzer | Pipeline 事件 + 新闻事件检测 | ✅ 生产 |
| DiscoveryPoolManager | 自动发现池入场/过期（7天） | ✅ 生产 |
| EarningsAnalyzer.update_tn | T+N 收益跟踪 | ✅ 生产 |

**市场分析器（由 Pipeline.run_market_snapshot 调用）：**

| Analyzer | 功能 | 状态 |
|----------|------|------|
| MarketAnalyzer | BTIQ 涨跌比 + MA5 趋势 + 超跌/冰点/过热信号 | 🔨 v2.12 开发中 |

> **旧代码状态：** `OversoldScanner`（analyzer.py 底部）将被重构为 `MarketAnalyzer`。逻辑迁移，接口升级为接收 MarketSnapshot 对象。

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
| market_snapshots | — | 🔨 v2.12：全市场快照（btiq/up/down/ma5/signal） |

## 飞书多维表格（5 张）

| 表 | table_id | 职责 |
|----|----------|------|
| 数据表 | tbluSQrjOW0tppTP | 主看板：buy/watch/hold 信号 |
| 发现池 | tblPKXYUsow2Pd6A | 自动发现候选股 |
| 事件 | tblUgPIXejUOggWx | 结构化事件 |
| T+N 跟踪 | tblNZIrovX0WRmW3 | 入池后收益跟踪 |
| 回测 | tblP6OwkzGQns8Uc | 历史信号收益 |

## Cron 时间线（v2.12 更新版）

```
── 单股通道 ──────────────────────────────────────────────
07:03  A股早报（巴菲特群飞书卡片）
07:05  早盘新披露扫描（18h 窗口）→ Pipeline.run() → DB
15:15  回调买入预测（全市场扫描）
15:30  盘后新披露扫描（4h 窗口）+ pool-monitor 收盘总结
18:05  A股晚报（指数 + 板块轮动）
18:30  回测更新（backtest_update + Bitable 写入）
21:00  晚间全量扫描（12h 窗口）
       → Pipeline.run() → fetch_and_apply_consensus
       → scan_beat_expectation + scan_new_high
       → auto_discover_pool + update_tn_tracking
       → EventAnalyzer（pipeline + news）
       → Bitable 3 张表同步
       → 飞书卡片推送

── 市场通道（v2.12 新增）─────────────────────────────────
09-14  超跌监控 每30分钟 → Pipeline.run_market_snapshot()
       → MarketSnapshotProvider.fetch_snapshot()
       → MarketAnalyzer.analyze()
       → market_snapshots 表
       → 飞书 DM 推送（signal=buy/warn 时）

── 已废弃 ──────────────────────────────────────────────
~~btiq_monitor.py (独立脚本) → 被 Pipeline.run_market_snapshot() 替代~~
~~09-14 盘中轻检 (pullback_predictor) → 已集成到 Pipeline~~
```

## 前端 (FastAPI + Jinja2 + HTMX)

```
用户浏览器 ←→ FastAPI (port 8080) ←→ SQLite + stocks.json
                    ↓
              Jinja2 模板渲染
                    ↓
              HTMX 动态交互
                    ↓
              SSE 实时日志流
                    ↓
              subprocess 调用 Pipeline
```

### 页面清单（v2.13）

| # | 页面 | 路由 | 布局 | 功能 | 状态 |
|---|------|------|------|------|------|
| 1 | 🏠 总览 | `/` `/dashboard` | 四层仪表盘 | 一句话摘要 + 行动预览 + 持仓卡片 + 折叠图表 | 🔨 v2.13 重构 |
| 2 | 📌 今日行动 | `/action` | 卡片列表 | 综合研判操作建议 + 决策按钮 | ✅ v2.9 |
| 3 | 📋 信号看板 | `/signals` | 表格 | 原始信号 + 评分条可视化 | 🔨 v2.13 美化 |
| 4 | 🔍 发现池 | `/discovery` | **卡片网格** | 自动发现候选股 + 评分条 + 一键升级 | 🔨 v2.13 重构 |
| 5 | 📰 事件流 | `/events` | **时间线** | 左侧时间线 + 右侧事件卡片 | 🔨 v2.13 重构 |
| 6 | 📈 T+N 跟踪 | `/tracking` | 表格 + 图表 | 入池后收益跟踪 | ✅ |
| 7 | 📊 策略回测 | `/backtest` | 表格 + 图表 | 历史信号收益 | ✅ |
| 8 | ⚙️ 系统控制 | `/system` | 仪表盘 | Pipeline 触发 + Cron | ✅ |
| 9 | 📒 持仓管理 | `/portfolio` | **分屏** | 左侧列表 + 右侧详情编辑 | 🔨 v2.13 美化 |
| 10 | 📉 超跌监控 | `/oversold` | 仪表盘 | BTIQ 趋势图 + MA5 + 信号时间线 | 🔨 v2.12 |

### 通用列表组件（v2.10）

所有列表页面复用 `list_controls` Jinja2 宏：
```
{% macro list_controls(search, sort_options, current_sort, current_order, page, total_pages, base_url, params) %}
  - 搜索框: ?search= 输入代码/名称
  - 排序: ?sort=score&order=desc 点击表头切换
  - 分页: ?page=2&page_size=20 HTMX 局部刷新
{% endmacro %}
```

后端统一通过 `paginate_query()` 函数处理分页参数和 COUNT 查询。

**技术栈：** FastAPI + Jinja2 + HTMX (本地) + Tailwind (本地) + SSE + Plotly (CDN按需)  
**数据源：** SQLite 直读（/data/smart_invest.db）+ stocks.json  
**Pipeline 触发：** subprocess.run() + SSE 实时输出  
**部署：** Docker Compose（与主系统同一容器）  
**详细 PRD：** → `docs/v2/frontend-prd.md`

### 前端 UX 架构（v2.13 重构）

**设计哲学：** 扫一眼就懂 > 信息完整。不同场景用不同布局，拒绝全局表格化。

#### 布局系统

| 布局类型 | 适用场景 | 使用页面 |
|----------|---------|---------|
| **仪表盘** | 概览、状态总览 | 总览、系统控制、超跌监控 |
| **卡片网格** | 候选浏览、对比选择 | 发现池 |
| **时间线** | 事件流、日志 | 事件流 |
| **操作卡片** | 有明确行动建议 | 今日行动 |
| **表格** | 需要逐行对比的数据 | 信号看板、T+N跟踪、回测 |
| **分屏** | 列表+详情/编辑 | 持仓管理 |

#### 信息层级规范

**总览页四层：**
```
┌─────────────────────────────────────────┐
│ 🟢 一句话状态摘要（大号文字+颜色强调）    │  ← 第一层：今日该看什么
├─────────────────────────────────────────┤
│ 📌 今日行动预览（2-3条卡片）              │  ← 第二层：最关键的操作
├──────────────────┬──────────────────────┤
│ 💼 持仓mini卡片  │ 📈 市场情绪          │  ← 第三层：扫一眼的状态
│ 福耀 ¥57.85     │ BTIQ: 79% 正常       │
│ 中国海油 ¥41.02 │ 上涨: 1200 下跌: 3800│
├──────────────────┴──────────────────────┤
│ 📊 信号趋势图（可折叠）                   │  ← 第四层：需要时查看的细节
│ 📰 最近事件（可折叠）                     │
└─────────────────────────────────────────┘
```

**发现池卡片网格：**
```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ 福耀玻璃      │  │ 鼎龙股份      │  │ 宝丰能源      │
│ 600660.SH    │  │ 300054.SZ    │  │ 600989.SH    │
│ ━━━━━━━━ 80分│  │ ━━━━━ 60分   │  │ ━━━━ 45分    │
│ 🔥 buy       │  │ 👀 watch     │  │ 👀 watch     │
│ 超预期 +9.1pp │  │ 扣非新高      │  │ 回调到位      │
│ [→ 持仓] [→ 关注] │  │ [→ 持仓] [→ 关注] │  │ [→ 持仓] [→ 关注] │
└──────────────┘  └──────────────┘  └──────────────┘
```

**事件流时间线：**
```
     03-28                    03-27                    03-26
       │                        │                        │
       ● ───────────────────── ● ───────────────────── ●
       │ 📈 福耀玻璃            │ 📉 中国海油            │ 📊 东方电气
       │ 超预期 +9.1pp          │ 减持公告               │ 重大合同
       │ 利好 · 汽车零部件      │ 利空 · 高管减持5%      │ 中性 · 风电项目
       │ 1d: +2.3%  5d: +5.1%  │                        │
```

#### 交互规范

| 交互 | 规范 |
|------|------|
| 决策按钮 | 点击 → spinner（0.5s）→ ✅ 成功反馈；卖出按钮二次确认弹窗 |
| 卡片升级 | 发现池 → 持仓/关注：一键操作，卡片翻转动画反馈 |
| 筛选变更 | URL params 更新，页面局部刷新（非整页重载） |
| 图表悬停 | Plotly hover 显示完整数据，暗色背景+白色文字 |
| 空状态 | 插画式空状态（☕ 今日无操作），带导航链接 |

#### 响应式断点

| 断点 | 布局 |
|------|------|
| ≥ 1024px (桌面) | 完整侧边栏 + 多栏布局 |
| 768-1023px (平板) | 汉堡菜单 + 双栏布局 |
| < 768px (手机) | 汉堡菜单 + **卡片式 feed**（彻底放弃表格） |

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

## 今日行动页架构（v2.9）

### 数据合成层（services.py → get_today_actions）

```
get_today_actions() = f(信号 × 持仓 × 行情 × 回调评分 × 发现池)

输入：
  - analysis_results (earnings_beat, profit_new_high, pullback_score)
  - stocks.json (持仓配置：target, stop_loss, entry, holding)
  - QuoteProvider (实时价格)
  - discovery_pool (是否在发现池)

输出（每个股票一条行动建议）：
  {
    priority: "buy" | "wait" | "adjust" | "none",
    emoji: "🔥" | "⏳" | "⚠️" | "☕",
    stock_code/name, current_price, change_pct,
    reasons: [...],          // 标签列表
    action_text: "...",      // "建议买入 ¥56-58，目标 ¥68，止损 ¥50"
    target, stop_loss,       // 来自 stocks.json
  }
```

### 决策规则

| 条件 | priority | action_text |
|------|----------|-------------|
| 超预期 + 回调到位 | buy 🔥 | 建议买入 ¥xx-xx，目标/止损 |
| 超预期 + 发现池内 | buy 🔥 | 建议买入 ¥xx-xx |
| 扣非新高 + 回调到位 | buy 🔥 | 可考虑买入 |
| 超预期但未回调 | wait ⏳ | 等回调再买 |
| 持仓 + 低于预期 | adjust ⚠️ | 考虑减仓，止损 ¥xx |

### 性能优化

- 只查持仓股 + 有信号的股票行情（≤20只），避免全库遍历
- 回调评分只查有信号的股票
- 加载时间：~3秒（含行情查询）

### 事件流架构

| 表 | 数据来源 | 内容 |
|---|---------|------|
| `events` | 新闻扫描（Step 3b） | 增持/减持/合同/财报/政策等公告 |
| `event_tracking` | Pipeline 信号 | 超预期/扣非新高 + T+N 收益 |

**合并显示：** 统一按时间倒序混排，卡片区分来源（新闻 vs 信号跟踪）

### 决策流转（v2.10 ✅ 已完成）

| 决策 | 效果 | 技术实现 |
|------|------|---------|
| 已买入 | 写 stocks.json + T+N 跟踪 | API 写入 stocks.json（entry/target/stop_loss） |
| 跳过 | 3天内不再出现 | 查 decision_log 最近3天 skip 记录过滤 |
| 观望 | 下次新信号再出现 | 查 decision_log 最近 watch 记录过滤 |

**设计原则：** 决策即行动，不是纯日志。已买入=加入跟踪池，跳过/观望=从今日行动页移除。

### 图表可视化（v2.11 ✅ 已完成）

| 页面 | 图表 | 说明 |
|------|------|------|
| 总览 | 近30天信号趋势 | Plotly area chart，按分析类型 |
| T+N跟踪 | 收益曲线 | Plotly line+marker，1d/5d/10d/20d |
| 回测 | 胜率+平均收益 | 双轴柱状图 |

**API：** `/api/chart/tn_returns`, `/api/chart/backtest_winrate`, `/api/chart/signal_trend`

### 持仓行情（v2.11 ✅ 已完成）

**实现：** QuoteProvider 接入，持仓页显示现价/涨跌/市值/盈亏金额/比例

### 卖出决策（v2.11 ✅ 已完成）

| 条件 | 动作 |
|------|------|
| 跌破止损价 | 卖出建议（优先级最高） |
| 达到目标价 | 获利了结建议 |
| 信号 avoid | 减仓建议 |

---

_架构 v2.10 | 2026-03-29 | 双通道架构（单股+市场）+ MarketSnapshotProvider + 超跌页面_
