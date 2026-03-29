# JARVIS 投资系统 — 功能说明

_版本：v2.18 | 更新：2026-03-29 16:27_

---

## 系统概述

A 股投资辅助系统，自动扫描财报披露、检测超预期信号、管理发现池、跟踪收益，提供可视化前端仪表盘。

## 核心功能

### 1. 📡 数据采集 Pipeline

自动扫描公告 → 采集财务数据 → 计算指标 → 写入数据库

| 模块 | 功能 | 数据源 |
|------|------|--------|
| DisclosureScanner | 扫描新披露公告（NOTICE_DATE） | 东方财富 |
| FinancialProvider | 财务三表 + 季度利润 | 东方财富 / Tushare |
| ConsensusProvider | 券商一致预期（双源取 max） | AkShare + 东方财富 F10 |
| QuoteProvider | 实时行情 | 腾讯行情 API / 东方财富 |
| NewsProvider | 个股新闻 | RSS + 东方财富 |
| KlineProvider | 日 K 线 | Tushare / 东方财富 |
| SectorProvider | 板块数据 | 东方财富 |

### 2. 🔍 信号分析

| 分析器 | 功能 | 输出 |
|--------|------|------|
| EarningsAnalyzer | 超预期检测（实际 vs 预期 ≥ 5%） | buy/watch/hold/avoid |
| EarningsAnalyzer | 扣非净利润新高检测 | buy/watch |
| PullbackAnalyzer | 回调买入四层漏斗评分 | 0-100 分 + signal |
| EventAnalyzer | 新闻事件检测（12 类关键词） | 利好/利空/中性 |
| OversoldScanner | 全市场超跌扫描（BTIQ 涨跌比） | buy_signal/caution/normal |
| MarketAnalyzer | 市场情绪分析 | BTIQ + MA5 趋势 |
| DiscoveryPoolManager | 自动发现池入场/过期（7 天） | 入池/过期 |

### 3. 📈 T+N 收益跟踪

入池股票自动跟踪 1/5/10/20 日收益，计算超额收益（alpha）

### 4. 📊 策略回测

历史信号收益统计，胜率 + 平均收益 + 最大回撤

### 5. 📰 事件流

新闻公告 + 信号跟踪统一时间线，按日期分组，支持筛选

### 6. 📌 决策流转

| 操作 | 效果 |
|------|------|
| 已买入 | 写入 stocks.json 入池跟踪 |
| 卖出 | 从 stocks.json 移除 + T+N 完成 |
| 跳过 | 3 天内不在今日行动出现 |
| 观望 | 新信号再现时出现 |

---

## 前端页面（10 页）

| 页面 | 路由 | 功能 |
|------|------|------|
| 🏠 总览 | `/` | 状态横幅 + 行动预览 + 持仓 mini + 图表折叠 |
| 📌 今日行动 | `/action` | 综合研判：信号×持仓×行情×回调 → 操作建议 + 决策按钮 |
| 📋 信号看板 | `/signals` | 超预期/扣非新高/回调买入信号，筛选+排序+分页 |
| 🔍 发现池 | `/discovery` | 自动发现候选股，卡片网格 + 评分条 + 升级入口 |
| 📰 事件流 | `/events` | 新闻+信号时间线，日期分组，多维筛选 |
| 📈 T+N 跟踪 | `/tracking` | 入池收益曲线 + 表格（横滑 + 入池时间 + 持有天数） |
| 📊 策略回测 | `/backtest` | 历史信号收益 + 胜率柱状图 |
| 📒 持仓管理 | `/portfolio` | 持仓/关注/发现池候选，增删改 + 卖出 + 搜索添加 |
| 📉 超跌监控 | `/oversold` | BTIQ 涨跌比趋势图 + 超跌信号时间线 |
| ⚙️ 系统控制 | `/system` | Pipeline 触发（SSE 日志）+ Cron 管理 + 自定义时间窗口 |

## 数据库（10 张表）

| 表 | 行数 | 用途 |
|----|------|------|
| stocks | 6,060 | 股票清单 |
| earnings | 4,686 | 财务数据 + 季度利润 |
| consensus | 9 | 券商一致预期 |
| analysis_results | 357 | 分析结果（信号+评分） |
| discovery_pool | 72 | 自动发现池 |
| events | 4 | 新闻事件 |
| event_tracking | 286 | T+N 收益跟踪 |
| decision_log | 14 | 决策记录 |
| backtest | 160 | 回测记录 |
| market_snapshots | 41 | 市场快照（超跌） |

## 定时任务（Cron）

| 时间 | 任务 | 说明 |
|------|------|------|
| 15:15 | 回调买入扫描 | PullbackAnalyzer 全市场扫描 |
| 15:35 | 盘后全量扫描 | Pipeline 6h 窗口 + 300 只 |
| 20:30 | 晚间扫描 | Pipeline 6h 窗口 |
| 06:00 | Web 服务重启 | 自动恢复 |
| 周日 10:00 | 回测更新 | 全量回测 |

## 技术栈

- **后端：** Python 3.11 + FastAPI + SQLite
- **前端：** Jinja2 + Tailwind CSS + Alpine.js + Plotly
- **部署：** Docker（docker compose up -d）
- **数据：** 东方财富 + AkShare + 腾讯行情 + RSS
