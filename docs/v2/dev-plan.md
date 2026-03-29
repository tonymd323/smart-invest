# JARVIS 投资系统 2.0 — 开发方案

_版本：v2.1 | 日期：2026-03-28 | P0 完成，P1 并行开发_

---

## 一、Agent 角色与职责

| Agent | 角色 | 职责 |
|-------|------|------|
| **JARVIS** | CEO | 任务分配、最终验收、上线切换 |
| **小马** | CTO | 全部代码开发、测试 |
| **小巴** | CIO | Cron 运维、Bitable 维护 |

## 二、已完成阶段

### Phase 0-4: P0 开发（✅ 3/28 完成）

| Phase | 内容 | 状态 |
|-------|------|------|
| Phase 0 | Spike 验证（东方财富 API 全链路） | ✅ |
| Phase 1 | 3 Provider + DB 清理 | ✅ |
| Phase 2 | Pipeline + Analyzer | ✅ |
| Phase 3 | Pusher + 集成测试 | ✅ |
| Phase 4 | 验收（106/106 passed） | ✅ |

### v2.1 修复（✅ 3/28 完成）

| 修复 | 说明 | 状态 |
|------|------|------|
| quarterly_net_profit | 累计→单季度差值计算 | ✅ |
| 超预期 N/A 处理 | 无 consensus 股票标 N/A | ✅ |
| NOTICE_DATE 扫描 | DisclosureScanner | ✅ |
| Cron 上线 | 3 个新任务 + 1 个禁用 | ✅ |

## 三、P1 并行开发方案（周末）

### Phase 5A: P0 高优（小马线 A）

**任务：** NewsProvider + 双池完整 + T+N 跟踪
**预估：** 4.5h（开发 3h + 测试 1.5h）
**依赖：** 无

```
线 A：
  ├─ core/data_provider.py
  │   └─ 新增 NewsProvider 类
  │       ├─ RSS 源采集（复用现有 rss-reader skill）
  │       ├─ 东方财富个股新闻 API
  │       └─ 降级：RSS → 东方财富
  │
  ├─ core/analyzer.py
  │   └─ 新增方法：
  │       ├─ auto_discover_pool() — 超预期/新高自动入池
  │       ├─ create_tn_tracking() — 入池后自动创建 T+N 跟踪
  │       └─ update_tn_tracking() — 更新 T+N 日收益
  │
  └─ tests/test_real.py
      ├─ T14: NewsProvider 采集
      ├─ T15: 发现池自动入场
      └─ T16: T+N 跟踪创建+更新
```

### Phase 5B: P1 集成（小马线 B）

**任务：** 回调预测集成 + 回测集成 + QuoteProvider
**预估：** 6h（开发 4.5h + 测试 1.5h）
**依赖：** 无（与 5A 并行）

```
线 B：
  ├─ core/data_provider.py
  │   └─ 新增 QuoteProvider 类
  │       ├─ 腾讯行情 API（复用 scripts/btiq_monitor.py 逻辑）
  │       ├─ 东方财富 Push2 备用
  │       └─ 统一接口：fetch(code) → QuoteData
  │
  ├─ core/analyzer.py
  │   └─ 新增 PullbackAnalyzer 类
  │       ├─ 复用 scanners/pullback_scanner.py 四层漏斗
  │       ├─ 复用 scanners/pullback_predictor.py 预测逻辑
  │       └─ 标准化输出写入 analysis_results
  │
  ├─ core/pipeline.py
  │   └─ 新增回测集成
  │       ├─ Pipeline.run() 后自动触发 backtest_update 逻辑
  │       └─ 复用 backtest_update.py 计算 T+N 收益
  │
  └─ tests/test_real.py
      ├─ T17: QuoteProvider 采集
      ├─ T18: PullbackAnalyzer 评分
      └─ T19: 回测计算
```

### Phase 5C: 联调 + P2 收尾（周日）

**任务：** 5 Provider 联调 + P2 功能 + 全量测试
**预估：** 3h
**依赖：** 5A + 5B 完成

```
联调：
  ├─ 5 Provider 全链路测试
  ├─ Pusher 集成回调 DM + 事件 DM
  ├─ discovery_pool 升级操作（U-07）
  └─ tests/test_real.py 全量跑（T1-T20+）
```

## 四、工时估算

| 阶段 | 线 | 负责人 | 开发 | 测试 | 总计 |
|------|----|--------|------|------|------|
| Phase 5A | A | 小马 | 3h | 1.5h | 4.5h |
| Phase 5B | B | 小马 | 4.5h | 1.5h | 6h |
| Phase 5C | 联调 | 小马 | 2h | 1h | 3h |
| **总计** | | | **9.5h** | **4h** | **~13.5h** |

**并行模式：** 5A + 5B 同时启动 → 周六完成 → 周日 5C 联调 → 周一测试

## 五、代码审查

| 变更类型 | 审查人 | 规则 |
|---------|--------|------|
| 新增 Provider | 小马自审 | 接口符合 BaseProvider 规范 |
| Analyzer 扩展 | 小马 → JARVIS | 确认分析逻辑正确 |
| Pusher 变更 | 小马自审 | 不影响现有推送 |
| 上线切换 | JARVIS | CEO 最终批准 |

## 六、风险

| 风险 | 对策 |
|------|------|
| 东方财富 API 限流 | 自动降级到 Tushare/AkShare |
| 改动影响 1.0 | v2.1 Cron 已独立，1.0 不受影响 |
| QuoteProvider 接口复杂 | 先做 HTTP 轮询版，WebSocket 后续迭代 |
| 测试覆盖不足 | 每个 Phase 强制跑 test_real.py |

---

## Phase 6: 数据质量修复（待启动）

### P0 修复（2-3h）

**scan_new_high 过滤：**
- 改 core/analyzer.py 的 scan_new_high 方法
- 增加 WHERE quarterly_net_profit > 0 过滤
- is_new_high=False 时不写入 analysis_results（或 signal=N/A）

**T+N 收益更新：**
- Cron 21:00 扫描后自动调用 update_tn_tracking()
- 从 prices 表获取入池后 N 日收盘价
- 计算 T+1/5/10/20/60 收益率

**Bitable 去重：**
- 写入前 feishu_bitable_app_table_record list 现有记录
- 按 (stock_code, report_date) 去重后只写新增

### P1 修复（2-3h）

**auto_discover_pool 字段补全：**
- 入池时查询公司名称（stocks.json + 腾讯行情 API）
- 查询市值（QuoteProvider 或东财 API）

**EventAnalyzer 格式化：**
- title 改为 "公司名 事件描述"
- detail 改为人类可读文本

### 测试

新增 T26-T30：
- T26: scan_new_high 过滤亏损股
- T27: T+N 收益计算
- T28: auto_discover_pool 名称+市值
- T29: EventAnalyzer 格式化
- T30: Bitable 去重

---

## Phase 7: 前端开发（确认启动）

**技术栈：** FastAPI + Jinja2 + HTMX + SSE + Plotly + Tailwind CDN  
**PRD：** → `docs/v2/frontend-prd.md`

### 开发排期

| 阶段 | 内容 | 工时 |
|------|------|------|
| P0 | 系统控制页（Pipeline 触发 + 实时日志） | 2h |
| P0 | 今日行动页（信号合成 + 建议） | 1.5h |
| P1 | 信号看板 + 发现池（筛选排序 + 决策记录） | 2.5h |
| P1 | 持仓快照（实时行情） | 1h |
| P2 | 事件流 + T+N 跟踪（图表） | 2h |
| P2 | 策略胜率看板 + 回测 | 1.5h |
| P3 | Docker 化部署 | 1h |
| **总计** | | **11.5h** |

### 核心创新

- **今日行动页**：系统直接告诉用户「该做什么」，而非被动浏览
- **决策记录**：用户标记每条信号的采纳/未采纳，系统计算采纳胜率
- **实时日志**：SSE 流式输出 Pipeline 运行过程

---

_开发方案 v2.4 | 2026-03-29 后端完成 + 前端脚手架完成 + 待页面交互完善 + Docker 化_

---

## 当前状态

### ✅ 已完成
- Phase 0-5C 全部后端开发 + 数据质量修复 + 超预期 v2.5
- 前端脚手架：FastAPI + 8 路由 + 9 模板 + SSE + 决策记录

### ⏳ 待做

| 任务 | 预估 | 优先级 |
|------|------|--------|
| events.html 完善（空模板） | 0.5h | 🔴 |
| Plotly 图表（T+N曲线 + 回测柱状 + 胜率） | 2h | 🟡 |
| 今日行动页逻辑增强 | 1h | 🟡 |
| 决策记录完整化（原因+胜率统计） | 1h | 🟡 |
| 持仓快照实时行情 | 0.5h | 🟢 |
| Docker 化部署 | 1h | 🟡 |
| CTO 审计遗留（8项 P2） | 按需 | ⚪ |
