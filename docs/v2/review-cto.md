# JARVIS 投资系统 2.0 — CTO 技术评审

_评审人：小马（CTO）| 日期：2026-03-28 | 版本：v1.0_

---

## 一、架构合理性

### 1.1 三层分离总体合理，但边界模糊

**[建议]** Pipeline 和 Analyzer 的职责边界不够清晰。PRD 说"Pipeline 检测超预期 → 即时推 DM"，但超预期检测在 1.0 中是 `earnings_scanner.py`（属于 Analyzer 层）。如果 Pipeline 也做检测，那 Analyzer 做什么？建议明确：Pipeline 只负责"采集+写入 SQLite"，所有信号检测（超预期、扣非新高、回调买入）都归 Analyzer。

**[严重]** 架构图中 Pipeline 完成后"触发 Analyzer"，但没有说明触发机制。是 subprocess 调用？还是共享同一进程？如果用 subprocess，当前 `earnings_scanner.py` 已经有 `ThreadPoolExecutor` 并行逻辑，和 Pipeline 串行调 Provider 的描述矛盾。需要明确执行模型。

**[建议]** 双池设计是好思路，但 `discovery_pool` 表在 PRD 中提到但没有出现在数据模型 section。发现池的入选规则是什么？谁来决定"发现→跟踪"的升级？这些在架构文档中缺失。

### 1.2 层间耦合问题

**[严重]** `pullback_scanner.py` 的 `scan_pullback_buy()` 函数直接 `import tushare as ts` 并调用 `pro.daily()`——这是一个 Scanner（Analyzer 层）直接调用数据源，跳过了 Provider 层。2.0 如果要实现 Provider 抽象，这个函数必须重构。否则降级策略对回调买入扫描完全无效。

**[严重]** `earnings_scanner.py` 同样直接调用 `tushare.pro_api()` 和 `akshare`，没有经过任何抽象层。整个 1.0 的扫描器和数据源是硬耦合的。2.0 要做的 Provider 抽象本质上是一次大的重构，不是增量改造。

**[表扬]** `formulas.py` 的抽取做得不错，纯计算函数（EMA/RSI/MACD/KDJ/烧香拜佛）和数据源完全解耦，这是 2.0 中可以直接复用的资产。

---

## 二、数据模型

### 2.1 新增表

**[建议]** `consensus` 表在 PRD 中提到但 Schema 中没有。需要补充：至少包含 stock_code, report_period, profit_yoy_expected, rev_yoy_expected, source, updated_at。

**[严重]** `discovery_pool` 表完全缺失。双池是 2.0 的核心卖点之一，连表结构都没设计。需要定义：stock_code, discovery_date, discovery_reason, discovery_score, pool_status, auto_expire_date 等。

**[严重]** `events` 表在架构中提到写入 `events` 表，但 Schema 中没有。`event_tracking` 和 `backtest` 表存在但没有被 PRD 引用——这两个表和新设计的 `events` 表是什么关系？会重叠吗？

### 2.2 字段完整性

**[建议]** `analysis_results` 表的 `analysis_type` 枚举值没有文档化。现有值是 `news_sentiment / earnings_beat / technical / risk`，但 2.0 要新增 `pullback_score / event_alert` 等。建议加 CHECK 约束或至少在 Schema 注释中枚举。

**[建议]** `earnings` 表有 `quarterly_profit_new_high` 字段，但 `earnings_scanner.py` 中的扣非新高检测结果并没有写入这张表（只写入 Bitable）。2.0 需要确保 SQLite 是 SSOT，Bitable 只是展示层。

**[表扬]** `event_tracking` 表设计得相当好，T+N 收益追踪、基准对比、alpha 计算都有，是一个实用的回测基础设施。

### 2.3 索引策略

**[建议]** 现有索引基本合理，但 `analysis_results` 表的 UNIQUE 约束 `(stock_code, analysis_type, created_at)` 有问题——`created_at` 是 `datetime('now', 'localtime')`，同秒内多次写入会冲突。改为 `(stock_code, analysis_type, date(created_at))` 更安全。

**[严重]** `backtest` 表没有索引。按 `(stock_code, event_date)` 查询是常见操作，至少需要一个 UNIQUE 约束或索引。而且这张表没有 `id` 主键列——SQLite 会自动用 rowid，但不符合其他表的命名惯例。

**[建议]** `news` 表如果有事件检测需求，应该加 `(sentiment, published_at)` 联合索引，否则 EventAnalyzer 扫描"过去 30 分钟的负面新闻"会全表扫描。

---

## 三、Provider 设计

### 3.1 抽象正确性

**[建议]** 5 个 Provider 的拆分合理（Financial / Consensus / Kline / Quote / News），但命名不一致。PRD 用 `FinancialProvider`，架构用同样的名字，但 1.0 代码中没有对应的类——这是全新实现，不是重构。确认没有遗漏的 Provider 吗？比如 `fund_flows` 表对应的资金流向数据，当前由谁采集？

**[严重]** 降级策略只写了一句话："东方财富→Tushare"。但每个 Provider 的降级逻辑不同：
- FinancialProvider：东方财富 HTTP vs Tushare SDK（数据格式完全不同）
- ConsensusProvider：东方财富 F10 vs AkShare（字段名不同）
- KlineProvider：Tushare vs 东方财富（推送 vs 拉取模式不同）

每种降级都需要数据格式转换层。这不只是一行 `try/except`，是需要写适配器的。建议为每个 Provider 定义标准输出格式（dataclass 或 TypedDict），然后各数据源实现 `fetch() -> StandardFormat`。

**[严重]** QuoteProvider（实时行情）写的是"东方财富 Push2→腾讯"，但 Push2 是 WebSocket 推送协议，而腾讯行情 API 是 HTTP 轮询。两种模式的抽象接口完全不同（stream vs poll），强行统一到一个 Provider 接口会很别扭。建议 QuoteProvider 内部用迭代器/回调模式，屏蔽底层差异。

### 3.2 线程安全

**[严重]** `earnings_scanner.py` 中每个线程 `pro_local = ts.pro_api()` 是正确的（Tushare 非线程安全）。但如果 Pipeline 也用了 Tushare SDK，串行调用没问题；如果 Analyzer 并行调用，要确保每个 Provider 实例是线程安全的。建议 Provider 用工厂模式，每次调用创建新实例或用连接池。

---

## 四、可扩展性

### 4.1 加新数据源

**[严重]** 当前要加一个新数据源（比如 Wind），改动范围：新建一个数据获取函数 → 修改 scanner 中的硬编码 import → 修改降级逻辑。至少涉及 2-3 个文件。2.0 的 Provider 抽象如果做好了，应该只改 1 个文件（新增 Provider 实现）+ 1 行配置（注册到降级链）。

**[建议]** 建议用注册模式：
```python
PROVIDERS = {
    'financial': [EastmoneyFinancialProvider, TushareFinancialProvider],
    'kline': [TushareKlineProvider, EastmoneyKlineProvider],
}
```
这样加数据源就是加一个类 + 注册，不需要改 Pipeline 代码。

### 4.2 加新分析器

**[表扬]** `pullback_scanner.py` 的四层漏斗设计很清晰，加新分析器（比如 P1 中的 A-05 超跌）只需要新建一个同结构的函数。formulas.py 的公共层保证了代码复用。

**[建议]** 但 Analyzer 的调度是硬编码的（`earnings_scanner.scan_earnings_beat()` + `pullback_scanner.scan_pullback_buy()`）。建议用注册模式，Analyzer 只需要遍历已注册的分析器列表。

---

## 五、技术债务

### 5.1 必须清理的

**[严重]** `database.py` 的 SCHEMA_SQL 中有 9 张表，但 PRD 说"删 3 张空表"。哪 3 张？`fund_flows` 和 `push_logs` 看起来使用率低（fund_flows 在 scanner 中没有采集逻辑，push_logs 的 status 字段没有被更新的代码）。如果确实不用了，删掉；如果只是暂时不用，标注为 P1。

**[严重]** `bitable_sync.py` 中 `BitableSync` 和 `BitableManager` 两个类功能高度重叠（都有 dedup、sync）。这是 1.0 迭代中遗留的双轨实现。2.0 应该合并为一个类。

**[严重]** `bitable_sync.py` 中 `sync_scan_results()` 返回的是 records list 而不是实际写入——注释说"暂不通过 CLI 写入"。这意味着 Bitable 写入实际上是在 Agent 层完成的。2.0 要明确这个边界：Pusher 是否应该直接调用 `feishu_bitable_app_table_record` 工具？还是导出 JSON 让 Agent 写？

**[建议]** `earnings_scanner.py` 中 `_fetch_disclosed_list()` 函数长达 150+ 行，包含了 6 种不同的数据源尝试（forecast / express / disclosure_date / fina_indicator / akshare）。这是 1.0 的补丁式开发产物。2.0 应该拆成独立的 Provider，每个返回标准化的 disclosure list。

**[建议]** `pullback_scanner.py` 中 `_check_market_env()` 直接 import tushare，和 scanner 的其他部分耦合。应该独立为 MarketEnvProvider 或作为 Pipeline 预计算结果传入。

### 5.2 建议清理的

**[建议]** `formulas.py` 中 `calc_kdj()` 用 for 循环实现，可以向量化提速（虽然数据量小无所谓）。`calc_rsi()` 同理。

**[建议]** `bitable_sync.py` 中 `_date_to_ts()` 和 `_parse_date()` 两个函数功能类似，合并。

**[建议]** 整个项目没有 type hints 的一致性。有的函数有，有的没有。2.0 应该统一（至少 Provider 接口要有）。

---

## 六、工时评估

### 6.1 总工时 9.5h 不现实

**[严重]** 9.5h 严重低估。以下是 CTO 视角的修正估算：

| Phase | 文档估算 | CTO 修正 | 理由 |
|-------|---------|---------|------|
| 1. Provider + DB 清理 | 2h | 3-4h | 5 个 Provider 各需要实现 + 降级逻辑 + 数据格式适配器，光 EastmoneyFinancialProvider 的 HTTP 接口调试就可能花 1h |
| 2. Pipeline + Analyzer | 3.5h | 4-5h | Pipeline 要串行调 5 个 Provider + 质量校验 + 触发 Analyzer；Analyzer 要重构 earnings_scanner 和 pullback_scanner 去耦合；EventAnalyzer 是全新开发 |
| 3. Pusher + 事件 + 测试 | 3h | 3-4h | 相对靠谱，但"降级测试（模拟东方财富挂掉）"这个 task 本身就要 1h+ |
| 4. 验收 | 1h | 2h | 对比 1.0 和 2.0 的输出需要构造 test fixtures + 运行两端 + diff |
| **总计** | **9.5h** | **12-15h** | — |

### 6.2 最大风险 Phase

**[严重]** **Phase 1 风险最大**。原因：
1. 东方财富 API 没有官方文档，全靠逆向工程。接口随时可能变。
2. 5 个 Provider 中的 EastmoneyFinancialProvider 和 ConsensusProvider 的 HTTP 接口格式（URL、参数、返回 JSON 结构）在现有代码中完全不存在——它们还是"待接入"状态。
3. 数据格式标准化（东方财富 vs Tushare 的字段映射）工作量容易被低估。

**[建议]** 建议 Phase 1 先只实现 1 个 Provider（FinancialProvider），跑通"采集→写入 SQLite→Analyzer 读取"全链路，验证架构可行性。不要一次性写 5 个 Provider。

### 6.3 PRD 的 16 个 P0 项

**[严重]** 9.5h 要做 16 个 P0 功能项？平均每个 35 分钟？这不现实。建议砍掉 P0 的范围：
- P-01/P-02/P-03（3 个 Pipeline 采集）是 P0
- I-01/I-02（SSOT + Provider）是 P0
- A-01/A-02/A-03（超预期/扣非/回调）是 P0（复用 1.0）
- A-04（回调买入预测）是 P1（全新功能，9.5h 内不可能做好）
- A-08（事件检测）是 P1（EventAnalyzer 全新开发）
- U-01/U-02/U-03/U-05（推送）中，日报是 P0，即时 DM 是 P1

---

## 七、测试策略

### 7.1 当前测试状态

**[严重]** 项目没有测试文件。没有 `tests/` 目录，没有 pytest，没有 mock。`pullback_scanner.py` 的 `__main__` 只是一个手动测试入口，`earnings_scanner.py` 连这个都没有。

### 7.2 2.0 测试建议

**[严重]** "对比 1.0 和 2.0 输出结果"这个验收标准需要具体的 test fixture：
1. 固定一组股票 + 固定日期的快照数据（CSV/JSON）
2. 用 1.0 代码跑一遍，保存输出
3. 用 2.0 代码跑同一组数据，diff 输出
4. 数值差异在允许范围内（比如浮点精度 0.01）

建议在 Phase 1 就建立 fixture，不要等 Phase 4 才做。

**[建议]** 每个 Provider 需要一个 mock 测试：当东方财富 API 返回 500 时，验证降级到 Tushare 是否正确。这种测试在本地就能跑，不需要真实 API。

**[建议]** Analyzer 的核心逻辑（超预期判定、回调评分）是纯函数，非常容易测试。给定固定的 DataFrame 输入，断言固定的输出。这应该是测试优先级最高的部分。

---

## 八、其他问题

### 8.1 Cron 时间线矛盾

**[建议]** PRD 说"发现池日报 21:00"，架构说"21:00 发现池日报（由 Pipeline 结果生成）"。但 Pipeline 的 cron 是 15:15。中间 6 小时的延迟是怎么回事？如果是数据积累（比如晚间还有披露），可以理解。但要写清楚 21:00 的日报是用 15:15 的 Pipeline 结果，还是重新跑一次 Pipeline。

### 8.2 Bitable 作为展示层 vs SSOT

**[建议]** PRD 说 SQLite 是 SSOT，Bitable 只是展示。但 1.0 中 BitableManager 维护了独立的去重缓存（`bitable_existing_*.json`），形成了双 SSOT 的局面。2.0 应该明确：Bitable 同步是幂等的，以 SQLite 为准，每次全量对比。

### 8.3 错误处理

**[严重]** 现有代码的错误处理是 `except Exception: pass`（见 `_fetch_disclosed_list` 中的多个 try/except）。这会吞掉所有错误，生产环境出了问题无法排查。2.0 的 I-04（错误处理）应该是 P0 而不是 P1。至少要有：
- 日志记录异常详情
- 降级时的通知机制
- 失败重试（至少 1 次）

### 8.4 事件检测的实时性

**[建议]** PRD 要求"新闻采集后 5 分钟内检测+推送"，但 Cron 最细粒度是"每 30 分钟"。5 分钟延迟和 30 分钟采样间隔矛盾。要么改 Cron 为每 5 分钟，要么接受最大 35 分钟的延迟（30 分钟采样 + 5 分钟处理）。

---

## 九、总结

| 维度 | 评分 | 一句话 |
|------|------|--------|
| 架构合理性 | ⭐⭐⭐⭐ | 三层分离方向对，但边界需细化 |
| 数据模型 | ⭐⭐⭐ | 核心表设计好，但有 3 张关键表缺失 |
| Provider 设计 | ⭐⭐⭐ | 抽象正确，但降级策略低估了复杂度 |
| 可扩展性 | ⭐⭐⭐⭐ | formulas.py 是亮点，但调度需要注册模式 |
| 技术债务 | ⭐⭐ | 1.0 硬耦合严重，Provider 抽象≈重构 |
| 工时评估 | ⭐⭐ | 9.5h 严重低估，实际 12-15h |
| 测试策略 | ⭐ | 零测试基础，这是最大的风险 |

**CTO 结论：** 架构方向正确，但执行计划过于乐观。建议：
1. **砍 P0 范围**：把 A-04、A-08、即时 DM 降到 P1
2. **修正工时**：12-15h 是保守估计
3. **Phase 1 先做 spike**：1 个 Provider 跑通全链路再铺开
4. **先写测试 fixture**：否则 Phase 4 验收无法进行
5. **错误处理提升到 P0**：`except: pass` 在生产环境是定时炸弹

---

_评审完成 | 2026-03-28 | 小马_
