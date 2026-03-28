# Phase 1 结果报告

**日期：** 2026-03-28
**状态：** ✅ 通过

---

## 交付物

### 1. data_provider.py — 3 个 Provider 完成

| Provider | 主源 | 降级源 | 状态 |
|----------|------|--------|------|
| FinancialProvider | 东方财富 API (datacenter-web) | Tushare fina_indicator | ✅ |
| ConsensusProvider | 东方财富 F10 | AkShare 同花顺预期 | ✅ |
| KlineProvider | Tushare pro.daily() | 东方财富 push2his | ✅ |

**关键设计决策：**
- 字段映射兼容 v1（`PARENT_NETPROFIT`）和 v2（`PARENTNETPROFIT`），通过 `_get_field()` 优先级取值
- 降级逻辑内置，`last_source` 属性追踪实际使用的数据源
- 支持预注入数据（测试友好）和实时 API 调用（生产环境）
- 所有 Provider 继承 `BaseProvider` 抽象基类，运行时强制实现 `fetch()`

### 2. database.py — Schema 清理

| 操作 | 表 |
|------|-----|
| 删除 | `news`、`fund_flows`、`push_logs`（均为空表） |
| 新增 | `consensus`（一致预期） |
| 新增 | `discovery_pool`（发现池） |
| 新增 | `events`（结构化事件） |
| 保留 | `stocks`、`earnings`、`prices`、`analysis_results`、`event_tracking`、`backtest` |

**新增表 Schema：**
- `consensus`：stock_code UNIQUE，含 eps/net_profit_yoy/rev_yoy/num_analysts/source
- `discovery_pool`：stock_code UNIQUE，含 source/score/signal/status（active/promoted/expired/removed）
- `events`：含 event_type/title/sentiment/severity，支持宏观和个股事件

### 3. bitable_sync.py — 类合并

**合并前：** BitableSync（记录生成）+ BitableManager（去重/同步）
**合并后：** 统一 BitableSync 类

保留的公开方法：
- `generate_scan_records()` — 生成扫描记录
- `generate_backtest_records()` — 生成回测记录
- `dedup_records()` — 去重（支持 existing_records 和 existing_keys 两种模式）
- `sync()` — 完整同步流程（去重 → 导出 → 缓存更新）
- `from_preset()` — 预设配置工厂方法
- 向后兼容：`sync_scan_results()` / `sync_backtest()` 别名

### 4. test_helpers.py — 测试 Fixture 补充

新增：
- `MOCK_CONSENSUS` — 3 只股票的一致预期 mock 数据
- `MOCK_KLINE` — 2 只股票的日K mock 数据
- `mock_provider_fallback()` — 上下文管理器，模拟主源失败降级场景
- `assert_provider_fallback()` — 断言降级结果
- `create_mock_consensus_provider()` / `create_mock_kline_provider()` — 快速创建 mock Provider

### 5. 测试结果

```
59 passed, 0 failed
```

覆盖：
- Provider 正常获取数据（Financial/Consensus/Kline）
- Provider 降级逻辑（主源空 → 降级源成功）
- v2 字段映射（无下划线命名）
- v1 兼容性（下划线命名）
- 数据库新表 CRUD
- 旧表不受影响
- BitableSync 合并后所有功能正常
- Pipeline + Analyzer 端到端

---

## 已知限制

1. **实时 API 调用**依赖外部网络（东方财富/同花顺），测试中使用预注入数据绕过
2. **Tushare/AkShare** 需要安装对应库和 Token 才能使用降级路径
3. **Pipeline** 当前仅支持 FinancialProvider，Phase 2 需适配多 Provider

## 下一步

Phase 2 目标：
1. Pipeline 适配 ConsensusProvider + KlineProvider
2. consensus 表写入（Pipeline 集成）
3. Analyzer 扩展（事件分析器）
4. 端到端验证（Pipeline → Analyzer → Bitable）
