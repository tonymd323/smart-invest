# Phase 2 结果报告

## 完成时间
2026-03-28 02:00 CST

## 交付物

### 1. pipeline.py — 统一数据采集入口
**路径**: `/root/.openclaw/workspace/smart-invest/pipeline.py`

**架构**:
```
stocks.json（跟踪池）
    +
discovery_pool 表（发现池）
    ↓
Pipeline.run()
    ├─ FinancialProvider → earnings 表
    ├─ ConsensusProvider → consensus 表
    └─ KlineProvider → prices 表
    ↓
DataQualityChecker（写入前校验）
    ↓
触发 analyzer.py（除非 --quiet）
```

**关键设计**:
- 股票池自动合并：`stocks.json`（持仓+备选）+ `discovery_pool`（active 状态）
- 并行采集：`ThreadPoolExecutor(max_workers=6)`，每只股票独立线程
- 速率控制：每个 Provider 独立超时，异常不中断其他 Provider
- 数据质量：范围检查、关键字段非空、异常值标记为 warning（不阻断）
- DB 路径正确传递：`_fetch_one_stock(db_path)` 使用传入路径，而非 `DB_PATH` 常量

**CLI**:
```bash
python3 pipeline.py                     # 全量采集 + 触发分析
python3 pipeline.py --quiet             # 只采集
python3 pipeline.py --codes 600660.SH   # 指定股票
python3 pipeline.py --skip-kline        # 跳过K线
python3 pipeline.py --workers 10        # 10线程并行
```

### 2. analyzer.py — 统一分析引擎
**路径**: `/root/.openclaw/workspace/smart-invest/analyzer.py`

**架构**:
```
SQLite DB
    ├─ earnings 表 ──→ 超预期分析（对比 consensus 表）
    ├─ earnings 表 ──→ 扣非新高分析（累计转单季度）
    ├─ prices 表  ──→ 回调买入评分（复用 calc_pullback_score）
    └─ consensus 表
    ↓
analysis_results 表
    ↓
discovery_pool 自动更新
    ↓
触发 pusher.py
```

**三种分析模式**:

| 模式 | --mode | 分析内容 |
|------|--------|---------|
| 全量 | full | 超预期 + 扣非新高 + 回调买入 |
| 仅财报 | earnings | 超预期 + 扣非新高 |
| 仅回调 | pullback | 回调买入评分 |

**分析逻辑复用**:
- 超预期：复用 `scanners/earnings_scanner.py` `_check_beat` 的核心评分逻辑
- 扣非新高：复用 `scanners/new_high_scanner.py` 的累计转单季度方法
- 回调买入：直接 `import scanners.pullback_scanner.calc_pullback_score`

**CLI**:
```bash
python3 analyzer.py                          # 全量分析
python3 analyzer.py --mode earnings          # 仅财报
python3 analyzer.py --mode pullback          # 仅回调
python3 analyzer.py --codes 600660.SH        # 指定股票
python3 analyzer.py --min-score 60           # 回调最低60分
python3 analyzer.py --no-push                # 不触发推送
```

### 3. 测试
**路径**: `/root/.openclaw/workspace/smart-invest/tests/test_phase2.py`

**28 个测试用例**:

| 类别 | 数量 | 覆盖 |
|------|------|------|
| DataQualityChecker | 4 | 有效数据/缺失字段/极端值/非数值 |
| Pipeline Write | 4 | earnings/consensus/prices 写入 + UPSERT |
| Pipeline E2E | 3 | 完整采集/Provider 异常/skip-kline |
| Analyzer 超预期 | 5 | 超预期/低于预期/无预期/过滤/写入DB |
| Analyzer 扣非新高 | 2 | 新高/非新高 |
| Analyzer 回调买入 | 3 | 评分计算/写入DB/数据不足跳过 |
| Analyzer E2E | 2 | 全量分析/仅财报模式 |
| Discovery Pool | 1 | 自动更新 |
| 回归验证 | 4 | daily_scan import / pullback_score import / Provider import / DB schema |

### 4. 现有代码改动（最小化）

**修改的文件**:
- `pipeline.py` — `_fetch_one_stock` 增加 `db_path` 参数，修复 DB 路径传递
- `analyzer.py` — 修复累计转单季度逻辑（正序处理 + 跳过首条无前值数据 + 取末元素为最新）

**未修改的文件**:
- `scanners/earnings_scanner.py` — 无改动
- `scanners/new_high_scanner.py` — 无改动
- `scanners/pullback_scanner.py` — 无改动
- `daily_scan.py` — 无改动
- `core/data_provider.py` — 无改动
- `core/database.py` — 无改动

## 测试结果
```
87 passed, 3 warnings in 4.13s
```
- Phase 0+1 旧测试：59/59 ✅
- Phase 2 新测试：28/28 ✅
- 回归验证：daily_scan.py import 正常 ✅

## 关键 Bug 修复
1. **DB 路径传递**: `_fetch_one_stock` 之前硬编码 `DB_PATH`，导致测试时写入错误的数据库。修复后使用传入的 `db_path` 参数。
2. **累计转单季度**: 原始逻辑从最新数据反向迭代，导致索引错误（`quarters[i-1]` 不是前一季度）。修复后改为正序处理 + 跳过首条无前值数据。
3. **最新值取错**: `quarterly_profits[0]` 是最早数据，应取 `[-1]`。修复。

## 架构验证

三层分离 ✅:
```
Pipeline（采集层）→ 只写 DB
Analyzer（分析层）→ 只读 DB → 写 analysis_results
daily_scan.py（1.0）→ 独立运行，不受影响
```

## 下一步
Phase 3：新闻 Provider + 事件驱动推送 + 发现池自动管理
