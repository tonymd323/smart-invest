# Phase 3 完成报告 — Pusher 推送层 + 集成测试

**日期**：2026-03-28  
**状态**：✅ 全部完成  
**测试**：106/106 通过（87 旧 + 19 新）

---

## 完成内容

### 1. card_generator.py 清理

**删除 7 个未使用方法**：
- `generate_daily_card` — 每日研报
- `generate_stock_card` — 个股分析
- `generate_pool_card` — 备选股池
- `generate_alert_card` — 预警
- `generate_surprise_card` — 超预期消息
- `generate_close_card` — 收盘简报
- `generate_open_check_card` — 开盘检查

**删除辅助代码**：
- `HoldingInfo` / `AlertInfo` dataclass
- `_fmt_change` / `_fmt_index` 静态方法

**保留**：
- `generate_daily_scan_card` — 飞书交互式卡片（含原生 table）
- `truncate` — 消息截断（feishu_pusher.py 依赖）

### 2. pusher.py — 统一推送入口

**文件**：`/root/.openclaw/workspace/smart-invest/pusher.py`（330 行）

**架构**：
```
Pusher.run(mode)
  ├── load_scan_results(db)  → beats / new_highs / pullback_signals
  ├── CardGenerator.generate_daily_scan_card() → 飞书卡片 JSON
  ├── load_pool_summary(db)  → active / promoted / expired 分组
  ├── generate_pool_card()   → 跟踪池概要卡片
  └── BitableSync.sync()     → 去重同步到飞书多维表格
```

**CLI 用法**：
```bash
python3 pusher.py                    # scan 模式（默认）
python3 pusher.py --mode scan        # 发现池日报
python3 pusher.py --mode pool        # 跟踪池概要
python3 pusher.py --mode all         # 全部
python3 pusher.py --no-bitable       # 不同步 Bitable
python3 pusher.py --db-path /tmp/x.db  # 指定 DB（测试用）
```

**设计约束满足**：
- ✅ 只读 DB，不调 Provider
- ✅ 复用 card_generator.generate_daily_scan_card
- ✅ Bitable 去重由 BitableSync 处理
- ✅ 输出 JSON 到 stdout（供 cron agent 读取）

### 3. tests/test_integration.py — 集成测试

**19 个测试，5 个测试类**：

| 类 | 测试数 | 覆盖 |
|---|---|---|
| TestPusherUnit | 5 | 初始化、scan/pool/all 模式、JSON 输出 |
| TestPusherDataFlow | 3 | 空 DB、有数据解析、状态分组 |
| TestEndToEndIntegration | 5 | 完整链路、discovery_pool 写入、卡片结构 |
| TestDailyScanUnchanged | 3 | import、generate_daily_scan_card、truncate |
| TestCardGeneratorCleanup | 3 | 旧方法删除验证、保留方法验证、旧 dataclass 验证 |

**pipeline_db fixture**：
- 预填 stocks（福耀玻璃、中国海油）
- 预填 earnings（多季度，含超预期数据）
- 预填 consensus（福耀玻璃有预期、中国海油无预期）
- 预填 prices（120 天随机 K 线）

### 4. 完整链路验证

```
Pipeline DB (预填数据)
  → analyzer.py --mode full --codes 600660.SH,600938.SH
  → SQLite: analysis_results + discovery_pool
  → pusher.py --mode all --no-bitable
  → stdout JSON (卡片数据)
```

### 5. 回归验证

- daily_scan.py 正常 import CardGenerator
- generate_daily_scan_card 功能不变
- truncate 方法保留（feishu_pusher.py 依赖）
- 旧测试 87/87 全部通过

---

## 文件变更清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `notifiers/card_generator.py` | 修改 | 删除 7 个旧方法 + 辅助代码 |
| `pusher.py` | 新建 | 统一推送入口 |
| `tests/test_integration.py` | 新建 | 19 个集成测试 |
| `docs/v2/progress.md` | 更新 | Phase 3 进度 |
| `docs/v2/phase3-result.md` | 新建 | 本文件 |

---

## 下一步：Phase 4

- 新闻 Provider（RSS + 东方财富个股新闻）
- 事件驱动推送
- Cron 任务配置
