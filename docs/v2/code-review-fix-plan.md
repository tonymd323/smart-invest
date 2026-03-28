# 代码审查修复计划 v2.3.1

_日期：2026-03-28 | 审查发现：7 严重 + 4 改进项_

## 修复顺序（风险递减）

### Phase 1: 连接泄漏（7 处）— 严重
统一用 try/finally 包裹 sqlite3.connect()。

| # | 文件 | 方法 | 操作 |
|---|------|------|------|
| 1 | core/analyzer.py:58 | scan_beat_expectation | 加 try/finally |
| 2 | core/analyzer.py:203 | scan_new_high | 加 try/finally |
| 3 | core/analyzer.py:677 | PullbackAnalyzer.scan | 加 try/finally + close() |
| 4 | core/analyzer.py:1344 | DiscoveryPoolManager.scan | 加 try/finally |
| 5 | core/analyzer.py:1381 | OversoldScanner.scan | 加 try/finally |
| 6 | core/pipeline.py:98 | Pipeline.run | 加 try/finally |
| 7 | core/pipeline.py:311 | run_backtest | 加 try/finally + close() |

### Phase 2: SQL f-string 清理（4 处）— 中等
将 f-string SQL 替换为参数化或安全拼接。

| # | 文件 | 行号 | 操作 |
|---|------|------|------|
| 8 | core/analyzer.py:71 | scan_beat_expectation | f-string → 安全拼接 |
| 9 | core/analyzer.py:216 | scan_new_high | 同上 |
| 10 | core/analyzer.py:684 | PullbackAnalyzer.scan | 同上 |
| 11 | core/disclosure_scanner.py:210 | diff_with_db | 同上 |

### Phase 3: 测试验证
- 运行 25 个测试确认无回归
- 提交 + 更新 progress.md

## 验收标准
- [ ] 7 处连接全部有 finally 保护
- [ ] 4 处 SQL 无 f-string 动态拼接
- [ ] 25/25 测试通过
- [ ] 进度更新到 progress.md
