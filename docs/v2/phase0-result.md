# Phase 0 Spike 结果

_时间：2026-03-28 01:35 | 状态：✅ 全部通过_

## 完成内容

1. `tests/fixtures/test_helpers.py` — SQLite 内存 DB helper + mock 数据
2. `tests/spike_financial_provider.py` — Live API 全链路测试
3. `core/database.py` — 添加 `PRAGMA journal_mode=WAL`
4. pytest 套件：21/21 passed

## 关键发现：字段名修正

| 我们假设的字段 | 东方财富实际字段 |
|--------------|----------------|
| PARENT_NETPROFIT | **PARENTNETPROFIT** |
| TOTAL_OPERATE_INCOME | **TOTALOPERATEREVE** |
| KCFJCXSYJLR（当作YoY） | 实际是扣非净利润**值** |
| YoY 增速 | **PARENTNETPROFITTZ** |
| 单季度营收 YoY | **DJD_TOI_YOY** |

ROE(ROEJQ)、毛利率(XSMLL)、EPS(EPSJB) 一致。

## 结论

- 东方财富 API ✅ 可替代 Tushare
- API 响应 ~100ms，免费无 token
- 字段映射表已确认，可进入 Phase 1
