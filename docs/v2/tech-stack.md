# JARVIS 投资系统 2.0 — 技术栈

_版本：v0.1 | 日期：2026-03-27_

---

## 技术选型

### 语言与运行时

| 项目 | 选型 | 说明 |
|------|------|------|
| 语言 | Python 3.11 | 现有代码基础，生态丰富 |
| 包管理 | pip + requirements.txt | 现有方案 |
| 运行环境 | OpenCloudOS 9.4 / Docker | 服务器已有 |

### 数据采集

| 数据源 | 用途 | 接口方式 | 状态 |
|--------|------|---------|------|
| **东方财富** | 财务数据/一致预期/实时行情/板块 | HTTP REST | 待接入（2.0 主力） |
| **Tushare** | 财务数据/日K/回测 | Python SDK | 现有，降级备用 |
| **AkShare** | 一致预期 | Python 函数 | 现有，降级备用 |
| **腾讯行情API** | 全市场实时报价 | HTTP GET | 现有 |
| **RSS** | 新闻资讯 | rss_reader skill | 现有 |

### 数据存储

| 存储 | 用途 | 说明 |
|------|------|------|
| **SQLite** | 主数据库 | 本地文件，零运维 |
| **飞书 Bitable** | 团队共享展示 | API 读写 |
| **JSON 文件** | 缓存/临时数据 | 去重缓存、待写入队列 |

### 数据处理

| 库 | 用途 |
|------|------|
| **numpy** | 数值计算（EMA/RSI/MACD/KDJ） |
| **pandas** | 数据处理（日K分析） |
| **tushare** | Tushare SDK |
| **akshare** | AkShare SDK |
| **requests** | HTTP 调用（东方财富/腾讯） |

### 消息推送

| 渠道 | 用途 | 接口 |
|------|------|------|
| **飞书卡片** | 早报/晚报/超预期/股票池 | OpenClaw message tool |
| **飞书DM** | 回调买入即时通知 | OpenClaw message tool |
| **飞书Bitable** | 结构化数据共享 | feishu_bitable 工具 |

### 调度

| 组件 | 用途 |
|------|------|
| **OpenClaw Cron** | 定时任务调度 |
| **Python subprocess** | Pipeline → Analyzer → Pusher 串联 |

---

## 依赖清单（requirements.txt）

```
numpy>=1.24
pandas>=2.0
tushare>=1.4.0
akshare>=1.12
requests>=2.31
```

_现有依赖不变，不引入新框架。_

---

_技术栈 v0.1 | 待小马（CTO）确认_
