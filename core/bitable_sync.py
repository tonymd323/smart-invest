"""
Bitable 同步模块 — v2 统一版
=============================
合并原 BitableSync（记录生成）和 BitableManager（去重/同步）为一个类。
保留两个类的公开方法，内部统一。

用法：
    sync = BitableSync(app_token="xxx", table_id="yyy")
    records = sync.generate_scan_records(beats, new_highs, industry_map)
    new_count = sync.sync(records)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger('bitable_sync')


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _date_to_ts(date_str: str) -> Optional[int]:
    """日期字符串转毫秒时间戳"""
    if not date_str or date_str == 'None':
        return None
    try:
        ds = str(date_str).replace('-', '').replace('/', '')[:8]
        dt = datetime.strptime(ds, '%Y%m%d')
        return int(dt.timestamp()) * 1000
    except ValueError:
        return None


def _parse_date(date_str: str) -> Optional[str]:
    """解析日期字符串为 ISO 格式"""
    if not date_str or date_str == 'None':
        return None
    date_str = str(date_str).replace('-', '').replace('/', '')
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return date_str


# ═══════════════════════════════════════════════════════════════════════════════
#  统一 Bitable 同步器（合并 BitableSync + BitableManager）
# ═══════════════════════════════════════════════════════════════════════════════

class BitableSync:
    """
    统一的飞书多维表格同步器。
    职责：记录生成 → 去重 → 导出 → 缓存更新

    用法：
        # 使用预设配置
        sync = BitableSync.from_preset('scan')
        records = sync.generate_scan_records(beats, new_highs)
        count = sync.sync(records)

        # 自定义配置
        sync = BitableSync(
            app_token="CvTRbdVyfa9PnMsnzIXcCSNmnnb",
            table_id="tbluSQrjOW0tppTP",
            dedup_keys=["股票代码", "报告期"],
        )
    """

    # 已知表格配置
    TABLES = {
        'scan': {
            'app_token': 'CvTRbdVyfa9PnMsnzIXcCSNmnnb',
            'table_id': 'tbluSQrjOW0tppTP',
            'dedup_keys': ['股票代码', '报告期'],
        },
        'backtest': {
            'app_token': 'CvTRbdVyfa9PnMsnzIXcCSNmnnb',
            'table_id': 'tblP6OwkzGQns8Uc',
            'dedup_keys': ['股票代码', '入池日期'],
        },
        'discovery_pool': {
            'app_token': 'CvTRbdVyfa9PnMsnzIXcCSNmnnb',
            'table_id': 'tblPKXYUsow2Pd6A',
            'dedup_keys': ['股票代码', '发现来源'],
        },
        'events': {
            'app_token': 'CvTRbdVyfa9PnMsnzIXcCSNmnnb',
            'table_id': 'tblUgPIXejUOggWx',
            'dedup_keys': ['股票代码', '事件类型', '标题'],
        },
        'tracking': {
            'app_token': 'CvTRbdVyfa9PnMsnzIXcCSNmnnb',
            'table_id': 'tblNZIrovX0WRmW3',
            'dedup_keys': ['股票代码', '事件类型', '入池日期'],
        },
    }

    def __init__(self, app_token: str = None, table_id: str = None,
                 scan_table_id: str = None, backtest_table_id: str = None,
                 dedup_keys: list = None):
        """
        Args:
            app_token: 多维表格 App Token
            table_id: 当前操作的 table_id（推荐）
            scan_table_id: 表1（每日扫描）的 table_id（向后兼容）
            backtest_table_id: 表2（回测记录）的 table_id（向后兼容）
            dedup_keys: 去重用的复合键字段列表
        """
        self.app_token = app_token
        self.table_id = table_id or scan_table_id
        self.scan_table_id = scan_table_id or table_id
        self.backtest_table_id = backtest_table_id
        self.dedup_keys = dedup_keys or ['股票代码', '报告期']

    @classmethod
    def from_preset(cls, table_name: str) -> 'BitableSync':
        """使用预设配置创建"""
        cfg = cls.TABLES.get(table_name)
        if not cfg:
            raise ValueError(f"未知表: {table_name}，可选: {list(cls.TABLES.keys())}")
        return cls(cfg['app_token'], cfg['table_id'], dedup_keys=cfg['dedup_keys'])

    # ── 记录生成（原 BitableSync）─────────────────────────────────────────

    def clear_table(self, table_id: str = None) -> dict:
        """已废弃：改为去重追加模式，不清空历史数据"""
        return {}

    def generate_scan_records(self, beats: list, new_highs: list,
                              industry_map: dict = None) -> list:
        """
        生成扫描结果的待写入记录列表。

        Args:
            beats: 超预期扫描结果
            new_highs: 扣非新高扫描结果
            industry_map: {stock_code: industry} 行业映射

        Returns:
            [{"fields": {...}}, ...] 格式，供 sync() 或工具 API 写入
        """
        records = []
        scan_ts = _date_to_ts(datetime.now().strftime('%Y-%m-%d'))

        # 超预期（含无预期的首次覆盖）
        for b in beats:
            has_consensus = b.get('consensus_available', True)
            fields = {
                '股票代码': b.get('code', ''),
                '公司名称': b.get('name', ''),
                '公告类型': b.get('disclosure_type', '财报'),
                '是否超预期': has_consensus,
                '信号类型': '超预期' if has_consensus else '首次覆盖',
                '是否扣非新高': False,
                '扫描日期': scan_ts,
            }
            if b.get('actual_profit_yoy') is not None:
                fields['利润增速'] = round(b['actual_profit_yoy'], 1)
            if b.get('expected_profit_yoy') is not None:
                fields['预期利润增速'] = round(b['expected_profit_yoy'], 1)
            if b.get('actual_rev_yoy') is not None:
                fields['营收增速'] = round(b['actual_rev_yoy'], 1)
            if b.get('expected_rev_yoy') is not None:
                fields['预期营收增速'] = round(b['expected_rev_yoy'], 1)
            if b.get('profit_diff') is not None:
                fields['超预期幅度'] = round(b['profit_diff'], 1)
            if b.get('profit_dedt') is not None:
                fields['扣非净利润(亿)'] = b['profit_dedt']
            if b.get('close'):
                fields['收盘价'] = b['close']
            if b.get('pe'):
                fields['PE'] = round(b['pe'], 2)
            if b.get('total_mv'):
                fields['总市值(亿)'] = round(b['total_mv'] / 10000, 2)
            if industry_map and b.get('code'):
                ind = industry_map.get(b['code'])
                if ind:
                    fields['行业'] = ind
            period_ts = _date_to_ts(b.get('period', ''))
            ann_ts = _date_to_ts(b.get('ann_date', ''))
            if period_ts:
                fields['报告期'] = period_ts
            if ann_ts:
                fields['披露日'] = ann_ts
            records.append({"fields": fields})

        # 扣非新高
        for h in new_highs:
            existing = next(
                (r for r in records if r['fields']['股票代码'] == h.get('code')),
                None,
            )
            if existing:
                existing['fields']['是否扣非新高'] = True
                if h.get('quarterly_profit') is not None:
                    existing['fields']['单季扣非(亿)'] = round(h['quarterly_profit'], 2)
                if h.get('growth_vs_high') is not None:
                    existing['fields']['超前高幅度(%)'] = round(h['growth_vs_high'], 1)
            else:
                fields = {
                    '股票代码': h.get('code', ''),
                    '公司名称': h.get('name', ''),
                    '公告类型': '财报',
                    '是否超预期': False,
                    '信号类型': '扣非新高',
                    '扣非净利润(亿)': h.get('quarterly_profit'),
                    '是否扣非新高': True,
                    '扫描日期': scan_ts,
                }
                if h.get('quarterly_profit') is not None:
                    fields['单季扣非(亿)'] = round(h['quarterly_profit'], 2)
                if h.get('growth_vs_high') is not None:
                    fields['超前高幅度(%)'] = round(h['growth_vs_high'], 1)
                if h.get('close'):
                    fields['收盘价'] = h['close']
                if h.get('pe'):
                    fields['PE'] = round(h['pe'], 2)
                if h.get('total_mv'):
                    fields['总市值(亿)'] = round(h['total_mv'] / 10000, 2)
                if industry_map and h.get('code'):
                    ind = industry_map.get(h['code'])
                    if ind:
                        fields['行业'] = ind
                period_ts = _date_to_ts(h.get('report_date', ''))
                if period_ts:
                    fields['报告期'] = period_ts
                    fields['披露日'] = period_ts
                records.append({"fields": fields})

        if not records:
            logger.info("  无扫描记录生成")
            return []

        logger.info(f"  生成 {len(records)} 条扫描记录")
        return records

    def generate_backtest_records(self, bt_results: list) -> list:
        """生成回测结果的待写入记录列表"""
        if not self.app_token or not self.backtest_table_id:
            return []

        records = []
        for bt in bt_results:
            records.append({"fields": {
                '股票代码': bt.get('stock_code', ''),
                '公司名称': bt.get('stock_name', ''),
                '入池日期': bt.get('event_date', ''),
                '入池价': bt.get('entry_price'),
                '5日收益(%)': bt.get('return_5d'),
                '10日收益(%)': bt.get('return_10d'),
                '20日收益(%)': bt.get('return_20d'),
                '60日收益(%)': bt.get('return_60d'),
                '沪深300同期(%)': bt.get('benchmark_return'),
                '超额收益(%)': bt.get('alpha'),
                '是否跑赢': bt.get('is_win', False),
            }})
        return records

    # ── 去重逻辑（原 BitableManager）──────────────────────────────────────

    def load_existing_keys(self, filepath: str = None) -> set:
        """从缓存文件加载已有记录的去重键集合"""
        if filepath is None:
            filepath = str(
                Path(__file__).parent.parent / 'data'
                / f'bitable_existing_{self.table_id}.json'
            )
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return set(data.get('keys', []))
        except (FileNotFoundError, json.JSONDecodeError):
            return set()

    def save_existing_keys(self, keys: set, filepath: str = None):
        """保存去重键集合到缓存文件"""
        if filepath is None:
            filepath = str(
                Path(__file__).parent.parent / 'data'
                / f'bitable_existing_{self.table_id}.json'
            )
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({'keys': list(keys), 'updated': datetime.now().isoformat()}, f)

    def extract_key(self, record: dict) -> str:
        """从记录中提取复合去重键"""
        fields = record.get('fields', record)
        parts = []
        for k in self.dedup_keys:
            v = fields.get(k, '')
            if isinstance(v, list) and v:
                v = v[0].get('text', '')
            parts.append(str(v))
        return '_'.join(parts)

    def dedup_records(self, new_records: list, existing_records: list = None,
                      existing_keys: set = None) -> list:
        """
        去重：同批内合并 + 跳过已有记录

        支持两种模式：
        1. existing_records：从 Bitable 读取的已有记录列表
        2. existing_keys：从缓存文件加载的去重键集合
        """
        # 构建已有记录的复合键集合
        if existing_keys is not None:
            pass  # 直接使用
        elif existing_records is not None:
            existing_keys = set()
            for rec in existing_records:
                existing_keys.add(self.extract_key(rec))
        else:
            existing_keys = self.load_existing_keys()

        # 同批内合并：同一复合键合并为一条
        merged = {}
        for rec in new_records:
            key = self.extract_key(rec)
            if key in merged:
                # 合并字段（不覆盖已有值）
                existing_fields = merged[key]['fields']
                new_fields = rec.get('fields', rec)
                for fk, fv in new_fields.items():
                    if fk not in existing_fields or existing_fields[fk] is None:
                        existing_fields[fk] = fv
                # 扫描结果专用合并逻辑
                if new_fields.get("是否扣非新高") and not existing_fields.get("是否扣非新高"):
                    existing_fields["是否扣非新高"] = True
                    if new_fields.get("单季扣非(亿)") is not None:
                        existing_fields["单季扣非(亿)"] = new_fields["单季扣非(亿)"]
                    if new_fields.get("超前高幅度(%)") is not None:
                        existing_fields["超前高幅度(%)"] = new_fields["超前高幅度(%)"]
            else:
                merged[key] = rec

        # 过滤：跳过已存在的
        filtered = [
            rec for key, rec in merged.items()
            if key not in existing_keys
        ]

        skipped = len(new_records) - len(filtered)
        if skipped > 0:
            logger.info(
                f"  去重：输入 {len(new_records)} → 合并 {len(merged)} "
                f"→ 跳过已有 {skipped} → 新增 {len(filtered)}"
            )

        return filtered

    # ── 同步流程 ──────────────────────────────────────────────────────────

    def sync(self, new_records: list, existing_keys: set = None,
             pending_path: str = None, update_cache: bool = True,
             max_batch_size: int = 200) -> int:
        """
        完整同步流程：去重 → 导出待写入文件 → 更新缓存。

        返回新增记录数。max_batch_size 控制分批导出防超时。
        """
        if not new_records:
            logger.info("  无新记录，跳过")
            return 0

        # 去重
        filtered = self.dedup_records(new_records, existing_keys=existing_keys)
        if not filtered:
            logger.info("  全部已存在，无需写入")
            return 0

        # 导出（P0#14: 分批防超时）
        if pending_path is None:
            pending_path = str(
                Path(__file__).parent.parent / 'data' / 'bitable_pending.json'
            )
        Path(pending_path).parent.mkdir(parents=True, exist_ok=True)

        if len(filtered) <= max_batch_size:
            with open(pending_path, 'w', encoding='utf-8') as f:
                json.dump(filtered, f, ensure_ascii=False, default=str)
            logger.info(f"  📝 已导出 {len(filtered)} 条到 {pending_path}")
        else:
            # 分批导出
            stem = Path(pending_path).stem
            suffix = Path(pending_path).suffix
            parent = Path(pending_path).parent
            for old in parent.glob(f"{stem}_part*{suffix}"):
                old.unlink()
            num_batches = (len(filtered) + max_batch_size - 1) // max_batch_size
            for i in range(num_batches):
                batch = filtered[i * max_batch_size : (i + 1) * max_batch_size]
                batch_path = str(parent / f"{stem}_part{i+1}{suffix}")
                with open(batch_path, 'w', encoding='utf-8') as f:
                    json.dump(batch, f, ensure_ascii=False, default=str)
                logger.info(f"  📝 第 {i+1}/{num_batches} 批: {len(batch)} 条 → {batch_path}")
            # 主文件写第一批（向后兼容）
            with open(pending_path, 'w', encoding='utf-8') as f:
                json.dump(filtered[:max_batch_size], f, ensure_ascii=False, default=str)

        # 更新缓存
        if update_cache:
            keys = self.load_existing_keys() if existing_keys is None else set(existing_keys)
            for rec in filtered:
                keys.add(self.extract_key(rec))
            self.save_existing_keys(keys)

        return len(filtered)

    def mark_written(self, written_records: list):
        """写入成功后，将记录的键加入缓存"""
        keys = self.load_existing_keys()
        for rec in written_records:
            keys.add(self.extract_key(rec))
        self.save_existing_keys(keys)

    def generate_discovery_pool_records(self, pool_entries: list) -> list:
        """将发现池入池结果转为 Bitable 记录"""
        records = []
        now_ts = int(datetime.now().timestamp() * 1000)
        source_map = {
            'earnings_beat': '超预期',
            'profit_new_high': '扣非新高',
            'pullback_buy': '回调买入',
        }
        for entry in pool_entries:
            code = entry.get('stock_code', '')
            name = entry.get('stock_name', code)
            source = source_map.get(entry.get('source', ''), entry.get('source', ''))
            signal = entry.get('signal', 'watch')
            score = entry.get('score', 0)
            # 过期时间
            expires_str = entry.get('expires_at', '')
            try:
                expires_ts = int(datetime.strptime(expires_str[:19], '%Y-%m-%d %H:%M:%S').timestamp() * 1000) if expires_str else now_ts + 7 * 86400000
            except (ValueError, IndexError):
                expires_ts = now_ts + 7 * 86400000
            records.append({"fields": {
                "股票代码": code,
                "公司名称": name,
                "发现来源": source,
                "信号": signal,
                "评分": score,
                "入池日期": now_ts,
                "过期日期": expires_ts,
                "状态": "active",
            }})
        if records:
            logger.info(f"  生成 {len(records)} 条发现池记录")
        return records

    def generate_event_records(self, events: list) -> list:
        """将事件列表转为 Bitable 记录"""
        records = []
        now_ts = int(datetime.now().timestamp() * 1000)
        for evt in events:
            code = evt.get('stock_code', '')
            event_type = evt.get('event_type', '')
            title = evt.get('title', '')
            content = evt.get('content', '')
            severity = evt.get('severity', 'normal')
            sentiment = evt.get('sentiment', 'neutral')
            records.append({"fields": {
                "股票代码": code,
                "事件类型": event_type,
                "标题": title,
                "详情": content,
                "严重度": severity,
                "情绪": sentiment,
                "发生时间": now_ts,
            }})
        if records:
            logger.info(f"  生成 {len(records)} 条事件记录")
        return records

    # ── 向后兼容方法 ──────────────────────────────────────────────────────

    # 保留旧接口别名
    def sync_scan_results(self, beats: list, new_highs: list,
                          industry_map: dict = None) -> list:
        """向后兼容：生成扫描记录（不执行同步，返回 records 列表）"""
        return self.generate_scan_records(beats, new_highs, industry_map)

    def sync_backtest(self, bt_results: list) -> list:
        """向后兼容：生成回测记录"""
        return self.generate_backtest_records(bt_results)


# 向后兼容：BitableManager 别名
# daily_scan.py 使用 BitableManager.from_preset('scan') + mgr.sync(records)
class BitableManager:
    """向后兼容封装 — 用 BitableSync 实现去重+同步"""

    def __init__(self, app_token: str, table_id: str):
        self.app_token = app_token
        self.table_id = table_id

    @classmethod
    def from_preset(cls, preset: str = 'scan') -> 'BitableManager':
        presets = {
            'scan': ('CvTRbdVyfa9PnMsnzIXcCSNmnnb', 'tbluSQrjOW0tppTP'),
            'backtest': ('CvTRbdVyfa9PnMsnzIXcCSNmnnb', 'tblP6OwkzGQns8Uc'),
            'discovery_pool': ('CvTRbdVyfa9PnMsnzIXcCSNmnnb', 'tblPKXYUsow2Pd6A'),
            'events': ('CvTRbdVyfa9PnMsnzIXcCSNmnnb', 'tblUgPIXejUOggWx'),
            'tracking': ('CvTRbdVyfa9PnMsnzIXcCSNmnnb', 'tblNZIrovX0WRmW3'),
        }
        app_token, table_id = presets.get(preset, presets['scan'])
        return cls(app_token, table_id)

    def sync(self, pending_records: list) -> int:
        """同步记录到 Bitable，返回新增条数"""
        if not pending_records:
            return 0
        sync = BitableSync(app_token=self.app_token, scan_table_id=self.table_id)
        return sync.sync(pending_records)
