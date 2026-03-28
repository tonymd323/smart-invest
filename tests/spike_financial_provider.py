#!/usr/bin/env python3
"""
Phase 0 Spike — 东方财富 API 全链路验证
采集 → 标准化 → SQLite 存储 → 读取验证

测试对象：福耀玻璃 600660.SH
"""

import sys
import os
import sqlite3
import logging
import json
from datetime import datetime

# 项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger('spike')

import requests


class SpikeFinancialProvider:
    """东方财富财报数据 Provider（Spike 版，直接调 API）"""

    BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    def fetch_quarterly(self, ts_code: str, period: str = None) -> list:
        """
        获取单季度财务数据。
        API: RPT_F10_FINANCE_MAINFINADATA
        返回: 原始 JSON data 列表
        """
        code = ts_code.replace('.SH', '').replace('.SZ', '')
        params = {
            'reportName': 'RPT_F10_FINANCE_MAINFINADATA',
            'columns': 'ALL',
            'filter': f'(SECURITY_CODE="{code}")',
            'pageSize': 10,
            'sortColumns': 'REPORT_DATE',
            'sortTypes': -1,
        }
        if period:
            params['filter'] += f'(REPORT_DATE=\'{period}\')'

        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get('success') and data.get('result', {}).get('data'):
                return data['result']['data']
            logger.warning(f"API 返回空数据: success={data.get('success')}, result keys={list(data.get('result', {}).keys()) if data.get('result') else 'None'}")
            return []
        except requests.exceptions.Timeout:
            logger.error(f"东方财富 API 超时 {ts_code}")
            return []
        except requests.exceptions.HTTPError as e:
            logger.error(f"东方财富 API HTTP 错误 {ts_code}: {e}")
            return []
        except Exception as e:
            logger.error(f"东方财富财务数据获取失败 {ts_code}: {e}")
            return []

    # 实际 API 字段映射（v2 修正版，基于 live API 验证）
    # 注意：原始任务中的字段名（PARENT_NETPROFIT 等）与实际 API 返回不一致
    # 实际 API 使用无下划线命名：PARENTNETPROFIT, TOTALOPERATEREVE
    _FIELD_MAP = {
        'PARENTNETPROFIT':    'net_profit',       # 归母净利润（元）
        'PARENTNETPROFITTZ':  'net_profit_yoy',   # 归母净利润同比（%）
        'TOTALOPERATEREVE':   'revenue',           # 营业总收入（元）
        'DJD_TOI_YOY':        'or_yoy',            # 单季度营收同比（%）
        'DJD_DPNP_YOY':       'dt_netprofit_yoy',  # 单季度归母净利润同比（%）
        'ROEJQ':              'roe',                # 加权 ROE（%）
        'XSMLL':              'gross_margin',       # 销售毛利率（%）
        'EPSJB':              'eps',                # 基本每股收益
    }

    def to_standard_format(self, raw: list) -> list:
        """将东方财富原始数据转换为标准格式"""
        results = []
        for r in raw:
            code = r.get('SECURITY_CODE', '')
            market = 'SH' if code.startswith('6') else 'SZ'
            record = {
                'ts_code': code + '.' + market,
                'report_date': (r.get('REPORT_DATE') or '')[:10],
            }
            for api_field, std_field in self._FIELD_MAP.items():
                record[std_field] = r.get(api_field)
            results.append(record)
        return results


def get_spike_db(path: str) -> sqlite3.Connection:
    """创建 spike 测试用 SQLite 数据库（WAL 模式）"""
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS earnings (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code                TEXT    NOT NULL,
            report_date               TEXT    NOT NULL,
            report_type               TEXT    DEFAULT 'Q4',
            revenue                   REAL,
            net_profit                REAL,
            net_profit_yoy            REAL,
            eps                       REAL,
            is_beat_expectation       INTEGER DEFAULT 0,
            expectation_diff_pct      REAL,
            quarterly_profit_new_high INTEGER DEFAULT 0,
            quarterly_net_profit      REAL,
            prev_quarterly_high       REAL,
            roe                       REAL,
            gross_margin              REAL,
            created_at                TEXT    DEFAULT (datetime('now', 'localtime')),
            UNIQUE(stock_code, report_date, report_type)
        );
    """)
    conn.commit()
    return conn


def run_spike():
    """执行 Spike 测试"""
    results = {
        'timestamp': datetime.now().isoformat(),
        'stock': '600660.SH (福耀玻璃)',
        'steps': {},
    }

    db_path = '/tmp/spike_test.db'

    try:
        # ── Step 1: 调用东方财富 API ──────────────────────────────────────
        logger.info("=" * 60)
        logger.info("Step 1: 调用东方财富 API 获取福耀玻璃财务数据")
        logger.info("=" * 60)

        provider = SpikeFinancialProvider()
        raw_data = provider.fetch_quarterly('600660.SH')

        step1 = {
            'api_url': provider.BASE_URL,
            'api_name': 'RPT_F10_FINANCE_MAINFINADATA',
            'records_fetched': len(raw_data),
            'status': 'PASS' if len(raw_data) > 0 else 'FAIL',
        }

        if raw_data:
            # 输出第一个记录的字段名，方便对照
            first = raw_data[0]
            step1['available_fields'] = sorted(first.keys())
            step1['sample_report_date'] = first.get('REPORT_DATE', '')
            step1['sample_net_profit'] = first.get('PARENT_NETPROFIT')
            step1['sample_revenue'] = first.get('TOTAL_OPERATE_INCOME')

        results['steps']['step1_api_call'] = step1
        logger.info(f"  获取到 {len(raw_data)} 条记录")
        if raw_data:
            logger.info(f"  最新报告期: {raw_data[0].get('REPORT_DATE', 'N/A')}")
            logger.info(f"  净利润: {raw_data[0].get('PARENT_NETPROFIT', 'N/A')}")
            logger.info(f"  营收: {raw_data[0].get('TOTAL_OPERATE_INCOME', 'N/A')}")

        # ── Step 2: 转换为标准格式 ─────────────────────────────────────────
        logger.info("")
        logger.info("Step 2: 转换为标准格式")
        logger.info("=" * 60)

        standard = provider.to_standard_format(raw_data)

        step2 = {
            'converted_records': len(standard),
            'status': 'PASS' if len(standard) == len(raw_data) else 'FAIL',
        }
        if standard:
            step2['sample'] = standard[0]
            # 验证关键字段非空
            s = standard[0]
            checks = {
                'ts_code_present': bool(s.get('ts_code')),
                'report_date_present': bool(s.get('report_date')),
                'net_profit_is_number': isinstance(s.get('net_profit'), (int, float)),
                'revenue_is_number': isinstance(s.get('revenue'), (int, float)),
                'roe_present': s.get('roe') is not None,
                'gross_margin_present': s.get('gross_margin') is not None,
            }
            step2['field_checks'] = checks
            step2['all_fields_valid'] = all(checks.values())

        results['steps']['step2_standardize'] = step2
        logger.info(f"  转换 {len(standard)} 条记录")
        if standard:
            logger.info(f"  样本: {json.dumps(standard[0], ensure_ascii=False, indent=2)}")

        # ── Step 3: 写入 SQLite ────────────────────────────────────────────
        logger.info("")
        logger.info("Step 3: 写入 SQLite earnings 表")
        logger.info("=" * 60)

        conn = get_spike_db(db_path)

        inserted = 0
        for s in standard:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO earnings
                    (stock_code, report_date, report_type, revenue, net_profit,
                     net_profit_yoy, eps, roe, gross_margin)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    s['ts_code'],
                    s['report_date'],
                    'Q4',  # 默认报告类型
                    s.get('revenue'),
                    s.get('net_profit'),
                    s.get('dt_netprofit_yoy'),
                    s.get('eps'),
                    s.get('roe'),
                    s.get('gross_margin'),
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"  插入失败 {s.get('report_date')}: {e}")

        conn.commit()

        step3 = {
            'inserted': inserted,
            'expected': len(standard),
            'status': 'PASS' if inserted == len(standard) else 'FAIL',
        }
        results['steps']['step3_sqlite_write'] = step3
        logger.info(f"  写入 {inserted}/{len(standard)} 条记录")

        # ── Step 4: 读取验证 ────────────────────────────────────────────────
        logger.info("")
        logger.info("Step 4: 从 SQLite 读取验证")
        logger.info("=" * 60)

        rows = conn.execute(
            "SELECT * FROM earnings WHERE stock_code = ? ORDER BY report_date DESC",
            ('600660.SH',)
        ).fetchall()

        row_dicts = [dict(r) for r in rows]

        step4 = {
            'rows_read': len(row_dicts),
            'matches_written': len(row_dicts) == inserted,
            'status': 'PASS' if len(row_dicts) == inserted and len(row_dicts) > 0 else 'FAIL',
        }
        if row_dicts:
            step4['sample_row'] = row_dicts[0]
            # 数据完整性检查
            r = row_dicts[0]
            integrity = {
                'stock_code_match': r['stock_code'] == '600660.SH',
                'report_date_present': bool(r['report_date']),
                'net_profit_positive': r['net_profit'] is not None and r['net_profit'] > 0,
                'revenue_positive': r['revenue'] is not None and r['revenue'] > 0,
            }
            step4['integrity_checks'] = integrity
            step4['data_integrity_ok'] = all(integrity.values())

        results['steps']['step4_read_verify'] = step4
        logger.info(f"  读取 {len(row_dicts)} 条记录")
        for r in row_dicts[:3]:
            logger.info(f"  {r['report_date']} | 净利润={r['net_profit']} | 营收={r['revenue']} | ROE={r['roe']}")

        # WAL 模式检查
        wal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        step5 = {
            'wal_enabled': wal_mode == 'wal',
            'mode': wal_mode,
            'status': 'PASS' if wal_mode == 'wal' else 'FAIL',
        }
        results['steps']['step5_wal_mode'] = step5
        logger.info(f"  WAL 模式: {wal_mode}")

        conn.close()

        # ── 总结 ────────────────────────────────────────────────────────────
        all_pass = all(
            s.get('status') == 'PASS'
            for s in results['steps'].values()
        )
        results['overall'] = 'PASS ✅' if all_pass else 'FAIL ❌'

        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Phase 0 Spike 结果: {results['overall']}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Spike 测试异常: {e}", exc_info=True)
        results['overall'] = f'ERROR ❌: {e}'

    finally:
        # 清理临时数据库
        if os.path.exists(db_path):
            os.unlink(db_path)

    return results


if __name__ == '__main__':
    results = run_spike()

    # 输出 JSON 结果到 stdout
    print("\n" + json.dumps(results, ensure_ascii=False, indent=2))
