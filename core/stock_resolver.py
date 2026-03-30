"""
股票身份解析器 — v2.23

任意格式输入 → canonical_code，防止重复入库。

功能：
  - resolve(input)：代码/名称 → canonical_code
  - ensure_exists(code, name)：入库前调用，不存在则创建
  - 批量清洗：合并重复记录
"""

import sqlite3
import logging
from typing import Optional, Dict
from core.data_normalizer import normalizer

logger = logging.getLogger(__name__)


class StockResolver:
    """股票身份解析器"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._cache: Dict[str, str] = {}  # name/code → canonical_code
        self._load_cache()

    def _load_cache(self):
        """加载 stocks 表到内存缓存"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT code, stock_name FROM stocks WHERE stock_name IS NOT NULL"
            ).fetchall()
            for r in rows:
                code = r['code']
                name = r['stock_name']
                if code:
                    self._cache[code.upper()] = code
                    # 也缓存无后缀版本
                    bare = code.split('.')[0] if '.' in code else code
                    self._cache[bare] = code
                if name:
                    self._cache[name] = code
            conn.close()
            logger.info(f"[StockResolver] 缓存加载完成: {len(self._cache)} 条映射")
        except Exception as e:
            logger.warning(f"[StockResolver] 缓存加载失败: {e}")

    def resolve(self, input_str: str) -> Optional[str]:
        """
        任意格式 → canonical_code

        Args:
            input_str: 股票代码或名称

        Returns:
            canonical_code（如 '000001.SZ'），找不到返回 None
        """
        if not input_str:
            return None

        input_str = str(input_str).strip()

        # 先尝试标准化为代码
        canonical = normalizer.normalize_code(input_str)
        if canonical and canonical in self._cache:
            return self._cache[canonical]

        # 尝试无后缀查找
        bare = input_str.split('.')[0] if '.' in input_str else input_str
        bare = bare.upper().replace('SH', '').replace('SZ', '').replace('BJ', '').strip()
        if bare in self._cache:
            return self._cache[bare]

        # 按名称查找
        if input_str in self._cache:
            return self._cache[input_str]

        # 数据库查
        return self._db_lookup(input_str)

    def _db_lookup(self, input_str: str) -> Optional[str]:
        """数据库精确+模糊查找"""
        try:
            conn = sqlite3.connect(self.db_path)
            # 精确匹配代码
            code = normalizer.normalize_code(input_str)
            if code:
                row = conn.execute(
                    "SELECT code FROM stocks WHERE code = ?", (code,)
                ).fetchone()
                if row:
                    conn.close()
                    self._cache[input_str] = row[0]
                    return row[0]

            # 精确匹配名称
            row = conn.execute(
                "SELECT code FROM stocks WHERE stock_name = ?", (input_str,)
            ).fetchone()
            if row:
                conn.close()
                self._cache[input_str] = row[0]
                return row[0]

            # 模糊匹配名称
            row = conn.execute(
                "SELECT code FROM stocks WHERE stock_name LIKE ? LIMIT 1",
                (f'%{input_str}%',)
            ).fetchone()
            conn.close()
            if row:
                self._cache[input_str] = row[0]
                return row[0]

        except Exception as e:
            logger.warning(f"[StockResolver] 数据库查找失败: {e}")

        return None

    def ensure_exists(self, code: str, name: str = None) -> str:
        """
        确保股票在 stocks 表中存在，返回 canonical_code。

        如果不存在则插入。如果已存在则更新名称（如果提供了更完整的名称）。

        Args:
            code: 股票代码（任意格式）
            name: 股票名称（可选）

        Returns:
            canonical_code
        """
        canonical = normalizer.normalize_code(code)
        if not canonical:
            logger.warning(f"[StockResolver] 无法标准化代码: {code}")
            return code

        # 检查缓存
        if canonical in self._cache:
            # 更新名称（如果提供了且缓存中没有）
            if name and canonical not in [k for k, v in self._cache.items() if v == canonical and k == name]:
                self._update_name(canonical, name)
            return canonical

        try:
            conn = sqlite3.connect(self.db_path)
            # 检查是否存在
            existing = conn.execute(
                "SELECT code, stock_name FROM stocks WHERE code = ?", (canonical,)
            ).fetchone()

            if existing:
                # 更新名称
                if name and not existing[1]:
                    conn.execute(
                        "UPDATE stocks SET stock_name = ? WHERE code = ?",
                        (name, canonical)
                    )
                    conn.commit()
                conn.close()
                self._cache[canonical] = canonical
                if name:
                    self._cache[name] = canonical
                return canonical

            # 插入新记录
            conn.execute(
                "INSERT INTO stocks (code, stock_name) VALUES (?, ?)",
                (canonical, name)
            )
            conn.commit()
            conn.close()

            self._cache[canonical] = canonical
            if name:
                self._cache[name] = canonical
            logger.info(f"[StockResolver] 新增股票: {canonical} {name or ''}")

        except Exception as e:
            logger.error(f"[StockResolver] ensure_exists 失败: {e}")

        return canonical

    def _update_name(self, code: str, name: str):
        """更新股票名称"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "UPDATE stocks SET stock_name = ? WHERE code = ? AND (stock_name IS NULL OR stock_name = '')",
                (name, code)
            )
            conn.commit()
            conn.close()
            self._cache[name] = code
        except Exception:
            pass

    def cleanup_duplicates(self) -> dict:
        """
        清洗重复股票记录。

        合并规则：如果 stocks 表中同时存在 '000001' 和 '000001.SZ'，
        保留 '000001.SZ'（带后缀的），将所有引用更新。

        Returns:
            清洗统计
        """
        stats = {'merged': 0, 'deleted': 0, 'errors': 0}

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row

            # 找出无后缀的代码
            bare_codes = conn.execute("""
                SELECT code FROM stocks 
                WHERE code NOT LIKE '%.%' AND length(code) = 6 AND code GLOB '[0-9]*'
            """).fetchall()

            for row in bare_codes:
                bare = row['code']
                # 判断交易所
                exchange = normalizer.EXCHANGE_MAP.get(bare[0], 'SZ')
                canonical = f'{bare}.{exchange}'

                # 检查 canonical 是否存在
                canonical_exists = conn.execute(
                    "SELECT code FROM stocks WHERE code = ?", (canonical,)
                ).fetchone()

                if canonical_exists:
                    # 需要合并：将 bare 的引用全部改为 canonical
                    for table in ['prices', 'earnings', 'analysis_results',
                                  'discovery_pool', 'event_tracking', 'events']:
                        try:
                            conn.execute(
                                f"UPDATE {table} SET stock_code = ? WHERE stock_code = ?",
                                (canonical, bare)
                            )
                        except Exception:
                            pass

                    # 删除重复的 stocks 记录
                    conn.execute("DELETE FROM stocks WHERE code = ?", (bare,))
                    stats['merged'] += 1
                    stats['deleted'] += 1
                    logger.info(f"[StockResolver] 合并: {bare} → {canonical}")
                else:
                    # bare 存在但 canonical 不存在，直接改名
                    stock_name = conn.execute(
                        "SELECT stock_name FROM stocks WHERE code = ?", (bare,)
                    ).fetchone()
                    name = stock_name[0] if stock_name else None

                    conn.execute(
                        "UPDATE stocks SET code = ? WHERE code = ?",
                        (canonical, bare)
                    )
                    # 同时更新其他表
                    for table in ['prices', 'earnings', 'analysis_results',
                                  'discovery_pool', 'event_tracking', 'events']:
                        try:
                            conn.execute(
                                f"UPDATE {table} SET stock_code = ? WHERE stock_code = ?",
                                (canonical, bare)
                            )
                        except Exception:
                            pass
                    stats['merged'] += 1
                    logger.info(f"[StockResolver] 重命名: {bare} → {canonical}")

            conn.commit()
            conn.close()

            # 重建缓存
            self._cache.clear()
            self._load_cache()

        except Exception as e:
            logger.error(f"[StockResolver] 清洗失败: {e}")
            stats['errors'] += 1

        return stats


# ── 工厂函数 ──────────────────────────────────────────────
def get_resolver(db_path: str = None) -> StockResolver:
    """获取 StockResolver 实例"""
    if db_path is None:
        from pathlib import Path
        db_path = str(Path(__file__).parent.parent / 'data' / 'smart_invest.db')
    return StockResolver(db_path)
