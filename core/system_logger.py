"""
系统日志模块 — 记录脚本运行历史

用法：
    from core.system_logger import SystemLogger
    logger = SystemLogger(db_path="data/smart_invest.db")

    # 用 with 语句自动记录成功/失败
    with logger.run("pipeline", "盘后扫描 12h") as log:
        log.detail("扫描发现 18 只新披露")
        log.detail("采集 15/18 成功")
        log.result("beats=3, highs=1, pool=2")

    # 手动记录
    logger.info("web", "服务启动")
    logger.error("pipeline", "连接超时", detail="东方财富 API 无响应")
"""

import sqlite3
import time
import traceback
from datetime import datetime
from contextlib import contextmanager
from typing import Optional


class RunContext:
    """单次运行的上下文，用于 with 语句自动记录成功/失败"""

    def __init__(self, logger: "SystemLogger", module: str, description: str):
        self.logger = logger
        self.module = module
        self.description = description
        self.details: list[str] = []
        self.result_text: Optional[str] = None
        self._start = time.time()
        self._success = True
        self._error: Optional[str] = None

    def detail(self, text: str):
        """追加执行详情"""
        self.details.append(text)

    def result(self, text: str):
        """设置结果摘要"""
        self.result_text = text

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = int((time.time() - self._start) * 1000)
        if exc_type:
            self._success = False
            self._error = f"{exc_type.__name__}: {exc_val}"
            self.details.append(f"异常: {traceback.format_exc()}")

        self.logger._write(
            module=self.module,
            description=self.description,
            status="success" if self._success else "error",
            detail="\n".join(self.details) if self.details else None,
            result=self.result_text,
            duration_ms=duration_ms,
            error=self._error,
        )
        return False  # 不吞异常


class SystemLogger:
    def __init__(self, db_path: str = "data/smart_invest.db"):
        self.db_path = db_path
        self._ensure_table()

    def _ensure_table(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS system_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                module TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL DEFAULT 'info',
                detail TEXT,
                result TEXT,
                duration_ms INTEGER,
                error TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_system_log_created ON system_log(created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_system_log_module ON system_log(module)")
        conn.commit()
        conn.close()

    def _write(self, module: str, description: str = None, status: str = "info",
               detail: str = None, result: str = None, duration_ms: int = None,
               error: str = None):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO system_log (module, description, status, detail, result, duration_ms, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (module, description, status, detail, result, duration_ms, error))
        conn.commit()
        conn.close()

    def run(self, module: str, description: str = None) -> RunContext:
        """返回运行上下文，用 with 语句自动记录"""
        return RunContext(self, module, description)

    def info(self, module: str, description: str, detail: str = None):
        """记录信息日志"""
        self._write(module=module, description=description, status="info", detail=detail)

    def success(self, module: str, description: str, result: str = None, duration_ms: int = None):
        """记录成功日志"""
        self._write(module=module, description=description, status="success", result=result, duration_ms=duration_ms)

    def error(self, module: str, description: str, error: str = None, detail: str = None):
        """记录错误日志"""
        self._write(module=module, description=description, status="error", error=error, detail=detail)

    def warn(self, module: str, description: str, detail: str = None):
        """记录警告日志"""
        self._write(module=module, description=description, status="warn", detail=detail)

    def get_logs(self, limit: int = 50, module: str = None, status: str = None) -> list[dict]:
        """获取日志列表"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        sql = "SELECT * FROM system_log WHERE 1=1"
        params = []
        if module:
            sql += " AND module = ?"
            params.append(module)
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def cleanup(self, keep_days: int = 30):
        """清理旧日志"""
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "DELETE FROM system_log WHERE created_at < datetime('now', 'localtime', ?)",
            (f"-{keep_days} days",)
        )
        conn.commit()
        deleted = conn.total_changes
        conn.close()
        return deleted
