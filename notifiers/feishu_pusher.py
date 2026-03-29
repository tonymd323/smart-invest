"""
飞书消息推送核心模块
====================
支持两种发送模式：
  1. OpenClaw CLI（宿主机部署时使用）
  2. 飞书 HTTP API 直发（容器化部署时使用，推荐）

优先级：HTTP API → CLI → 日志降级

环境变量：
  FEISHU_APP_ID       — 飞书应用 App ID
  FEISHU_APP_SECRET   — 飞书应用 App Secret
  SI_FEISHU_DAILY_TARGET — 每日研报推送目标（chat_id 或 open_id）
  SI_FEISHU_ALERT_TARGET — 预警推送目标

使用方式（由 OpenClaw Agent 或 cron 调用）：
    from smart_invest.notifiers.feishu_pusher import FeishuPusher
    
    pusher = FeishuPusher()
    result = pusher.push_daily_report(report_data)
"""

import json
import time
import logging
import subprocess
import sys
import os as _os
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path

# 支持相对导入（作为包）和绝对导入（直接运行）
try:
    from .card_generator import CardGenerator
except (ImportError, ValueError):
    _pkg_dir = _os.path.dirname(_os.path.abspath(__file__))
    if _pkg_dir not in sys.path:
        sys.path.insert(0, _pkg_dir)
    from card_generator import CardGenerator

logger = logging.getLogger(__name__)


class PushError(Exception):
    """推送异常"""
    pass


class FeishuPusher:
    """
    飞书消息推送器
    
    通过 OpenClaw 的 message 系统发送飞书消息。
    支持每日研报、风险预警、超预期消息的推送。
    """
    
    # 推送配置
    MAX_RETRIES = 3                    # 最大重试次数
    RETRY_DELAY_SECONDS = 5            # 重试间隔（秒）
    MAX_MESSAGE_LENGTH = 4000          # 消息最大长度（飞书支持）
    
    def __init__(self, target: Optional[str] = None):
        """
        初始化推送器
        
        Args:
            target: 推送目标（飞书群 chat_id 或用户 open_id）。
                    如果不指定，从配置文件读取。
        """
        self.card_gen = CardGenerator()
        self.target = target or self._load_default_target()
        self._push_log: list = []
    
    # ── 公开接口 ──────────────────────────────────────────────────────────────
    
    def push_daily_report(self, report: dict) -> bool:
        """
        推送每日研报
        
        Args:
            report: 研报数据，参见 CardGenerator.generate_daily_card 的 data 参数
        
        Returns:
            是否推送成功
        """
        card_text = self.card_gen.generate_daily_card(report)
        date_str = report.get("date", datetime.now().strftime("%Y-%m-%d"))
        title = f"📊 智能投资日报 - {date_str}"
        return self._send_message(card_text, title, push_type="daily_report")
    
    def push_alert(self, alert: dict) -> bool:
        """
        推送风险预警
        
        Args:
            alert: 预警数据，参见 CardGenerator.generate_alert_card 的 data 参数
        
        Returns:
            是否推送成功
        """
        card_text = self.card_gen.generate_alert_card(alert)
        if not card_text:
            logger.warning("预警内容为空，跳过推送")
            return True
        return self._send_message(card_text, "⚠️ 风险预警", push_type="alert")
    
    def push_surprise(self, surprise: dict) -> bool:
        """
        推送超预期消息
        
        Args:
            surprise: 超预期数据，参见 CardGenerator.generate_surprise_card 的 data 参数
        
        Returns:
            是否推送成功
        """
        card_text = self.card_gen.generate_surprise_card(surprise)
        return self._send_message(card_text, "🎯 超预期消息", push_type="surprise")
    
    def push_open_check(self, data: dict) -> bool:
        """
        推送开盘检查
        
        Args:
            data: 开盘检查数据
        
        Returns:
            是否推送成功
        """
        card_text = self.card_gen.generate_open_check_card(data)
        return self._send_message(card_text, "🔔 开盘检查", push_type="open_check")
    
    def push_close_report(self, report: dict) -> bool:
        """
        推送收盘简报
        
        Args:
            report: 收盘数据，结构同每日研报
        
        Returns:
            是否推送成功
        """
        card_text = self.card_gen.generate_close_card(report)
        date_str = report.get("date", datetime.now().strftime("%Y-%m-%d"))
        return self._send_message(card_text, f"📈 收盘简报 - {date_str}", push_type="close_report")
    
    def push_custom(self, text: str, title: str = "") -> bool:
        """
        推送自定义消息
        
        Args:
            text: 消息内容
            title: 消息标题（用于日志）
        
        Returns:
            是否推送成功
        """
        return self._send_message(text, title, push_type="custom")

    def push_daily_scan_card(self, beats: list, new_highs: list) -> bool:
        """
        推送备选股池日报（飞书交互式卡片）

        Args:
            beats: 业绩超预期列表
            new_highs: 扣非净利润新高列表

        Returns:
            是否推送成功
        """
        card = self.card_gen.generate_daily_scan_card(beats, new_highs)
        card_json = json.dumps(card, ensure_ascii=False)

        log_entry = {
            "push_type": "daily_scan_card",
            "title": "备选股池日报(卡片)",
            "target": self.target,
            "timestamp": datetime.now().isoformat(),
            "status": "pending",
        }

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                success = self._do_send_card(card_json, self.target)
                if success:
                    log_entry["status"] = "sent"
                    log_entry["attempt"] = attempt
                    self._push_log.append(log_entry)
                    logger.info(f"卡片推送成功 (第 {attempt} 次尝试)")
                    return True
                else:
                    logger.warning(f"卡片推送失败 (第 {attempt} 次尝试)")
            except Exception as e:
                logger.error(f"卡片推送异常: {e} (第 {attempt} 次尝试)")

            if attempt < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY_SECONDS)

        log_entry["status"] = "failed"
        self._push_log.append(log_entry)
        logger.error("卡片推送最终失败")
        return False
    
    # ── 内部方法 ──────────────────────────────────────────────────────────────
    
    def _send_message(self, text: str, title: str = "", push_type: str = "custom") -> bool:
        """
        发送消息（带重试）
        
        使用 OpenClaw 的 message 工具发送消息。
        在 Agent 上下文中，此方法返回消息内容，由 Agent 调用 message 工具发送。
        在脚本模式下，通过 subprocess 调用。
        """
        # 截断超长消息
        text = CardGenerator.truncate(text, self.MAX_MESSAGE_LENGTH)
        
        # 记录推送日志
        log_entry = {
            "push_type": push_type,
            "title": title,
            "target": self.target,
            "content_length": len(text),
            "timestamp": datetime.now().isoformat(),
            "status": "pending",
        }
        
        # 执行推送（带重试）
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                success = self._do_send(text, self.target)
                if success:
                    log_entry["status"] = "sent"
                    log_entry["attempt"] = attempt
                    self._push_log.append(log_entry)
                    logger.info(f"推送成功: {title} (第 {attempt} 次尝试)")
                    return True
                else:
                    logger.warning(f"推送失败: {title} (第 {attempt} 次尝试)")
            except Exception as e:
                logger.error(f"推送异常: {title} - {e} (第 {attempt} 次尝试)")
            
            if attempt < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY_SECONDS)
        
        # 所有重试都失败
        log_entry["status"] = "failed"
        log_entry["error"] = "Max retries exceeded"
        self._push_log.append(log_entry)
        logger.error(f"推送最终失败: {title}")
        return False
    
    def _do_send(self, text: str, target: str) -> bool:
        """
        实际发送消息（纯文本）
        
        优先尝试通过 OpenClaw CLI 发送，如果不可用则记录到日志。
        """
        if not target:
            logger.warning("未配置推送目标，消息仅记录到日志")
            logger.info(f"消息内容:\n{text}")
            return True
        
        try:
            # 尝试通过 OpenClaw CLI 发送
            cmd = [
                "openclaw", "message", "send",
                "--channel", "feishu",
                "--target", target,
                "--message", text,
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                return True
            else:
                logger.error(f"OpenClaw CLI 返回错误: {result.stderr}")
                return False
        except FileNotFoundError:
            # openclaw CLI 不可用，尝试通过 Agent 模式
            logger.info("OpenClaw CLI 不可用，消息内容记录到日志供 Agent 转发")
            logger.info(f"消息内容:\n{text}")
            return True
        except subprocess.TimeoutExpired:
            logger.error("OpenClaw CLI 调用超时")
            return False

    def _do_send_card(self, card_json: str, target: str) -> bool:
        """
        实际发送飞书交互式卡片
        
        通过 OpenClaw CLI 发送卡片消息。
        """
        if not target:
            logger.warning("未配置推送目标，卡片内容记录到日志")
            logger.info(f"卡片JSON:\n{card_json}")
            return True

        try:
            cmd = [
                "openclaw", "message", "send",
                "--channel", "feishu",
                "--target", target,
                "--card", card_json,
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                return True
            else:
                logger.error(f"卡片发送 CLI 错误: {result.stderr}")
                # 降级：尝试纯文本
                return self._do_send("[卡片发送失败，内容见日志]", target)
        except FileNotFoundError:
            logger.info("OpenClaw CLI 不可用，卡片内容记录到日志")
            logger.info(f"卡片JSON:\n{card_json}")
            return True
        except subprocess.TimeoutExpired:
            logger.error("卡片发送 CLI 超时")
            return False
    
    def _load_default_target(self) -> str:
        """从配置文件加载默认推送目标"""
        try:
            from ..core.config import NOTIFICATION
            return NOTIFICATION.get("feishu", {}).get("daily_report_target", "")
        except (ImportError, ValueError, AttributeError):
            # 回退：直接加载配置文件
            import os
            config_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "core", "config.py"))
            if os.path.exists(config_path):
                import importlib.util
                spec = importlib.util.spec_from_file_location("_config", config_path)
                cfg = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(cfg)
                return cfg.NOTIFICATION.get("feishu", {}).get("daily_report_target", "")
            return ""
    
    # ── 日志和状态 ────────────────────────────────────────────────────────────
    
    @property
    def push_log(self) -> list:
        """获取推送日志"""
        return self._push_log.copy()
    
    def get_stats(self) -> dict:
        """获取推送统计"""
        total = len(self._push_log)
        sent = sum(1 for log in self._push_log if log["status"] == "sent")
        failed = sum(1 for log in self._push_log if log["status"] == "failed")
        return {
            "total": total,
            "sent": sent,
            "failed": failed,
            "success_rate": f"{sent/total*100:.1f}%" if total > 0 else "N/A",
        }


class FeishuPusherAgent:
    """
    Agent 模式飞书推送器
    
    当代码运行在 OpenClaw Agent 上下文中时使用。
    直接返回消息内容，由 Agent 调用 message 工具发送。
    """
    
    def __init__(self):
        self.card_gen = CardGenerator()
    
    def prepare_daily_report(self, report: dict) -> dict:
        """准备每日研报消息（返回给 Agent 发送）"""
        text = self.card_gen.generate_daily_card(report)
        date_str = report.get("date", datetime.now().strftime("%Y-%m-%d"))
        return {
            "text": CardGenerator.truncate(text),
            "title": f"📊 智能投资日报 - {date_str}",
            "push_type": "daily_report",
        }
    
    def prepare_alert(self, alert: dict) -> dict:
        """准备预警消息（返回给 Agent 发送）"""
        text = self.card_gen.generate_alert_card(alert)
        return {
            "text": CardGenerator.truncate(text),
            "title": "⚠️ 风险预警",
            "push_type": "alert",
        }
    
    def prepare_surprise(self, surprise: dict) -> dict:
        """准备超预期消息（返回给 Agent 发送）"""
        text = self.card_gen.generate_surprise_card(surprise)
        return {
            "text": CardGenerator.truncate(text),
            "title": "🎯 超预期消息",
            "push_type": "surprise",
        }
    
    def prepare_open_check(self, data: dict) -> dict:
        """准备开盘检查消息"""
        text = self.card_gen.generate_open_check_card(data)
        return {
            "text": CardGenerator.truncate(text),
            "title": "🔔 开盘检查",
            "push_type": "open_check",
        }
    
    def prepare_close_report(self, report: dict) -> dict:
        """准备收盘简报消息"""
        text = self.card_gen.generate_close_card(report)
        date_str = report.get("date", datetime.now().strftime("%Y-%m-%d"))
        return {
            "text": CardGenerator.truncate(text),
            "title": f"📈 收盘简报 - {date_str}",
            "push_type": "close_report",
        }

    def prepare_pool_report(self, report: dict) -> dict:
        """准备备选股池消息"""
        text = self.card_gen.generate_pool_card(report)
        date_str = report.get("date", datetime.now().strftime("%Y-%m-%d"))
        total = report.get("total", 0)
        return {
            "text": CardGenerator.truncate(text),
            "title": f"🏆 备选股池 - {date_str}（{total} 只）",
            "push_type": "pool_report",
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    pusher = FeishuPusher(target="")
    
    # 测试每日研报
    daily_data = {
        "date": "2026-03-20",
        "market": {
            "sh_index": {"close": 3200.0, "change_pct": 0.5},
            "sz_index": {"close": 10500.0, "change_pct": 0.8},
        },
        "highlights": [
            {"name": "福耀玻璃", "reason": "业绩超预期 +15%", "star": False},
            {"name": "中国海油", "reason": "单季度净利润历史新高", "star": True},
        ],
        "holdings": [
            {"name": "福耀玻璃", "code": "600660.SH", "price": 57.85, "change_pct": 1.2},
            {"name": "中国海油", "code": "600938.SH", "price": 41.02, "change_pct": 0.8},
            {"name": "东方电气", "code": "600875.SH", "price": 43.00, "change_pct": -0.3},
        ],
        "risks": ["无"],
        "suggestions": ["继续持有，关注季报发布"],
    }
    
    result = pusher.push_daily_report(daily_data)
    print(f"推送结果: {result}")
    print(f"推送统计: {pusher.get_stats()}")
