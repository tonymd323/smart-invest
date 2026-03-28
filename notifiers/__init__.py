"""
飞书推送模块
============
提供飞书消息推送和卡片生成功能。

使用方式：
    from smart_invest.notifiers import FeishuPusher, FeishuPusherAgent, CardGenerator
    
    # Agent 模式（推荐）
    agent = FeishuPusherAgent()
    msg = agent.prepare_daily_report(data)
    # 然后由 Agent 调用 message 工具发送 msg["text"]
    
    # 直接推送模式（需要配置推送目标）
    pusher = FeishuPusher(target="oc_xxx")
    pusher.push_daily_report(data)
"""

from .card_generator import CardGenerator
from .feishu_pusher import FeishuPusher, FeishuPusherAgent

__all__ = [
    "CardGenerator",
    "FeishuPusher",
    "FeishuPusherAgent",
]
