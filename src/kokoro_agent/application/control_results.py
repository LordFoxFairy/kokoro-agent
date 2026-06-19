"""应用层：control 决定回灌模型的工具结果文案。"""

from __future__ import annotations


def rejection_result(tool_name: str) -> str:
    """用户主动点击 reject 时回给模型的结果文案。"""
    return f"用户拒绝了工具 {tool_name} 的调用。"
