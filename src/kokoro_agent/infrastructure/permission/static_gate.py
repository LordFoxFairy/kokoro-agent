"""静态门控：被拦工具替换为返回拦截文案的桩，不真正执行。"""

from __future__ import annotations

from collections.abc import Sequence

from langchain_core.tools import StructuredTool

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.infrastructure.json_types import JsonValue
from kokoro_agent.infrastructure.permission.rules import tool_allowed


def gate_tools(
    tools: Sequence[StructuredTool], mode: PermissionMode
) -> list[StructuredTool]:
    """按权限模式包装工具：被拦的工具执行时返回拦截结果（模型据此调整），不真正执行。"""
    if mode == "auto":
        return list(tools)
    return [tool if tool_allowed(mode, tool.name) else _gate(tool, mode) for tool in tools]


def _gate(tool: StructuredTool, mode: PermissionMode) -> StructuredTool:
    message = f"工具 {tool.name} 被 {mode} 权限模式拦截：需要更高的信任档位才能执行。"

    def blocked_sync(*_args: JsonValue, **_kwargs: JsonValue) -> str:
        return message

    async def blocked_async(*_args: JsonValue, **_kwargs: JsonValue) -> str:
        return message

    return StructuredTool(
        name=tool.name,
        description=tool.description,
        args_schema=tool.args_schema,
        func=blocked_sync,
        coroutine=blocked_async,
    )
