from __future__ import annotations

from collections.abc import Sequence

from langchain_core.tools import StructuredTool

from kokoro_agent.domain.run_request import PermissionMode

# 各权限模式下被拦的 Kokoro 注入工具名（deepagents 内部工具门控见 HITL spec follow-up）。
# auto 全放行；default 拦外部副作用（fetch_url）；plan 只读规划（再拦 runtime 子代理 "agent"）。
_BLOCKED: dict[PermissionMode, frozenset[str]] = {
    "auto": frozenset(),
    "default": frozenset({"fetch_url"}),
    "plan": frozenset({"fetch_url", "agent"}),
}


def tool_allowed(mode: PermissionMode, tool_name: str) -> bool:
    return tool_name not in _BLOCKED[mode]


def gate_tools(
    tools: Sequence[StructuredTool], mode: PermissionMode
) -> list[StructuredTool]:
    """按权限模式包装工具：被拦的工具执行时返回拦截结果（模型据此调整），不真正执行。"""
    if mode == "auto":
        return list(tools)
    return [tool if tool_allowed(mode, tool.name) else _gate(tool, mode) for tool in tools]


def _gate(tool: StructuredTool, mode: PermissionMode) -> StructuredTool:
    message = f"工具 {tool.name} 被 {mode} 权限模式拦截：需要更高的信任档位才能执行。"

    def blocked_sync(*_args: object, **_kwargs: object) -> str:
        return message

    async def blocked_async(*_args: object, **_kwargs: object) -> str:
        return message

    return StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]  # langchain from_function classmethod is partially typed
        name=tool.name,
        description=tool.description,
        args_schema=tool.args_schema,
        func=blocked_sync,
        coroutine=blocked_async,
        infer_schema=False,
    )
