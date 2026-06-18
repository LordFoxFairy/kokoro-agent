from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import cast

from deepagents import FilesystemPermission
from langchain_core.tools import StructuredTool

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.infrastructure.approval_policy import approval_policy
from kokoro_agent.infrastructure.control import (
    DecisionCursor,
    await_decision,
    rejection_result,
)
from kokoro_agent.infrastructure.json_types import JsonValue
from kokoro_agent.infrastructure.transport import StreamPort


def blocked_tools(mode: PermissionMode) -> frozenset[str]:
    """该权限模式下被拦的工具集：auto 不拦 / default 拦敏感集 / plan 只读再加严。"""
    policy = approval_policy()
    if mode == "auto":
        return frozenset()
    if mode == "plan":
        return policy.requires_approval_tools | policy.plan_only_blocked_tools
    return policy.requires_approval_tools


def tool_allowed(mode: PermissionMode, tool_name: str) -> bool:
    return tool_name not in blocked_tools(mode)


def fs_permissions(mode: PermissionMode) -> list[FilesystemPermission]:
    """deepagents 内部文件系统工具的门控（经 create_deep_agent(permissions=)）：
    plan 只读——拦写操作 write_file/edit_file，读类(ls/read_file/glob/grep)放行；
    auto/default 不限文件系统（default 只拦外部网络 fetch_url，见 blocked_tools）。"""
    if mode == "plan":
        return [FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")]
    return []


def gate_tools(
    tools: Sequence[StructuredTool], mode: PermissionMode
) -> list[StructuredTool]:
    """按权限模式包装工具：被拦的工具执行时返回拦截结果（模型据此调整），不真正执行。"""
    if mode == "auto":
        return list(tools)
    return [tool if tool_allowed(mode, tool.name) else _gate(tool, mode) for tool in tools]


def gate_tools_interactive(
    tools: Sequence[StructuredTool],
    mode: PermissionMode,
    run_id: str,
    port: StreamPort,
) -> list[StructuredTool]:
    """交互式门控：被门控工具调用时阻塞等审批（control 流），approve 跑真工具 / reject 回拒绝。
    translator 在 tool.invoked 后补 tool.awaiting_approval 让前端弹审批（见 drive_agent_events）。"""
    if mode == "auto":
        return list(tools)
    # 同一 run 的所有门控工具共享一个决定游标：决定按到达顺序逐个消费，互不串读。
    cursor = DecisionCursor()
    return [
        tool
        if tool_allowed(mode, tool.name)
        else _approval_gate(tool, run_id, port, cursor)
        for tool in tools
    ]


def _approval_gate(
    tool: StructuredTool,
    run_id: str,
    port: StreamPort,
    cursor: DecisionCursor,
) -> StructuredTool:
    async def gated_async(**kwargs: JsonValue) -> str:
        decision = await await_decision(port, run_id, cursor)
        if decision != "approve":
            return rejection_result(tool.name)

        if tool.coroutine is not None:
            coroutine = cast("Callable[..., Awaitable[str]]", tool.coroutine)
            return await coroutine(**kwargs)

        func = cast("Callable[..., str] | None", tool.func)
        if func is None:
            msg = f"tool {tool.name} has no callable execution path"
            raise RuntimeError(msg)
        return func(**kwargs)

    def gated_sync(**_kwargs: JsonValue) -> str:
        msg = "approval-gated tool requires async execution"
        raise RuntimeError(msg)

    return StructuredTool(
        name=tool.name,
        description=tool.description,
        args_schema=tool.args_schema,
        func=gated_sync,
        coroutine=gated_async,
    )


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
