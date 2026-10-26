"""MCP 稳定工具面（D9 前缀不变量）：恒定三工具，server/tool/schema 全是返回数据。

模型永远只见 mcp_list_tools / mcp_describe_tool / mcp_call——远端 server 的
schema/顺序漂移、本 run 的 server 集差异都不改工具面字节，前缀缓存不被打穿。
装配只接受明确Connection引用；每次发现与调用重新核当前owner状态。
标准SDK连接惰性化，凭据不缓存，错误只返回固定类别。
"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，create_deep_agent 同类豁免）。

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Sequence
from contextvars import ContextVar
from typing import Any

from langchain.tools import ToolRuntime
from langchain_core.tools import StructuredTool, ToolException
from langchain_mcp_adapters.callbacks import CallbackContext, Callbacks
from mcp.shared.context import RequestContext
from mcp.types import (
    ElicitRequestFormParams,
    ElicitRequestParams,
    ElicitResult,
    CallToolResult,
)
from langgraph.errors import GraphInterrupt
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from kokoro_agent.hitl import InputRejected, request_input
from kokoro_agent.clients.mcp import McpClient, McpClientError, McpConnectionRef
from kokoro_agent.tools.middleware import current_mcp_tool_call_id


def mcp_tool_name(server: str, tool: str) -> str:
    return f"mcp__{server}__{tool}"


# elicitation 桥：MCP server 的 elicitation 请求在 adapter 的后台 receive loop 任务里被回调，
# 而 request_human/interrupt 必须在工具节点执行栈上抛（挂起点由 checkpoint 承载）。桥把请求从
# receive loop 递给 mcp_call 的执行任务处理，回程 ElicitResult 原路返还——两任务经 ContextVar 配对
# （receive loop 由 ainvoke 内 spawn，继承设桥时的上下文；并发 mcp_call 各自隔离）。
_ElicitItem = tuple[ElicitRequestFormParams, "asyncio.Future[ElicitResult]"]


class _ElicitBridge:
    """把到达 receive loop 的 elicitation 请求交给 tool-node 任务，回程结果原路返还。"""

    def __init__(self) -> None:
        self._inbox: asyncio.Queue[_ElicitItem] = asyncio.Queue()

    async def submit(self, params: ElicitRequestFormParams) -> ElicitResult:
        # receive loop 侧：投请求、等 tool-node 决策结果（挂起期间本 await 随会话 teardown 被取消）。
        fut: asyncio.Future[ElicitResult] = asyncio.get_running_loop().create_future()
        await self._inbox.put((params, fut))
        return await fut

    async def next(self) -> _ElicitItem:
        # tool-node 侧：取下一条待处理 elicitation。
        return await self._inbox.get()


_ELICIT_BRIDGE: ContextVar[_ElicitBridge | None] = ContextVar(
    "kokoro_mcp_elicit_bridge", default=None
)


async def _on_elicitation(
    mcp_context: RequestContext[Any, Any, Any],
    params: ElicitRequestParams,
    context: CallbackContext,
) -> ElicitResult:
    del (
        mcp_context,
        context,
    )  # 桥不需要会话/回调上下文，仅按 ContextVar 配对当前执行任务。
    bridge = _ELICIT_BRIDGE.get()
    # 无桥（如无工具运行时的直调路径）或非 form 形态（URL elicitation，V1 不做表单驱动）：礼貌 decline。
    if bridge is None or not isinstance(params, ElicitRequestFormParams):
        return ElicitResult(action="decline")
    return await bridge.submit(params)


async def _call_with_elicitation(
    invocation: Awaitable[CallToolResult],
    server: str,
    tool: str,
    request_id: str,
) -> str:
    """在工具节点执行栈上跑 mcp_call，并把中途的 elicitation 请求桥接为 request_input 暂停。

    request_id=mcp_call 的 tool_id（工具边界幂等锚）。首个 elicitation 触发 request_input：首跑抛
    GraphInterrupt 挂起（in-flight 调用随之 teardown），同一会话仍活跃时可将人给的 value 桥回 ElicitResult；reject→decline。
    真正挂起会关闭会话，journal 保留 unknown outcome，恢复不重新发送原 tools/call。
    """
    bridge = _ElicitBridge()
    token = _ELICIT_BRIDGE.set(bridge)
    invoke_task: asyncio.Future[CallToolResult] = asyncio.ensure_future(invocation)
    next_task: asyncio.Task[_ElicitItem] | None = None
    try:
        while True:
            if next_task is None:
                next_task = asyncio.ensure_future(bridge.next())
            await asyncio.wait(
                {invoke_task, next_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if invoke_task.done():
                exc = invoke_task.exception()
                if isinstance(exc, GraphInterrupt):
                    raise exc
                if exc is not None:  # 远端执行失败：错误文本给模型自纠，不炸 run。
                    raise ToolException("error: MCP_TOOL_CALL_FAILED") from None
                result = invoke_task.result()
                if result.isError:
                    raise ToolException("error: " + result.model_dump_json())
                return result.model_dump_json()
            params, fut = next_task.result()
            next_task = None
            outcome = request_input(
                request_id=request_id,
                schema=params.requestedSchema,
                context={
                    "name": mcp_tool_name(server, tool),
                    "args": {"message": params.message},
                },
            )
            if isinstance(outcome, InputRejected):
                fut.set_result(ElicitResult(action="decline"))
            else:
                fut.set_result(
                    ElicitResult.model_validate(
                        {"action": "accept", "content": outcome.value}
                    )
                )
    finally:
        _ELICIT_BRIDGE.reset(token)
        if next_task is not None:
            next_task.cancel()
        if not invoke_task.done():
            invoke_task.cancel()
        # 挂起/异常路径下 in-flight 调用的收尾：远端可能已有效果，journal 保留 unknown outcome，吞掉清理噪声。
        with contextlib.suppress(BaseException):
            await invoke_task


class ListToolsArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class DescribeToolArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    server: str = Field(description="MCP server 名（来自 mcp_list_tools）。")
    tool: str = Field(description="工具名（来自 mcp_list_tools）。")


class CallToolArgs(BaseModel):
    # extra="allow"：图内 ToolNode 把注入的 ToolRuntime 并入 args 后再过本模型校验——forbid 会误伤、
    # ignore 会丢弃注入项（union 注解不进 StructuredTool 的重注入集），唯 allow 能让 runtime 穿过校验
    # 交回协程。模型面 schema 经 tool_call_schema 排除注入字段，仍只暴露 server/tool/arguments；
    # 协程 **_injected 兜住任何越过 allow 的多余字段，绝不 TypeError。
    model_config = ConfigDict(strict=True, extra="allow")

    server: str = Field(description="MCP server 名。")
    tool: str = Field(description="要调用的工具名（先用 mcp_describe_tool 看参数）。")
    arguments: dict[str, JsonValue] = Field(
        default_factory=dict, description="工具参数（按 describe 返回的 schema 提供）。"
    )


MCP_0_DESCRIPTION = "列出本次运行可用的外部（MCP）server 与工具。调用前先用 mcp_describe_tool 查看参数。"
MCP_1_DESCRIPTION = "查看某个 MCP 工具的说明与参数 schema。"
MCP_2_DESCRIPTION = (
    "调用一个 MCP 工具（server + tool + arguments）。只允许本次运行授权的 server。"
)
MCP_TOOL_METADATA: tuple[tuple[str, str, type[BaseModel]], ...] = (
    ("mcp_list_tools", MCP_0_DESCRIPTION, ListToolsArgs),
    ("mcp_describe_tool", MCP_1_DESCRIPTION, DescribeToolArgs),
    ("mcp_call", MCP_2_DESCRIPTION, CallToolArgs),
)


def make_mcp_tools(
    references: Sequence[McpConnectionRef],
    client: McpClient | None,
    *,
    run_id: str,
) -> tuple[StructuredTool, StructuredTool, StructuredTool]:
    """One stable tool surface; every read/call rechecks current owner authorization."""
    granted = {ref.connection_id: ref for ref in references}
    if len(granted) != len(references) or granted and client is None:
        raise McpClientError("MCP_CLIENT_REQUIRED")

    async def list_tools() -> str:
        if not granted:
            return "本次运行没有可用的 MCP server。"
        assert client is not None
        lines: list[str] = []
        for server, reference in granted.items():
            try:
                tools = await client.list_tools(reference)
                lines.extend(
                    f"{server}/{tool.name} — {tool.description.splitlines()[0] if tool.description else ''}"
                    for tool in tools
                )
                if not tools:
                    lines.append(f"{server}: （白名单内无可用工具）")
            except asyncio.CancelledError:
                raise
            except Exception:
                lines.append(f"{server}: [不可达] MCP_CURRENT_READ_FAILED")
        return "\n".join(lines)

    async def describe_tool(server: str, tool: str) -> str:
        reference = granted.get(server)
        if reference is None or client is None:
            return "error: MCP_CONNECTION_NOT_DECLARED"
        try:
            tools = await client.list_tools(reference)
            found = next((value for value in tools if value.name == tool), None)
            if found is None:
                return "error: MCP_TOOL_NOT_ALLOWED"
            return f"{server}/{tool}\n{found.description}\n参数 schema：{json.dumps(found.input_schema, ensure_ascii=False)}"
        except asyncio.CancelledError:
            raise
        except Exception:
            return "error: MCP_CURRENT_READ_FAILED"

    async def call_tool(
        server: str,
        tool: str,
        arguments: dict[str, JsonValue] | None = None,
        *,
        runtime: ToolRuntime[Any, Any] | None = None,
        **_injected: object,
    ) -> str:
        del (
            runtime,
            _injected,
        )  # The model/framework field is not a journal-bound call identity.
        reference = granted.get(server)
        call_id = current_mcp_tool_call_id(run_id)
        if reference is None or client is None:
            raise ToolException("error: MCP_CONNECTION_NOT_DECLARED")
        if call_id is None:
            raise ToolException("error: MCP_JOURNAL_CALL_REQUIRED")
        try:
            return await _call_with_elicitation(
                client.call_tool(
                    reference,
                    tool,
                    arguments or {},
                    tool_call_id=call_id,
                    callbacks=Callbacks(on_elicitation=_on_elicitation),
                ),
                server,
                tool,
                call_id,
            )
        except (asyncio.CancelledError, GraphInterrupt, ToolException):
            raise
        except Exception:
            raise ToolException("error: MCP_TOOL_CALL_FAILED") from None

    return (
        StructuredTool(
            name=MCP_TOOL_METADATA[0][0],
            description=MCP_0_DESCRIPTION,
            args_schema=ListToolsArgs,
            coroutine=list_tools,
        ),
        StructuredTool(
            name=MCP_TOOL_METADATA[1][0],
            description=MCP_1_DESCRIPTION,
            args_schema=DescribeToolArgs,
            coroutine=describe_tool,
        ),
        StructuredTool(
            name=MCP_TOOL_METADATA[2][0],
            description=MCP_2_DESCRIPTION,
            args_schema=CallToolArgs,
            coroutine=call_tool,
            handle_tool_error=True,
        ),
    )
