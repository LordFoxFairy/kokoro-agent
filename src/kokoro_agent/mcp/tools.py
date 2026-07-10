"""MCP 稳定工具面（D9 前缀不变量）：恒定三工具，server/tool/schema 全是返回数据。

模型永远只见 mcp_list_tools / mcp_describe_tool / mcp_call——远端 server 的
schema/顺序漂移、本 run 的 server 集差异都不改工具面字节，前缀缓存不被打穿。
连接惰性化：装配期只校验名字（配置即授权边界，未知名 fail-loud），
首次使用才连；运行时不可达降级为该次调用的 error 文本（不拖死 run）。
"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，build_agent 同类豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Mapping, Sequence
from contextvars import ContextVar
from typing import Any

from langchain.tools import ToolRuntime
from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import BaseTool, StructuredTool
from langchain_mcp_adapters.callbacks import CallbackContext, Callbacks
from langchain_mcp_adapters.client import MultiServerMCPClient
from mcp.shared.context import RequestContext
from mcp.types import ElicitRequestFormParams, ElicitRequestParams, ElicitResult
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from kokoro_agent.hitl import InputRejected, request_input
from kokoro_agent.mcp.config import McpServerConfig, select_servers
from kokoro_agent.mcp.servers import McpConnectionError, build_connections


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
    del mcp_context, context  # 桥不需要会话/回调上下文，仅按 ContextVar 配对当前执行任务。
    bridge = _ELICIT_BRIDGE.get()
    # 无桥（如无工具运行时的直调路径）或非 form 形态（URL elicitation，V1 不做表单驱动）：礼貌 decline。
    if bridge is None or not isinstance(params, ElicitRequestFormParams):
        return ElicitResult(action="decline")
    return await bridge.submit(params)


def _stringify_result(result: object) -> str:
    # MCP 工具返回标准 content blocks（外部载荷）：稳定面统一收敛为字符串。
    return result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, default=str)


# 内层 adapter 工具经空 callbacks 调用：与 langgraph 回调树脱钩，避免作为嵌套工具泄漏进 v3 投影
# （模型只见稳定三工具，远端工具往返是 mcp_call 的实现细节，不单独上 wire）。
_DETACHED_CONFIG: RunnableConfig = {"callbacks": []}


async def _call_with_elicitation(
    found: BaseTool,
    server: str,
    tool: str,
    arguments: dict[str, JsonValue] | None,
    request_id: str,
) -> str:
    """在工具节点执行栈上跑 mcp_call，并把中途的 elicitation 请求桥接为 request_input 暂停。

    request_id=mcp_call 的 tool_id（工具边界幂等锚）。首个 elicitation 触发 request_input：首跑抛
    GraphInterrupt 挂起（in-flight 调用随之 teardown），resume/replay 时 request_input 原地返回人给的
    value，桥回 ElicitResult(accept) 让 server 续跑；reject→decline。多轮 elicitation 逐个走同一循环。
    """
    bridge = _ElicitBridge()
    token = _ELICIT_BRIDGE.set(bridge)
    invoke_task: asyncio.Task[object] = asyncio.ensure_future(
        found.ainvoke(arguments or {}, _DETACHED_CONFIG)
    )
    next_task: asyncio.Task[_ElicitItem] | None = None
    try:
        while True:
            if next_task is None:
                next_task = asyncio.ensure_future(bridge.next())
            await asyncio.wait({invoke_task, next_task}, return_when=asyncio.FIRST_COMPLETED)
            if invoke_task.done():
                exc = invoke_task.exception()
                if exc is not None:  # 远端执行失败：错误文本给模型自纠，不炸 run。
                    return f"error: {mcp_tool_name(server, tool)} 调用失败：{exc}"
                return _stringify_result(invoke_task.result())
            params, fut = next_task.result()
            next_task = None
            outcome = request_input(
                request_id=request_id,
                schema=params.requestedSchema,
                context={"name": mcp_tool_name(server, tool), "args": {"message": params.message}},
            )
            if isinstance(outcome, InputRejected):
                fut.set_result(ElicitResult(action="decline"))
            else:
                fut.set_result(
                    ElicitResult.model_validate({"action": "accept", "content": outcome.value})
                )
    finally:
        _ELICIT_BRIDGE.reset(token)
        if next_task is not None:
            next_task.cancel()
        if not invoke_task.done():
            invoke_task.cancel()
        # 挂起/异常路径下 in-flight 调用的收尾：其结局无关紧要（replay 会重跑），吞掉清理噪声。
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


def make_mcp_tools(
    server_names: Sequence[str],
    registry: Mapping[str, McpServerConfig],
) -> tuple[StructuredTool, StructuredTool, StructuredTool]:
    """per-run 闭包三工具。装配期校验名字但不连接；run 内按 server 缓存 tools/list。"""

    # 未知名在装配期 fail-loud（配置即授权边界）；连接推迟到首次使用。
    granted = select_servers(registry, server_names)
    tools_cache: dict[str, list[BaseTool]] = {}

    async def _server_tools(server: str) -> list[BaseTool]:
        if server not in tools_cache:
            config = granted[server]
            # elicitation callback 随 client 烘进每个工具：ainvoke 建会话时用它把中途请求桥给执行任务。
            client = MultiServerMCPClient(
                build_connections({server: config}),
                callbacks=Callbacks(on_elicitation=_on_elicitation),
            )
            try:
                raw = await client.get_tools(server_name=server)
            except Exception as exc:  # 运行时不可达=外部常态：该次调用降级，不炸 run。
                raise McpConnectionError(f"mcp server {server!r} unreachable: {exc}") from exc
            allowed = frozenset(config.allowed_tools)
            tools_cache[server] = [t for t in raw if t.name in allowed]
        return tools_cache[server]

    async def list_tools() -> str:
        if not granted:
            return "本次运行没有可用的 MCP server。"
        lines: list[str] = []
        for server in granted:
            try:
                tools = await _server_tools(server)
            except McpConnectionError as exc:
                lines.append(f"{server}: [不可达] {exc}")
                continue
            if not tools:
                lines.append(f"{server}: （白名单内无可用工具）")
            for tool in tools:
                summary = (tool.description or "").strip().splitlines()[0] if tool.description else ""
                lines.append(f"{server}/{tool.name} — {summary}")
        return "\n".join(lines)

    async def _find_tool(server: str, tool: str) -> BaseTool | str:
        if server not in granted:
            return f"error: MCP server {server!r} 不在本次运行的授权集内（用 mcp_list_tools 查看）。"
        try:
            tools = await _server_tools(server)
        except McpConnectionError as exc:
            return f"error: {exc}"
        for candidate in tools:
            if candidate.name == tool:
                return candidate
        return f"error: server {server!r} 没有名为 {tool!r} 的工具（或不在白名单内）。"

    async def describe_tool(server: str, tool: str) -> str:
        found = await _find_tool(server, tool)
        if isinstance(found, str):
            return found
        # 上游注解声称 type[BaseModel]，但 adapters 工具运行时给 dict（live e2e 实证）：dict 分支优先。
        schema: object = found.tool_call_schema
        raw: object = schema if isinstance(schema, dict) else schema.model_json_schema()
        rendered = json.dumps(raw, ensure_ascii=False, default=str)
        return f"{server}/{tool}\n{(found.description or '').strip()}\n参数 schema：{rendered}"

    async def call_tool(
        server: str,
        tool: str,
        arguments: dict[str, JsonValue] | None = None,
        *,
        runtime: ToolRuntime[Any, Any] | None = None,
        **_injected: object,
    ) -> str:
        found = await _find_tool(server, tool)
        if isinstance(found, str):
            return found
        if runtime is None:
            # 无工具运行时（如单测直调 ainvoke）：不接 elicitation 桥，直接调用。
            try:
                return _stringify_result(await found.ainvoke(arguments or {}, _DETACHED_CONFIG))
            except Exception as exc:  # 远端执行失败：错误文本给模型自纠，不炸 run。
                return f"error: {mcp_tool_name(server, tool)} 调用失败：{exc}"
        # tool_call_id 在图内工具节点恒有值（幂等锚）；类型上收窄空值兜底。
        return await _call_with_elicitation(
            found, server, tool, arguments, runtime.tool_call_id or ""
        )

    return (
        StructuredTool(
            name="mcp_list_tools",
            description="列出本次运行可用的外部（MCP）server 与工具。调用前先用 mcp_describe_tool 查看参数。",
            args_schema=ListToolsArgs,
            coroutine=list_tools,
        ),
        StructuredTool(
            name="mcp_describe_tool",
            description="查看某个 MCP 工具的说明与参数 schema。",
            args_schema=DescribeToolArgs,
            coroutine=describe_tool,
        ),
        StructuredTool(
            name="mcp_call",
            description="调用一个 MCP 工具（server + tool + arguments）。只允许本次运行授权的 server。",
            args_schema=CallToolArgs,
            coroutine=call_tool,
        ),
    )
