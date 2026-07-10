"""MCP 稳定工具面（D9 前缀不变量）：恒定三工具，server/tool/schema 全是返回数据。

模型永远只见 mcp_list_tools / mcp_describe_tool / mcp_call——远端 server 的
schema/顺序漂移、本 run 的 server 集差异都不改工具面字节，前缀缓存不被打穿。
连接惰性化：装配期只校验名字（配置即授权边界，未知名 fail-loud），
首次使用才连；运行时不可达降级为该次调用的 error 文本（不拖死 run）。
"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，build_agent 同类豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

from langchain_core.tools import BaseTool, StructuredTool
from langchain_mcp_adapters.client import MultiServerMCPClient
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from kokoro_agent.mcp.config import McpServerConfig, select_servers
from kokoro_agent.mcp.servers import McpConnectionError, build_connections


def mcp_tool_name(server: str, tool: str) -> str:
    return f"mcp__{server}__{tool}"


class ListToolsArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class DescribeToolArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    server: str = Field(description="MCP server 名（来自 mcp_list_tools）。")
    tool: str = Field(description="工具名（来自 mcp_list_tools）。")


class CallToolArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

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
            client = MultiServerMCPClient(build_connections({server: config}))
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

    async def call_tool(server: str, tool: str, arguments: dict[str, JsonValue] | None = None) -> str:
        found = await _find_tool(server, tool)
        if isinstance(found, str):
            return found
        try:
            result: object = await found.ainvoke(arguments or {})
        except Exception as exc:  # 远端执行失败：错误文本给模型自纠，不炸 run。
            return f"error: {mcp_tool_name(server, tool)} 调用失败：{exc}"
        return result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, default=str)

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
