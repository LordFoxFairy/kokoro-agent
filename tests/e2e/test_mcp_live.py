"""MCP live 集成：进程内 FastMCP(streamable-http) 真服务器 × langchain-mcp-adapters 全链。

覆盖单测 fake client 够不到的整合面：真 HTTP 传输、白名单过滤、mcp__ 重命名、
真实调用往返、不可达 fail-closed。无外部依赖，可进 CI。
"""

# BaseTool.ainvoke 上游注解含裸 dict（langchain-core Runnable 泛型缺口）：仅豁免该成员访问。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import asyncio
import socket
import threading
import time

import pytest
import uvicorn
from mcp.server.fastmcp import FastMCP

from kokoro_agent.mcp.config import McpServerConfig
from kokoro_agent.mcp.tools import make_mcp_tools


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _echo(text: str) -> str:
    """Echo the given text back."""
    return f"echo:{text}"


def _secret(text: str) -> str:
    """Must never be exposed (outside the allowlist)."""
    return "leak"


def _build_fixture_app() -> FastMCP:
    # stateless：每请求独立会话，适配 adapters 的短连接调用模式。
    server = FastMCP("fixture", stateless_http=True)
    server.add_tool(_echo, name="echo")
    server.add_tool(_secret, name="secret")
    return server


@pytest.fixture(scope="module")
def mcp_base_url():
    port = _free_port()
    app = _build_fixture_app().streamable_http_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    while time.time() < deadline and not server.started:
        time.sleep(0.05)
    if not server.started:
        raise RuntimeError("fixture mcp server failed to start")
    yield f"http://127.0.0.1:{port}/mcp"
    server.should_exit = True
    thread.join(timeout=5)


def _registry(url: str, allowed: list[str]) -> dict[str, McpServerConfig]:
    return {
        "fx": McpServerConfig(
            transport="streamable_http", url=url, allowed_tools=allowed, timeout_s=10
        )
    }


async def test_live_roundtrip_via_stable_surface(mcp_base_url: str) -> None:
    # 稳定三工具全链：list（白名单过滤 secret）→ describe（schema）→ call（真调用往返）。
    list_tool, describe_tool, call_tool = make_mcp_tools(["fx"], _registry(mcp_base_url, ["echo"]))
    listed: str = await list_tool.ainvoke({})
    assert "fx/echo" in listed and "secret" not in listed
    described: str = await describe_tool.ainvoke({"server": "fx", "tool": "echo"})
    assert "text" in described
    result: str = await call_tool.ainvoke(
        {"server": "fx", "tool": "echo", "arguments": {"text": "你好"}}
    )
    # MCP 工具返回标准 content blocks（外部载荷）：稳定面已序列化为字符串。
    assert "echo:你好" in result


async def test_live_unreachable_degrades_per_call() -> None:
    # 惰性连接：装配不炸；运行时不可达降级为 error 文本（不炸 run）。
    list_tool, _, call_tool = make_mcp_tools(
        ["fx"], _registry(f"http://127.0.0.1:{_free_port()}/mcp", ["echo"])
    )
    listed: str = await asyncio.wait_for(list_tool.ainvoke({}), timeout=30)
    assert "不可达" in listed
    result: str = await asyncio.wait_for(
        call_tool.ainvoke({"server": "fx", "tool": "echo", "arguments": {}}), timeout=30
    )
    assert "error" in result
