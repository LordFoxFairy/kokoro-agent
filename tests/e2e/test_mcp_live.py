"""MCP live 集成：进程内 FastMCP(streamable-http) 真服务器 × langchain-mcp-adapters 全链。

覆盖单测 fake client 够不到的整合面：真 HTTP 传输、白名单过滤、mcp__ 重命名、
真实调用往返、不可达 fail-closed。无外部依赖，可进 CI。
"""

from __future__ import annotations

# BaseTool.ainvoke 的 Input 泛型未参数化：真实第三方边界，沿 build_agent.py 先例做文件级最窄豁免。
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

import asyncio
import socket
import threading
import time

import pytest
import uvicorn
from mcp.server.fastmcp import FastMCP

from kokoro_agent.contract import McpServer
from kokoro_agent.mcp.servers import McpConnectionError
from kokoro_agent.mcp.tools import load_mcp_tools


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


def _server(url: str, allowed: list[str]) -> McpServer:
    return McpServer(
        name="fx", transport="streamable_http", url=url, allowed_tools=allowed, timeout_s=10
    )


async def test_live_roundtrip_with_allowlist(mcp_base_url: str) -> None:
    tools = await load_mcp_tools([_server(mcp_base_url, ["echo"])])
    # 白名单过滤 secret；命名规则 mcp__{server}__{tool}。
    assert [tool.name for tool in tools] == ["mcp__fx__echo"]
    result = await tools[0].ainvoke({"text": "你好"})
    # MCP 工具返回标准 content blocks：emit 层以 .text 收窄，此处按块形断言。
    assert isinstance(result, list)
    first = result[0]
    assert isinstance(first, dict) and first["type"] == "text" and first["text"] == "echo:你好"


async def test_live_unreachable_fails_closed() -> None:
    dead = _server(f"http://127.0.0.1:{_free_port()}/mcp", ["echo"])
    with pytest.raises(McpConnectionError):
        await asyncio.wait_for(load_mcp_tools([dead]), timeout=30)
