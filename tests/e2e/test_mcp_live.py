"""Real owned MCP HTTP × current run client × stable tools and durable journal.

Owner responses are controlled; this is not actual IAM/Platform acceptance.
Retired YAML/command tests are replaced, not preserved as a compatibility runtime.
"""

from __future__ import annotations

# BaseTool.ainvoke is the upstream unparameterized test boundary.
# pyright: reportUnknownMemberType=false

import asyncio
from collections.abc import AsyncGenerator
import socket

import pytest
import uvicorn
from langchain_core.messages import ToolMessage
from mcp.server.fastmcp import FastMCP
from pydantic import JsonValue, TypeAdapter

from kokoro_agent.clients.mcp import McpClientError, McpConnectionRef
from kokoro_agent.clients.mcp_runtime import PlatformMcpClient
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as p
from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r
from kokoro_agent.mcp.tools import make_mcp_tools
from support.managed_mcp import ManagedMcpOwner
from support.fakes import FakeRunRepository
from support.tool_journal import journal_middleware as _mw, journal_request as _request

pytestmark = pytest.mark.e2e


@pytest.fixture
async def managed_owner() -> AsyncGenerator[ManagedMcpOwner, None]:
    mcp = FastMCP("owned-managed", stateless_http=True)

    def echo(
        text: str, maximum: int = 0, metadata: dict[str, str] | None = None
    ) -> str:
        owner.tool_calls += 1
        return f"echo:{text}:{maximum}:{metadata}"

    mcp.add_tool(echo)
    schema = TypeAdapter(dict[str, JsonValue]).validate_python(
        (await mcp.list_tools())[0].inputSchema
    )
    app = uvicorn.Server(
        uvicorn.Config(
            mcp.streamable_http_app(), log_level="error", timeout_graceful_shutdown=1
        )
    )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        task = asyncio.create_task(app.serve(sockets=[sock]))
        try:

            async def ready() -> None:
                while not app.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(ready(), 5)
            owner = ManagedMcpOwner(
                endpoint=f"http://127.0.0.1:{port}/mcp", schemas={"echo": schema}
            )
            yield owner
        finally:
            app.should_exit = True
            await asyncio.wait_for(task, 5)


async def test_real_sdk_current_owner_and_journal_roundtrip(
    managed_owner: ManagedMcpOwner,
) -> None:
    client = PlatformMcpClient(
        managed_owner, packages=None, run_id="rn", egress_mode="off"
    )
    ref = McpConnectionRef(connection_id="connection-1")
    await client.check((ref,))
    tools = make_mcp_tools((ref,), client, run_id="rn")
    assert "connection-1/echo" in await tools[0].ainvoke({})
    assert '"text"' in await tools[1].ainvoke(
        {"server": "connection-1", "tool": "echo"}
    )
    store = FakeRunRepository()
    journal = await _mw(store)

    async def invoke(request: object) -> ToolMessage:
        output = await tools[2].ainvoke(
            {"server": "connection-1", "tool": "echo", "arguments": {"text": "本地"}}
        )
        return ToolMessage(content=output, name="mcp_call", tool_call_id="c1")

    first = await journal.awrap_tool_call(_request("mcp_call"), invoke)
    assert isinstance(first, ToolMessage) and "echo:本地" in first.text
    before = len(managed_owner.requests)
    await journal.awrap_tool_call(_request("mcp_call"), invoke)
    assert len(managed_owner.requests) == before
    grants = [
        req
        for req in managed_owner.requests
        if isinstance(req, p.AuthorizeMcpToolRequest)
    ]
    uses = [
        req.credential_use.field
        for req in managed_owner.requests
        if isinstance(req, r.AcquireMcpRuntimeCredentialRequest)
        and req.credential_use is not None
    ]
    assert len(grants) == 1 and uses == ["discovery", "discovery", "invocation"]
    managed_owner.revoked = True
    assert "CURRENT_READ_FAILED" in await tools[1].ainvoke(
        {"server": "connection-1", "tool": "echo"}
    )


async def test_wrong_grant_schema_and_resolution_never_reach_call(
    managed_owner: ManagedMcpOwner,
) -> None:
    client = PlatformMcpClient(
        managed_owner, packages=None, run_id="rn", egress_mode="off"
    )
    ref = McpConnectionRef(connection_id="connection-1")
    for kind in ("bad_grant", "bad_resolution", "bad_schema"):
        setattr(managed_owner, kind, True)
        with pytest.raises((McpClientError, RuntimeError)):
            await client.call_tool(ref, "echo", {"text": "never"}, tool_call_id=kind)
        assert managed_owner.tool_calls == 0
        setattr(managed_owner, kind, False)
    managed_owner.endpoint = "http://127.0.0.1:1/mcp"
    from kokoro_agent.mcp.managed_session import McpSessionError

    with pytest.raises(McpSessionError, match="MCP_SESSION_FAILED"):
        await client.call_tool(
            ref, "echo", {"text": "unreachable"}, tool_call_id="unreachable"
        )
    assert managed_owner.tool_calls == 0


async def test_arguments_are_frozen_before_any_owner_await(
    managed_owner: ManagedMcpOwner,
) -> None:
    client = PlatformMcpClient(
        managed_owner, packages=None, run_id="rn", egress_mode="off"
    )
    nested: dict[str, JsonValue] = {"key": "original"}
    arguments: dict[str, JsonValue] = {
        "text": "original",
        "maximum": 9223372036854775807,
        "metadata": nested,
    }

    def mutate() -> None:
        arguments.update(text="mutated-after-grant")
        nested.update(key="mutated-after-grant")

    managed_owner.on_authorize = mutate
    result = await client.call_tool(
        McpConnectionRef(connection_id="connection-1"),
        "echo",
        arguments,
        tool_call_id="mutation",
    )
    assert "echo:original" in result.model_dump_json()
    assert "9223372036854775807" in result.model_dump_json()
    assert "mutated-after-grant" not in result.model_dump_json()
    assert arguments["text"] == "mutated-after-grant"


async def test_managed_stdio_current_grant_roundtrip_removes_process_and_files() -> (
    None
):
    from copy import deepcopy
    import os
    import json
    from pathlib import Path
    import httpx
    from protobuf import Oneof
    from mcp.types import TextContent
    from kokoro_agent.clients.mcp_package_transport import McpPackageTransport
    from integration.test_mcp_managed_session import fixture

    payload, stdio_resolution, stdio_material = fixture()
    assert (
        stdio_resolution.transport is not None
        and stdio_resolution.transport.field == "stdio"
    )
    assert (
        stdio_material.transport is not None
        and stdio_material.transport.field == "stdio"
    )
    stdio = deepcopy(stdio_resolution.transport.value)
    credential = deepcopy(stdio_material.transport.value)
    local = FastMCP("metadata-only")

    async def echo(wait: bool = False) -> dict[str, JsonValue]:
        return {}

    local.add_tool(echo)
    schema = TypeAdapter(dict[str, JsonValue]).validate_python(
        (await local.list_tools())[0].inputSchema
    )

    class StdioOwner(ManagedMcpOwner):
        async def send(self, request: object, *, timeout_s: float = 10) -> object:
            # The public sender protocol is narrowed by actual call sites below.
            if not isinstance(
                request,
                (
                    p.GetMcpConnectionRequest,
                    p.GetMcpConnectorRequest,
                    p.ListMcpServerDeclarationsRequest,
                    p.AuthorizeMcpToolRequest,
                    r.ResolveMcpRuntimeConnectionRequest,
                    r.AcquireMcpRuntimeCredentialRequest,
                ),
            ):
                raise AssertionError(type(request))
            response = await super().send(request, timeout_s=timeout_s)
            if isinstance(response, p.ListMcpServerDeclarationsResponse):
                response.servers[0].transport = p.McpTransport.STDIO
            if (
                isinstance(response, r.ResolveMcpRuntimeConnectionResponse)
                and response.resolution is not None
            ):
                response.resolution.transport = Oneof("stdio", deepcopy(stdio))
            if isinstance(response, r.AcquireMcpRuntimeCredentialResponse):
                response.transport = Oneof("stdio", deepcopy(credential))
            return response

    owner = StdioOwner(endpoint="unused", schemas={"echo": schema})

    async def get(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(payload))

    async with McpPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(get)
    ) as packages:
        client = PlatformMcpClient(owner, packages=packages, run_id="stdio")
        result = await client.call_tool(
            McpConnectionRef(connection_id="connection-1"),
            "echo",
            {},
            tool_call_id="stdio-call",
        )
    assert isinstance(result.content[0], TextContent)
    value = json.loads(result.content[0].text)
    assert not Path(value["cwd"]).exists()
    with pytest.raises(ProcessLookupError):
        os.kill(value["pid"], 0)
