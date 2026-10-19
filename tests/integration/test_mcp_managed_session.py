"""Real owned stdio SDK lifecycle; no model, network provider or deployment YAML."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import time
import zipfile

import httpx
import pytest
from protobuf import Oneof
from pydantic import JsonValue

from mcp.types import TextContent

from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r


pytestmark = pytest.mark.integration


def fixture() -> tuple[
    bytes, r.McpRuntimeResolution, r.AcquireMcpRuntimeCredentialResponse
]:
    content = (
        f"#!{sys.executable}\n"
        + """from mcp.server.fastmcp import FastMCP
import asyncio,os
server=FastMCP('owned-lifecycle')
@server.tool()
async def echo(wait: bool=False) -> dict:
    if wait: await asyncio.sleep(60)
    return {'cwd':os.getcwd(),'pid':os.getpid()}
server.run(transport='stdio')
"""
    ).encode()
    manifest: dict[str, JsonValue] = {
        "schema_version": 1,
        "entrypoint": "bin/server",
        "files": [
            {"path": "bin/server", "sha256": hashlib.sha256(content).hexdigest()}
        ],
        "declarations": [
            {
                "kind": "tool",
                "selector": "echo",
                "display_name": "Echo",
                "input_schema_digest": "a" * 64,
                "output_schema_digest": "",
                "required_scopes": [],
            }
        ],
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("mcp.json", json.dumps(manifest).encode())
        entry = zipfile.ZipInfo("bin/server")
        entry.external_attr = 0o100755 << 16
        archive.writestr(entry, content)
    payload = buffer.getvalue()
    expiry = int(time.time()) + 30
    resolution = r.McpRuntimeResolution(
        resolution_ref="fixture-resolution",
        expires_at_unix_seconds=expiry,
        transport=Oneof(
            "stdio",
            r.McpRuntimeStdioConnection(
                runtime_artifact_ref="mcp-package:11111111-1111-4111-8111-111111111111",
                source_version_id="source",
                artifact_sha256=hashlib.sha256(payload).hexdigest(),
                entrypoint="bin/server",
            ),
        ),
    )
    material = r.AcquireMcpRuntimeCredentialResponse(
        resolution_ref_sha256=hashlib.sha256(b"fixture-resolution").hexdigest(),
        expires_at_unix_seconds=expiry,
        transport=Oneof(
            "stdio",
            r.McpRuntimeStdioCredential(
                package_download=r.McpRuntimePackageDownload(
                    url_utf8=b"https://objects.test/fixture?secret=signature",
                    size_bytes=len(payload),
                    expires_at_unix_seconds=expiry,
                )
            ),
        ),
    )
    return payload, resolution, material


async def test_initialize_list_call_and_exit_remove_process_and_directory() -> None:
    from kokoro_agent.clients.mcp_package_transport import McpPackageTransport
    from kokoro_agent.mcp.managed_session import managed_mcp_session

    payload, resolution, material = fixture()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(payload))

    async with McpPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as packages:
        async with managed_mcp_session(
            resolution, material, packages=packages
        ) as session:
            assert [tool.name for tool in (await session.list_tools()).tools] == [
                "echo"
            ]
            result = await session.call_tool("echo", {})
            assert isinstance(result.content[0], TextContent)
            value = json.loads(result.content[0].text)
            directory = Path(value["cwd"])
            pid = value["pid"]
            assert directory.is_dir()
        assert not directory.exists()
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    assert material.transport is None


async def test_cancel_inflight_closes_child_and_material() -> None:
    from kokoro_agent.clients.mcp_package_transport import McpPackageTransport
    from kokoro_agent.mcp.managed_session import managed_mcp_session

    payload, resolution, material = fixture()
    entered = asyncio.Event()
    values: list[tuple[Path, int]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(payload))

    async with McpPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as packages:

        async def invoke() -> None:
            async with managed_mcp_session(
                resolution, material, packages=packages
            ) as session:
                first = await session.call_tool("echo", {})
                assert isinstance(first.content[0], TextContent)
                value = json.loads(first.content[0].text)
                values.append((Path(value["cwd"]), value["pid"]))
                entered.set()
                await session.call_tool("echo", {"wait": True})

        task = asyncio.create_task(invoke())
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert material.transport is None and not values[0][0].exists()
    with pytest.raises(ProcessLookupError):
        os.kill(values[0][1], 0)


async def test_human_pause_preserves_interrupt_and_cleans_session() -> None:
    from langgraph.errors import GraphInterrupt
    from kokoro_agent.clients.mcp_package_transport import McpPackageTransport
    from kokoro_agent.mcp.managed_session import managed_mcp_session

    payload, resolution, material = fixture()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(payload))

    process: tuple[Path, int] | None = None
    async with McpPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as packages:
        with pytest.raises(GraphInterrupt):
            async with managed_mcp_session(
                resolution, material, packages=packages
            ) as session:
                first = await session.call_tool("echo", {})
                assert isinstance(first.content[0], TextContent)
                value = json.loads(first.content[0].text)
                process = (Path(value["cwd"]), value["pid"])
                raise GraphInterrupt()
    assert process is not None
    assert material.transport is None and not process[0].exists()
    with pytest.raises(ProcessLookupError):
        os.kill(process[1], 0)


async def test_owned_http_sdk_roundtrip_and_material_release() -> None:
    import socket
    import uvicorn
    from mcp.server.fastmcp import FastMCP
    from kokoro_agent.mcp.managed_session import managed_mcp_session

    server_mcp = FastMCP("owned-http", stateless_http=True)

    def echo(text: str) -> str:
        return text

    server_mcp.add_tool(echo)

    server = uvicorn.Server(
        uvicorn.Config(
            server_mcp.streamable_http_app(),
            log_level="error",
            timeout_graceful_shutdown=1,
        )
    )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        task = asyncio.create_task(server.serve(sockets=[sock]))
        try:

            async def ready() -> None:
                while not server.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(ready(), 5)
            expiry = int(time.time()) + 30
            resolution = r.McpRuntimeResolution(
                resolution_ref="http-fixture",
                expires_at_unix_seconds=expiry,
                transport=Oneof(
                    "streamable_http",
                    r.McpRuntimeHttpConnection(
                        canonical_endpoint=f"http://127.0.0.1:{port}/mcp"
                    ),
                ),
            )
            material = r.AcquireMcpRuntimeCredentialResponse(
                resolution_ref_sha256=hashlib.sha256(b"http-fixture").hexdigest(),
                expires_at_unix_seconds=expiry,
                transport=Oneof("streamable_http", r.McpRuntimeHttpCredential()),
            )
            # Explicit loopback opt-in belongs only to this owned fixture, not normal config.
            async with managed_mcp_session(
                resolution, material, packages=None, egress_mode="off"
            ) as session:
                assert [tool.name for tool in (await session.list_tools()).tools] == [
                    "echo"
                ]
                result = await session.call_tool("echo", {"text": "owned-roundtrip"})
                assert (
                    isinstance(result.content[0], TextContent)
                    and result.content[0].text == "owned-roundtrip"
                )
            assert material.transport is None
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 5)
