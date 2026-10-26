"""Real owned stdio SDK lifecycle; no model, network provider or deployment YAML."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
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
from support.managed_mcp import isolated_mcp_server_state


pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def owned_provider_shutdown() -> AsyncGenerator[None]:
    # Autouse enters before provider setup; existing test finally blocks have
    # already stopped and awaited their Server before this fixture tears down.
    async with isolated_mcp_server_state():
        yield


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


async def test_owned_sse_sdk_roundtrip_202_ack_and_material_release() -> None:
    """New fixed wire case reaches the official SSE SDK, not the HTTP branch."""
    import socket
    import uvicorn
    from mcp.server.fastmcp import FastMCP
    from kokoro_agent.mcp.managed_session import managed_mcp_session

    def field(tag: int, value: bytes) -> bytes:
        size = len(value)
        length = bytearray()
        while size > 127:
            length.append((size & 127) | 128)
            size >>= 7
        length.append(size)
        return bytes([tag]) + bytes(length) + value

    calls: list[str] = []
    mcp = FastMCP("owned-sse")

    def echo(text: str) -> str:
        calls.append(text)
        return text

    mcp.add_tool(echo)
    server = uvicorn.Server(
        uvicorn.Config(mcp.sse_app(), log_level="error", timeout_graceful_shutdown=1)
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
            # Exact producer tags 10/5; old generated code retains unknown wire
            # and must fail, rather than a missing new Python symbol failing collection.
            endpoint = f"http://127.0.0.1:{port}/sse"
            resolution = r.McpRuntimeResolution.from_binary(
                r.McpRuntimeResolution(
                    resolution_ref="sse-fixture", expires_at_unix_seconds=expiry
                ).to_binary()
                + field(0x52, field(0x0A, endpoint.encode()))
            )
            material = r.AcquireMcpRuntimeCredentialResponse.from_binary(
                r.AcquireMcpRuntimeCredentialResponse(
                    resolution_ref_sha256=hashlib.sha256(b"sse-fixture").hexdigest(),
                    expires_at_unix_seconds=expiry,
                ).to_binary()
                + field(0x2A, b"")
            )
            async with managed_mcp_session(
                resolution, material, packages=None, egress_mode="off"
            ) as session:
                assert [tool.name for tool in (await session.list_tools()).tools] == [
                    "echo"
                ]
                payload = "owned-sse-call" + "x" * 20_000
                result = await session.call_tool("echo", {"text": payload})
                assert isinstance(result.content[0], TextContent)
                assert result.content[0].text == payload
            assert calls == [payload]
            assert material.transport is None
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 5)


async def testowned_provider_shutdown_does_not_retire_the_next_sdk_server() -> None:
    from sse_starlette.sse import AppStatus

    previous = AppStatus.should_exit
    assert previous is False
    async with isolated_mcp_server_state():
        await test_owned_sse_sdk_roundtrip_202_ack_and_material_release()
        watchers = [
            task
            for task in asyncio.all_tasks()
            if getattr(task.get_coro(), "__qualname__", "") == "_shutdown_watcher"
        ]
        assert watchers
        # Exercise the real SDK polling its now-stopped provider; no status
        # injection, arbitrary sleep or client/HTTP protocol assertion changes.
        await asyncio.wait_for(asyncio.gather(*watchers), 2)
        assert AppStatus.should_exit is True
    assert AppStatus.should_exit is previous
    async with isolated_mcp_server_state():
        await test_owned_http_sdk_roundtrip_and_material_release()
    assert AppStatus.should_exit is previous


@pytest.mark.parametrize(
    "ack_mode",
    [
        "gzip-first",
        "gzip-late",
        "large-first",
        "large-late",
        "valid-pending",
        "cancel",
        "hitl",
        "expire",
    ],
)
async def test_native_sse_late_result_cannot_hide_observed_bad_ack(
    ack_mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real native SDK + HTTP: result/ACK order never grants a failed session success."""
    import gzip
    import json
    import socket
    import uvicorn
    from mcp.server.fastmcp import FastMCP
    from starlette.requests import Request
    from starlette.types import Scope, Receive, Send, Message
    from kokoro_agent.mcp import managed_session as module
    from kokoro_agent.mcp.sse_transport import SseClientFactory

    factories: list[SseClientFactory] = []
    build = module.build_sse_client_factory

    def observe(endpoint: str, *, mode: str) -> SseClientFactory:
        result = build(endpoint, mode=mode)
        factories.append(result)
        return result

    monkeypatch.setattr(module, "build_sse_client_factory", observe)
    called = asyncio.Event()
    result_gate = asyncio.Event()
    received = asyncio.Event()
    pending_ack = asyncio.Event()
    close_ack = asyncio.Event()
    if not ack_mode.endswith("first") and ack_mode not in {"cancel", "hitl", "expire"}:
        result_gate.set()
    count = 0
    mcp = FastMCP("ack-order")

    async def echo() -> str:
        nonlocal count
        count += 1
        called.set()
        await result_gate.wait()
        return "actual-get-result"

    mcp.add_tool(echo)
    native_app = mcp.sse_app()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] != "POST":
            return await native_app(scope, receive, send)
        body = await Request(scope, receive).body()
        used = False

        async def replay() -> Message:
            nonlocal used
            if not used:
                used = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        if json.loads(body).get("method") != "tools/call":
            return await native_app(scope, replay, send)

        async def capture(message: Message) -> None:
            pass  # Native server still hands the actual request to its own ClientSession.

        await native_app(scope, replay, capture)
        await called.wait()
        if not ack_mode.endswith("first"):
            await received.wait()
        headers = [(b"content-type", b"text/plain")]
        if ack_mode.startswith("gzip"):
            headers.append((b"content-encoding", b"gzip"))
        await send({"type": "http.response.start", "status": 202, "headers": headers})
        if ack_mode == "valid-pending":
            await send({"type": "http.response.body", "body": b"Ac", "more_body": True})
            pending_ack.set()
            await close_ack.wait()
            await send({"type": "http.response.body", "body": b"cepted"})
        else:
            await send(
                {
                    "type": "http.response.body",
                    "body": b"a" * 4_194_305
                    if ack_mode.startswith("large")
                    else gzip.compress(b"Accepted"),
                }
            )

    server = uvicorn.Server(
        uvicorn.Config(
            app, log_level="error", lifespan="off", timeout_graceful_shutdown=1
        )
    )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        task = asyncio.create_task(server.serve(sockets=[sock]))
        try:
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
            expiry = int(time.time()) + 10
            resolution = r.McpRuntimeResolution(
                resolution_ref="ack-order",
                expires_at_unix_seconds=expiry,
                transport=Oneof(
                    "sse",
                    r.McpRuntimeSseConnection(
                        canonical_endpoint=f"http://127.0.0.1:{port}/sse"
                    ),
                ),
            )
            material = r.AcquireMcpRuntimeCredentialResponse(
                resolution_ref_sha256=hashlib.sha256(b"ack-order").hexdigest(),
                # At least a full second reaches the real blocked tool; using
                # floor(now)+1 could expire during SDK initialization instead.
                expires_at_unix_seconds=int(time.time()) + 2
                if ack_mode == "expire"
                else expiry,
                transport=Oneof("sse", r.McpRuntimeSseCredential()),
            )
            observed_results: list[str] = []
            observed_failures: list[bool] = []

            async def observed_failure() -> None:
                while True:
                    try:
                        factories[0].raise_if_failed()
                    except ValueError:
                        observed_failures.append(True)
                        return
                    await asyncio.sleep(0.001)

            async def run() -> None:
                async with module.managed_mcp_session(
                    resolution, material, packages=None, egress_mode="off"
                ) as session:
                    await session.list_tools()  # Preload native output schema; no post-result refresh hides the race.
                    rpc = asyncio.create_task(session.call_tool("echo", {}))
                    if ack_mode in {"cancel", "hitl"}:
                        from langgraph.errors import GraphInterrupt

                        await asyncio.wait_for(called.wait(), 1)
                        rpc.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await rpc
                        if ack_mode == "hitl":
                            raise GraphInterrupt()
                        raise asyncio.CancelledError()
                    if ack_mode.endswith("first"):
                        await asyncio.wait_for(observed_failure(), 2)
                        result_gate.set()
                    result = await asyncio.wait_for(
                        rpc, 5 if ack_mode == "expire" else 2
                    )
                    assert (
                        isinstance(result.content[0], TextContent)
                        and result.content[0].text == "actual-get-result"
                    )
                    observed_results.append(result.content[0].text)
                    received.set()
                    if ack_mode == "valid-pending":
                        await asyncio.wait_for(pending_ack.wait(), 2)
                    else:
                        await asyncio.wait_for(observed_failure(), 2)

            if ack_mode == "valid-pending":
                await run()
            elif ack_mode in {"cancel", "hitl"}:
                from langgraph.errors import GraphInterrupt

                with pytest.raises(
                    GraphInterrupt if ack_mode == "hitl" else asyncio.CancelledError
                ):
                    await run()
                factories[
                    0
                ].raise_if_failed()  # Plain teardown/cancellation is not observed transport failure.
            else:
                started = time.monotonic()
                remaining = material.expires_at_unix_seconds - time.time()
                with pytest.raises(module.McpSessionError, match="MCP_SESSION_FAILED"):
                    await run()
                if ack_mode == "expire":
                    assert (
                        remaining - 0.05 <= time.monotonic() - started < remaining + 0.6
                    )
            assert observed_results == (
                []
                if ack_mode in {"cancel", "hitl", "expire"}
                else ["actual-get-result"]
            )
            assert bool(observed_failures) is (
                ack_mode not in {"valid-pending", "cancel", "hitl", "expire"}
            )
            assert count == 1 and material.transport is None
        finally:
            close_ack.set()
            result_gate.set()
            received.set()
            server.should_exit = True
            await asyncio.wait_for(task, 5)


async def test_real_mcp_handled_failure_and_recovery_keep_callback_journal_and_safe_status() -> (
    None
):
    """Real loopback MCP and native ToolNode, no provider/model credentials or shared stores."""
    import socket
    import uvicorn
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langgraph.stream import CustomTransformer
    from mcp.server.fastmcp import FastMCP
    from pydantic import TypeAdapter

    from kokoro_agent.clients.mcp import McpConnectionRef
    from kokoro_agent.clients.mcp_runtime import PlatformMcpClient
    from kokoro_agent.execution.events import RunEmitter
    from kokoro_agent.execution.publish_agent_events import pump_run
    from kokoro_agent.mcp.tools import make_mcp_tools
    from support.chat import FakeChatRepository
    from support.deepagents import create_test_deep_agent
    from support.fakes import FakeBus, FakeRunRepository
    from support.local_fake import LocalFakeChatModel
    from support.managed_mcp import ManagedMcpOwner
    from support.tool_journal import journal_middleware, journal_request

    mcp = FastMCP("owned-tool-status", stateless_http=True)

    def echo(fail: bool) -> str:
        owner.tool_calls += 1
        if fail:
            raise ValueError("owned recoverable argument")
        return "error: ordinary successful content"

    mcp.add_tool(echo)
    schema = TypeAdapter(dict[str, JsonValue]).validate_python(
        (await mcp.list_tools())[0].inputSchema
    )
    server = uvicorn.Server(
        uvicorn.Config(
            mcp.streamable_http_app(),
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
            owner = ManagedMcpOwner(
                endpoint=f"http://127.0.0.1:{port}/mcp", schemas={"echo": schema}
            )
            ref = McpConnectionRef(connection_id="connection-1")
            client = PlatformMcpClient(
                owner, packages=None, run_id="rn", egress_mode="off"
            )
            tools = make_mcp_tools((ref,), client, run_id="rn")
            store = FakeRunRepository()
            journal = await journal_middleware(store)
            agent = create_test_deep_agent(
                model=LocalFakeChatModel.with_script(
                    [
                        AIMessage(
                            content="",
                            tool_calls=[
                                {
                                    "name": "mcp_call",
                                    "id": "mcp-fail",
                                    "type": "tool_call",
                                    "args": {
                                        "server": "connection-1",
                                        "tool": "echo",
                                        "arguments": {"fail": True},
                                    },
                                }
                            ],
                        ),
                        AIMessage(
                            content="",
                            tool_calls=[
                                {
                                    "name": "mcp_call",
                                    "id": "mcp-success",
                                    "type": "tool_call",
                                    "args": {
                                        "server": "connection-1",
                                        "tool": "echo",
                                        "arguments": {"fail": False},
                                    },
                                }
                            ],
                        ),
                        AIMessage(content="done"),
                    ]
                ),
                tools=list(tools),
                middleware=[journal],
                subagents=[],
                system_prompt="Use the provided tool calls.",
                permissions=[],
                interrupt_on={},
            )
            chat = FakeChatRepository()
            emitter = await RunEmitter.attach(
                FakeBus(),
                "rn",
                tenant_id="tenant",
                namespace="ns",
                session_id="session",
                chat_repository=chat,
            )
            emitter.bind_tool_display_codes((("mcp_call", "tool.mcp.call"),))
            run = await agent.astream_events(
                {"messages": [HumanMessage(content="go")]},
                version="v3",
                config={},
                transformers=[CustomTransformer],
            )
            async with run:
                await asyncio.wait_for(
                    pump_run(emitter, run, source_for=lambda _: "runtime-custom"), 15
                )
                assert await run.interrupted() is False
            activities = [
                json.loads(row.payload_json)
                for row in chat.records
                if row.event_type == "activity"
            ]
            assert [item["status"] for item in activities] == [
                "running",
                "failed",
                "running",
                "completed",
            ]
            assert {item["display_code"] for item in activities} == {"tool.mcp.call"}
            assert len({item["activity_id"] for item in activities}) == 2
            assert all(
                set(item)
                == {"activity", "activity_id", "segment_id", "status", "display_code"}
                for item in activities
            )
            assert store.tool_journal[("rn", "mcp-fail")]["status"] == "failed"
            assert store.tool_journal[("rn", "mcp-fail")]["is_error"] is True
            assert store.tool_journal[("rn", "mcp-success")]["status"] == "succeeded"
            assert store.tool_journal[("rn", "mcp-success")]["is_error"] is False
            assert "owned recoverable argument" in str(
                store.tool_journal[("rn", "mcp-fail")]["result"]
            )
            assert owner.tool_calls == 2
            frozen = [row.model_dump_json() for row in chat.records]

            async def never_resend(request: object) -> ToolMessage:
                raise AssertionError("failed call replay must not contact MCP again")

            replay = await journal.awrap_tool_call(
                journal_request("mcp_call", "mcp-fail"), never_resend
            )
            assert isinstance(replay, ToolMessage) and replay.status == "error"
            assert owner.tool_calls == 2
            assert [row.model_dump_json() for row in chat.records] == frozen
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 5)
