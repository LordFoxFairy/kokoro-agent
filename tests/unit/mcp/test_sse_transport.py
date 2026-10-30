"""SSE-only bounds and real output-handler scoping, without external connections."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine, Callable
import io
import logging
from logging.handlers import QueueHandler
from pathlib import Path
import queue
import subprocess
import sys
import textwrap

import httpx
import pytest

BASE = "https://mcp.example.test/sse"


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            await asyncio.sleep(0)
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def factory(
    handler: Callable[[httpx.Request], httpx.Response]
    | Callable[[httpx.Request], Coroutine[None, None, httpx.Response]],
):
    from kokoro_agent.mcp.sse_transport import build_sse_client_factory

    return build_sse_client_factory(
        BASE, mode="off", transport=httpx.MockTransport(handler)
    )


async def get_stream(client: httpx.AsyncClient) -> bytes:
    async with client.stream("GET", BASE) as response:
        return await response.aread()


@pytest.mark.parametrize("configured", [None, "gzip", "identity"])
async def test_identity_default_never_overwrites_configured_secret_header(
    configured: str | None,
) -> None:
    seen: list[str] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["accept-encoding"])
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Chunks([b": keepalive\n\n"]),
        )

    headers = {} if configured is None else {"Accept-Encoding": configured}
    async with factory(handle)(headers=headers) as client:
        assert await get_stream(client) == b": keepalive\n\n"
    assert seen == [configured or "identity"]
    assert dict(client.headers) == {}


@pytest.mark.parametrize("encoding", ["gzip", "br", "identity, gzip"])
async def test_compression_is_rejected_before_decompression_or_buffering(
    encoding: str,
) -> None:
    body = Chunks([b"not a compressed body"])

    async def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-encoding": encoding}, stream=body)

    async with factory(handle)() as client:
        with pytest.raises(ValueError, match="MCP_SSE_RESPONSE_INVALID"):
            await get_stream(client)
    assert body.closed


@pytest.mark.parametrize("extra", [0, 1])
async def test_session_stream_exact_four_mib_limit(extra: int) -> None:
    body = Chunks([b":" + b"a" * (4_194_304 - 1), b"x" * extra])

    async def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body)

    async with factory(handle)() as client:
        if extra:
            with pytest.raises(ValueError, match="MCP_SSE_BODY_TOO_LARGE"):
                await get_stream(client)
        else:
            assert len(await get_stream(client)) == 4_194_304
    assert body.closed


@pytest.mark.parametrize("extra", [0, 1])
async def test_single_post_ack_exact_four_mib_limit_and_no_retry(extra: int) -> None:
    calls: list[str] = []
    body = Chunks([b"a" * 4_194_304, b"x" * extra])

    async def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(
                200,
                stream=Chunks(
                    [b"event: endpoint\ndata: /messages?session_id=canary\n\n"]
                ),
            )
        return httpx.Response(202, stream=body)

    async with factory(handle)() as client:
        await get_stream(client)
        if extra:
            with pytest.raises(ValueError, match="MCP_SSE_BODY_TOO_LARGE"):
                await client.post(
                    "https://mcp.example.test/messages?session_id=canary", json={}
                )
        else:
            assert (
                len(
                    (
                        await client.post(
                            "https://mcp.example.test/messages?session_id=canary",
                            json={},
                        )
                    ).content
                )
                == 4_194_304
            )
    assert calls == ["GET", "POST"] and body.closed


@pytest.mark.parametrize(
    "advertised",
    [
        "https://other.test/messages",
        "http://mcp.example.test/messages",
        "https://mcp.example.test:444/messages",
        "https://mcp.example.test:0/messages",
        "/messages#",
        "/messages#fragment",
        "/messages\\other",
        "/messages\x00bad",
        "https://user@mcp.example.test/messages",
    ],
)
async def test_endpoint_origin_and_raw_url_rejected_before_sdk_or_post(
    advertised: str,
) -> None:
    calls: list[str] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(
            200, stream=Chunks([f"event: endpoint\ndata: {advertised}\n\n".encode()])
        )

    async with factory(handle)() as client:
        with pytest.raises(ValueError, match="MCP_SSE_ENDPOINT_INVALID"):
            await get_stream(client)
    assert calls == ["GET"]


@pytest.mark.parametrize("extra", [0, 1])
async def test_endpoint_full_query_url_8192_byte_boundary(extra: int) -> None:
    target = "https://mcp.example.test/messages?q="
    target += "a" * (8192 - len(target) + extra)

    async def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, stream=Chunks([f"event: endpoint\ndata: {target}\n\n".encode()])
        )

    async with factory(handle)() as client:
        if extra:
            with pytest.raises(ValueError, match="MCP_SSE_ENDPOINT_INVALID"):
                await get_stream(client)
        else:
            assert (await get_stream(client)).endswith(b"\n\n")


async def test_rejected_post_retires_only_its_session_and_never_forwards_cookies() -> (
    None
):
    calls: list[tuple[str, str | None]] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.headers.get("cookie")))
        return httpx.Response(
            200,
            headers={"set-cookie": "unapproved=canary; Path=/"},
            stream=Chunks([b"event: endpoint\ndata: /messages\n\n"]),
        )

    build = factory(handle)
    async with build() as client:
        await get_stream(client)
        await client.post("https://mcp.example.test/messages", json={})
        with pytest.raises(ValueError, match="MCP_SSE_ENDPOINT_INVALID"):
            await client.post("https://other.test/messages", json={})
    with pytest.raises(ValueError, match="MCP_SSE_SESSION_FAILED"):
        build.raise_if_failed()
    assert calls == [("GET", None), ("POST", None)]
    untouched = factory(handle)
    untouched.raise_if_failed()


async def test_observer_preserves_sdk_decoder_crlf_utf8_comments_and_large_result_bytes() -> (
    None
):
    # BOM is passed through exactly as the locked SDK handles it; no re-encoding.
    chunks = [
        b"\xef\xbb",
        b"\xbf: comment\r",
        b"\n\r\nevent: endpoint\rdata: /messages?name=",
        "中".encode()[:1],
        "中".encode()[1:],
        b"\r\n\r\nevent: message\ndata: " + b"x" * 20_000 + b"\n\n",
    ]

    async def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=Chunks(chunks))

    async with factory(handle)() as client:
        assert await get_stream(client) == b"".join(chunks)


async def test_origin_is_checked_before_existing_dns_pin_mutates_request_host() -> None:
    from kokoro_agent.mcp.sse_transport import build_sse_client_factory

    seen: list[tuple[str, str, object]] = []

    async def resolve(host: str) -> list[str]:
        assert host == "mcp.example.test"
        return ["93.184.216.34"]

    async def handle(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.url.host,
                request.headers["host"],
                request.extensions.get("sni_hostname"),
            )
        )
        return httpx.Response(
            200,
            stream=Chunks(
                [
                    b"event: endpoint\ndata: /messages?q=one\n\n"
                    if request.method == "GET"
                    else b"Accepted"
                ]
            ),
        )

    build = build_sse_client_factory(
        BASE, mode="strict", resolver=resolve, transport=httpx.MockTransport(handle)
    )
    async with build() as client:
        await get_stream(client)
        assert (
            await client.post("https://mcp.example.test/messages?q=one", json={})
        ).status_code == 200
    assert seen == [("93.184.216.34", "mcp.example.test", "mcp.example.test")] * 2


async def test_strict_mixed_dns_set_is_rejected_without_connection() -> None:
    from kokoro_agent.mcp.sse_transport import build_sse_client_factory
    from kokoro_agent.mcp.egress import EgressBlocked

    async def resolve(host: str) -> list[str]:
        return ["93.184.216.34", "127.0.0.1"]

    calls: list[object] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200)

    build = build_sse_client_factory(
        BASE, mode="strict", resolver=resolve, transport=httpx.MockTransport(handle)
    )
    async with build() as client:
        with pytest.raises(EgressBlocked):
            await get_stream(client)
    assert calls == []


async def test_actual_output_caplog_queue_and_concurrent_http_sources_are_scoped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import mcp.shared.session
    from kokoro_agent.mcp.sse_transport import sse_sdk_scope

    pathname = str(Path(mcp.shared.session.__file__))
    output = io.StringIO()
    sink = logging.StreamHandler(output)
    records: queue.Queue[logging.LogRecord] = queue.Queue()
    queued = QueueHandler(records)
    root = logging.getLogger()
    root.addHandler(sink)
    root.addHandler(queued)
    canary = "SSE_PRIVATE_QUERY_AND_HEADER"
    gate = asyncio.Event()

    async def unrelated() -> None:
        await gate.wait()
        logging.getLogger("httpx").warning("unrelated-http-kept")
        logging.getLogger().warning("unrelated-root-kept")
        logging.getLogger("client").warning("unrelated-client-kept")

    other = asyncio.create_task(unrelated())
    caplog.set_level(logging.DEBUG)
    try:
        with sse_sdk_scope():
            with sse_sdk_scope():
                record = logging.LogRecord(
                    "root",
                    logging.ERROR,
                    pathname,
                    444,
                    "native %s",
                    (canary,),
                    (ValueError, ValueError(canary), None),
                )
                root.handle(record)
            gate.set()
            await other
        logging.getLogger("client").warning("after-sse-kept")
    finally:
        root.removeHandler(sink)
        root.removeHandler(queued)
    assert canary not in output.getvalue() and canary not in caplog.text
    captured = list(records.queue)
    assert canary not in " ".join(str(r.__dict__) for r in captured)
    safe = next(r for r in captured if r.pathname == pathname)
    assert safe.levelno == logging.ERROR and "MCP_SSE" in safe.getMessage()
    assert safe.exc_info is None and safe.exc_text is None
    for text in [
        "unrelated-http-kept",
        "unrelated-root-kept",
        "unrelated-client-kept",
        "after-sse-kept",
    ]:
        assert text in output.getvalue() and text in caplog.text
    assert not sink.filters and not queued.filters


@pytest.mark.parametrize("suppressed", ["none", "info", "debug"])
def test_unconfigured_logging_rejects_sse_before_sdk_or_lazy_sink_creation(
    suppressed: str,
) -> None:
    program = textwrap.dedent(
        """
        import asyncio, hashlib, logging, sys, time
        from contextlib import asynccontextmanager
        from protobuf import Oneof
        from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r
        from kokoro_agent.mcp import managed_session as module
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)
        fallback_filters = list(logging.lastResort.filters)
        calls = []
        @asynccontextmanager
        async def forbidden(*args, **kwargs):
            calls.append(True)
            getattr(logging, sys.argv[1], lambda *a: None)('SSE_PRIVATE_CANARY')
            raise AssertionError('SDK must not be entered')
            yield
        module._standard_session = forbidden
        expiry = int(time.time()) + 10
        resolution = r.McpRuntimeResolution(
            resolution_ref='no-log-sink', expires_at_unix_seconds=expiry,
            transport=Oneof('sse', r.McpRuntimeSseConnection(canonical_endpoint='https://mcp.example.test/sse')))
        material = r.AcquireMcpRuntimeCredentialResponse(
            resolution_ref_sha256=hashlib.sha256(b'no-log-sink').hexdigest(),
            expires_at_unix_seconds=expiry, transport=Oneof('sse', r.McpRuntimeSseCredential()))
        async def main():
            try:
                async with module.managed_mcp_session(resolution, material, packages=None):
                    raise AssertionError('session yielded')
            except module.McpSessionError as error:
                assert str(error) == 'MCP_SESSION_FAILED'
            else:
                raise AssertionError('unconfigured logging was admitted')
        asyncio.run(main())
        assert calls == [] and material.transport is None
        assert not root.handlers and not root.filters
        assert logging.lastResort.filters == fallback_filters
        """
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", program, suppressed],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "PRIVATE_CANARY" not in result.stderr


@pytest.mark.parametrize("first_exit", ["a", "b"])
def test_actual_handlers_remain_leased_until_all_concurrent_sse_scopes_exit(
    first_exit: str,
) -> None:
    program = textwrap.dedent(
        """
        import asyncio, logging, sys
        import mcp.shared.session
        from kokoro_agent.mcp.sse_transport import sse_sdk_scope
        root = logging.getLogger()
        root.handlers.clear()
        sink = logging.StreamHandler()
        root.addHandler(sink)  # Explicit process entry logging, never SDK configuration.
        source = mcp.shared.session.__file__
        def emit(code): exec(compile(code, source, 'exec'))
        async def main():
            ready = {key: asyncio.Event() for key in ('a', 'b')}
            exited = asyncio.Event()
            unrelated_go = asyncio.Event()
            async def unrelated():
                await unrelated_go.wait()
                logging.warning('UNRELATED_ROOT_KEPT')
            other = asyncio.create_task(unrelated())
            async def run(key):
                with sse_sdk_scope():
                    ready[key].set()
                    await ready['a' if key == 'b' else 'b'].wait()
                    if key != sys.argv[1]:
                        await exited.wait()
                        assert sink.filters and logging.lastResort.filters
                        emit("logging.getLogger('mcp.client.sse').warning('SSE_SURVIVOR_PRIVATE_CANARY')")
                        unrelated_go.set()
                        await other
                if key == sys.argv[1]: exited.set()
            await asyncio.gather(run('a'), run('b'))
        asyncio.run(main())
        assert not root.filters and not logging.lastResort.filters and not sink.filters
        logging.getLogger('mcp.client.sse').warning('AFTER_SCOPE_KEPT')
        """
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", program, first_exit],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "PRIVATE_CANARY" not in result.stderr
    assert "MCP_SSE phase=session" in result.stderr
    assert (
        "UNRELATED_ROOT_KEPT" in result.stderr and "AFTER_SCOPE_KEPT" in result.stderr
    )


async def test_real_httpx_io_canary_is_clean_but_other_http_request_is_unchanged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from kokoro_agent.mcp.sse_transport import sse_sdk_scope

    canary = "SSE_ONLY_SESSION_QUERY"

    async def handle(request: httpx.Request) -> httpx.Response:
        data = (
            f"event: endpoint\ndata: /messages?session_id={canary}\n\n".encode()
            if request.method == "GET"
            else b"Accepted"
        )
        return httpx.Response(
            200 if request.method == "GET" else 202, stream=Chunks([data])
        )

    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="httpx")
    with sse_sdk_scope():
        async with factory(handle)() as client:
            await get_stream(client)
            assert (
                await client.post(
                    f"https://mcp.example.test/messages?session_id={canary}", json={}
                )
            ).status_code == 202
        # Even the same caller task is not an SSE I/O scope outside that factory.
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200))
        ) as other:
            await other.get("https://unrelated.test/ordinary-kept")
    assert canary not in caplog.text
    assert "ordinary-kept" in caplog.text
    assert any(
        r.name == "httpx" and "MCP_SSE" in r.getMessage() and r.levelno == logging.INFO
        for r in caplog.records
    )


async def test_locked_sdk_shared_session_root_validation_is_sanitized(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import anyio
    from mcp import ClientSession, types
    from mcp.shared.message import SessionMessage
    from kokoro_agent.mcp.sse_transport import sse_sdk_scope

    read_send, read_receive = anyio.create_memory_object_stream[
        SessionMessage | Exception
    ](1)
    write_send, write_receive = anyio.create_memory_object_stream[SessionMessage](1)
    caplog.set_level(logging.DEBUG)
    with sse_sdk_scope():
        async with ClientSession(read_receive, write_send):
            await read_send.send(
                SessionMessage(
                    types.JSONRPCMessage(
                        types.JSONRPCRequest(
                            jsonrpc="2.0",
                            id=1,
                            method="UNKNOWN_SSE_CANARY_METHOD",
                            params={"private": "NATIVE_PRIVATE_CANARY"},
                        )
                    )
                )
            )
            await asyncio.wait_for(write_receive.receive(), 1)
    await read_send.aclose()
    await write_receive.aclose()
    assert "NATIVE_PRIVATE_CANARY" not in caplog.text
    assert "UNKNOWN_SSE_CANARY_METHOD" not in caplog.text
    assert any(
        r.name == "root"
        and r.levelno == logging.WARNING
        and "MCP_SSE" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.parametrize("mode", ["expired", "wrong-case", "slots", "unknown"])
async def test_invalid_material_never_connects_and_always_releases_header_bytes(
    mode: str,
) -> None:
    import hashlib
    import time
    from protobuf import Oneof
    from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r
    from kokoro_agent.mcp.managed_session import managed_mcp_session, McpSessionError

    secret = r.McpRuntimeSecretValue(name="x-api-key", value=b"PRIVATE_CREDENTIAL")
    resolution = r.McpRuntimeResolution(
        resolution_ref="material-test",
        expires_at_unix_seconds=int(time.time()) + 30,
        transport=Oneof(
            "sse",
            r.McpRuntimeSseConnection(
                canonical_endpoint=BASE,
                header_slots=["other"] if mode == "slots" else ["x-api-key"],
            ),
        ),
    )
    material = r.AcquireMcpRuntimeCredentialResponse(
        resolution_ref_sha256=hashlib.sha256(b"material-test").hexdigest(),
        expires_at_unix_seconds=1 if mode == "expired" else int(time.time()) + 30,
        transport=Oneof("streamable_http", r.McpRuntimeHttpCredential(headers=[secret]))
        if mode == "wrong-case"
        else Oneof("sse", r.McpRuntimeSseCredential(headers=[secret])),
    )
    if mode == "unknown":
        resolution = r.McpRuntimeResolution.from_binary(
            resolution.to_binary() + b"\xf8\x07\x01"
        )
    with pytest.raises(McpSessionError, match="MCP_SESSION_FAILED"):
        async with managed_mcp_session(resolution, material, packages=None):
            pytest.fail("invalid material reached SDK")
    assert material.transport is None and secret.value == b""
