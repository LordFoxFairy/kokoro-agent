"""Exact-size managed downloads share the isolated ObjectStore byte boundary."""

from __future__ import annotations

import asyncio
import hashlib
import time

import httpx
import pytest

from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r


def download() -> r.McpRuntimePackageDownload:
    return r.McpRuntimePackageDownload(
        url_utf8=b"https://objects.test/package?signature=secret",
        required_headers=[
            r.McpRuntimeSecretValue(name="x-object-key", value=b"fixture-only")
        ],
        size_bytes=3,
        expires_at_unix_seconds=int(time.time()) + 30,
    )


async def test_download_exact_headers_size_expiry_and_safe_failures() -> None:
    from kokoro_agent.clients.mcp_package_transport import (
        McpPackageTransport,
        McpTransferError,
    )

    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.headers["x-object-key"] == "fixture-only"
        assert (
            "cookie" not in request.headers and "authorization" not in request.headers
        )
        return httpx.Response(200, stream=httpx.ByteStream(b"zip"))

    async with McpPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as client:
        ref = download()
        assert (
            await client.get(
                ref, hashlib.sha256(b"zip").hexdigest(), int(time.time()) + 30
            )
            == b"zip"
        )
        for invalid in [
            r.McpRuntimePackageDownload(
                url_utf8=ref.url_utf8,
                required_headers=ref.required_headers,
                size_bytes=4,
                expires_at_unix_seconds=ref.expires_at_unix_seconds,
            ),
            r.McpRuntimePackageDownload(
                url_utf8=b"https://evil.test/x",
                size_bytes=3,
                expires_at_unix_seconds=ref.expires_at_unix_seconds,
            ),
            r.McpRuntimePackageDownload(
                url_utf8=ref.url_utf8,
                required_headers=[
                    r.McpRuntimeSecretValue(name="cookie", value=b"secret")
                ],
                size_bytes=3,
                expires_at_unix_seconds=ref.expires_at_unix_seconds,
            ),
        ]:
            with pytest.raises(McpTransferError) as caught:
                await client.get(
                    invalid, hashlib.sha256(b"zip").hexdigest(), int(time.time()) + 30
                )
            assert caught.value.__context__ is None and "secret" not in str(
                caught.value
            )
        for blocked in ["Authorization", "X-Kokoro-Internal-Secret", "proxy-custom"]:
            invalid = download()
            invalid.required_headers.append(
                r.McpRuntimeSecretValue(name=blocked, value=b"internal-secret")
            )
            with pytest.raises(McpTransferError):
                await client.get(
                    invalid, hashlib.sha256(b"zip").hexdigest(), int(time.time()) + 30
                )
        assert calls == 2


async def test_cancel_closes_body_and_never_retries() -> None:
    from kokoro_agent.clients.mcp_package_transport import McpPackageTransport

    entered = asyncio.Event()
    closed = asyncio.Event()
    calls = 0

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            entered.set()
            await asyncio.Event().wait()
            yield b"zip"

        async def aclose(self) -> None:
            closed.set()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=Body())

    async with McpPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as client:
        task = asyncio.create_task(
            client.get(
                download(), hashlib.sha256(b"zip").hexdigest(), int(time.time()) + 30
            )
        )
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert calls == 1 and closed.is_set()
