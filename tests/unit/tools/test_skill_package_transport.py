from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib

import httpx
import pytest
from protobuf.wkt import Timestamp

from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb


def reference(**changes: object) -> pb.PackageTransferReference:
    value = pb.PackageTransferReference(
        url="https://objects.test/package?signature=secret",
        method="GET",
        required_headers={},
        expires_at=Timestamp.from_datetime(
            datetime.now(timezone.utc) + timedelta(minutes=1)
        ),
    )
    for key, item in changes.items():
        setattr(value, key, item)
    return value


@pytest.mark.asyncio
async def test_signed_get_isolated_and_hash_verified() -> None:
    from kokoro_agent.clients.skill_package_transport import SkillPackageTransport

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.extensions["timeout"] == {
            "connect": 3,
            "read": 10,
            "write": 10,
            "pool": 3,
        }
        assert (
            "authorization" not in request.headers and "cookie" not in request.headers
        )
        return httpx.Response(200, stream=httpx.ByteStream(b"zip"))

    async with SkillPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as client:
        assert (
            await client.get(reference(), hashlib.sha256(b"zip").hexdigest()) == b"zip"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"method": "PUT"},
        {"url": "https://evil.test/x"},
        {"url": "https://objects.test:0/package"},
        {"url": "https://u:p@objects.test/x"},
        {"url": "https://objects.test/x#x"},
        {"required_headers": {"Authorization": "secret"}},
        {"required_headers": {"cookie": "secret"}},
        {
            "expires_at": Timestamp.from_datetime(
                datetime.now(timezone.utc) - timedelta(seconds=1)
            )
        },
    ],
)
async def test_invalid_reference_zero_socket(changes: dict[str, object]) -> None:
    from kokoro_agent.clients.skill_package_transport import (
        SkillPackageTransport,
        SkillTransferError,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid transfer must not send")

    async with SkillPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(SkillTransferError):
            await client.get(reference(**changes), "0" * 64)


@pytest.mark.asyncio
async def test_signed_url_is_absent_from_httpx_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging
    from kokoro_agent.clients.skill_package_transport import SkillPackageTransport

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(b"zip"))

    with caplog.at_level(logging.INFO, logger="httpx"):
        async with SkillPackageTransport(
            "https://objects.test", transport=httpx.MockTransport(handler)
        ) as client:
            await client.get(reference(), hashlib.sha256(b"zip").hexdigest())
    assert "signature=secret" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,headers,body",
    [
        (302, {"location": "https://evil.test"}, b""),
        (206, {}, b"zip"),
        (200, {"content-encoding": "gzip"}, b"zip"),
        (200, {"content-length": "33554433"}, b"zip"),
        (200, {"content-length": "1"}, b"zip"),
        (200, {"content-length": "wat"}, b"zip"),
        (200, {}, b"corrupt"),
        (200, {}, b"x" * 33554433),
    ],
)
async def test_signed_get_rejects_status_encoding_size_and_digest(
    status: int, headers: dict[str, str], body: bytes
) -> None:
    from kokoro_agent.clients.skill_package_transport import (
        SkillPackageTransport,
        SkillTransferError,
    )

    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, headers=headers, stream=httpx.ByteStream(body))

    async with SkillPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(SkillTransferError) as error:
            await client.get(reference(), hashlib.sha256(b"zip").hexdigest())
    assert calls == 1
    assert error.value.__context__ is None
    assert "secret" not in str(error.value)


@pytest.mark.asyncio
async def test_signed_get_real_owned_http_and_cancellation() -> None:
    import asyncio
    from kokoro_agent.clients.skill_package_transport import SkillPackageTransport

    received: list[bytes] = []
    waiting = asyncio.Event()
    closed = asyncio.Event()

    async def handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            received.append(headers)
            if len(received) == 1:
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nSet-Cookie: secret=x\r\nConnection: close\r\n\r\nzip"
                )
                await writer.drain()
            else:
                waiting.set()
                await reader.read()
                closed.set()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    async with server:
        origin = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        async with SkillPackageTransport(origin) as client:
            ref = reference(url=origin + "/package?signature=secret")
            assert await client.get(ref, hashlib.sha256(b"zip").hexdigest()) == b"zip"
            task = asyncio.create_task(
                client.get(ref, hashlib.sha256(b"zip").hexdigest())
            )
            await asyncio.wait_for(waiting.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(closed.wait(), 2)
    assert len(received) == 2
    assert all(
        b"Cookie:" not in request and b"Authorization:" not in request
        for request in received
    )


@pytest.mark.asyncio
async def test_signed_get_total_deadline_closes_stream() -> None:
    import asyncio
    from collections.abc import AsyncIterator
    from kokoro_agent.clients.skill_package_transport import (
        SkillPackageTransport,
        SkillTransferError,
    )

    class Stream(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            await asyncio.sleep(5)
            yield b"zip"

        async def aclose(self) -> None:
            self.closed = True

    stream = Stream()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async with SkillPackageTransport(
        "https://objects.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(SkillTransferError, match="UNAVAILABLE"):
            await client.get(
                reference(
                    expires_at=Timestamp.from_datetime(
                        datetime.now(timezone.utc) + timedelta(milliseconds=50)
                    )
                ),
                hashlib.sha256(b"zip").hexdigest(),
            )
    assert stream.closed
