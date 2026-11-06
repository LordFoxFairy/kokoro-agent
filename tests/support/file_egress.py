"""SDK and archive doubles for the bounded Custom API file-exit tests."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Never

from pydantic import SecretStr
import httpx
from e2b import Sandbox as E2BSandbox
from e2b.connection_config import ConnectionConfig
from packaging.version import Version
from kokoro_agent.sandbox.e2b_backend import E2BSandboxBackend, SandboxClient


class Environment:
    def __init__(self) -> None:
        self.calls = 0
        self.revoked = False

    async def acquire(
        self, *, deadline_monotonic: float
    ) -> tuple[tuple[str, SecretStr], ...]:
        self.calls += 1
        if self.revoked:
            raise RuntimeError("private-upstream-value")
        return (("QA_KEY", SecretStr("private-原值-marker")),)


class Stream:
    def __init__(self, raw: bytes) -> None:
        self.raw = raw
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield self.raw[:4]
        yield self.raw[4:]

    def close(self) -> None:
        self.closed = True


class Files:
    def __init__(self) -> None:
        self.raw = b"clean\x00binary"
        self.streams: list[Stream] = []

    def read(
        self,
        path: str,
        format: str,
        request_timeout: float | None = None,
        stream_idle_timeout: float | None = None,
    ) -> object:
        assert format == "stream"
        assert path == "/home/user/report.bin"
        assert request_timeout is not None and request_timeout > 0
        assert stream_idle_timeout == request_timeout
        stream = Stream(self.raw)
        self.streams.append(stream)
        return stream

    def write(self, path: str, data: str | bytes) -> object:
        raise AssertionError("file exit must not write")


class Commands:
    def run(
        self,
        cmd: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        envs: dict[str, str] | None = None,
    ) -> Never:
        raise AssertionError("file exit must not use execute with formatting")


class Sandbox:
    sandbox_id = "sdk-double"

    def __init__(self) -> None:
        self.files = Files()
        self.commands = Commands()
        self.downloads: list[AsyncFileBody] = []

    def download_url(
        self,
        path: str,
        user: str | None = None,
        use_signature_expiration: int | None = None,
    ) -> str:
        assert path == "/home/user/report.bin"
        assert use_signature_expiration is not None
        return "https://sdk-download.invalid/files?signature=private-url-marker"


class SlowStream(Stream):
    def __init__(self, timeout: float) -> None:
        super().__init__(b"")
        self.timeout = timeout

    def __iter__(self) -> Iterator[bytes]:
        threading.Event().wait(self.timeout)
        raise TimeoutError("private-url-marker")
        yield b""


class SlowFiles(Files):
    def read(
        self,
        path: str,
        format: str,
        request_timeout: float | None = None,
        stream_idle_timeout: float | None = None,
    ) -> object:
        assert request_timeout is not None
        assert stream_idle_timeout is not None
        threading.Event().wait(request_timeout * 0.7)
        stream = SlowStream(stream_idle_timeout)
        self.streams.append(stream)
        return stream


class SlowSandbox(Sandbox):
    def __init__(self) -> None:
        super().__init__()
        self.files = SlowFiles()


class DeadlineE2BBackend(E2BSandboxBackend):
    """Apply a short existing file budget while exercising public download."""

    def __init__(self, sandbox: SandboxClient) -> None:
        super().__init__(sandbox, environment_provider=Environment())
        assert self._file_egress is not None
        self._file_egress.timeout = 1


class AsyncFileBody(httpx.AsyncByteStream):
    def __init__(self, raw: bytes, delay: float = 0) -> None:
        self.raw = raw
        self.delay = delay
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.raw[:4]
        if self.delay:
            await asyncio.sleep(self.delay)
        yield self.raw[4:]

    async def aclose(self) -> None:
        self.closed = True


class LocalE2BFileServer:
    """Real loopback HTTP transport, never an E2B service or cloud sandbox."""

    def __init__(self, phase: str) -> None:
        self.phase = phase
        self.server: asyncio.Server | None = None
        self.tasks: set[asyncio.Task[None]] = set()
        self.entered = asyncio.Event()
        self.closed = asyncio.Event()
        self.requests: list[bytes] = []

    async def start(self) -> E2BSandbox:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        assert isinstance(port, int)
        origin = f"http://127.0.0.1:{port}"
        return E2BSandbox(
            sandbox_id="sdk-local",
            sandbox_domain="local.invalid",
            envd_version=Version("0.5.0"),
            envd_access_token="private-url-marker",
            traffic_access_token=None,
            connection_config=ConnectionConfig(
                domain="local.invalid",
                debug=False,
                api_key="e2b_" + "0" * 64,
                validate_api_key=False,
                api_url=origin,
                sandbox_url=origin,
                access_token="fake-access",
                request_timeout=2,
                proxy=None,
            ),
        )

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        try:
            self.requests.append(await reader.readuntil(b"\r\n\r\n"))
            self.entered.set()
            if self.phase == "headers":
                assert await reader.read(1) == b""
                return
            if self.phase == "redirect":
                writer.write(
                    b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\n"
                    b"Location: /other?signature=private-url-marker\r\n\r\n"
                )
                await writer.drain()
                assert await reader.read(1) == b""
                return
            if self.phase == "handshake-body":
                await asyncio.sleep(0.7)
            writer.write(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
                b"X-Echo: private-url-marker\r\n\r\n4\r\nclea\r\n"
            )
            await writer.drain()
            if self.phase == "drip":
                for _ in range(8):
                    await asyncio.sleep(0.2)
                    writer.write(b"1\r\nn\r\n")
                    await writer.drain()
            assert await reader.read(1) == b""
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self.tasks.discard(task)
            self.closed.set()

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        pending = tuple(self.tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


class ArchiveClient:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.uploaded: list[tuple[str, bytes]] = []
        self.entered: asyncio.Event | None = None
        self.released: threading.Event | None = None
        self.loop = asyncio.get_running_loop()
        self.closed = False

    def put_object(self, *, Body: bytes, Bucket: str, Key: str) -> object:
        assert Bucket == "egress-test"
        if self.entered is not None and self.released is not None:
            self.loop.call_soon_threadsafe(self.entered.set)
            assert self.released.wait(5)
        (self.root / Key.removeprefix("run/")).write_text("private-原值-marker")
        self.uploaded.append((Key, Body))
        return {}

    def close(self) -> None:
        self.closed = True
