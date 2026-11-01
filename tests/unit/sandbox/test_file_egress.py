"""Custom API file exits must inspect whole original bytes before projection."""

from __future__ import annotations

import asyncio
import os
import threading
import time
import logging
from logging.handlers import QueueHandler
from queue import SimpleQueue
from pathlib import Path

import pytest
import httpx

from support.file_egress import (
    ArchiveClient,
    AsyncFileBody,
    DeadlineE2BBackend,
    Environment,
    LocalE2BFileServer,
    Sandbox,
    SlowSandbox,
)
from pydantic import SecretStr

from kokoro_agent.sandbox.archive import S3Archiver, S3Workspace
from kokoro_agent.sandbox.docker_backend import (
    ArchivingDockerShellBackend,
    DockerShellBackend,
)
from kokoro_agent.sandbox.e2b_backend import E2BSandboxBackend
from kokoro_agent.sandbox.execution_environment import ExecutionEnvironmentRunner
from kokoro_agent.sandbox.file_egress import (
    FileEgress,
    MAX_BATCH_FILES,
    MAX_FILE_BYTES,
    local_reader,
)


def backend(root: Path, provider: Environment) -> DockerShellBackend:
    return DockerShellBackend(
        root=root,
        container_id="unused",
        timeout=5,
        max_output_bytes=1024,
        environment_provider=provider,
        environment_loop=asyncio.get_running_loop(),
    )


@pytest.mark.asyncio
async def test_secret_outside_requested_page_denies_entire_read(tmp_path: Path) -> None:
    (tmp_path / "report.txt").write_text("safe\nprivate-原值-marker\n")
    provider = Environment()
    result = await backend(tmp_path, provider).aread("/report.txt", 0, 1)
    assert result.error is not None
    assert result.file_data is None
    assert "private" not in repr(result)


@pytest.mark.asyncio
async def test_binary_download_denies_secret_and_rechecks_revocation(
    tmp_path: Path,
) -> None:
    (tmp_path / "report.bin").write_bytes(b"\x00" + "private-原值-marker".encode())
    provider = Environment()
    instance = backend(tmp_path, provider)
    denied = await instance.adownload_files(["/report.bin"])
    assert denied[0].content is None
    assert denied[0].error is not None
    (tmp_path / "report.bin").write_bytes(b"clean\x00bytes")
    allowed = await instance.adownload_files(["/report.bin"])
    assert allowed[0].content == b"clean\x00bytes"
    provider.revoked = True
    revoked = await instance.adownload_files(["/report.bin"])
    assert revoked[0].content is None
    assert provider.calls == 3


@pytest.mark.asyncio
async def test_clean_read_keeps_original_newlines_and_sync_bridge(
    tmp_path: Path,
) -> None:
    (tmp_path / "report.txt").write_bytes(b"first\r\nsecond\n")
    provider = Environment()
    instance = backend(tmp_path, provider)
    result = await instance.aread("/report.txt", 0, 1)
    assert result.file_data == {"content": "first\r\n", "encoding": "utf-8"}
    downloaded = await asyncio.to_thread(instance.download_files, ["/report.txt"])
    assert downloaded[0].content == b"first\r\nsecond\n"
    assert instance.read("/report.txt").error is not None
    assert provider.calls == 2


@pytest.mark.asyncio
async def test_escape_symlink_fifo_and_oversized_files_fail_closed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (tmp_path / "outside.txt").write_bytes(b"outside")
    (root / "link").symlink_to(tmp_path, target_is_directory=True)
    (root / "leaf").symlink_to(tmp_path / "outside.txt")
    os.mkfifo(root / "pipe")
    with (root / "large").open("wb") as file:
        file.truncate(MAX_FILE_BYTES + 1)
    instance = backend(root, Environment())
    for path in ("../outside.txt", "/link/outside.txt", "/leaf", "/pipe", "/large"):
        result = await instance.adownload_files([path])
        assert result[0].content is None
        assert result[0].path == ""
    assert (tmp_path / "outside.txt").read_bytes() == b"outside"


@pytest.mark.asyncio
async def test_api_metadata_and_grep_never_delegate_to_unchecked_sdk(
    tmp_path: Path,
) -> None:
    (tmp_path / "private-原值-marker").write_text("private-原值-marker")
    instance = backend(tmp_path, Environment())
    assert (await instance.als("/")).entries is None
    assert (await instance.aglob("*")).matches is None
    assert (await instance.agrep("marker")).matches is None


@pytest.mark.asyncio
async def test_archive_uses_checked_bytes_even_if_sink_changes_original(
    tmp_path: Path,
) -> None:
    (tmp_path / "report.txt").write_bytes(b"clean original")
    provider = Environment()
    egress = FileEgress(
        ExecutionEnvironmentRunner(
            provider, max_output_bytes=100, owner_loop=asyncio.get_running_loop()
        ),
        local_reader(tmp_path),
        5,
    )
    uploaded: list[bytes] = []

    def sink(content: bytes, path: str) -> None:
        assert path == "report.txt"
        (tmp_path / path).write_text("private-原值-marker")
        uploaded.append(content)

    await asyncio.to_thread(egress.archive, ["report.txt"], sink)
    assert uploaded == [b"clean original"]
    with pytest.raises(RuntimeError, match="sandbox_file_denied"):
        await asyncio.to_thread(egress.archive, ["report.txt"], sink)
    assert uploaded == [b"clean original"]
    assert provider.calls == 2


@pytest.mark.asyncio
async def test_cancelled_read_owns_thread_and_drops_late_plaintext() -> None:
    entered = asyncio.Event()
    released = threading.Event()
    exited = threading.Event()
    loop = asyncio.get_running_loop()

    def reader(path: str, maximum: int, deadline: float) -> bytes:
        loop.call_soon_threadsafe(entered.set)
        assert released.wait(5)
        exited.set()
        return b"late file bytes"

    egress = FileEgress(
        ExecutionEnvironmentRunner(Environment(), max_output_bytes=100), reader, 5
    )
    task = asyncio.create_task(egress.adownload(["report.txt"]))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    released.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert exited.is_set()


@pytest.mark.asyncio
async def test_e2b_stream_checks_original_bytes_and_always_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = Sandbox()
    download_transport(sandbox, monkeypatch)
    instance = E2BSandboxBackend(sandbox, environment_provider=Environment())
    clean = await instance.adownload_files(["/report.bin"])
    assert clean[0].content == b"clean\x00binary"
    sandbox.files.raw = b"\x00" + "private-原值-marker".encode()
    denied = await instance.adownload_files(["/report.bin"])
    assert denied[0].content is None
    assert len(sandbox.downloads) == 2
    assert all(stream.closed for stream in sandbox.downloads)
    assert (await instance.agrep("marker")).error is not None


@pytest.mark.asyncio
async def test_e2b_oversize_stream_closes_without_exporting_partial_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = Sandbox()
    sandbox.files.raw = b"x" * (MAX_FILE_BYTES + 1)
    download_transport(sandbox, monkeypatch)
    instance = E2BSandboxBackend(sandbox, environment_provider=Environment())
    denied = await instance.adownload_files(["/report.bin"])
    assert denied[0].content is None
    assert sandbox.downloads[0].closed


def download_transport(sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch) -> None:
    async def handle(request: httpx.Request) -> httpx.Response:
        body = AsyncFileBody(sandbox.files.raw)
        sandbox.downloads.append(body)
        return httpx.Response(200, stream=body)

    def transport(**kwargs: object) -> httpx.MockTransport:
        assert kwargs["trust_env"] is False
        assert kwargs["retries"] == 0
        return httpx.MockTransport(handle)

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", transport)


@pytest.mark.asyncio
async def test_e2b_absolute_deadline_includes_handshake_and_blocked_next_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = AsyncFileBody(b"clean body", delay=1)

    async def handle(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.7)
        return httpx.Response(200, stream=body)

    def transport(**kwargs: object) -> httpx.MockTransport:
        return httpx.MockTransport(handle)

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", transport)
    sandbox = SlowSandbox()
    instance = DeadlineE2BBackend(sandbox)
    started = time.monotonic()
    denied = await instance.adownload_files(["/report.bin"])
    assert denied[0].content is None
    assert time.monotonic() - started < 1.4
    assert body.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["headers", "handshake-body", "drip", "redirect"])
async def test_e2b_public_sdk_url_real_http_has_total_deadline_and_closes(
    phase: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = LocalE2BFileServer(phase)
    sandbox = await server.start()
    instance = DeadlineE2BBackend(sandbox)
    caplog.set_level(logging.DEBUG)
    started = time.monotonic()
    try:
        denied = await instance.adownload_files(["/report.bin"])
        assert denied[0].content is None
        assert time.monotonic() - started < 1.4
        if phase != "drip":
            await asyncio.wait_for(server.closed.wait(), 0.4)
        assert len(server.requests) == 1
        target = server.requests[0].split(b" ")[1].decode()
        assert "signature=" in target
        assert target not in caplog.text
        assert "private-url-marker" not in caplog.text
        assert "E2B_FILE transport" in caplog.text
    finally:
        await server.close()
    assert not server.tasks


@pytest.mark.asyncio
async def test_e2b_cancel_waits_real_http_close_and_does_not_return_late_bytes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = LocalE2BFileServer("headers")
    instance = DeadlineE2BBackend(await server.start())
    queue: SimpleQueue[logging.LogRecord] = SimpleQueue()
    handler = QueueHandler(queue)
    logging.getLogger().addHandler(handler)
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="httpx")
    task = asyncio.create_task(instance.adownload_files(["/report.bin"]))
    try:
        await asyncio.wait_for(server.entered.wait(), 1)
        logging.getLogger("httpx").info("public-unrelated-marker")
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1.4)
        await asyncio.wait_for(server.closed.wait(), 0.4)
        assert "public-unrelated-marker" in caplog.text
        assert "private-url-marker" not in caplog.text
        while not queue.empty():
            record = queue.get_nowait()
            assert "signature=" not in record.getMessage()
            assert "private-url-marker" not in record.getMessage()
    finally:
        await server.close()
        await asyncio.gather(task, return_exceptions=True)
        logging.getLogger().removeHandler(handler)


@pytest.mark.asyncio
async def test_e2b_executor_cleanup_is_owned_even_after_transfer_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    timed_out = asyncio.Event()
    released = threading.Event()
    exited = threading.Event()
    owner = asyncio.get_running_loop()

    def blocking_resolver() -> None:
        owner.call_soon_threadsafe(entered.set)
        assert released.wait(5)
        exited.set()

    async def handle(request: httpx.Request) -> httpx.Response:
        try:
            await asyncio.get_running_loop().run_in_executor(None, blocking_resolver)
        except asyncio.CancelledError:
            owner.call_soon_threadsafe(timed_out.set)
            raise
        return httpx.Response(200, content=b"late bytes")

    def transport(**kwargs: object) -> httpx.MockTransport:
        return httpx.MockTransport(handle)

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", transport)
    task = asyncio.create_task(
        DeadlineE2BBackend(Sandbox()).adownload_files(["/report.bin"])
    )
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(timed_out.wait(), 1.4)
        assert not task.done()
        assert not exited.is_set()
        released.set()
        denied = await task
        assert denied[0].content is None
        assert exited.is_set()
    finally:
        released.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_oversized_batch_and_private_reader_failure_are_fail_closed() -> None:
    calls: list[str] = []

    def reader(path: str, maximum: int, deadline: float) -> bytes:
        calls.append(path)
        raise RuntimeError("private-原值-marker")

    egress = FileEgress(
        ExecutionEnvironmentRunner(Environment(), max_output_bytes=100), reader, 5
    )
    denied = await egress.adownload(["report.txt"] * (MAX_BATCH_FILES + 1))
    assert all(result.content is None for result in denied)
    assert calls == []
    failed = await egress.adownload(["report.txt"])
    assert failed[0].content is None
    assert "private" not in repr(failed)


def archiving_backend(
    root: Path, client: ArchiveClient, monkeypatch: pytest.MonkeyPatch
) -> ArchivingDockerShellBackend:
    def factory(*args: object, **kwargs: object) -> ArchiveClient:
        return client

    monkeypatch.setattr("kokoro_agent.sandbox.archive.boto3.client", factory)
    archiver = S3Archiver(
        S3Workspace(type="s3", endpoint="http://unused.invalid", bucket="egress-test"),
        access_key=SecretStr("fake-access"),
        secret_key=SecretStr("fake-secret"),
    )
    return ArchivingDockerShellBackend(
        root=root,
        container_id="unused",
        archiver=archiver,
        prefix="run",
        timeout=5,
        max_output_bytes=1024,
        environment_provider=Environment(),
        environment_loop=asyncio.get_running_loop(),
    )


@pytest.mark.asyncio
async def test_archive_adapter_denies_secret_and_uploads_same_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    client = ArchiveClient(tmp_path)
    instance = archiving_backend(tmp_path, client, monkeypatch)
    try:
        await instance.awrite("/secret.txt", "private-原值-marker")
        assert client.uploaded == []
        await instance.awrite("/clean.txt", "clean snapshot")
        assert client.uploaded == [("run/clean.txt", b"clean snapshot")]
        assert (tmp_path / "clean.txt").read_text() == "private-原值-marker"
        assert "private-原值-marker" not in caplog.text
        assert "sandbox_file_unavailable" in caplog.text
    finally:
        await instance.aclose_resources()
    assert client.closed


@pytest.mark.asyncio
async def test_cancelled_archive_waits_owned_sink_and_stops_next_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = ArchiveClient(tmp_path)
    client.entered = asyncio.Event()
    client.released = threading.Event()
    instance = archiving_backend(tmp_path, client, monkeypatch)
    task = asyncio.create_task(
        instance.aupload_files([("/a.txt", b"first"), ("/b.txt", b"second")])
    )
    try:
        await asyncio.wait_for(client.entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        client.released.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.uploaded == [("run/a.txt", b"first")]
    finally:
        client.released.set()
        await asyncio.gather(task, return_exceptions=True)
        await instance.aclose_resources()
    assert client.closed
