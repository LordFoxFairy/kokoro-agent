"""Custom API file exits must inspect whole original bytes before projection."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
import logging
from logging.handlers import QueueHandler
from queue import SimpleQueue
from pathlib import Path
from collections.abc import Callable

import pytest
import httpx
from e2b.envd.filesystem import filesystem_pb2 as pb

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
    Reader,
)


def backend(
    root: Path, provider: Environment, *, max_output_bytes: int = 1024
) -> DockerShellBackend:
    return DockerShellBackend(
        root=root,
        container_id="unused",
        timeout=5,
        max_output_bytes=max_output_bytes,
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


@pytest.mark.asyncio
async def test_protected_docker_tools_restore_safe_navigation_and_literal_search(
    tmp_path: Path,
) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "root.txt").write_bytes(b"literal [a]\r\nsecond\n")
    (tmp_path / "docs" / "nested.txt").write_text("literal [a]\n")
    provider = Environment()
    instance = backend(tmp_path, provider)
    listed = await instance.als("/")
    assert listed.error is None
    assert listed.entries is not None
    assert {entry["path"] for entry in listed.entries} == {"/docs", "/root.txt"}
    shallow = await instance.aglob("*.txt")
    assert shallow.error is None
    assert shallow.matches is not None
    assert [entry["path"] for entry in shallow.matches] == ["/root.txt"]
    recursive = await instance.aglob("**/*.txt")
    assert recursive.matches is not None
    assert [entry["path"] for entry in recursive.matches] == [
        "/docs/nested.txt",
        "/root.txt",
    ]
    searched = await instance.agrep("[a]", glob="**/*.txt")
    assert searched.error is None
    assert searched.matches == [
        {"path": "/docs/nested.txt", "line": 1, "text": "literal [a]"},
        {"path": "/root.txt", "line": 1, "text": "literal [a]\r"},
    ]
    synchronous = await asyncio.to_thread(instance.grep, "second", "/root.txt")
    assert synchronous.matches == [{"path": "/root.txt", "line": 2, "text": "second"}]
    directory = await instance.als("/docs/")
    assert directory.entries == [
        {"path": "/docs/nested.txt", "is_dir": False, "size": 12}
    ]
    file_glob = await instance.agrep("second", "/root.txt", glob="*.txt")
    assert file_glob.matches == synchronous.matches
    assert provider.calls == 7


@pytest.mark.asyncio
async def test_protected_search_checks_whole_bytes_before_matching_or_binary_skip(
    tmp_path: Path,
) -> None:
    (tmp_path / "clean.txt").write_text("wanted\n")
    (tmp_path / "late.txt").write_text("wanted\nprivate-原值-marker\n")
    instance = backend(tmp_path, Environment())
    result = await instance.agrep("wanted")
    assert result.error is not None
    assert result.matches is None
    assert "wanted" not in repr(result)
    (tmp_path / "late.txt").write_bytes(b"\x00" + "private-原值-marker".encode())
    result = await instance.agrep("wanted")
    assert result.error is not None
    assert result.matches is None
    (tmp_path / "late.txt").write_bytes(b"\xffwanted")
    assert (await instance.agrep("wanted")).matches == [
        {"path": "/clean.txt", "line": 1, "text": "wanted"}
    ]


@pytest.mark.asyncio
async def test_discovery_checks_unmatched_original_names_and_reacquires(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (tmp_path / "clean.txt").write_text("wanted")
    private = tmp_path / "private-原值-marker.bin"
    private.write_bytes(b"clean")
    provider = Environment()
    instance = backend(tmp_path, provider)
    assert (await instance.aglob("**/*.txt")).matches is None
    assert (await instance.agrep("wanted", glob="**/*.txt")).matches is None
    private.unlink()
    assert (await instance.aglob("**/*.txt")).matches == [
        {"path": "/clean.txt", "is_dir": False, "size": 6}
    ]
    provider.revoked = True
    assert (await instance.als("/")).entries is None
    assert provider.calls == 4
    assert "private-原值-marker" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["leaf-link", "directory-link", "fifo"])
async def test_discovery_refuses_links_and_special_files_before_filtering(
    tmp_path: Path, kind: str
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "clean.txt").write_text("wanted")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("private-原值-marker")
    if kind == "leaf-link":
        (root / "ignored.bin").symlink_to(outside / "secret.txt")
    elif kind == "directory-link":
        (root / "ignored.bin").symlink_to(outside, target_is_directory=True)
    else:
        os.mkfifo(root / "ignored.bin")
    instance = backend(root, Environment())
    assert (await instance.als("/")).entries is None
    assert (await instance.aglob("**/*.txt")).matches is None
    assert (await instance.agrep("wanted", glob="**/*.txt")).matches is None
    assert (outside / "secret.txt").read_text() == "private-原值-marker"


@pytest.mark.asyncio
async def test_discovery_path_and_glob_modes_and_empty_literal_are_explicit(
    tmp_path: Path,
) -> None:
    (tmp_path / ".hidden.txt").write_text("literal.*\n")
    instance = backend(tmp_path, Environment())
    result = await instance.aglob("**/?.txt")
    assert result.matches == []
    result = await instance.aglob("**/[.]hidden.txt")
    assert result.matches == [{"path": "/.hidden.txt", "is_dir": False, "size": 10}]
    assert (await instance.agrep(".*")).matches == [
        {"path": "/.hidden.txt", "line": 1, "text": "literal.*"}
    ]
    assert (await instance.agrep("")).matches is None
    for invalid in ("../*", "a/**bad", "{a,b}", "@(a)", "//a"):
        assert (await instance.aglob(invalid)).matches is None
    for invalid in ("../", "/a/../b", "//a", "a//b"):
        assert (await instance.als(invalid)).entries is None
    assert (await instance.als("/missing")).entries == []
    assert (await instance.agrep("needle", "/missing")).matches == []
    assert (await instance.aglob("*", "/missing")).matches == []


@pytest.mark.asyncio
async def test_discovery_file_count_is_bounded_before_glob_filter(
    tmp_path: Path,
) -> None:
    for index in range(MAX_BATCH_FILES):
        (tmp_path / f"f{index:03}.bin").write_bytes(b"")
    instance = backend(tmp_path, Environment(), max_output_bytes=100_000)
    allowed = await instance.als("/")
    assert allowed.entries is not None and len(allowed.entries) == MAX_BATCH_FILES
    (tmp_path / "overflow.bin").write_bytes(b"")
    assert (await instance.aglob("**/*.txt")).matches is None
    assert (await instance.als("/")).entries is None
    assert (await instance.agrep("wanted", glob="**/*.txt")).matches is None


@pytest.mark.asyncio
async def test_discovery_directory_count_and_original_name_bytes_are_bounded(
    tmp_path: Path,
) -> None:
    many = tmp_path / "many"
    many.mkdir()
    for index in range(1025):
        (many / f"d{index}").mkdir()
    instance = backend(tmp_path, Environment(), max_output_bytes=100_000)
    assert (await instance.aglob("**/*.txt", "/many")).matches is None
    names = tmp_path / ("nested" * 6)
    names.mkdir()
    for index in range(MAX_BATCH_FILES):
        (names / ("x" * 240 + str(index))).write_bytes(b"")
    assert (await instance.aglob("**/*.txt", "/" + names.name)).matches is None


@pytest.mark.asyncio
async def test_discovery_result_budget_is_exact_and_never_returns_a_prefix(
    tmp_path: Path,
) -> None:
    (tmp_path / "clean.txt").write_text("first\nfirst\n")
    expected = [
        {"path": "/clean.txt", "line": 1, "text": "first"},
        {"path": "/clean.txt", "line": 2, "text": "first"},
    ]
    size = len(json.dumps(expected, ensure_ascii=False, separators=(",", ":")).encode())
    allowed = backend(tmp_path, Environment(), max_output_bytes=size)
    denied = backend(tmp_path, Environment(), max_output_bytes=size - 1)
    assert (await allowed.agrep("first")).matches == expected
    assert (await denied.agrep("first")).matches is None
    tiny = backend(tmp_path, Environment(), max_output_bytes=1)
    assert (await tiny.aglob("not-found")).matches is None


@pytest.mark.asyncio
async def test_grep_checks_per_file_and_shared_snapshot_byte_budgets(
    tmp_path: Path,
) -> None:
    instance = backend(tmp_path, Environment())
    for name in ("a.bin", "b.bin"):
        with (tmp_path / name).open("wb") as handle:
            handle.truncate(MAX_FILE_BYTES)
    assert (await instance.agrep("wanted")).matches == []
    (tmp_path / "c.bin").write_bytes(b"x")
    assert (await instance.agrep("wanted")).matches is None
    (tmp_path / "c.bin").unlink()
    with (tmp_path / "a.bin").open("wb") as handle:
        handle.truncate(MAX_FILE_BYTES + 1)
    assert (await instance.agrep("wanted")).matches is None


@pytest.mark.asyncio
async def test_cancelled_grep_waits_owned_reader_and_never_emits_late_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "clean.txt").write_text("wanted")
    provider = Environment()
    instance = backend(tmp_path, provider)
    entered = asyncio.Event()
    released = threading.Event()
    exited = threading.Event()
    owner = asyncio.get_running_loop()
    original = local_reader(tmp_path)

    def reader(path: str, maximum: int, deadline: float) -> bytes:
        owner.call_soon_threadsafe(entered.set)
        assert released.wait(5)
        exited.set()
        return original(path, maximum, deadline)

    # Replace the adapter's real I/O dependency; keep public tool/guard behavior.
    def reader_factory(root: Path) -> Reader:
        return reader

    monkeypatch.setattr(
        "kokoro_agent.sandbox.docker_backend.local_reader", reader_factory
    )
    instance = backend(tmp_path, provider)
    task = asyncio.create_task(instance.agrep("wanted"))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        released.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert exited.is_set()
        assert provider.calls == 1
    finally:
        released.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_grep_rechecks_replaced_file_bytes_in_same_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "clean.txt").write_text("wanted")
    original = local_reader(tmp_path)

    def reader(path: str, maximum: int, deadline: float) -> bytes:
        (tmp_path / path).write_text("private-原值-marker")
        return original(path, maximum, deadline)

    def reader_factory(root: Path) -> Reader:
        return reader

    monkeypatch.setattr(
        "kokoro_agent.sandbox.docker_backend.local_reader", reader_factory
    )
    provider = Environment()
    result = await backend(tmp_path, provider).agrep("wanted")
    assert result.error is not None
    assert result.matches is None
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_protected_grep_shared_deadline_drops_late_reader_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "clean.txt").write_text("wanted")

    def reader(path: str, maximum: int, deadline: float) -> bytes:
        time.sleep(1.05)
        return b"wanted"

    def reader_factory(root: Path) -> Reader:
        return reader

    monkeypatch.setattr(
        "kokoro_agent.sandbox.docker_backend.local_reader", reader_factory
    )
    instance = DockerShellBackend(
        root=tmp_path,
        container_id="unused",
        timeout=1,
        max_output_bytes=1024,
        environment_provider=Environment(),
        environment_loop=asyncio.get_running_loop(),
    )
    result = await instance.agrep("wanted")
    assert result.error is not None
    assert result.matches is None


@pytest.mark.asyncio
async def test_discovery_depth_is_bounded_even_when_no_file_matches(
    tmp_path: Path,
) -> None:
    directory = tmp_path
    for _ in range(65):
        directory = directory / "d"
        directory.mkdir()
    assert (await backend(tmp_path, Environment()).aglob("**/*.txt")).matches is None


@pytest.mark.asyncio
async def test_plain_docker_file_tools_keep_native_sdk_behavior(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "nested.txt").write_text("native literal [a]\n")
    instance = DockerShellBackend(
        root=tmp_path, container_id="unused", timeout=5, max_output_bytes=1024
    )
    listed = await instance.als("/")
    assert listed.error is None
    assert listed.entries is not None and listed.entries[0]["path"] == "/docs/"
    # Native FilesystemBackend rglob remains recursive for a shallow pattern.
    matched = await instance.aglob("*.txt")
    assert matched.error is None
    assert matched.matches is not None
    assert [entry["path"] for entry in matched.matches] == ["/docs/nested.txt"]
    searched = await instance.agrep("[a]")
    assert searched.matches == [
        {"path": "/docs/nested.txt", "line": 1, "text": "native literal [a]"}
    ]


class MetadataSandbox(Sandbox):
    """Official protobuf over a finite transport double; never a cloud sandbox."""

    def __init__(self) -> None:
        from e2b.connection_config import ConnectionConfig

        super().__init__()
        self.envd_api_url = "https://metadata.invalid"
        self.connection_config = ConnectionConfig(
            api_key="e2b_" + "0" * 40,
            extra_sandbox_headers={"X-Access-Token": "private-rpc-token"},
        )

    def download_url(
        self,
        path: str,
        user: str | None = None,
        use_signature_expiration: int | None = None,
    ) -> str:
        assert path.startswith("/home/user/")
        return "https://metadata.invalid/download/" + path.removeprefix("/home/user/")


def metadata_transport(
    monkeypatch: pytest.MonkeyPatch,
    files: dict[str, bytes],
    *,
    modify: Callable[[dict[str, pb.EntryInfo]], None] | None = None,
) -> tuple[list[str], list[AsyncFileBody]]:
    calls: list[str] = []
    bodies: list[AsyncFileBody] = []
    entries = {
        "/home/user": pb.EntryInfo(
            name="user", path="/home/user", type=pb.FILE_TYPE_DIRECTORY, mode=0o755
        )
    }
    for name, raw in files.items():
        parts = name.split("/")
        for index in range(1, len(parts)):
            path = "/home/user/" + "/".join(parts[:index])
            entries[path] = pb.EntryInfo(
                name=parts[index - 1],
                path=path,
                type=pb.FILE_TYPE_DIRECTORY,
                mode=0o755,
            )
        path = "/home/user/" + name
        entries[path] = pb.EntryInfo(
            name=parts[-1], path=path, type=pb.FILE_TYPE_FILE, mode=0o644, size=len(raw)
        )

    if modify is not None:
        modify(entries)

    async def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.method == "GET":
            raw = files[request.url.path.removeprefix("/download/")]
            headers = {}
        else:
            assert request.headers["x-access-token"] == "private-rpc-token"
            assert request.headers["content-type"] == "application/proto"
            assert request.headers["connect-protocol-version"] == "1"
            if request.url.path == "/filesystem.Filesystem/Stat":
                stat = pb.StatRequest.FromString(request.content)
                if stat.path not in entries:
                    return httpx.Response(404, content=b'{"code":"not_found"}')
                raw = pb.StatResponse(entry=entries[stat.path]).SerializeToString()
            else:
                assert request.url.path == "/filesystem.Filesystem/ListDir"
                listed = pb.ListDirRequest.FromString(request.content)
                assert listed.depth == 1
                raw = pb.ListDirResponse(
                    entries=[
                        entry
                        for path, entry in entries.items()
                        if path.rpartition("/")[0] == listed.path
                    ]
                ).SerializeToString()
            headers = {"content-type": "application/proto"}
        body = AsyncFileBody(raw)
        bodies.append(body)
        return httpx.Response(200, stream=body, headers=headers)

    def transport(**kwargs: object) -> httpx.MockTransport:
        assert kwargs["trust_env"] is False
        assert kwargs["retries"] == 0
        return httpx.MockTransport(handle)

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", transport)
    return calls, bodies


@pytest.mark.asyncio
async def test_protected_e2b_unary_tools_restore_navigation_and_literal_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = Environment()
    calls, bodies = metadata_transport(
        monkeypatch,
        {"root.txt": b"literal [a]\r\nsecond\n", "docs/nested.txt": b"literal [a]\n"},
    )
    instance = E2BSandboxBackend(
        MetadataSandbox(),
        environment_provider=provider,
        environment_loop=asyncio.get_running_loop(),
    )
    listed = await instance.als("/")
    assert listed.error is None
    assert listed.entries is not None
    assert {entry["path"] for entry in listed.entries} == {"/docs", "/root.txt"}
    globbed = await instance.aglob("**/*.txt")
    assert globbed.error is None
    assert globbed.matches is not None
    assert [entry["path"] for entry in globbed.matches] == [
        "/docs/nested.txt",
        "/root.txt",
    ]
    searched = await instance.agrep("[a]", glob="**/*.txt")
    assert searched.error is None
    assert searched.matches == [
        {"path": "/docs/nested.txt", "line": 1, "text": "literal [a]"},
        {"path": "/root.txt", "line": 1, "text": "literal [a]\r"},
    ]
    synced = await asyncio.to_thread(instance.grep, "second", "/root.txt")
    assert synced.matches == [{"path": "/root.txt", "line": 2, "text": "second"}]
    assert provider.calls == 4
    assert "/filesystem.Filesystem/ListDir" in calls
    assert all(body.closed for body in bodies)


@pytest.mark.asyncio
@pytest.mark.parametrize("place", ["unmatched-name", "unmatched-line", "binary"])
async def test_e2b_metadata_and_content_are_checked_before_projection(
    monkeypatch: pytest.MonkeyPatch,
    place: str,
) -> None:
    files = {"safe.txt": b"needle\n"}
    if place == "unmatched-name":
        files["private-原值-marker.bin"] = b"safe"
    elif place == "unmatched-line":
        files["last.txt"] = b"needle\nprivate-" + "原值-marker".encode()
    else:
        files["last.txt"] = b"\x00private-" + "原值-marker".encode()
    _, bodies = metadata_transport(monkeypatch, files)
    provider = Environment()
    instance = E2BSandboxBackend(MetadataSandbox(), environment_provider=provider)
    denied = await instance.agrep("needle", glob="*.txt")
    assert denied.error == "sandbox_file_unavailable"
    assert denied.matches is None
    assert all(body.closed for body in bodies)
    before = len(bodies)
    provider.revoked = True
    assert (await instance.als("/")).entries is None
    assert len(bodies) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "unknown-type",
        "symlink",
        "empty-symlink",
        "escaped-path",
        "wrong-name",
        "backslash",
    ],
)
async def test_e2b_raw_entries_refuse_unknown_links_and_wrong_bindings(
    monkeypatch: pytest.MonkeyPatch,
    invalid: str,
) -> None:
    def modify(entries: dict[str, pb.EntryInfo]) -> None:
        entry = entries["/home/user/safe.txt"]
        if invalid == "unknown-type":
            entry.MergeFromString(bytes([pb.EntryInfo.TYPE_FIELD_NUMBER << 3, 99]))
        elif invalid in {"symlink", "empty-symlink"}:
            entry.symlink_target = "" if invalid == "empty-symlink" else "/etc/passwd"
        elif invalid == "escaped-path":
            entry.path = "/etc/safe.txt"
        elif invalid == "wrong-name":
            entry.name = "../safe.txt"
        else:
            entry.name = "safe\\file.txt"
            entry.path = "/home/user/" + entry.name

    _, bodies = metadata_transport(monkeypatch, {"safe.txt": b"safe"}, modify=modify)
    instance = E2BSandboxBackend(MetadataSandbox(), environment_provider=Environment())
    denied = await instance.aglob("no-match*")
    assert denied.error == "sandbox_file_unavailable"
    assert denied.matches is None
    assert all(body.closed for body in bodies)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "capacity", ["files", "entries", "names", "depth", "total-wire"]
)
async def test_e2b_discovery_budgets_apply_before_filtering(
    monkeypatch: pytest.MonkeyPatch,
    capacity: str,
) -> None:
    files = {"safe.txt": b"safe"}
    if capacity == "files":
        files = {f"file-{index}": b"safe" for index in range(129)}
    elif capacity == "entries":
        files = {f"dir-{index}/file": b"safe" for index in range(1025)}
    elif capacity == "names":
        files = {f"dir-{index}/" + "x" * 255: b"safe" for index in range(128)}
    elif capacity == "depth":
        files = {"dir/" * 64 + "file": b"safe"}
    elif capacity == "total-wire":
        files = {f"dir-{index}/file": b"safe" for index in range(12)}

    def modify(entries: dict[str, pb.EntryInfo]) -> None:
        if capacity == "total-wire":
            for entry in entries.values():
                if entry.type == pb.FILE_TYPE_FILE:
                    entry.metadata["padding"] = "x" * 195000

    _, bodies = metadata_transport(monkeypatch, files, modify=modify)
    instance = E2BSandboxBackend(MetadataSandbox(), environment_provider=Environment())
    denied = await instance.aglob("no-match*")
    assert denied.error == "sandbox_file_unavailable"
    assert denied.matches is None
    assert all(body.closed for body in bodies)


@pytest.mark.asyncio
async def test_e2b_grep_preserves_empty_binary_and_whole_snapshot_budgets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bodies = metadata_transport(
        monkeypatch, {"binary": b"\x00needle", "text": b"needle\n"}
    )
    instance = E2BSandboxBackend(
        MetadataSandbox(), environment_provider=Environment(), max_output_bytes=1
    )
    assert (await instance.agrep("")).matches is None
    assert bodies == []
    assert (await instance.agrep("needle")).matches is None
    assert all(body.closed for body in bodies)
    _, bodies = metadata_transport(
        monkeypatch, {"first": b"needle", "large": b"x" * (MAX_FILE_BYTES + 1)}
    )
    instance = E2BSandboxBackend(MetadataSandbox(), environment_provider=Environment())
    assert (await instance.agrep("needle")).matches is None
    assert all(body.closed for body in bodies)
