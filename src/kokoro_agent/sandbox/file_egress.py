"""Bounded original-byte snapshots for Custom API file exits.

No credential cache, rewriting, encoded-content inspection or network policy.
"""

from __future__ import annotations

import base64
import mimetypes
import os
import stat
import threading
import time
from collections.abc import Callable, Iterable
from contextlib import ExitStack
from pathlib import Path

from deepagents.backends.protocol import FileData, FileDownloadResponse, ReadResult
from deepagents.backends.utils import check_empty_content

from kokoro_agent.sandbox.execution_environment import (
    ExecutionBoundaryError,
    ExecutionEnvironmentRunner,
)

MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_BATCH_FILES = 128
MAX_BATCH_BYTES = 16 * 1024 * 1024
FILE_UNAVAILABLE = "sandbox_file_unavailable"
Reader = Callable[[str, int, float], bytes]


def relative_path(path: str) -> str:
    parts = path.removeprefix("/").split("/")
    if (
        not path
        or len(path.encode()) > 4096
        or any(part in {"", ".", ".."} for part in parts)
        or "\\" in path
        or "\x00" in path
    ):
        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
    return "/".join(parts)


def local_reader(root: Path) -> Reader:
    def read(path: str, maximum: int, deadline: float) -> bytes:
        parts = relative_path(path).split("/")
        flags = os.O_RDONLY | os.O_NOFOLLOW
        with ExitStack() as stack:
            descriptor = os.open(root, flags | os.O_DIRECTORY)
            stack.callback(os.close, descriptor)
            for part in parts[:-1]:
                descriptor = os.open(part, flags | os.O_DIRECTORY, dir_fd=descriptor)
                stack.callback(os.close, descriptor)
            descriptor = os.open(parts[-1], flags | os.O_NONBLOCK, dir_fd=descriptor)
            stack.callback(os.close, descriptor)
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            chunks: list[bytes] = []
            count = 0
            while count <= maximum:
                if time.monotonic() >= deadline:
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                chunk = os.read(descriptor, min(65536, maximum + 1 - count))
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)
                count += len(chunk)
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)

    return read


def local_archive_paths(root: Path) -> Iterable[str]:
    """Enumerate bounded regular candidates without following directory links."""
    visited = 0
    with ExitStack() as stack:
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, descriptor)
        for current, directories, files, current_fd in os.fwalk(
            ".", follow_symlinks=False, dir_fd=descriptor
        ):
            visited += len(directories) + len(files)
            if visited > 1024:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            directories[:] = sorted(
                name
                for name in directories
                if not name.startswith(".")
                and name not in {"node_modules", "__pycache__"}
            )
            for name in directories:
                if stat.S_ISLNK(
                    os.stat(name, dir_fd=current_fd, follow_symlinks=False).st_mode
                ):
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            for name in sorted(files):
                if not name.startswith("."):
                    prefix = "" if current == "." else current.removeprefix("./") + "/"
                    yield prefix + name


class FileEgress:
    """Each operation reacquires and keeps inspection material call-local."""

    def __init__(
        self, runner: ExecutionEnvironmentRunner, reader: Reader, timeout: int
    ) -> None:
        self.runner = runner
        self.reader = reader
        self.timeout = timeout

    def operation(
        self, paths: list[str]
    ) -> Callable[
        [Callable[[bytes], None], float, threading.Event], list[FileDownloadResponse]
    ]:
        def read(
            inspect: Callable[[bytes], None],
            deadline: float,
            cancelled: threading.Event,
        ) -> list[FileDownloadResponse]:
            del cancelled  # inspect checks the owned operation's cancellation event.
            if not 1 <= len(paths) <= MAX_BATCH_FILES:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            total = 0
            results: list[FileDownloadResponse] = []
            for path in paths:
                inspect(path.encode())
                relative = relative_path(path)
                raw = self.reader(
                    relative, min(MAX_FILE_BYTES, MAX_BATCH_BYTES - total), deadline
                )
                if type(raw) is not bytes or len(raw) > MAX_FILE_BYTES:
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                total += len(raw)
                if total > MAX_BATCH_BYTES:
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                inspect(raw)
                results.append(FileDownloadResponse(path=path, content=raw))
            return results

        return read

    def download(self, paths: list[str]) -> list[FileDownloadResponse]:
        try:
            return self.runner.guard(self.timeout, self.operation(paths))
        except ExecutionBoundaryError:
            return [
                FileDownloadResponse(path="", error="permission_denied") for _ in paths
            ]

    async def adownload(self, paths: list[str]) -> list[FileDownloadResponse]:
        try:
            return await self.runner.aguard(self.timeout, self.operation(paths))
        except ExecutionBoundaryError:
            return [
                FileDownloadResponse(path="", error="permission_denied") for _ in paths
            ]

    @staticmethod
    def project(
        response: FileDownloadResponse, path: str, offset: int, limit: int
    ) -> ReadResult:
        if (
            response.content is None
            or type(offset) is not int
            or type(limit) is not int
            or offset < 0
            or limit < 1
        ):
            return ReadResult(error=FILE_UNAVAILABLE)
        raw = response.content
        mime = mimetypes.guess_type(path)[0] or ""
        try:
            content = raw.decode("utf-8")
        except UnicodeError:
            content = None
        if (
            content is None
            or "\x00" in content
            or mime.startswith(("image/", "audio/", "video/"))
            or mime
            in {
                "application/pdf",
                "application/vnd.ms-powerpoint",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            }
        ):
            return ReadResult(
                file_data=FileData(
                    content=base64.b64encode(raw).decode("ascii"), encoding="base64"
                )
            )
        empty = check_empty_content(content)
        if empty:
            return ReadResult(file_data=FileData(content=empty, encoding="utf-8"))
        lines = content.splitlines(keepends=True)
        if offset >= len(lines):
            return ReadResult(error=FILE_UNAVAILABLE)
        return ReadResult(
            file_data=FileData(
                content="".join(lines[offset : offset + limit]), encoding="utf-8"
            )
        )

    def read(self, path: str, offset: int, limit: int) -> ReadResult:
        return self.project(self.download([path])[0], path, offset, limit)

    async def aread(self, path: str, offset: int, limit: int) -> ReadResult:
        return self.project((await self.adownload([path]))[0], path, offset, limit)

    def archive(
        self,
        paths: Iterable[str],
        sink: Callable[[bytes, str], None],
        cancelled: threading.Event | None = None,
    ) -> None:
        def send(
            inspect: Callable[[bytes], None],
            deadline: float,
            cancelled: threading.Event,
        ) -> None:
            total = 0
            for index, path in enumerate(paths):
                if index >= MAX_BATCH_FILES:
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                results = self.operation([path])(inspect, deadline, cancelled)
                content = results[0].content
                if content is None:
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                total += len(content)
                if total > MAX_BATCH_BYTES:
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                inspect(b"")
                sink(content, relative_path(path))
                inspect(b"")

        self.runner.guard(self.timeout, send, cancelled)
