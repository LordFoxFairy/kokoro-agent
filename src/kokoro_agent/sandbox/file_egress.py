"""Bounded original-byte snapshots for Custom API file exits.

No credential cache, rewriting, encoded-content inspection or network policy.
"""

from __future__ import annotations

import base64
import fnmatch
import json
import mimetypes
import os
import stat
import threading
import time
from collections.abc import Callable, Iterable
from contextlib import ExitStack
from pathlib import Path

from deepagents.backends.protocol import (
    FileData,
    FileDownloadResponse,
    FileInfo,
    GlobResult,
    GrepMatch,
    GrepResult,
    LsResult,
    ReadResult,
)
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


# Discovery budgets apply before filtering, never to a successful prefix.
MAX_DISCOVERY_ENTRIES = 1024
MAX_DISCOVERY_NAME_BYTES = 32 * 1024
MAX_DISCOVERY_DEPTH = 64
MAX_DISCOVERY_RESULT_BYTES = 100_000
Inspector = Callable[[bytes], None]
ListingOperation = Callable[[Inspector, float, threading.Event], list[FileInfo]]
SearchOperation = Callable[[Inspector, float, threading.Event], list[GrepMatch]]


def _discovery_base(path: str) -> str:
    base = "" if path in {"/", "."} else relative_path(path.removesuffix("/"))
    if base and len(base.split("/")) > MAX_DISCOVERY_DEPTH:
        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
    return base


def _glob_components(pattern: str) -> tuple[str, ...]:
    parts = tuple(pattern.removeprefix("/").split("/"))
    if (
        len(pattern.encode()) > 4096
        or len(parts) > MAX_DISCOVERY_DEPTH
        or any(part in {"", ".", ".."} for part in parts)
        or any("**" in part and part != "**" for part in parts)
        or any(character in pattern for character in "\\\x00{}()")
    ):
        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
    return parts


def _matches_glob(path: str, pattern: tuple[str, ...]) -> bool:
    # A small component automaton keeps ** matching finite without filesystem I/O.
    def closure(states: set[int]) -> set[int]:
        result = set(states)
        for index in range(len(pattern)):
            if index in result and pattern[index] == "**":
                result.add(index + 1)
        return result

    states = closure({0})
    for component in path.split("/"):
        following: set[int] = set()
        for index in states:
            if index == len(pattern):
                continue
            if pattern[index] == "**":
                following.add(index)
            elif fnmatch.fnmatchcase(component, pattern[index]):
                following.add(index + 1)
        states = closure(following)
    return len(pattern) in states


def _entry_relative(path: str, base: str) -> str:
    if path == "/" + base:
        return path.rsplit("/", 1)[-1]
    return path.removeprefix("/" + base + "/" if base else "/")


MetadataReader = Callable[
    [str, bool, Inspector, float, threading.Event], list[FileInfo]
]


class SnapshotFileTools:
    """Shared checked metadata and whole-byte snapshot projections."""

    def __init__(
        self, egress: FileEgress, entries: MetadataReader, max_output_bytes: int
    ) -> None:
        self.egress = egress
        self.entries = entries
        self.result_limit = min(max_output_bytes, MAX_DISCOVERY_RESULT_BYTES)

    def _bounded(self, item: FileInfo | GrepMatch, used: int) -> int:
        size = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode())
        used += size + (1 if used > 2 else 0)
        if used > self.result_limit:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        return used

    def _listing(self, path: str, pattern: str | None) -> ListingOperation:
        def operation(
            inspect: Inspector, deadline: float, cancelled: threading.Event
        ) -> list[FileInfo]:
            if pattern is not None:
                inspect(pattern.encode())
            components = _glob_components(pattern) if pattern is not None else None
            entries = self.entries(
                path, components is not None, inspect, deadline, cancelled
            )
            base = _discovery_base(path)
            result: list[FileInfo] = []
            used = 2
            if used > self.result_limit:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            for entry in entries:
                inspect(b"")
                if components is not None and (
                    entry.get("is_dir")
                    or not _matches_glob(
                        _entry_relative(entry["path"], base), components
                    )
                ):
                    continue
                used = self._bounded(entry, used)
                result.append(entry)
            return result

        return operation

    def _search(
        self, pattern: str, path: str | None, glob: str | None
    ) -> SearchOperation:
        def operation(
            inspect: Inspector, deadline: float, cancelled: threading.Event
        ) -> list[GrepMatch]:
            inspect(pattern.encode())
            if not pattern or len(pattern.encode()) > 4096:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            if glob is not None:
                inspect(glob.encode())
            components = _glob_components(glob) if glob is not None else None
            base_path = path if path is not None else "/"
            entries = self.entries(base_path, True, inspect, deadline, cancelled)
            base = _discovery_base(base_path)
            paths = [
                entry["path"]
                for entry in entries
                if not entry.get("is_dir")
                and (
                    components is None
                    or _matches_glob(_entry_relative(entry["path"], base), components)
                )
            ]
            snapshots = (
                self.egress.operation(paths)(inspect, deadline, cancelled)
                if paths
                else []
            )
            result: list[GrepMatch] = []
            used = 2
            if used > self.result_limit:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            for snapshot in snapshots:
                inspect(b"")
                raw = snapshot.content
                if raw is None:
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                try:
                    text = raw.decode("utf-8")
                except UnicodeError:
                    continue
                if "\x00" in text:
                    continue
                for line_number, line in enumerate(text.split("\n"), 1):
                    inspect(b"")
                    if pattern not in line:
                        continue
                    match: GrepMatch = {
                        "path": snapshot.path,
                        "line": line_number,
                        "text": line,
                    }
                    used = self._bounded(match, used)
                    result.append(match)
            return result

        return operation

    def ls(self, path: str) -> LsResult:
        try:
            return LsResult(
                entries=self.egress.runner.guard(
                    self.egress.timeout, self._listing(path, None)
                )
            )
        except ExecutionBoundaryError:
            return LsResult(error=FILE_UNAVAILABLE)

    async def als(self, path: str) -> LsResult:
        try:
            return LsResult(
                entries=await self.egress.runner.aguard(
                    self.egress.timeout, self._listing(path, None)
                )
            )
        except ExecutionBoundaryError:
            return LsResult(error=FILE_UNAVAILABLE)

    def glob(self, pattern: str, path: str) -> GlobResult:
        try:
            return GlobResult(
                matches=self.egress.runner.guard(
                    self.egress.timeout, self._listing(path, pattern)
                )
            )
        except ExecutionBoundaryError:
            return GlobResult(error=FILE_UNAVAILABLE)

    async def aglob(self, pattern: str, path: str) -> GlobResult:
        try:
            return GlobResult(
                matches=await self.egress.runner.aguard(
                    self.egress.timeout, self._listing(path, pattern)
                )
            )
        except ExecutionBoundaryError:
            return GlobResult(error=FILE_UNAVAILABLE)

    def grep(self, pattern: str, path: str | None, glob: str | None) -> GrepResult:
        try:
            return GrepResult(
                matches=self.egress.runner.guard(
                    self.egress.timeout, self._search(pattern, path, glob)
                )
            )
        except ExecutionBoundaryError:
            return GrepResult(error=FILE_UNAVAILABLE)

    async def agrep(
        self, pattern: str, path: str | None, glob: str | None
    ) -> GrepResult:
        try:
            return GrepResult(
                matches=await self.egress.runner.aguard(
                    self.egress.timeout, self._search(pattern, path, glob)
                )
            )
        except ExecutionBoundaryError:
            return GrepResult(error=FILE_UNAVAILABLE)


class LocalFileTools(SnapshotFileTools):
    """Docker discovery keeps its original dirfd/nofollow metadata source."""

    def __init__(self, egress: FileEgress, root: Path, max_output_bytes: int) -> None:
        self.root = root
        super().__init__(egress, self._local_entries, max_output_bytes)

    def _local_entries(
        self,
        path: str,
        recursive: bool,
        inspect: Inspector,
        deadline: float,
        cancelled: threading.Event,
    ) -> list[FileInfo]:
        del deadline, cancelled  # The guard inspector owns both checks.
        inspect(path.encode())
        base = _discovery_base(path)
        flags = os.O_RDONLY | os.O_NOFOLLOW
        entries: list[FileInfo] = []
        visited = files = name_bytes = 0

        def record(relative: str, info: os.stat_result) -> None:
            nonlocal visited, files, name_bytes
            inspect(relative.encode())
            relative_path(relative)
            visited += 1
            name_bytes += len(relative.encode())
            is_directory = stat.S_ISDIR(info.st_mode)
            if not is_directory and not stat.S_ISREG(info.st_mode):
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            files += not is_directory
            if (
                visited > MAX_DISCOVERY_ENTRIES
                or files > MAX_BATCH_FILES
                or name_bytes > MAX_DISCOVERY_NAME_BYTES
                or len(relative.split("/")) > MAX_DISCOVERY_DEPTH
            ):
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            entries.append(
                {"path": "/" + relative, "is_dir": is_directory, "size": info.st_size}
            )

        def walk(descriptor: int, prefix: str, depth: int) -> None:
            inspect(b"")
            if depth > MAX_DISCOVERY_DEPTH:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            with os.scandir(descriptor) as children:
                for child in children:
                    inspect(child.name.encode())
                    relative = prefix + child.name
                    info = child.stat(follow_symlinks=False)
                    record(relative, info)
                    if recursive and stat.S_ISDIR(info.st_mode):
                        child_fd = os.open(
                            child.name, flags | os.O_DIRECTORY, dir_fd=descriptor
                        )
                        try:
                            walk(child_fd, relative + "/", depth + 1)
                        finally:
                            os.close(child_fd)

        with ExitStack() as stack:
            descriptor = os.open(self.root, flags | os.O_DIRECTORY)
            stack.callback(os.close, descriptor)
            try:
                for part in base.split("/") if base else ():
                    descriptor = os.open(part, flags | os.O_NONBLOCK, dir_fd=descriptor)
                    stack.callback(os.close, descriptor)
                info = os.fstat(descriptor)
            except FileNotFoundError:
                return []
            if stat.S_ISDIR(info.st_mode):
                walk(descriptor, base + "/" if base else "", 0)
            elif stat.S_ISREG(info.st_mode):
                record(base, info)
            else:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        return sorted(entries, key=lambda item: item["path"])
