"""Bounded official E2B Filesystem unary reads, without spawning a process.

Stat/ListDir are path based; they do not provide Docker's atomic dirfd guarantee.
Generated messages stay here. No API environment values enter the RPC requests.
"""

from __future__ import annotations

import asyncio
import base64
import threading
import time
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

import httpx
from deepagents.backends.protocol import FileInfo
from e2b.connection_config import ConnectionConfig, default_username
from e2b.envd.filesystem import filesystem_connect, filesystem_pb2 as pb

from kokoro_agent.sandbox.execution_environment import ExecutionBoundaryError
from kokoro_agent.sandbox.file_egress import (
    FILE_UNAVAILABLE,
    MAX_BATCH_FILES,
    MAX_DISCOVERY_DEPTH,
    MAX_DISCOVERY_ENTRIES,
    MAX_DISCOVERY_NAME_BYTES,
    Inspector,
    relative_path,
)

MAX_METADATA_BODY_BYTES = 256 * 1024
MAX_METADATA_TOTAL_BYTES = 2 * 1024 * 1024
MAX_METADATA_REQUESTS = MAX_DISCOVERY_ENTRIES + MAX_DISCOVERY_DEPTH + 1
_WORKDIR = "/home/user"


@runtime_checkable
class FilesystemSandbox(Protocol):
    @property
    def envd_api_url(self) -> str: ...

    @property
    def connection_config(self) -> ConnectionConfig: ...


@dataclass(slots=True)
class _Budget:
    bytes: int = 0
    requests: int = 0
    names: int = 0
    files: int = 0
    entries: int = 0


class E2BMetadata:
    """One guarded discovery owns one async transport and its entire traversal."""

    def __init__(self, sandbox: object) -> None:
        self.sandbox = sandbox

    def __call__(
        self,
        path: str,
        recursive: bool,
        inspect: Inspector,
        deadline: float,
        cancelled: threading.Event,
    ) -> list[FileInfo]:
        inspect(path.encode())
        base = "" if path in {"/", "."} else relative_path(path.removesuffix("/"))
        if base and len(base.split("/")) > MAX_DISCOVERY_DEPTH:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        if not isinstance(self.sandbox, FilesystemSandbox):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        return asyncio.run(self._run(base, recursive, inspect, deadline, cancelled))

    async def _run(
        self,
        base: str,
        recursive: bool,
        inspect: Inspector,
        deadline: float,
        cancelled: threading.Event,
    ) -> list[FileInfo]:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or cancelled.is_set():
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        async with asyncio.timeout(remaining):
            task = asyncio.create_task(
                self._discover(base, recursive, inspect, deadline)
            )
            try:
                while not task.done():
                    inspect(b"")
                    if cancelled.is_set():
                        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                    await asyncio.wait({task}, timeout=0.025)
                inspect(b"")
                return await task
            finally:
                if not task.done():
                    task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _body(
        self, response: httpx.Response, budget: _Budget, inspect: Inspector
    ) -> bytes:
        if response.headers.get("content-encoding", "identity") != "identity":
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        length = response.headers.get("content-length")
        if length is not None and (
            not length.isascii()
            or not length.isdecimal()
            or len(length) > 20
            or int(length) > MAX_METADATA_BODY_BYTES
        ):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        body = bytearray()
        async for chunk in response.aiter_raw():
            inspect(b"")
            budget.bytes += len(chunk)
            if (
                len(body) + len(chunk) > MAX_METADATA_BODY_BYTES
                or budget.bytes > MAX_METADATA_TOTAL_BYTES
            ):
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            body.extend(chunk)
        if length is not None and len(body) != int(length):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        raw = bytes(body)
        inspect(raw)
        return raw

    async def _rpc(
        self,
        transport: httpx.AsyncBaseTransport,
        operation: Literal["Stat", "ListDir"],
        path: str,
        budget: _Budget,
        inspect: Inspector,
        deadline: float,
    ) -> bytes | None:
        inspect(b"")
        if not isinstance(self.sandbox, FilesystemSandbox):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        budget.requests += 1
        if budget.requests > MAX_METADATA_REQUESTS:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        origin = httpx.URL(self.sandbox.envd_api_url)
        if (
            origin.scheme not in {"http", "https"}
            or not origin.host
            or origin.userinfo
            or origin.query
            or origin.fragment
            or origin.path not in {"", "/"}
        ):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        message = (
            pb.StatRequest(path=path)
            if operation == "Stat"
            else pb.ListDirRequest(path=path, depth=1)
        )
        # A fixed explicit template user avoids private envd-version access.
        user = base64.b64encode((default_username + ":").encode()).decode("ascii")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        request = httpx.Request(
            "POST",
            str(origin).rstrip("/")
            + "/"
            + filesystem_connect.FilesystemName
            + "/"
            + operation,
            headers={
                **self.sandbox.connection_config.sandbox_headers,
                "authorization": "Basic " + user,
                "connect-protocol-version": "1",
                "content-type": "application/proto",
                "content-encoding": "identity",
                "accept-encoding": "identity",
            },
            content=message.SerializeToString(),
            extensions={
                "timeout": dict.fromkeys(
                    ("connect", "read", "write", "pool"), remaining
                )
            },
        )
        response = await transport.handle_async_request(request)
        try:
            raw = await self._body(response, budget, inspect)
            if response.status_code == 404 and operation == "Stat":
                return None
            if (
                response.status_code != 200
                or response.headers.get("content-type", "").partition(";")[0]
                != "application/proto"
            ):
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            return raw
        finally:
            await response.aclose()

    def _entry(
        self, entry: pb.EntryInfo, expected: str, inspect: Inspector
    ) -> FileInfo:
        inspect(entry.name.encode())
        inspect(entry.path.encode())
        if (
            entry.path != expected
            or entry.name != expected.rsplit("/", 1)[-1]
            or entry.HasField("symlink_target")
            or entry.type not in {pb.FILE_TYPE_FILE, pb.FILE_TYPE_DIRECTORY}
            or entry.size < 0
        ):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        # Official envd reports Go permission bits; POSIX S_IFMT is not valid here.
        relative = entry.path.removeprefix(_WORKDIR + "/")
        if entry.path != _WORKDIR:
            relative_path(relative)
            if len(relative.split("/")) > MAX_DISCOVERY_DEPTH:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        return {
            "path": "/" + relative,
            "is_dir": entry.type == pb.FILE_TYPE_DIRECTORY,
            "size": entry.size,
        }

    async def _discover(
        self, base: str, recursive: bool, inspect: Inspector, deadline: float
    ) -> list[FileInfo]:
        budget = _Budget()
        entries: list[FileInfo] = []
        async with httpx.AsyncHTTPTransport(
            trust_env=False,
            retries=0,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
        ) as transport:
            target = _WORKDIR
            info: FileInfo | None = None
            for part in [""] + (base.split("/") if base else []):
                if part:
                    target += "/" + part
                raw = await self._rpc(
                    transport, "Stat", target, budget, inspect, deadline
                )
                if raw is None:
                    return []
                stat = pb.StatResponse.FromString(raw)
                if not stat.HasField("entry"):
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                info = self._entry(stat.entry, target, inspect)
                if (
                    target == _WORKDIR
                    or target != _WORKDIR + ("/" + base if base else "")
                ) and not info.get("is_dir"):
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            if info is None:
                raise ExecutionBoundaryError(FILE_UNAVAILABLE)
            if not info.get("is_dir"):
                return [info]
            pending = [target]
            seen: set[str] = set()
            while pending:
                directory = pending.pop()
                raw = await self._rpc(
                    transport, "ListDir", directory, budget, inspect, deadline
                )
                if raw is None:
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                listed = pb.ListDirResponse.FromString(raw)
                for entry in listed.entries:
                    inspect(entry.name.encode())
                    inspect(entry.path.encode())
                    if not entry.name or "/" in entry.name or entry.name in {".", ".."}:
                        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                    item = self._entry(entry, directory + "/" + entry.name, inspect)
                    relative = item["path"].removeprefix("/")
                    budget.entries += 1
                    budget.files += not item.get("is_dir")
                    budget.names += len(relative.encode())
                    if (
                        item["path"] in seen
                        or budget.entries > MAX_DISCOVERY_ENTRIES
                        or budget.files > MAX_BATCH_FILES
                        or budget.names > MAX_DISCOVERY_NAME_BYTES
                    ):
                        raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                    seen.add(item["path"])
                    entries.append(item)
                    if recursive and item.get("is_dir"):
                        pending.append(entry.path)
            return sorted(entries, key=lambda item: item["path"])
