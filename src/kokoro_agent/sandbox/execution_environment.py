"""Ephemeral per-execution environments, with limited literal-output redaction.

This does not prevent encoded disclosure, files or network exfiltration. Values
never belong to backend state, commands, runtime profiles or persisted requests.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import math
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, TypeVar

from deepagents.backends.protocol import ExecuteResponse
from pydantic import SecretStr


class ExecutionEnvironmentProvider(Protocol):
    """Bound to trusted Run identity by the factory, not by tool arguments."""

    async def acquire(
        self, *, deadline_monotonic: float
    ) -> tuple[tuple[str, SecretStr], ...]: ...


_NAME = re.compile(r"[A-Z_][A-Z0-9_]{0,127}\Z")
_RESERVED = frozenset(
    {
        "PATH",
        "HOME",
        "SHELL",
        "ENV",
        "IFS",
        "CDPATH",
        "PWD",
        "OLDPWD",
        "USER",
        "LOGNAME",
        "TMPDIR",
        "TMP",
        "TEMP",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "NODE_OPTIONS",
        "NODE_PATH",
        "RUBYOPT",
        "RUBYLIB",
        "PERL5OPT",
        "PERL5LIB",
        "GIT_CONFIG",
        "SHELLOPTS",
        "LANG",
        "LC_ALL",
        "TZ",
        "ZDOTDIR",
        "JAVA_TOOL_OPTIONS",
        "JDK_JAVA_OPTIONS",
        "_JAVA_OPTIONS",
        "CLASSPATH",
        "GCONV_PATH",
        "GETCONF_DIR",
        "CURL_CA_BUNDLE",
        "REQUESTS_CA_BUNDLE",
        "ENV_SUPATH",
        "SSLKEYLOGFILE",
    }
)
_PREFIXES = (
    "__",
    "KOKORO_",
    "DOCKER_",
    "E2B_",
    "LD_",
    "DYLD_",
    "PYTHON",
    "NODE_",
    "OPENSSL_",
    "BASH",
    "GIT_CONFIG_",
)


@dataclass(frozen=True, slots=True)
class _Environment:
    entries: tuple[tuple[str, SecretStr], ...] = field(repr=False)

    @classmethod
    def checked(cls, entries: tuple[tuple[str, SecretStr], ...]) -> _Environment:
        names: set[str] = set()
        snapshot: list[tuple[str, SecretStr]] = []
        size = 0
        if len(entries) > 128:
            raise ValueError("invalid_execution_environment")
        for entry in entries:
            if len(entry) != 2:
                raise ValueError("invalid_execution_environment")
            name, secret = entry
            if (
                not _NAME.fullmatch(name)
                or name in names
                or name in _RESERVED
                or name.startswith(_PREFIXES)
            ):
                raise ValueError("invalid_execution_environment")
            value = secret.get_secret_value()
            if (
                not value
                or len(value.encode()) > 4096
                or any(c in value for c in "\x00\r\n")
            ):
                raise ValueError("invalid_execution_environment")
            names.add(name)
            snapshot.append((name, SecretStr(value)))
            size += len(name.encode()) + len(value.encode())
        if size > 65536:
            raise ValueError("invalid_execution_environment")
        return cls(tuple(snapshot))

    def values(self) -> dict[str, str]:
        return {name: value.get_secret_value() for name, value in self.entries}

    def redact(self, text: str) -> str:
        for value in sorted(
            {v.get_secret_value() for _, v in self.entries}, key=len, reverse=True
        ):
            text = text.replace(value, "[REDACTED]")
        return text

    def inspect(self, data: bytes) -> None:
        if any(
            secret.get_secret_value().encode() in data for _, secret in self.entries
        ):
            raise ExecutionBoundaryError("sandbox_file_denied")


Operation = Callable[[dict[str, str], int], ExecuteResponse]
T = TypeVar("T")


class ExecutionBoundaryError(RuntimeError):
    """Stable, non-sensitive failure at a protected file exit."""


def _failure(code: str) -> ExecuteResponse:
    return ExecuteResponse(output=code, exit_code=1, truncated=False)


class ExecutionEnvironmentRunner:
    """No credentials retained between calls; sync bridges only from worker threads."""

    def __init__(
        self,
        provider: ExecutionEnvironmentProvider,
        *,
        max_output_bytes: int,
        owner_loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        if max_output_bytes < 1:
            raise ValueError("invalid_execution_output_limit")
        self._provider = provider
        self._owner_loop = owner_loop
        self._max_output_bytes = max_output_bytes

    async def _acquire(self, deadline: float) -> _Environment:
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            return _Environment.checked(
                await self._provider.acquire(deadline_monotonic=deadline)
            )

    def _guarded(
        self,
        environment: _Environment,
        deadline: float,
        cancelled: threading.Event,
        operation: Callable[[Callable[[bytes], None], float, threading.Event], T],
    ) -> T:
        def inspect(data: bytes) -> None:
            if cancelled.is_set() or time.monotonic() >= deadline:
                raise ExecutionBoundaryError("sandbox_file_unavailable")
            environment.inspect(data)

        try:
            inspect(b"")
            result = operation(inspect, deadline, cancelled)
            inspect(b"")
            return result
        except ExecutionBoundaryError:
            raise
        except Exception:
            raise ExecutionBoundaryError("sandbox_file_unavailable") from None

    async def aguard(
        self,
        timeout: int,
        operation: Callable[[Callable[[bytes], None], float, threading.Event], T],
    ) -> T:
        deadline = time.monotonic() + timeout
        cancelled = threading.Event()
        try:
            environment = await self._acquire(deadline)
        except asyncio.CancelledError:
            raise asyncio.CancelledError from None
        except Exception:
            raise ExecutionBoundaryError("sandbox_file_unavailable") from None
        task = asyncio.create_task(
            asyncio.to_thread(
                self._guarded, environment, deadline, cancelled, operation
            )
        )
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled.set()
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not task.cancelled():
                task.exception()
            raise asyncio.CancelledError from None

    def guard(
        self,
        timeout: int,
        operation: Callable[[Callable[[bytes], None], float, threading.Event], T],
        cancelled: threading.Event | None = None,
    ) -> T:
        loop = self._owner_loop
        if loop is None or not loop.is_running():
            raise ExecutionBoundaryError("sandbox_file_unavailable")
        try:
            if asyncio.get_running_loop() is loop:
                raise ExecutionBoundaryError("sandbox_file_unavailable")
        except RuntimeError as error:
            if isinstance(error, ExecutionBoundaryError):
                raise
        deadline = time.monotonic() + timeout
        future = asyncio.run_coroutine_threadsafe(self._acquire(deadline), loop)
        try:
            environment = future.result(timeout=max(0, deadline - time.monotonic()))
        except Exception:
            future.cancel()
            raise ExecutionBoundaryError("sandbox_file_unavailable") from None
        return self._guarded(
            environment, deadline, cancelled or threading.Event(), operation
        )

    def _perform(
        self,
        environment: _Environment,
        deadline: float,
        operation: Operation,
        cancelled: threading.Event,
    ) -> ExecuteResponse:
        remaining = deadline - time.monotonic()
        if cancelled.is_set() or remaining <= 0:
            return _failure("sandbox_execution_cancelled")
        try:
            result = operation(environment.values(), max(1, math.ceil(remaining)))
            # Redact before clipping: truncating a secret first leaks its prefix.
            output = environment.redact(result.output)
            raw = output.encode()
            clipped = len(raw) > self._max_output_bytes
            return ExecuteResponse(
                output=raw[: self._max_output_bytes].decode(errors="replace")
                if clipped
                else output,
                exit_code=result.exit_code,
                truncated=result.truncated or clipped,
            )
        except Exception:
            # SDK/subprocess exceptions may contain env, command output or auth.
            return _failure("sandbox_execution_failed")

    async def aexecute(self, timeout: int, operation: Operation) -> ExecuteResponse:
        if timeout <= 0:
            return _failure("sandbox_environment_unavailable")
        deadline = time.monotonic() + timeout
        cancelled = threading.Event()
        try:
            environment = await self._acquire(deadline)
        except asyncio.CancelledError:
            raise asyncio.CancelledError from None
        except Exception:
            return _failure("sandbox_environment_unavailable")
        task = asyncio.create_task(
            asyncio.to_thread(
                self._perform, environment, deadline, operation, cancelled
            )
        )
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled.set()
            # Own the blocking operation until it ends; this is not a remote kill.
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
            # Consume any failure without attaching its potentially sensitive text.
            if not task.cancelled():
                task.exception()
            raise asyncio.CancelledError from None

    def execute(self, timeout: int, operation: Operation) -> ExecuteResponse:
        loop = self._owner_loop
        if loop is None or not loop.is_running() or timeout <= 0:
            return _failure("sandbox_environment_unavailable")
        try:
            if asyncio.get_running_loop() is loop:
                return _failure("sandbox_environment_sync_on_owner_loop")
        except RuntimeError:
            pass
        deadline = time.monotonic() + timeout
        future: concurrent.futures.Future[_Environment] | None = None
        try:
            future = asyncio.run_coroutine_threadsafe(self._acquire(deadline), loop)
            environment = future.result(timeout=max(0, deadline - time.monotonic()))
        except Exception:
            if future is not None:
                future.cancel()
            return _failure("sandbox_environment_unavailable")
        return self._perform(environment, deadline, operation, threading.Event())
