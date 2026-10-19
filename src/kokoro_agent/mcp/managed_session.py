"""One current-material SDK session; no command registry, cache or retry loop."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
import hashlib
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import time

from langchain_mcp_adapters.callbacks import Callbacks
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.sessions import Connection
from langgraph.errors import GraphInterrupt
from mcp import ClientSession

from kokoro_agent.clients.mcp_package_transport import McpPackageTransport
from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r
from kokoro_agent.mcp.egress import build_mcp_client_factory
from kokoro_agent.mcp.package import McpPackage, validate_mcp_package


class McpSessionError(RuntimeError):
    """Safe session error; no URL, environment, command or upstream exception."""


def clear_mcp_material(material: r.AcquireMcpRuntimeCredentialResponse) -> None:
    """Drop decoded secret references; Python immutable byte allocation is not zeroizable."""
    transport = material.transport
    if transport is not None:
        if transport.field == "streamable_http":
            for value in transport.value.headers:
                value.value = b""
        elif transport.field == "stdio":
            for value in transport.value.environment:
                value.value = b""
            download = transport.value.package_download
            if download is not None:
                download.url_utf8 = b""
                for value in download.required_headers:
                    value.value = b""
    material.transport = None


@contextmanager
def staged_package(package: McpPackage) -> Generator[Path, None, None]:
    with TemporaryDirectory(prefix="kokoro-mcp-") as directory:
        root = Path(directory)
        for path, content in package.files.items():
            destination = root / path
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o700 if path in package.executables else 0o600,
            )
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
        yield root


def _http(
    resolution: r.McpRuntimeHttpConnection,
    material: r.McpRuntimeHttpCredential,
    mode: str,
) -> Connection:
    headers: dict[str, str] = {}
    for value in material.headers:
        lower = value.name.lower()
        if (
            lower in headers
            or lower
            in {
                "cookie",
                "host",
                "connection",
                "content-length",
                "transfer-encoding",
                "proxy-authorization",
            }
            or re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", lower) is None
        ):
            raise ValueError("invalid header")
        decoded = value.value.decode("utf-8", errors="strict")
        if not decoded or any(ord(c) < 32 or ord(c) == 127 for c in decoded):
            raise ValueError("invalid header")
        headers[lower] = decoded
    if set(headers) != {name.lower() for name in resolution.header_slots}:
        raise ValueError("slot mismatch")
    connection: Connection = {
        "transport": "streamable_http",
        "url": resolution.canonical_endpoint,
        "headers": headers,
    }
    factory = build_mcp_client_factory(mode)
    if factory is not None:
        connection["httpx_client_factory"] = factory
    return connection


@asynccontextmanager
async def _standard_session(
    connection: Connection, callbacks: Callbacks | None
) -> AsyncGenerator[ClientSession, None]:
    # A graph pause belongs to the caller, not to the SDK TaskGroup. Teardown
    # before rethrowing prevents it being wrapped in a generic ExceptionGroup.
    pause: GraphInterrupt | None = None
    async with MultiServerMCPClient(
        {"managed": connection}, callbacks=callbacks
    ).session("managed") as session:
        try:
            yield session
        except GraphInterrupt as error:
            pause = error
    if pause is not None:
        raise pause


@asynccontextmanager
async def managed_mcp_session(
    resolution: r.McpRuntimeResolution,
    material: r.AcquireMcpRuntimeCredentialResponse,
    *,
    packages: McpPackageTransport | None,
    callbacks: Callbacks | None = None,
    egress_mode: str = "strict",
) -> AsyncGenerator[ClientSession, None]:
    connection: Connection | None = None
    failed = False
    try:
        if (
            resolution.to_binary() != resolution.to_binary(write_unknown_fields=False)
            or material.to_binary() != material.to_binary(write_unknown_fields=False)
            or not resolution.resolution_ref
            or material.resolution_ref_sha256
            != hashlib.sha256(resolution.resolution_ref.encode("utf-8")).hexdigest()
            or resolution.transport is None
            or material.transport is None
            or resolution.transport.field != material.transport.field
        ):
            raise ValueError("material binding mismatch")
        remaining = (
            min(resolution.expires_at_unix_seconds, material.expires_at_unix_seconds)
            - time.time()
        )
        if remaining <= 0 or remaining > 30:
            raise ValueError("material expiry invalid")
        async with asyncio.timeout(remaining):
            if resolution.transport.field == "streamable_http":
                if material.transport.field != "streamable_http":
                    raise ValueError("transport mismatch")
                connection = _http(
                    resolution.transport.value, material.transport.value, egress_mode
                )
                async with _standard_session(connection, callbacks) as session:
                    yield session
            else:
                if material.transport.field != "stdio" or packages is None:
                    raise ValueError("package transport unavailable")
                stdio = resolution.transport.value
                secret = material.transport.value
                if (
                    stdio.environment_slots
                    or secret.environment
                    or not stdio.source_version_id
                    or re.fullmatch(
                        r"mcp-package:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                        stdio.runtime_artifact_ref,
                    )
                    is None
                    or secret.package_download is None
                ):
                    raise ValueError("stdio source invalid")
                payload = await packages.get(
                    secret.package_download,
                    stdio.artifact_sha256,
                    material.expires_at_unix_seconds,
                )
                package = validate_mcp_package(
                    payload,
                    artifact_sha256=stdio.artifact_sha256,
                    entrypoint=stdio.entrypoint,
                )
                if time.time() >= min(
                    resolution.expires_at_unix_seconds, material.expires_at_unix_seconds
                ):
                    raise ValueError("material expired before process launch")
                with staged_package(package) as root:
                    connection = {
                        "transport": "stdio",
                        "command": str(root / package.entrypoint),
                        "args": [],
                        "cwd": str(root),
                        "env": {},
                    }
                    async with _standard_session(connection, callbacks) as session:
                        yield session
    except (asyncio.CancelledError, GraphInterrupt):
        raise
    except Exception:
        failed = True
    finally:
        clear_mcp_material(material)
        if connection is not None:
            if connection["transport"] == "streamable_http":
                headers = connection.get("headers")
                if headers is not None:
                    headers.clear()
            elif connection["transport"] == "stdio":
                environment = connection.get("env")
                if environment is not None:
                    environment.clear()
    if failed:
        raise McpSessionError("MCP_SESSION_FAILED") from None
