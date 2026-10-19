"""MCP's exact-size, short-lived GET material, without owner credential injection."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import httpx
from kokoro_agent.clients.package_transport import (
    PackageTransport,
    PackageTransferError,
)
from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as pb


class McpTransferError(RuntimeError):
    """Safe download failure, never carries signed URL or header values."""


class McpPackageTransport:
    def __init__(
        self, origin: str, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self._packages = PackageTransport(origin, transport=transport)

    async def __aenter__(self) -> McpPackageTransport:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._packages.aclose()

    async def get(
        self,
        reference: pb.McpRuntimePackageDownload,
        content_digest: str,
        material_expires_at: int,
    ) -> bytes:
        failure = "MCP_TRANSFER_INVALID"
        try:
            if len(reference.required_headers) > 32:
                raise ValueError("header budget")
            headers: dict[str, str] = {}
            for header in reference.required_headers:
                if (
                    re.fullmatch(r"[a-z0-9-]{1,64}", header.name) is None
                    or header.name.startswith(("x-kokoro-", "proxy-"))
                    or header.name
                    in {"authorization", "cookie", "host", "connection", "set-cookie"}
                    or header.name in headers
                ):
                    raise ValueError("duplicate header")
                value = header.value.decode("utf-8", errors="strict")
                if not 0 < len(value.encode("utf-16-le")) // 2 <= 1024:
                    raise ValueError("header value budget")
                headers[header.name] = value
            expiry = datetime.fromtimestamp(
                min(reference.expires_at_unix_seconds, material_expires_at),
                timezone.utc,
            )
            return await self._packages.get(
                url=reference.url_utf8.decode("utf-8", errors="strict"),
                headers=headers,
                expiry=expiry,
                digest=content_digest,
                exact_size=reference.size_bytes,
            )
        except PackageTransferError as error:
            failure = "MCP_" + str(error)
        except (ValueError, OverflowError, OSError):
            pass
        raise McpTransferError(failure) from None
