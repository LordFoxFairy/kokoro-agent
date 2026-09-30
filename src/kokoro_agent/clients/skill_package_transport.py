"""Isolated, bounded Storage-signed GET; no owner credentials enter this pool."""

from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb


class SkillTransferError(RuntimeError):
    """Stable transfer error without URL/header/response disclosure."""


def _origin(value: str, *, config: bool = False) -> tuple[str, str, int]:
    if not value or "\\" in value or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        raise ValueError("invalid object origin")
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port == 0
        or "#" in value
        or config
        and (parsed.path not in {"", "/"} or parsed.query)
    ):
        raise ValueError("invalid object origin")
    return (
        parsed.scheme,
        parsed.hostname,
        parsed.port or (443 if parsed.scheme == "https" else 80),
    )


class SkillPackageTransport:
    def __init__(
        self, origin: str, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self._origin = _origin(origin, config=True)
        # Use HTTPX's public transport directly: AsyncClient logs full signed URLs
        # at INFO and maintains a cookie jar, neither belongs to this byte boundary.
        self._http = transport or httpx.AsyncHTTPTransport(
            trust_env=False,
            retries=0,
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )

    async def __aenter__(self) -> SkillPackageTransport:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def get(
        self, reference: pb.PackageTransferReference, content_digest: str
    ) -> bytes:
        failure = "SKILL_TRANSFER_INVALID"
        try:
            if (
                reference.method != "GET"
                or _origin(reference.url) != self._origin
                or reference.required_headers
                or reference.expires_at is None
                or reference.expires_at.to_datetime() <= datetime.now(timezone.utc)
                or re.fullmatch(r"[0-9a-f]{64}", content_digest) is None
            ):
                raise ValueError("invalid reference")
            expiry = reference.expires_at.to_datetime()
            # Explicit Request prevents both default auth and cookies set by a prior response.
            request = httpx.Request(
                "GET",
                reference.url,
                headers={"accept-encoding": "identity"},
                extensions={
                    "timeout": {"connect": 3, "read": 10, "write": 10, "pool": 3}
                },
            )
            async with asyncio.timeout(
                min(30, (expiry - datetime.now(timezone.utc)).total_seconds())
            ):
                response = await self._http.handle_async_request(request)
                try:
                    if (
                        response.status_code != 200
                        or response.headers.get("content-encoding", "identity")
                        != "identity"
                    ):
                        raise ValueError("invalid response")
                    size = response.headers.get("content-length")
                    if size is not None and (
                        not size.isascii()
                        or not size.isdecimal()
                        or int(size) > 33554432
                    ):
                        raise ValueError("invalid size")
                    content = bytearray()
                    digest = hashlib.sha256()
                    async for chunk in response.aiter_raw():
                        if len(content) + len(chunk) > 33554432:
                            raise ValueError("size exceeded")
                        content.extend(chunk)
                        digest.update(chunk)
                    if (
                        size is not None
                        and len(content) != int(size)
                        or digest.hexdigest() != content_digest
                        or expiry <= datetime.now(timezone.utc)
                    ):
                        raise ValueError("invalid body")
                    return bytes(content)
                finally:
                    await response.aclose()
        except (httpx.HTTPError, TimeoutError):
            failure = "SKILL_TRANSFER_UNAVAILABLE"
        except (ValueError, OverflowError):
            pass
        raise SkillTransferError(failure) from None
