"""One isolated, bounded ObjectStore GET pool, without cookies or workload tokens."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx


class PackageTransferError(RuntimeError):
    """Safe transfer category, never includes reference or upstream exception."""


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


class PackageTransport:
    def __init__(
        self, origin: str, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self._origin = _origin(origin, config=True)
        self._http = transport or httpx.AsyncHTTPTransport(
            trust_env=False,
            retries=0,
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def get(
        self,
        *,
        url: str,
        headers: Mapping[str, str],
        expiry: datetime,
        digest: str,
        exact_size: int | None = None,
    ) -> bytes:
        failure = "TRANSFER_INVALID"
        request: httpx.Request | None = None
        try:
            if (
                _origin(url) != self._origin
                or expiry <= datetime.now(timezone.utc)
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                or exact_size is not None
                and not 0 < exact_size <= 33554432
            ):
                raise ValueError("invalid reference")
            forbidden = {
                "host",
                "cookie",
                "set-cookie",
                "connection",
                "content-length",
                "transfer-encoding",
                "accept-encoding",
                "proxy-authorization",
                "proxy-connection",
                "upgrade",
                "te",
                "trailer",
            }
            seen: set[str] = set()
            for name, value in headers.items():
                lower = name.lower()
                if (
                    lower in seen
                    or lower in forbidden
                    or re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is None
                    or not value
                    or any(ord(c) < 32 or ord(c) == 127 for c in value)
                ):
                    raise ValueError("invalid header")
                seen.add(lower)
            request = httpx.Request(
                "GET",
                url,
                headers={"accept-encoding": "identity", **headers},
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
                        or exact_size is not None
                        and int(size) != exact_size
                    ):
                        raise ValueError("invalid size")
                    content = bytearray()
                    actual = hashlib.sha256()
                    async for chunk in response.aiter_raw():
                        if len(content) + len(chunk) > (
                            exact_size if exact_size is not None else 33554432
                        ):
                            raise ValueError("size exceeded")
                        content.extend(chunk)
                        actual.update(chunk)
                    if (
                        size is not None
                        and len(content) != int(size)
                        or exact_size is not None
                        and len(content) != exact_size
                        or actual.hexdigest() != digest
                        or expiry <= datetime.now(timezone.utc)
                    ):
                        raise ValueError("invalid body")
                    return bytes(content)
                finally:
                    await response.aclose()
        except (httpx.HTTPError, TimeoutError):
            failure = "TRANSFER_UNAVAILABLE"
        except (ValueError, OverflowError, httpx.InvalidURL):
            pass
        finally:
            if request is not None:
                request.headers.clear()
        raise PackageTransferError(failure) from None
