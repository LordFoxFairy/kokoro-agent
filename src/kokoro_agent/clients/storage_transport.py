"""Seven generated Storage RPCs and signed ObjectStore PUT for Agent delivery."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol
from urllib.parse import urlsplit

import httpx
from pyqwest import Client, HTTPTransport

from kokoro_agent.clients.platform_tokens import validate_owner_url
from kokoro_agent.clients.platform_transport import BoundedConnectTransport
from kokoro_agent.clients.storage import StorageClientError
from kokoro_agent.generated.kokoro.storage.v2 import storage_pb as pb
from kokoro_agent.generated.kokoro.storage.v2.storage_connect import (
    StorageServiceClient,
)


class DeliveryTransport(Protocol):
    async def call(
        self, method: str, request: object, headers: dict[str, str]
    ) -> object: ...

    async def put(self, reference: pb.TransferReference, content: bytes) -> None: ...


class StorageDeliveryTransport:
    """Process-owned generated Connect client and isolated signed-URL PUT pool."""

    def __init__(
        self,
        base_url: str,
        object_origin: str,
        secret: str,
        *,
        put_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not secret:
            raise ValueError("Storage service credential is required")
        origin = validate_owner_url(base_url)
        self._object_origin = _origin(object_origin, allow_root_path=True)
        self._secret = secret
        pool = HTTPTransport(
            tls_include_system_certs=True,
            follow_redirects=False,
            connect_timeout=3,
            read_timeout=15,
            enable_otel=False,
        )
        self._pool = pool
        self._client = StorageServiceClient(
            origin,
            http_client=Client(BoundedConnectTransport(pool)),
            read_max_bytes=1_048_576,
            timeout_ms=15_000,
            send_compression=None,
        )
        self._put_client = httpx.AsyncClient(
            trust_env=False,
            follow_redirects=False,
            timeout=30,
            transport=put_transport,
        )

    async def __aenter__(self) -> StorageDeliveryTransport:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        try:
            await self._client.close()
        finally:
            try:
                await self._pool.aclose()
            finally:
                await self._put_client.aclose()

    async def call(
        self, method: str, request: object, headers: dict[str, str]
    ) -> object:
        # Only the seven Agent-delivery RPCs are reachable through this dispatch.
        metadata = {
            **headers,
            "x-kokoro-service": "kokoro-agent",
            "x-kokoro-internal-secret": self._secret,
        }
        if method == "create_upload" and isinstance(request, pb.CreateUploadRequest):
            return await self._client.create_upload(request, headers=metadata)
        if method == "complete_upload" and isinstance(
            request, pb.CompleteUploadRequest
        ):
            return await self._client.complete_upload(request, headers=metadata)
        if method == "abort_upload" and isinstance(request, pb.AbortUploadRequest):
            return await self._client.abort_upload(request, headers=metadata)
        if method == "get_upload_status" and isinstance(
            request, pb.GetUploadStatusRequest
        ):
            return await self._client.get_upload_status(request, headers=metadata)
        if method == "get_scan_status" and isinstance(request, pb.GetScanStatusRequest):
            return await self._client.get_scan_status(request, headers=metadata)
        if method == "create_artifact" and isinstance(
            request, pb.CreateArtifactRequest
        ):
            return await self._client.create_artifact(request, headers=metadata)
        if method == "finalize_artifact" and isinstance(
            request, pb.FinalizeArtifactRequest
        ):
            return await self._client.finalize_artifact(request, headers=metadata)
        raise StorageClientError("STORAGE_OPERATION_FORBIDDEN")

    async def put(self, reference: pb.TransferReference, content: bytes) -> None:
        try:
            transfer_origin = _origin(reference.url, allow_root_path=False)
        except ValueError as exc:
            raise StorageClientError("STORAGE_UPLOAD_REFERENCE_INVALID") from exc
        expiry = reference.expires_at
        now = datetime.now(timezone.utc)
        if (
            transfer_origin != self._object_origin
            or reference.method != "PUT"
            or expiry is None
            or expiry.to_datetime() <= now
            or expiry.to_datetime() > now + timedelta(seconds=901)
            or reference.required_headers.keys() != {"content-type"}
            or not reference.required_headers.get("content-type")
            or "\r" in reference.required_headers["content-type"]
            or "\n" in reference.required_headers["content-type"]
        ):
            raise StorageClientError("STORAGE_UPLOAD_REFERENCE_INVALID")
        async with self._put_client.stream(
            "PUT",
            reference.url,
            headers=dict(reference.required_headers),
            content=content,
        ) as response:
            if response.status_code // 100 != 2:
                raise StorageClientError("STORAGE_UPLOAD_PUT_FAILED", retryable=True)


def _origin(value: str, *, allow_root_path: bool) -> tuple[str, str, int]:
    if "\\" in value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("invalid object origin")
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid object origin") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or (allow_root_path and (parsed.path not in {"", "/"} or parsed.query))
    ):
        raise ValueError("invalid object origin")
    return (
        parsed.scheme,
        parsed.hostname,
        port or (443 if parsed.scheme == "https" else 80),
    )
