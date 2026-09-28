"""Generated Connect dispatch and signed PUT do not cross credential boundaries."""

from __future__ import annotations

import httpx
import pytest
from datetime import datetime, timedelta, timezone
from protobuf.wkt import Timestamp

from kokoro_agent.clients.storage import StorageClientError
from kokoro_agent.clients.storage_transport import StorageDeliveryTransport
from kokoro_agent.generated.kokoro.storage.v2 import storage_pb as pb


class _Stub:
    def __init__(self) -> None:
        self.headers: dict[str, str] | None = None

    async def create_upload(
        self, request: pb.CreateUploadRequest, *, headers: dict[str, str]
    ) -> pb.CreateUploadResponse:
        assert request.filename == "report.pdf"
        self.headers = headers
        return pb.CreateUploadResponse(upload_id="upload-1")

    async def close(self) -> None:
        pass


async def test_generated_rpc_gets_only_trusted_worker_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with StorageDeliveryTransport(
        "http://127.0.0.1:4010", "http://127.0.0.1:9000", "secret"
    ) as transport:
        stub = _Stub()
        monkeypatch.setattr(transport, "_client", stub)
        response = await transport.call(
            "create_upload",
            pb.CreateUploadRequest(filename="report.pdf"),
            {"x-kokoro-tenant-id": "tenant-1"},
        )
        assert isinstance(response, pb.CreateUploadResponse)
        assert stub.headers == {
            "x-kokoro-tenant-id": "tenant-1",
            "x-kokoro-service": "kokoro-agent",
            "x-kokoro-internal-secret": "secret",
        }
        with pytest.raises(StorageClientError, match="FORBIDDEN"):
            await transport.call("list_final_artifacts", object(), {})


async def test_signed_put_never_carries_internal_service_secret() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    async with StorageDeliveryTransport(
        "http://127.0.0.1:4010",
        "http://127.0.0.1:9000",
        "secret",
        put_transport=httpx.MockTransport(handler),
    ) as transport:
        await transport.put(
            pb.TransferReference(
                url="http://127.0.0.1:9000/upload-1",
                method="PUT",
                required_headers={"content-type": "application/pdf"},
                expires_at=Timestamp.from_datetime(
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                ),
            ),
            b"final report",
        )
        assert len(seen) == 1
        assert seen[0].content == b"final report"
        assert seen[0].headers["content-type"] == "application/pdf"
        assert "x-kokoro-internal-secret" not in seen[0].headers
        with pytest.raises(StorageClientError, match="INVALID"):
            await transport.put(
                pb.TransferReference(
                    url="http://127.0.0.1:9000/upload-1",
                    method="PUT",
                    required_headers={"x-kokoro-internal-secret": "leak"},
                    expires_at=Timestamp.from_datetime(
                        datetime.now(timezone.utc) + timedelta(minutes=5)
                    ),
                ),
                b"final report",
            )


async def test_signed_put_rejects_cross_origin_expired_and_redirect() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"location": "http://127.0.0.1:1/leak"})

    async with StorageDeliveryTransport(
        "http://127.0.0.1:4010",
        "http://127.0.0.1:9000",
        "secret",
        put_transport=httpx.MockTransport(handler),
    ) as transport:
        for url in (
            "http://127.0.0.1:1/leak",
            "http://user@127.0.0.1:9000/upload",
            "http://127.0.0.1:9000/upload#frag",
        ):
            with pytest.raises(StorageClientError, match="REFERENCE_INVALID"):
                await transport.put(
                    pb.TransferReference(
                        url=url,
                        method="PUT",
                        required_headers={"content-type": "application/pdf"},
                        expires_at=Timestamp.from_datetime(
                            datetime.now(timezone.utc) + timedelta(minutes=5)
                        ),
                    ),
                    b"bytes",
                )
        with pytest.raises(StorageClientError, match="REFERENCE_INVALID"):
            await transport.put(
                pb.TransferReference(
                    url="http://127.0.0.1:9000/upload",
                    method="PUT",
                    required_headers={"content-type": "application/pdf"},
                    expires_at=Timestamp.from_datetime(
                        datetime.now(timezone.utc) - timedelta(seconds=1)
                    ),
                ),
                b"bytes",
            )
        assert seen == []
        with pytest.raises(StorageClientError, match="PUT_FAILED"):
            await transport.put(
                pb.TransferReference(
                    url="http://127.0.0.1:9000/upload",
                    method="PUT",
                    required_headers={"content-type": "application/pdf"},
                    expires_at=Timestamp.from_datetime(
                        datetime.now(timezone.utc) + timedelta(minutes=5)
                    ),
                ),
                b"bytes",
            )
        assert len(seen) == 1
