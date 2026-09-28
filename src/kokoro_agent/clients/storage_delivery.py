"""One claimed Agent Run publishes an immutable workspace file as a Storage Artifact.

Storage owns every Upload/Asset/Artifact fact. This client retains only the Agent
tool-call intent and validates the persisted Run/lease before every owner call.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Protocol, TypeVar

import httpx
from connectrpc.errors import ConnectError
from connectrpc.code import Code
from pydantic import JsonValue, TypeAdapter, ValidationError

from kokoro_agent.clients.storage import (
    DeliveryReceipt,
    DeliveryRecoveryRequest,
    DeliveryRequest,
    StorageClientError,
)
from kokoro_agent.domain.run.models import LeaseFence, ToolJournalRecord
from kokoro_agent.generated.kokoro.common.v1.common_pb import CommandIdentity
from kokoro_agent.generated.kokoro.storage.v2 import storage_pb as pb
from kokoro_agent.clients.storage_transport import DeliveryTransport
from kokoro_agent.protocol import RunRequest

_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])
_Response = TypeVar("_Response")


def _expect(value: object, expected: type[_Response]) -> _Response:
    if not isinstance(value, expected):
        raise StorageClientError("STORAGE_RESPONSE_TYPE_INVALID")
    return value


class DeliveryRuns(Protocol):
    async def get_request(self, run_id: str) -> RunRequest | None: ...

    async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool: ...

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None: ...

    async def journal_delivery_intent(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, intent: str
    ) -> bool: ...


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _command(seed: str, stage: str, fields: object) -> CommandIdentity:
    return CommandIdentity(
        command_id=_digest(["delivery", seed, stage]),
        request_digest=_digest(["delivery-v1", stage, fields]),
    )


def _artifact_kind(mime: str) -> pb.ArtifactKind:
    top = mime.partition("/")[0]
    if top == "image":
        return pb.ArtifactKind.IMAGE
    if top == "audio":
        return pb.ArtifactKind.AUDIO
    if top == "video":
        return pb.ArtifactKind.VIDEO
    if mime in {"application/zip", "application/x-tar", "application/gzip"}:
        return pb.ArtifactKind.ARCHIVE
    if mime in {"application/json", "text/csv"}:
        return pb.ArtifactKind.DATA
    if mime.startswith("text/x-") or mime in {
        "text/javascript",
        "application/javascript",
    }:
        return pb.ArtifactKind.CODE
    return pb.ArtifactKind.DOCUMENT


class StorageDeliveryClient:
    """Run-fenced, replayable seven-RPC Artifact delivery orchestration."""

    def __init__(self, runs: DeliveryRuns, transport: DeliveryTransport) -> None:
        self._runs = runs
        self._transport = transport

    async def _canonical(self, request: DeliveryRequest) -> RunRequest:
        lease = request.lease
        if lease is None or not await self._runs.is_lease_current(
            request.run_id, lease
        ):
            raise StorageClientError("DELIVERY_LEASE_STALE")
        run = await self._runs.get_request(request.run_id)
        if run is None or run.execution_identity != request.identity:
            raise StorageClientError("DELIVERY_RUN_MISMATCH")
        identity = run.execution_identity
        if identity.subject.kind != "user" or not identity.subject.opaque_ref:
            raise StorageClientError("DELIVERY_SUBJECT_INVALID")
        if not run.session_id:
            raise StorageClientError("DELIVERY_SCOPE_INVALID")
        return run

    async def recover(self, request: DeliveryRecoveryRequest) -> DeliveryReceipt | None:
        """Replay a completed owner upload without reopening the workspace file.

        The journal is the only source of file metadata here. In particular a
        missing file must never make us invent a new Upload/Artifact command.
        """
        if request.lease is None or not request.tool_call_id:
            raise StorageClientError("DELIVERY_TOOL_CONTEXT_MISSING")
        recorded = await self._runs.get_tool_journal(
            request.run_id, request.tool_call_id
        )
        if recorded is None:
            return None
        if recorded.name != "deliver" or recorded.status != "started":
            raise StorageClientError("DELIVERY_JOURNAL_STATE_INVALID")
        if not recorded.result:
            return None
        try:
            frozen = _JSON_OBJECT.validate_json(recorded.result)
        except ValidationError as exc:
            raise StorageClientError("DELIVERY_INTENT_INVALID") from exc
        run = await self._canonical(
            DeliveryRequest(
                request_id=request.request_id,
                run_id=request.run_id,
                namespace="",
                identity=request.identity,
                path=request.path,
                title=request.title,
                note=request.note,
                mime_type="application/octet-stream",
                content_sha256="",
                content=b"",
                lease=request.lease,
                tool_call_id=request.tool_call_id,
            )
        )
        expected = {
            "version": 1,
            "run_id": run.run_id,
            "session_id": run.session_id,
            "tenant_ref": run.execution_identity.tenant_ref,
            "subject_ref": run.execution_identity.subject.opaque_ref,
            "tool_call_id": request.tool_call_id,
            "path": request.path,
            "title": request.title,
            "note": request.note,
        }
        if any(frozen.get(key) != value for key, value in expected.items()):
            raise StorageClientError("DELIVERY_INTENT_MISMATCH")
        digest = frozen.get("content_sha256")
        size = frozen.get("size_bytes")
        mime = frozen.get("mime_type")
        upload_id = frozen.get("upload_id")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            or type(size) is not int
            or size < 0
            or not isinstance(mime, str)
            or not mime
            or (upload_id is not None and not isinstance(upload_id, str))
        ):
            raise StorageClientError("DELIVERY_INTENT_INVALID")
        if not upload_id:
            return None
        owner_request = DeliveryRequest(
            request_id=request.request_id,
            run_id=request.run_id,
            namespace="",
            identity=request.identity,
            path=request.path,
            title=request.title,
            note=request.note,
            mime_type=mime,
            content_sha256=digest,
            content=b"",
            lease=request.lease,
            tool_call_id=request.tool_call_id,
        )
        status = _expect(
            await self._call(
                owner_request,
                "get_upload_status",
                pb.GetUploadStatusRequest(upload_id=upload_id),
            ),
            pb.GetUploadStatusResponse,
        )
        if (
            status.upload_id != upload_id
            or status.expected_sha256 != digest
            or status.expected_size_bytes != size
            or status.mime_type != mime
        ):
            raise StorageClientError("DELIVERY_UPLOAD_MISMATCH")
        if status.state == pb.UploadState.PENDING:
            return None
        if status.state != pb.UploadState.COMPLETED or not status.asset_id:
            raise StorageClientError("DELIVERY_UPLOAD_NOT_COMPLETED")
        await self._require_clean(owner_request, status.asset_id)
        return await self._finalize(
            owner_request, asset_id=status.asset_id, size_bytes=size
        )

    async def _call(
        self, request: DeliveryRequest, method: str, message: object
    ) -> object:
        run = await self._canonical(request)
        headers = {
            "x-kokoro-tenant-id": run.execution_identity.tenant_ref,
            "x-kokoro-subject-id": run.execution_identity.subject.opaque_ref,
            "x-kokoro-request-id": request.request_id,
            "x-kokoro-scope-kind": "conversation",
            "x-kokoro-scope-id": run.session_id,
        }
        try:
            return await self._transport.call(method, message, headers)
        except ConnectError as exc:
            if exc.code == Code.CANCELED:
                raise asyncio.CancelledError from None
            retryable = exc.code in {
                Code.UNKNOWN,
                Code.DEADLINE_EXCEEDED,
                Code.RESOURCE_EXHAUSTED,
                Code.ABORTED,
                Code.INTERNAL,
                Code.UNAVAILABLE,
            }
            raise StorageClientError(
                f"STORAGE_{method.upper()}_{exc.code.name}", retryable=retryable
            ) from None

    async def _put(
        self, request: DeliveryRequest, reference: pb.TransferReference
    ) -> None:
        await self._canonical(request)
        try:
            await self._transport.put(reference, request.content)
        except (httpx.HTTPError, TimeoutError) as exc:
            raise StorageClientError(
                "STORAGE_UPLOAD_PUT_FAILED", retryable=True
            ) from exc

    async def _intent(
        self, request: DeliveryRequest, value: Mapping[str, object]
    ) -> None:
        assert request.lease is not None and request.tool_call_id is not None
        if not await self._runs.journal_delivery_intent(
            request.run_id,
            request.lease,
            request.tool_call_id,
            json.dumps(value, sort_keys=True, separators=(",", ":")),
        ):
            raise StorageClientError("DELIVERY_INTENT_NOT_DURABLE")

    async def _abort_unstarted_upload(
        self, request: DeliveryRequest, seed: str, upload_id: str
    ) -> None:
        """Abort only a proven pending upload before this attempt has sent bytes."""
        status = _expect(
            await self._call(
                request,
                "get_upload_status",
                pb.GetUploadStatusRequest(upload_id=upload_id),
            ),
            pb.GetUploadStatusResponse,
        )
        if (
            status.upload_id != upload_id
            or status.expected_sha256 != request.content_sha256
            or status.expected_size_bytes != len(request.content)
            or status.mime_type != request.mime_type
        ):
            raise StorageClientError("DELIVERY_UPLOAD_MISMATCH")
        if status.state != pb.UploadState.PENDING:
            return
        aborted = _expect(
            await self._call(
                request,
                "abort_upload",
                pb.AbortUploadRequest(
                    command=_command(
                        seed, "abort_upload", [upload_id, "invalid_upload_reference"]
                    ),
                    upload_id=upload_id,
                    reason="invalid_upload_reference",
                ),
            ),
            pb.AbortUploadResponse,
        )
        if aborted.upload_id != upload_id or aborted.state != pb.UploadState.ABORTED:
            raise StorageClientError("DELIVERY_ABORT_MISMATCH")

    async def publish(self, request: DeliveryRequest) -> DeliveryReceipt:
        run = await self._canonical(request)
        if not request.tool_call_id or not request.lease:
            raise StorageClientError("DELIVERY_TOOL_CONTEXT_MISSING")
        if hashlib.sha256(request.content).hexdigest() != request.content_sha256:
            raise StorageClientError("DELIVERY_BYTES_MISMATCH")
        if (
            not request.title
            or not request.mime_type
            or not request.path.startswith("/")
        ):
            raise StorageClientError("DELIVERY_METADATA_INVALID")
        seed = _digest([request.run_id, request.tool_call_id])
        frozen: dict[str, object] = {
            "version": 1,
            "run_id": run.run_id,
            "session_id": run.session_id,
            "tenant_ref": run.execution_identity.tenant_ref,
            "subject_ref": run.execution_identity.subject.opaque_ref,
            "tool_call_id": request.tool_call_id,
            "path": request.path,
            "title": request.title,
            "note": request.note,
            "mime_type": request.mime_type,
            "size_bytes": len(request.content),
            "content_sha256": request.content_sha256,
        }
        prior = await self._runs.get_tool_journal(request.run_id, request.tool_call_id)
        if prior is not None:
            if prior.name != "deliver" or prior.status != "started":
                raise StorageClientError("DELIVERY_JOURNAL_STATE_INVALID")
            if prior.result:
                try:
                    recorded = _JSON_OBJECT.validate_json(prior.result)
                except ValidationError as exc:
                    raise StorageClientError("DELIVERY_INTENT_INVALID") from exc
                if any(recorded.get(key) != value for key, value in frozen.items()):
                    raise StorageClientError("DELIVERY_INTENT_MISMATCH")
                frozen.update(recorded)
        await self._intent(request, frozen)

        upload_id_value = frozen.get("upload_id")
        if upload_id_value is not None and not isinstance(upload_id_value, str):
            raise StorageClientError("DELIVERY_INTENT_INVALID")
        upload_id = upload_id_value
        asset_id: str | None = None
        if upload_id:
            status = _expect(
                await self._call(
                    request,
                    "get_upload_status",
                    pb.GetUploadStatusRequest(upload_id=upload_id),
                ),
                pb.GetUploadStatusResponse,
            )
            if (
                status.expected_sha256 != request.content_sha256
                or status.expected_size_bytes != len(request.content)
                or status.mime_type != request.mime_type
            ):
                raise StorageClientError("DELIVERY_UPLOAD_MISMATCH")
            if status.state == pb.UploadState.COMPLETED:
                asset_id = status.asset_id or None
            elif status.state != pb.UploadState.PENDING:
                raise StorageClientError("DELIVERY_UPLOAD_NOT_PENDING")

        create_command = _command(
            seed,
            "create_upload",
            [
                PurePosixPath(request.path).name,
                request.mime_type,
                len(request.content),
                request.content_sha256,
            ],
        )
        if asset_id is None:
            created = _expect(
                await self._call(
                    request,
                    "create_upload",
                    pb.CreateUploadRequest(
                        command=create_command,
                        filename=PurePosixPath(request.path).name,
                        mime_type=request.mime_type,
                        size_bytes=len(request.content),
                        content_sha256=request.content_sha256,
                        upload_purpose=pb.UploadPurpose.ARTIFACT,
                    ),
                ),
                pb.CreateUploadResponse,
            )
            if upload_id is not None and upload_id != created.upload_id:
                raise StorageClientError("DELIVERY_UPLOAD_ID_MISMATCH")
            upload_id = created.upload_id
            if not upload_id:
                raise StorageClientError("DELIVERY_UPLOAD_ID_MISSING")
            frozen["upload_id"] = upload_id
            await self._intent(request, frozen)
            if created.upload_reference is None:
                await self._abort_unstarted_upload(request, seed, upload_id)
                raise StorageClientError("STORAGE_UPLOAD_REFERENCE_MISSING")
            if (
                created.upload_reference.method != "PUT"
                or not created.upload_reference.url
            ):
                await self._abort_unstarted_upload(request, seed, upload_id)
                raise StorageClientError("STORAGE_UPLOAD_REFERENCE_INVALID")
            try:
                await self._put(request, created.upload_reference)
            except StorageClientError as exc:
                if exc.code == "STORAGE_UPLOAD_REFERENCE_INVALID":
                    await self._abort_unstarted_upload(request, seed, upload_id)
                raise
            completed = _expect(
                await self._call(
                    request,
                    "complete_upload",
                    pb.CompleteUploadRequest(
                        command=_command(
                            seed,
                            "complete_upload",
                            [upload_id, request.content_sha256, len(request.content)],
                        ),
                        upload_id=upload_id,
                        content_sha256=request.content_sha256,
                        size_bytes=len(request.content),
                    ),
                ),
                pb.CompleteUploadResponse,
            )
            if completed.upload_id != upload_id or not completed.asset_id:
                raise StorageClientError("DELIVERY_COMPLETION_MISMATCH")
            asset_id = completed.asset_id
        if not asset_id:
            raise StorageClientError("DELIVERY_ASSET_ID_MISSING")
        await self._require_clean(request, asset_id)

        return await self._finalize(
            request, asset_id=asset_id, size_bytes=len(request.content)
        )

    async def _require_clean(self, request: DeliveryRequest, asset_id: str) -> None:
        scan = _expect(
            await self._call(
                request, "get_scan_status", pb.GetScanStatusRequest(asset_id=asset_id)
            ),
            pb.GetScanStatusResponse,
        )
        for _ in range(10):
            if scan.asset_id != asset_id:
                raise StorageClientError("DELIVERY_SCAN_MISMATCH")
            if scan.state == pb.ScanState.CLEAN:
                break
            if scan.state in {pb.ScanState.INFECTED, pb.ScanState.UNKNOWN}:
                raise StorageClientError("DELIVERY_SCAN_REJECTED")
            await asyncio.sleep(0.5)
            scan = _expect(
                await self._call(
                    request,
                    "get_scan_status",
                    pb.GetScanStatusRequest(asset_id=asset_id),
                ),
                pb.GetScanStatusResponse,
            )
        else:
            raise StorageClientError("DELIVERY_SCAN_PENDING", retryable=True)

    async def _finalize(
        self, request: DeliveryRequest, *, asset_id: str, size_bytes: int
    ) -> DeliveryReceipt:
        run = await self._canonical(request)
        seed = _digest([request.run_id, request.tool_call_id])
        kind = _artifact_kind(request.mime_type)
        artifact = _expect(
            await self._call(
                request,
                "create_artifact",
                pb.CreateArtifactRequest(
                    command=_command(
                        seed,
                        "create_artifact",
                        [
                            asset_id,
                            request.content_sha256,
                            int(kind),
                            request.title,
                            run.run_id,
                        ],
                    ),
                    asset_id=asset_id,
                    artifact_id="",  # Storage derives this from the frozen command_id.
                    content_sha256=request.content_sha256,
                    kind=kind,
                    title=request.title,
                    source_run_id=run.run_id,
                ),
            ),
            pb.CreateArtifactResponse,
        )
        if (
            not artifact.artifact_id
            or artifact.asset_id != asset_id
            or artifact.content_sha256 != request.content_sha256
            or artifact.kind != kind
            or artifact.title != request.title
            or artifact.source_run_id != run.run_id
        ):
            raise StorageClientError("DELIVERY_ARTIFACT_MISMATCH")
        finalized = _expect(
            await self._call(
                request,
                "finalize_artifact",
                pb.FinalizeArtifactRequest(
                    command=_command(
                        seed,
                        "finalize_artifact",
                        [asset_id, artifact.artifact_id, request.content_sha256],
                    ),
                    asset_id=asset_id,
                    artifact_id=artifact.artifact_id,
                    content_sha256=request.content_sha256,
                ),
            ),
            pb.FinalizeArtifactResponse,
        )
        if (
            finalized.state != pb.ArtifactState.FINAL
            or finalized.artifact_id != artifact.artifact_id
            or finalized.asset_id != asset_id
            or finalized.content_sha256 != request.content_sha256
        ):
            raise StorageClientError("DELIVERY_FINAL_MISMATCH")
        return DeliveryReceipt(
            artifact_id=finalized.artifact_id,
            asset_id=finalized.asset_id,
            content_sha256=finalized.content_sha256,
            size_bytes=size_bytes,
            mime_type=request.mime_type,
            replayed=finalized.replayed,
        )
