"""One claimed Agent Run publishes an immutable workspace file as a Storage Artifact.

Storage owns every Upload/Asset/Artifact fact. This client retains only the Agent
tool-call intent and validates the persisted Run/lease before every owner call.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
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
    InputResourceReadReceipt,
    InputResourceReadRequest,
    StorageClientError,
)
from kokoro_agent.domain.chat.projection import safe_delivery_display_path
from kokoro_agent.domain.run.models import LeaseFence, ToolJournalRecord
from kokoro_agent.domain.run.workspace_output import (
    MAX_OUTPUT_BYTES,
    output_path_digest,
    workspace_output_mime,
)
from kokoro_agent.generated.kokoro.common.v1.common_pb import CommandIdentity
from kokoro_agent.generated.kokoro.storage.v3 import storage_pb as pb
from kokoro_agent.clients.storage_transport import DeliveryTransport
from kokoro_agent.protocol import ArtifactKind, RunRequest
from kokoro_agent.protocol.control import StorageResourceRef

_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])
_Response = TypeVar("_Response")
_BASE_INTENT_KEYS = frozenset(
    {
        "version",
        "run_id",
        "session_id",
        "tenant_ref",
        "subject_ref",
        "tool_call_id",
        "path",
        "title",
        "note",
        "mime_type",
        "size_bytes",
        "content_sha256",
    }
)
_ADMITTED_INTENT_KEYS = _BASE_INTENT_KEYS | {"upload_id"}
_SELECTED_INTENT_KEYS = _ADMITTED_INTENT_KEYS | {
    "source_version_id",
    "complete_command_id",
    "complete_request_digest",
}
_ARTIFACT_KIND_FROM_OWNER: dict[int, ArtifactKind] = {
    int(pb.ArtifactKind.DOCUMENT): "document",
    int(pb.ArtifactKind.CODE): "code",
    int(pb.ArtifactKind.IMAGE): "image",
    int(pb.ArtifactKind.AUDIO): "audio",
    int(pb.ArtifactKind.VIDEO): "video",
    int(pb.ArtifactKind.DATA): "data",
    int(pb.ArtifactKind.ARCHIVE): "archive",
    int(pb.ArtifactKind.OTHER): "other",
}


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
        command_id=_digest(["delivery-v2", seed, stage]),
        request_digest=_digest(["delivery-v2", stage, fields]),
    )


def _intent_stage(value: Mapping[str, object]) -> tuple[str, str | None]:
    keys = frozenset(value) - {"display_path"}
    if type(value.get("version")) is not int or value["version"] != 2:
        raise StorageClientError("DELIVERY_INTENT_VERSION_UNSUPPORTED")
    display_path = value.get("display_path")
    if "display_path" in value and (
        not isinstance(display_path, str)
        or display_path != value.get("path")
        or not safe_delivery_display_path(display_path)
    ):
        raise StorageClientError("DELIVERY_INTENT_INVALID")
    if not isinstance(display_path, str):
        display_path = None
    if keys == _BASE_INTENT_KEYS:
        return "frozen", display_path
    if keys == _ADMITTED_INTENT_KEYS:
        return "admitted", display_path
    if keys == _SELECTED_INTENT_KEYS:
        return "selected", display_path
    raise StorageClientError("DELIVERY_INTENT_INVALID")


def _source_version(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or value == "null"
        or not value.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise StorageClientError("DELIVERY_INTENT_INVALID")
    return value


def _owner_source_version(status: pb.GetUploadStatusResponse) -> str | None:
    if not status.has_field("source_version_id"):
        return None
    try:
        return _source_version(status.source_version_id)
    except StorageClientError as exc:
        raise StorageClientError("DELIVERY_SOURCE_VERSION_CONFLICT") from exc


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
    if top == "text" or mime in {
        "application/pdf",
        "application/rtf",
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.oasis.opendocument.text",
        "application/vnd.ms-powerpoint",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }:
        return pb.ArtifactKind.DOCUMENT
    return pb.ArtifactKind.OTHER


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
        stage, display_path = _intent_stage(frozen)
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
            "version": 2,
            "run_id": run.run_id,
            "session_id": run.session_id,
            "tenant_ref": run.execution_identity.tenant_ref,
            "subject_ref": run.execution_identity.subject.opaque_ref,
            "tool_call_id": request.tool_call_id,
            "path": request.path,
            "title": request.title,
            "note": request.note,
        }
        if any(
            key not in frozen or frozen[key] != value for key, value in expected.items()
        ):
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
        ):
            raise StorageClientError("DELIVERY_INTENT_INVALID")
        if stage == "frozen":
            return None
        if not isinstance(upload_id, str) or not upload_id:
            raise StorageClientError("DELIVERY_INTENT_INVALID")
        source_version_id = (
            _source_version(frozen.get("source_version_id"))
            if stage == "selected"
            else None
        )
        seed = _digest([request.run_id, request.tool_call_id])
        complete_command = (
            _command(
                seed,
                "complete_upload",
                [upload_id, digest, size, source_version_id],
            )
            if source_version_id is not None
            else None
        )
        if complete_command is not None and (
            frozen.get("complete_command_id") != complete_command.command_id
            or frozen.get("complete_request_digest") != complete_command.request_digest
        ):
            raise StorageClientError("DELIVERY_INTENT_INVALID")
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
        owner_source_version = _owner_source_version(status)
        if source_version_id is None:
            if owner_source_version is not None:
                raise StorageClientError("DELIVERY_SOURCE_VERSION_CONFLICT")
        elif (
            owner_source_version is not None
            and owner_source_version != source_version_id
        ):
            raise StorageClientError("DELIVERY_SOURCE_VERSION_CONFLICT")
        if status.state == pb.UploadState.PENDING:
            if source_version_id is None or complete_command is None:
                return None
            completed = _expect(
                await self._call(
                    owner_request,
                    "complete_upload",
                    pb.CompleteUploadRequest(
                        command=complete_command,
                        upload_id=upload_id,
                        content_sha256=digest,
                        size_bytes=size,
                        source_version_id=source_version_id,
                    ),
                ),
                pb.CompleteUploadResponse,
            )
            if completed.upload_id != upload_id or not completed.asset_id:
                raise StorageClientError("DELIVERY_COMPLETION_MISMATCH")
            asset_id = completed.asset_id
        elif (
            status.state != pb.UploadState.COMPLETED
            or source_version_id is None
            or owner_source_version != source_version_id
            or not status.asset_id
        ):
            raise StorageClientError("DELIVERY_UPLOAD_NOT_COMPLETED")
        else:
            asset_id = status.asset_id
        await self._require_clean(owner_request, asset_id)
        return await self._finalize(
            owner_request, asset_id=asset_id, size_bytes=size, display_path=display_path
        )

    async def _call(
        self, request: DeliveryRequest, method: str, message: object
    ) -> object:
        run = await self._canonical(request)
        return await self._owner_call(run, request.request_id, method, message)

    async def _owner_call(
        self, run: RunRequest, request_id: str, method: str, message: object
    ) -> object:
        headers = {
            "x-kokoro-tenant-id": run.execution_identity.tenant_ref,
            "x-kokoro-subject-id": run.execution_identity.subject.opaque_ref,
            "x-kokoro-request-id": request_id,
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

    async def upload_workspace_output(
        self,
        run: RunRequest,
        lease: LeaseFence,
        version_id: str,
        path: str,
        content_sha256: str,
        content: bytes,
        selected: tuple[str, str] | None,
        freeze: Callable[[str, str], Awaitable[bool]],
    ) -> str:
        """Complete one exact process Asset; never create an Artifact."""
        if (
            output_path_digest(path) is None
            or len(content) > MAX_OUTPUT_BYTES
            or hashlib.sha256(content).hexdigest() != content_sha256
            or run.execution_identity.subject.kind != "user"
            or run.feature_key != "chat"
            or await self._runs.get_request(run.run_id) != run
        ):
            raise StorageClientError("WORKSPACE_OUTPUT_BINDING_INVALID")

        async def current() -> None:
            if not await self._runs.is_lease_current(run.run_id, lease):
                raise StorageClientError("WORKSPACE_OUTPUT_LEASE_STALE")

        request_id = _digest(["workspace-output:v1", run.run_id, version_id])
        filename, mime = PurePosixPath(path).name, workspace_output_mime(path)
        await current()
        if selected is None:
            created = _expect(
                await self._owner_call(
                    run,
                    request_id,
                    "create_upload",
                    pb.CreateUploadRequest(
                        command=_command(
                            request_id,
                            "create_upload",
                            [
                                filename,
                                mime,
                                len(content),
                                content_sha256,
                                int(pb.UploadPurpose.WORKSPACE_OUTPUT),
                            ],
                        ),
                        filename=filename,
                        mime_type=mime,
                        size_bytes=len(content),
                        content_sha256=content_sha256,
                        upload_purpose=pb.UploadPurpose.WORKSPACE_OUTPUT,
                    ),
                ),
                pb.CreateUploadResponse,
            )
            if not created.upload_id or created.upload_reference is None:
                raise StorageClientError("WORKSPACE_OUTPUT_UPLOAD_MISMATCH")
            if created.upload_reference.required_headers.get("content-type") != mime:
                raise StorageClientError("WORKSPACE_OUTPUT_UPLOAD_MISMATCH")
            await current()
            try:
                source_version = await self._transport.put(
                    created.upload_reference, content
                )
            except (httpx.HTTPError, TimeoutError) as exc:
                raise StorageClientError(
                    "STORAGE_UPLOAD_PUT_FAILED", retryable=True
                ) from exc
            selected = (created.upload_id, _source_version(source_version))
            if not await freeze(*selected):
                raise StorageClientError("WORKSPACE_OUTPUT_LEASE_STALE")
        upload_id, source_version = selected
        await current()
        completed = _expect(
            await self._owner_call(
                run,
                request_id,
                "complete_upload",
                pb.CompleteUploadRequest(
                    command=_command(
                        request_id,
                        "complete_upload",
                        [upload_id, content_sha256, len(content), source_version],
                    ),
                    upload_id=upload_id,
                    content_sha256=content_sha256,
                    size_bytes=len(content),
                    source_version_id=source_version,
                ),
            ),
            pb.CompleteUploadResponse,
        )
        if (
            completed.upload_id != upload_id
            or not completed.asset_id
            or completed.scan_state != pb.ScanState.CLEAN
        ):
            raise StorageClientError("WORKSPACE_OUTPUT_NOT_CLEAN")
        await current()
        return completed.asset_id

    async def _read_canonical(
        self, request: InputResourceReadRequest
    ) -> tuple[RunRequest, StorageResourceRef]:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError
        run = await self._runs.get_request(request.run_id)
        if (
            run is None
            or run.run_id != request.run_id
            or run.execution_identity != request.identity
        ):
            raise StorageClientError("INPUT_RUN_MISMATCH")
        if run.execution_identity.subject.kind != "user":
            raise StorageClientError("INPUT_READ_CONTEXT_INVALID")
        reference = next(
            (
                ref
                for ref in run.selected_resource_refs
                if ref.kind != "final_answer" and ref == request.reference
            ),
            None,
        )
        if reference is None:
            raise StorageClientError("INPUT_RESOURCE_NOT_SELECTED")
        if not await self._runs.is_lease_current(request.run_id, request.lease):
            raise StorageClientError("INPUT_LEASE_STALE")
        return run, reference

    async def _read_call(
        self,
        request: InputResourceReadRequest,
        run: RunRequest,
        reference: StorageResourceRef,
        method: str,
        message: object,
    ) -> object:
        headers = {
            "x-kokoro-tenant-id": run.execution_identity.tenant_ref,
            "x-kokoro-subject-id": run.execution_identity.subject.opaque_ref,
            "x-kokoro-request-id": request.request_id,
            "x-kokoro-scope-kind": reference.source_scope.kind,
            "x-kokoro-scope-id": reference.source_scope.id,
        }
        try:
            return await self._transport.call(method, message, headers)
        except ConnectError as exc:
            if exc.code == Code.CANCELED:
                raise asyncio.CancelledError from None
            raise StorageClientError(
                f"STORAGE_{method.upper()}_{exc.code.name}",
                retryable=exc.code
                in {
                    Code.UNKNOWN,
                    Code.DEADLINE_EXCEEDED,
                    Code.RESOURCE_EXHAUSTED,
                    Code.ABORTED,
                    Code.INTERNAL,
                    Code.UNAVAILABLE,
                },
            ) from None

    async def read_input(
        self, request: InputResourceReadRequest
    ) -> InputResourceReadReceipt:
        """Read only the exact frozen input, rechecking its consumer lease after I/O."""
        run, reference = await self._read_canonical(request)
        manifest = reference.model_dump(mode="json")
        seed = _digest([run.run_id, "selected-input", manifest])
        if reference.kind == "artifact":
            method = "get_final_artifact_download_reference"
            message = pb.GetFinalArtifactDownloadReferenceRequest(
                command=_command(seed, method, [run.run_id, manifest]),
                artifact_id=reference.resource_id,
                asset_id=reference.asset_id,
                content_sha256=reference.content_sha256,
                run_id=reference.source_run_id,
                consumer_run_id=run.run_id,
            )
        else:
            method = "get_download_reference"
            message = pb.GetDownloadReferenceRequest(
                command=_command(seed, method, [run.run_id, manifest]),
                asset_id=reference.asset_id,
                content_sha256=reference.content_sha256,
                consumer_run_id=run.run_id,
            )
        response = await self._read_call(request, run, reference, method, message)
        await self._read_canonical(request)
        if reference.kind == "artifact":
            final = _expect(response, pb.GetFinalArtifactDownloadReferenceResponse)
            if final.artifact_id != reference.resource_id:
                raise StorageClientError("INPUT_READ_RESPONSE_MISMATCH")
            response = final
        else:
            asset = _expect(response, pb.GetDownloadReferenceResponse)
            if (
                asset.size_bytes != int(reference.size_bytes)
                or asset.mime_type != reference.mime_type
                or asset.scan_state != pb.ScanState.CLEAN
            ):
                raise StorageClientError("INPUT_READ_RESPONSE_MISMATCH")
            response = asset
        if (
            response.asset_id != reference.asset_id
            or response.content_sha256 != reference.content_sha256
            or response.download_reference is None
        ):
            raise StorageClientError("INPUT_READ_RESPONSE_MISMATCH")
        try:
            content = await self._transport.get(response.download_reference)
        except (httpx.HTTPError, TimeoutError) as exc:
            raise StorageClientError(
                "STORAGE_DOWNLOAD_GET_FAILED", retryable=True
            ) from exc
        await self._read_canonical(request)
        if (
            len(content) != int(reference.size_bytes)
            or hashlib.sha256(content).hexdigest() != reference.content_sha256
        ):
            raise StorageClientError("INPUT_READ_CONTENT_MISMATCH")
        return InputResourceReadReceipt(
            resource_id=reference.resource_id,
            asset_id=reference.asset_id,
            content_sha256=reference.content_sha256,
            content=content,
        )

    async def _put(
        self, request: DeliveryRequest, reference: pb.TransferReference
    ) -> str:
        await self._canonical(request)
        try:
            return await self._transport.put(reference, request.content)
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
            raise StorageClientError("DELIVERY_INTENT_NOT_DURABLE", retryable=True)

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
            "version": 2,
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
                _intent_stage(recorded)
                if any(
                    key not in recorded or recorded[key] != value
                    for key, value in frozen.items()
                ):
                    raise StorageClientError("DELIVERY_INTENT_MISMATCH")
                frozen.update(recorded)
        if (prior is None or not prior.result) and request.display_path is not None:
            frozen["display_path"] = request.display_path
        stage, display_path = _intent_stage(frozen)
        await self._intent(request, frozen)

        upload_id_value = frozen.get("upload_id")
        if upload_id_value is not None and not isinstance(upload_id_value, str):
            raise StorageClientError("DELIVERY_INTENT_INVALID")
        upload_id = upload_id_value
        source_version_id = (
            _source_version(frozen.get("source_version_id"))
            if stage == "selected"
            else None
        )
        complete_command = (
            _command(
                seed,
                "complete_upload",
                [
                    upload_id,
                    request.content_sha256,
                    len(request.content),
                    source_version_id,
                ],
            )
            if upload_id is not None and source_version_id is not None
            else None
        )
        if complete_command is not None and (
            frozen.get("complete_command_id") != complete_command.command_id
            or frozen.get("complete_request_digest") != complete_command.request_digest
        ):
            raise StorageClientError("DELIVERY_INTENT_INVALID")
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
                status.upload_id != upload_id
                or status.expected_sha256 != request.content_sha256
                or status.expected_size_bytes != len(request.content)
                or status.mime_type != request.mime_type
            ):
                raise StorageClientError("DELIVERY_UPLOAD_MISMATCH")
            owner_source_version = _owner_source_version(status)
            if source_version_id is None:
                if owner_source_version is not None:
                    raise StorageClientError("DELIVERY_SOURCE_VERSION_CONFLICT")
            elif (
                owner_source_version is not None
                and owner_source_version != source_version_id
            ):
                raise StorageClientError("DELIVERY_SOURCE_VERSION_CONFLICT")
            if status.state == pb.UploadState.COMPLETED:
                if (
                    source_version_id is None
                    or owner_source_version != source_version_id
                ):
                    raise StorageClientError("DELIVERY_SOURCE_VERSION_CONFLICT")
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
        if asset_id is None and source_version_id is None:
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
                source_version_id = await self._put(request, created.upload_reference)
            except StorageClientError as exc:
                if exc.code == "STORAGE_UPLOAD_REFERENCE_INVALID":
                    await self._abort_unstarted_upload(request, seed, upload_id)
                raise
            complete_command = _command(
                seed,
                "complete_upload",
                [
                    upload_id,
                    request.content_sha256,
                    len(request.content),
                    source_version_id,
                ],
            )
            frozen.update(
                {
                    "source_version_id": source_version_id,
                    "complete_command_id": complete_command.command_id,
                    "complete_request_digest": complete_command.request_digest,
                }
            )
            await self._intent(request, frozen)
        if asset_id is None:
            if (
                upload_id is None
                or source_version_id is None
                or complete_command is None
            ):
                raise StorageClientError("DELIVERY_INTENT_INVALID")
            completed = _expect(
                await self._call(
                    request,
                    "complete_upload",
                    pb.CompleteUploadRequest(
                        command=complete_command,
                        upload_id=upload_id,
                        content_sha256=request.content_sha256,
                        size_bytes=len(request.content),
                        source_version_id=source_version_id,
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
            request,
            asset_id=asset_id,
            size_bytes=len(request.content),
            display_path=display_path,
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
        self,
        request: DeliveryRequest,
        *,
        asset_id: str,
        size_bytes: int,
        display_path: str | None,
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
        artifact_kind = _ARTIFACT_KIND_FROM_OWNER.get(int(artifact.kind))
        if (
            not artifact.artifact_id
            or artifact.asset_id != asset_id
            or artifact.content_sha256 != request.content_sha256
            or artifact.kind != kind
            or artifact_kind is None
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
            artifact_kind=artifact_kind,
            content_sha256=finalized.content_sha256,
            size_bytes=size_bytes,
            mime_type=request.mime_type,
            replayed=finalized.replayed,
            display_path=display_path,
        )
