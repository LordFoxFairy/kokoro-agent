"""Storage delivery uses only the persisted Run and the claimed lease."""

from __future__ import annotations

import hashlib
import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from deepagents.backends.local_shell import LocalShellBackend

from kokoro_agent.clients.storage import (
    DeliveryRecoveryRequest,
    DeliveryRequest,
    StorageClientError,
)
from kokoro_agent.clients.storage_delivery import StorageDeliveryClient
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.domain.run.models import ToolJournalRecord
from kokoro_agent.generated.kokoro.storage.v3 import storage_pb as pb
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.tools.deliver import make_deliver_tool


_IDENTITY = ExecutionIdentity(
    tenant_ref="tenant-1",
    actor=IdentityRef(kind="user", opaque_ref="user-1"),
    subject=IdentityRef(kind="user", opaque_ref="user-1"),
    identity_assertion_ref="assertion-1",
)
_RUN = RunRequest(
    kind="run.request",
    run_id="run-1",
    session_id="conversation-1",
    feature_key="chat",
    selected_skill_source_refs=(),
    execution_identity=_IDENTITY,
    input=RunInput(message_id="msg-1", content="create a report"),
)
_LEASE = LeaseFence(owner="worker-1", generation=1)
_CONTENT = b"final report"
_HASH = hashlib.sha256(_CONTENT).hexdigest()


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class FakeRuns:
    def __init__(self) -> None:
        self.current = True
        self.request = _RUN
        self.intents: list[tuple[str, str, str]] = []
        self.last_intent: str | None = None
        self.reject_selected = False

    async def get_request(self, run_id: str) -> RunRequest | None:
        assert run_id == _RUN.run_id
        return self.request

    async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool:
        assert run_id == _RUN.run_id and lease == _LEASE
        return self.current

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None:
        if self.last_intent is None:
            return None
        return ToolJournalRecord(
            name="deliver", status="started", result=self.last_intent, is_error=False
        )

    async def journal_delivery_intent(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, intent: str
    ) -> bool:
        self.intents.append((run_id, tool_call_id, intent))
        if self.reject_selected and "source_version_id" in json.loads(intent):
            return False
        self.last_intent = intent
        return self.current


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, dict[str, str]]] = []
        self.puts: list[bytes] = []
        self.scan = pb.ScanState.CLEAN
        self.completed = False
        self.fail_artifact_once = False
        self.after_create: Callable[[], None] | None = None
        self.after_put: Callable[[], None] | None = None
        self.bad_reference = False
        self.missing_reference = False
        self.invalid_reference_on_put = False
        self.connect_error: Code | None = None
        self.finalize_reply_lost = False
        self.artifact_kind_override: pb.ArtifactKind | None = None
        self.fail_complete_once = False
        self.pending_source_version: str | None = None
        self.put_version = "version-A"

    async def call(self, method: str, request: object, headers: dict[str, str]):
        self.calls.append((method, request, headers))
        if self.connect_error is not None:
            raise ConnectError(self.connect_error, "upstream message must not escape")
        if method == "create_upload":
            if self.after_create is not None:
                self.after_create()
            return pb.CreateUploadResponse(
                upload_id="upload-1",
                upload_reference=None
                if self.missing_reference
                else pb.TransferReference(
                    url="http://127.0.0.1:9000/upload-1",
                    method="GET" if self.bad_reference else "PUT",
                ),
            )
        if method == "get_upload_status":
            return pb.GetUploadStatusResponse(
                upload_id="upload-1",
                state=pb.UploadState.COMPLETED
                if self.completed
                else pb.UploadState.PENDING,
                expected_sha256=_HASH,
                expected_size_bytes=len(_CONTENT),
                mime_type="application/pdf",
                asset_id="asset-1" if self.completed else None,
                source_version_id=(
                    "version-A" if self.completed else self.pending_source_version
                ),
            )
        if method == "complete_upload":
            assert isinstance(request, pb.CompleteUploadRequest)
            assert request.source_version_id == "version-A"
            if self.fail_complete_once:
                self.fail_complete_once = False
                raise StorageClientError("network", retryable=True)
            self.completed = True
            return pb.CompleteUploadResponse(
                upload_id="upload-1", asset_id="asset-1", scan_state=self.scan
            )
        if method == "get_scan_status":
            return pb.GetScanStatusResponse(asset_id="asset-1", state=self.scan)
        if method == "abort_upload":
            return pb.AbortUploadResponse(
                upload_id="upload-1", state=pb.UploadState.ABORTED
            )
        if method == "create_artifact":
            assert isinstance(request, pb.CreateArtifactRequest)
            if self.fail_artifact_once:
                self.fail_artifact_once = False
                raise StorageClientError("network", retryable=True)
            return pb.CreateArtifactResponse(
                artifact_id="artifact-1",
                asset_id="asset-1",
                content_sha256=_HASH,
                state=pb.ArtifactState.DRAFT,
                kind=request.kind
                if self.artifact_kind_override is None
                else self.artifact_kind_override,
                title="Report",
                source_run_id="run-1",
            )
        if method == "finalize_artifact":
            if self.finalize_reply_lost:
                self.finalize_reply_lost = False
                raise ConnectError(Code.UNAVAILABLE, "reply lost after owner final")
            return pb.FinalizeArtifactResponse(
                artifact_id="artifact-1",
                asset_id="asset-1",
                content_sha256=_HASH,
                state=pb.ArtifactState.FINAL,
            )
        raise AssertionError(method)

    async def put(self, reference: pb.TransferReference, content: bytes) -> str:
        assert reference.method == "PUT"
        if self.invalid_reference_on_put:
            raise StorageClientError("STORAGE_UPLOAD_REFERENCE_INVALID")
        self.puts.append(content)
        if self.after_put is not None:
            self.after_put()
        return self.put_version


def _request() -> DeliveryRequest:
    return DeliveryRequest(
        request_id="trace-1",
        run_id="run-1",
        namespace="untrusted-namespace",
        identity=_IDENTITY,
        path="/report.pdf",
        title="Report",
        note="",
        mime_type="application/pdf",
        content_sha256=_HASH,
        content=_CONTENT,
        lease=_LEASE,
        tool_call_id="tool-1",
    )


async def test_publish_uses_canonical_conversation_scope_and_stable_commands() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    receipt = await StorageDeliveryClient(runs, transport).publish(_request())

    assert receipt.artifact_id == "artifact-1"
    assert receipt.asset_id == "asset-1"
    assert receipt.artifact_kind == "document"
    assert transport.puts == [_CONTENT]
    assert runs.intents and runs.intents[0][:2] == ("run-1", "tool-1")
    assert [name for name, _, _ in transport.calls] == [
        "create_upload",
        "complete_upload",
        "get_scan_status",
        "create_artifact",
        "finalize_artifact",
    ]
    for _, _, headers in transport.calls:
        assert headers["x-kokoro-tenant-id"] == "tenant-1"
        assert headers["x-kokoro-subject-id"] == "user-1"
        assert headers["x-kokoro-scope-kind"] == "conversation"
        assert headers["x-kokoro-scope-id"] == "conversation-1"
        assert "untrusted-namespace" not in str(headers)
    create = transport.calls[3][1]
    assert isinstance(create, pb.CreateArtifactRequest)
    assert create.artifact_id == ""
    assert create.source_run_id == "run-1"
    complete = transport.calls[1][1]
    assert isinstance(complete, pb.CompleteUploadRequest)
    assert complete.source_version_id == "version-A"
    assert complete.command is not None
    seed = _digest(["run-1", "tool-1"])
    assert complete.command.command_id == _digest(
        ["delivery-v2", seed, "complete_upload"]
    )
    assert complete.command.request_digest == _digest(
        [
            "delivery-v2",
            "complete_upload",
            ["upload-1", _HASH, len(_CONTENT), "version-A"],
        ]
    )
    intents = [json.loads(value) for _, _, value in runs.intents]
    assert [set(value) for value in intents] == [
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
        },
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
            "upload_id",
        },
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
            "upload_id",
            "source_version_id",
            "complete_command_id",
            "complete_request_digest",
        },
    ]
    assert all(value["version"] == 2 for value in intents)


async def test_stale_lease_prevents_first_external_call() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    runs.current = False
    with pytest.raises(StorageClientError):
        await StorageDeliveryClient(runs, transport).publish(_request())
    assert transport.calls == [] and transport.puts == []


async def test_lease_lost_after_upload_admission_prevents_signed_put() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.after_create = lambda: setattr(runs, "current", False)
    with pytest.raises(StorageClientError, match="INTENT_NOT_DURABLE"):
        await StorageDeliveryClient(runs, transport).publish(_request())
    assert [name for name, _, _ in transport.calls] == ["create_upload"]
    assert transport.puts == []


async def test_same_lease_selected_cas_loser_is_retryable_and_never_completes() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    runs.reject_selected = True
    transport.put_version = "version-B"
    with pytest.raises(StorageClientError, match="INTENT_NOT_DURABLE") as exc:
        await StorageDeliveryClient(runs, transport).publish(_request())
    assert exc.value.retryable
    assert runs.current
    assert transport.puts == [_CONTENT]
    assert "complete_upload" not in [name for name, _, _ in transport.calls]
    assert "create_artifact" not in [name for name, _, _ in transport.calls]


async def test_pending_selected_intent_recovers_without_workspace_or_second_put() -> (
    None
):
    runs, transport = FakeRuns(), FakeTransport()
    transport.fail_complete_once = True
    client = StorageDeliveryClient(runs, transport)
    with pytest.raises(StorageClientError, match="network"):
        await client.publish(_request())
    selected = json.loads(runs.last_intent or "{}")
    assert selected["source_version_id"] == "version-A"
    assert len(transport.puts) == 1

    receipt = await client.recover(_recovery())

    assert receipt is not None and receipt.artifact_id == "artifact-1"
    assert len(transport.puts) == 1
    assert [name for name, _, _ in transport.calls].count("create_upload") == 1
    assert [name for name, _, _ in transport.calls].count("complete_upload") == 2


async def test_selected_locator_mismatch_never_completes() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.fail_complete_once = True
    client = StorageDeliveryClient(runs, transport)
    with pytest.raises(StorageClientError):
        await client.publish(_request())
    transport.pending_source_version = "version-B"
    before = [name for name, _, _ in transport.calls].count("complete_upload")

    with pytest.raises(StorageClientError, match="SOURCE_VERSION_CONFLICT"):
        await client.recover(_recovery())

    assert [name for name, _, _ in transport.calls].count("complete_upload") == before


async def test_owner_source_version_without_durable_selection_fails_closed() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.after_put = lambda: setattr(runs, "current", False)
    with pytest.raises(StorageClientError, match="INTENT_NOT_DURABLE"):
        await StorageDeliveryClient(runs, transport).publish(_request())
    admitted = json.loads(runs.intents[-2][2])
    runs.current = True
    runs.last_intent = json.dumps(admitted)
    transport.pending_source_version = "version-A"

    with pytest.raises(StorageClientError, match="SOURCE_VERSION_CONFLICT"):
        await StorageDeliveryClient(runs, transport).recover(_recovery())
    assert [name for name, _, _ in transport.calls].count("complete_upload") == 0


def _recovery_intent(*, stage: str, upload_id: object = "upload-1") -> str:
    value: dict[str, object] = {
        "version": 2,
        "run_id": "run-1",
        "session_id": "conversation-1",
        "tenant_ref": "tenant-1",
        "subject_ref": "user-1",
        "tool_call_id": "tool-1",
        "path": "/report.pdf",
        "title": "Report",
        "note": "",
        "mime_type": "application/pdf",
        "size_bytes": len(_CONTENT),
        "content_sha256": _HASH,
    }
    if stage in {"admitted", "selected"}:
        value["upload_id"] = upload_id
    if stage == "selected":
        value.update(
            {
                "source_version_id": "version-A",
                "complete_command_id": "0" * 64,
                "complete_request_digest": "0" * 64,
            }
        )
    return json.dumps(value)


@pytest.mark.parametrize("stage", ["admitted", "selected"])
@pytest.mark.parametrize("upload_id", [None, ""])
async def test_recovery_rejects_non_string_or_empty_upload_for_closed_stage(
    stage: str, upload_id: object
) -> None:
    runs, transport = FakeRuns(), FakeTransport()
    runs.last_intent = _recovery_intent(stage=stage, upload_id=upload_id)

    with pytest.raises(StorageClientError, match="DELIVERY_INTENT_INVALID"):
        await StorageDeliveryClient(runs, transport).recover(_recovery())

    assert transport.calls == [] and transport.puts == []


async def test_recovery_allows_only_frozen_stage_to_have_no_upload() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    runs.last_intent = _recovery_intent(stage="frozen")

    assert await StorageDeliveryClient(runs, transport).recover(_recovery()) is None
    assert transport.calls == [] and transport.puts == []


async def test_corrupt_closed_stage_stops_actual_tool_before_workspace_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs, transport = FakeRuns(), FakeTransport()
    runs.last_intent = _recovery_intent(stage="selected", upload_id=None)
    backend = LocalShellBackend(root_dir=tmp_path, virtual_mode=True)
    original_download = backend.adownload_files
    downloads = 0

    async def counted_download(paths: list[str]):
        nonlocal downloads
        downloads += 1
        return await original_download(paths)

    monkeypatch.setattr(backend, "adownload_files", counted_download)

    def tool_call_id(_run_id: str) -> str:
        return "tool-1"

    monkeypatch.setattr(
        "kokoro_agent.tools.deliver.current_delivery_tool_call_id", tool_call_id
    )
    tool = make_deliver_tool(
        backend,
        StorageDeliveryClient(runs, transport),
        namespace="local:conversation-1",
        run_id="run-1",
        identity=_IDENTITY,
        lease=_LEASE,
    )

    invoke = tool.coroutine
    assert invoke is not None
    result = await invoke(path="/report.pdf", title="Report")

    assert isinstance(result, str) and "DELIVERY_INTENT_INVALID" in result
    assert downloads == 0
    assert transport.calls == [] and transport.puts == []


async def test_infected_scan_never_creates_artifact() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.scan = pb.ScanState.INFECTED
    with pytest.raises(StorageClientError):
        await StorageDeliveryClient(runs, transport).publish(_request())
    assert "create_artifact" not in [name for name, _, _ in transport.calls]


async def test_confirmed_pending_upload_aborts_only_before_any_put() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.bad_reference = True
    with pytest.raises(StorageClientError, match="REFERENCE_INVALID"):
        await StorageDeliveryClient(runs, transport).publish(_request())
    assert transport.puts == []
    assert [name for name, _, _ in transport.calls] == [
        "create_upload",
        "get_upload_status",
        "abort_upload",
    ]


@pytest.mark.parametrize("failure", ["missing_reference", "invalid_reference_on_put"])
async def test_pre_put_reference_failure_aborts_confirmed_pending(failure: str) -> None:
    runs, transport = FakeRuns(), FakeTransport()
    setattr(transport, failure, True)
    with pytest.raises(StorageClientError, match="REFERENCE_"):
        await StorageDeliveryClient(runs, transport).publish(_request())
    assert transport.puts == []
    assert [name for name, _, _ in transport.calls] == [
        "create_upload",
        "get_upload_status",
        "abort_upload",
    ]


async def test_body_identity_must_match_persisted_run() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    spoofed = _request().model_copy(
        update={"identity": _IDENTITY.model_copy(update={"tenant_ref": "other"})}
    )
    with pytest.raises(StorageClientError):
        await StorageDeliveryClient(runs, transport).publish(spoofed)
    assert transport.calls == []


async def test_non_user_subject_never_enters_conversation_artifact_scope() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    project = _IDENTITY.model_copy(
        update={"subject": IdentityRef(kind="project", opaque_ref="project-1")}
    )
    runs.request = _RUN.model_copy(update={"execution_identity": project})
    with pytest.raises(StorageClientError, match="SUBJECT_INVALID"):
        await StorageDeliveryClient(runs, transport).publish(
            _request().model_copy(update={"identity": project})
        )
    assert transport.calls == []


async def test_recovery_after_complete_reuses_upload_and_original_commands() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.fail_artifact_once = True
    client = StorageDeliveryClient(runs, transport)

    with pytest.raises(StorageClientError):
        await client.publish(_request())
    assert transport.completed and runs.last_intent is not None
    assert len(transport.puts) == 1

    receipt = await StorageDeliveryClient(runs, transport).publish(_request())
    assert receipt.artifact_id == "artifact-1"
    assert len(transport.puts) == 1
    assert [name for name, _, _ in transport.calls].count("create_upload") == 1
    assert [name for name, _, _ in transport.calls].count("get_upload_status") == 1
    create_calls = [
        message for name, message, _ in transport.calls if name == "create_artifact"
    ]
    assert len(create_calls) == 2
    assert isinstance(create_calls[0], pb.CreateArtifactRequest)
    assert isinstance(create_calls[1], pb.CreateArtifactRequest)
    assert create_calls[0].command == create_calls[1].command


def _recovery() -> DeliveryRecoveryRequest:
    return DeliveryRecoveryRequest(
        request_id="trace-recovery",
        run_id="run-1",
        identity=_IDENTITY,
        path="/report.pdf",
        title="Report",
        note="",
        lease=_LEASE,
        tool_call_id="tool-1",
    )


async def test_final_owner_reply_lost_recovers_without_workspace_bytes() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.finalize_reply_lost = True
    client = StorageDeliveryClient(runs, transport)
    with pytest.raises(StorageClientError) as exc:
        await client.publish(_request())
    assert exc.value.retryable
    before_puts = list(transport.puts)
    receipt = await client.recover(_recovery())
    assert receipt is not None
    assert receipt.artifact_kind == "document"
    assert (receipt.artifact_id, receipt.asset_id, receipt.content_sha256) == (
        "artifact-1",
        "asset-1",
        _HASH,
    )
    assert transport.puts == before_puts
    assert [name for name, _, _ in transport.calls].count("create_upload") == 1


async def test_recovery_rejects_changed_owner_kind_without_new_upload() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.finalize_reply_lost = True
    client = StorageDeliveryClient(runs, transport)
    with pytest.raises(StorageClientError):
        await client.publish(_request())
    transport.artifact_kind_override = pb.ArtifactKind.OTHER
    with pytest.raises(StorageClientError, match="ARTIFACT_MISMATCH"):
        await client.recover(_recovery())
    assert [name for name, _, _ in transport.calls].count("create_upload") == 1


@pytest.mark.parametrize(
    ("mime", "expected"),
    [
        ("application/pdf", "document"),
        ("text/x-python", "code"),
        ("image/png", "image"),
        ("audio/mpeg", "audio"),
        ("video/mp4", "video"),
        ("application/json", "data"),
        ("application/zip", "archive"),
        ("text/plain", "document"),
        ("application/octet-stream", "other"),
        ("application/x-kokoro-unknown", "other"),
    ],
)
async def test_owner_kind_round_trips_to_receipt(mime: str, expected: str) -> None:
    runs, transport = FakeRuns(), FakeTransport()
    receipt = await StorageDeliveryClient(runs, transport).publish(
        _request().model_copy(update={"mime_type": mime})
    )
    assert receipt.artifact_kind == expected


@pytest.mark.parametrize("kind", [0, 999, pb.ArtifactKind.OTHER])
async def test_owner_kind_must_be_known_and_match_request(kind: int) -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.artifact_kind_override = cast(pb.ArtifactKind, kind)
    with pytest.raises(StorageClientError, match="ARTIFACT_MISMATCH"):
        await StorageDeliveryClient(runs, transport).publish(_request())
    assert "finalize_artifact" not in [name for name, _, _ in transport.calls]


async def test_unknown_mime_reaches_explicit_owner_other_kind() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    receipt = await StorageDeliveryClient(runs, transport).publish(
        _request().model_copy(update={"mime_type": "application/octet-stream"})
    )
    assert receipt.artifact_kind == "other"
    create = next(
        request for name, request, _ in transport.calls if name == "create_artifact"
    )
    assert isinstance(create, pb.CreateArtifactRequest)
    assert create.kind == pb.ArtifactKind.OTHER


async def test_recovery_rejects_changed_tool_arguments_before_owner_io() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.finalize_reply_lost = True
    client = StorageDeliveryClient(runs, transport)
    with pytest.raises(StorageClientError):
        await client.publish(_request())
    before = len(transport.calls)
    with pytest.raises(StorageClientError, match="INTENT_MISMATCH"):
        await client.recover(_recovery().model_copy(update={"title": "Other"}))
    assert len(transport.calls) == before


@pytest.mark.parametrize(
    ("code", "retryable"),
    [
        (Code.INVALID_ARGUMENT, False),
        (Code.PERMISSION_DENIED, False),
        (Code.UNAVAILABLE, True),
    ],
)
async def test_owner_connect_error_classification(code: Code, retryable: bool) -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.connect_error = code
    with pytest.raises(StorageClientError) as exc:
        await StorageDeliveryClient(runs, transport).publish(_request())
    assert exc.value.retryable is retryable
    assert "upstream message" not in str(exc.value)


async def test_owner_cancel_preserves_task_cancellation() -> None:
    runs, transport = FakeRuns(), FakeTransport()
    transport.connect_error = Code.CANCELED
    with pytest.raises(asyncio.CancelledError):
        await StorageDeliveryClient(runs, transport).publish(_request())
