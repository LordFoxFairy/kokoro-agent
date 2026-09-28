"""Storage delivery uses only the persisted Run and the claimed lease."""

from __future__ import annotations

import hashlib
import asyncio
from collections.abc import Callable

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from kokoro_agent.clients.storage import (
    DeliveryRecoveryRequest,
    DeliveryRequest,
    StorageClientError,
)
from kokoro_agent.clients.storage_delivery import StorageDeliveryClient
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.domain.run.models import ToolJournalRecord
from kokoro_agent.generated.kokoro.storage.v2 import storage_pb as pb
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest


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
    execution_identity=_IDENTITY,
    input=RunInput(message_id="msg-1", content="create a report"),
)
_LEASE = LeaseFence(owner="worker-1", generation=1)
_CONTENT = b"final report"
_HASH = hashlib.sha256(_CONTENT).hexdigest()


class FakeRuns:
    def __init__(self) -> None:
        self.current = True
        self.request = _RUN
        self.intents: list[tuple[str, str, str]] = []
        self.last_intent: str | None = None

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
        self.bad_reference = False
        self.missing_reference = False
        self.invalid_reference_on_put = False
        self.connect_error: Code | None = None
        self.finalize_reply_lost = False

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
            )
        if method == "complete_upload":
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
            if self.fail_artifact_once:
                self.fail_artifact_once = False
                raise StorageClientError("network", retryable=True)
            return pb.CreateArtifactResponse(
                artifact_id="artifact-1",
                asset_id="asset-1",
                content_sha256=_HASH,
                state=pb.ArtifactState.DRAFT,
                kind=pb.ArtifactKind.DOCUMENT,
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

    async def put(self, reference: pb.TransferReference, content: bytes) -> None:
        assert reference.method == "PUT"
        if self.invalid_reference_on_put:
            raise StorageClientError("STORAGE_UPLOAD_REFERENCE_INVALID")
        self.puts.append(content)


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
    assert (receipt.artifact_id, receipt.asset_id, receipt.content_sha256) == (
        "artifact-1",
        "asset-1",
        _HASH,
    )
    assert transport.puts == before_puts
    assert [name for name, _, _ in transport.calls].count("create_upload") == 1


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
