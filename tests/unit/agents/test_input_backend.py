"""Selected inputs expose immutable bytes through native file routes only."""

from __future__ import annotations

import hashlib
import json

import pytest
from deepagents.backends import CompositeBackend, StateBackend
from deepagents.backends.protocol import PERMISSION_DENIED
from kokoro_agent.clients.storage import (
    InputResourceReadReceipt,
    InputResourceReadRequest,
    StorageClientError,
)
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.execution.input_backend import (
    InputResourceBackend,
    INPUTS_ROOT,
    input_resource_prompt,
)
from kokoro_agent.protocol.control import AssetResourceRef, AssetSourceScope
from kokoro_agent.protocol import RunRequest, ExecutionIdentity, IdentityRef, RunInput
from support.fakes import FakeRunRepository


class Reader:
    def __init__(self) -> None:
        self.requests: list[InputResourceReadRequest] = []

    async def read_input(
        self, request: InputResourceReadRequest
    ) -> InputResourceReadReceipt:
        self.requests.append(request)
        return InputResourceReadReceipt(
            resource_id="asset-1",
            asset_id="asset-1",
            content_sha256=hashlib.sha256(b"original bytes\n").hexdigest(),
            content=b"original bytes\n",
        )


def request() -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id="run-1",
        session_id="session-1",
        feature_key="chat",
        project_ref=None,
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant",
            actor=IdentityRef(kind="user", opaque_ref="user"),
            subject=IdentityRef(kind="user", opaque_ref="user"),
            identity_assertion_ref="assertion",
        ),
        input=RunInput(message_id="message", content="read input"),
        selected_resource_refs=(
            AssetResourceRef(
                kind="asset",
                resource_id="asset-1",
                asset_id="asset-1",
                content_sha256=hashlib.sha256(b"original bytes\n").hexdigest(),
                size_bytes="15",
                mime_type="text/plain",
                filename="not-a-path.txt",
                source_scope=AssetSourceScope(kind="personal", id="user"),
            ),
        ),
    )


async def test_native_route_reads_lists_greps_downloads_only_indexed_selected_input() -> (
    None
):
    reader = Reader()
    route = InputResourceBackend(
        request(), LeaseFence(owner="worker", generation=1), reader
    )
    backend = CompositeBackend(default=StateBackend(), routes={INPUTS_ROOT: route})
    listed = await backend.als(INPUTS_ROOT)
    assert [item["path"] for item in listed.entries or []] == ["/.inputs/0"]
    assert "original bytes" in str((await backend.aread("/.inputs/0")).file_data)
    assert (await backend.adownload_files(["/.inputs/0"]))[
        0
    ].content == b"original bytes\n"
    assert [
        item["path"] for item in (await backend.aglob("*", INPUTS_ROOT)).matches or []
    ] == ["/.inputs/0"]
    assert [
        item["path"]
        for item in (await backend.agrep("original", INPUTS_ROOT)).matches or []
    ] == ["/.inputs/0"]
    before = len(reader.requests)
    assert (await route.adownload_files(["/not-a-path.txt", "/../0", "/1"]))[
        0
    ].content is None
    assert len(reader.requests) == before
    assert (await backend.awrite("/.inputs/0", "changed")).error == PERMISSION_DENIED
    assert (
        await backend.aedit("/.inputs/0", "original", "changed")
    ).error == PERMISSION_DENIED
    assert (await backend.aupload_files([("/.inputs/0", b"changed")]))[
        0
    ].error == PERMISSION_DENIED
    assert all(
        item.reference == request().selected_resource_refs[0]
        for item in reader.requests
    )


_ANSWER_BYTES = " 原样𠮷\r\n\t".encode()


def answer_request() -> RunRequest:
    data = json.loads(request().model_dump_json())
    data["selected_resource_refs"] = [
        {
            "kind": "final_answer",
            "resource_id": "message-source",
            "source_scope": {"kind": "conversation", "id": "session-source"},
            "source_run_id": "run-source",
            "source_final_segment_id": "seg-source",
            "source_revision": "4",
            "content": _ANSWER_BYTES.decode(),
            "content_sha256": hashlib.sha256(_ANSWER_BYTES).hexdigest(),
            "size_bytes": str(len(_ANSWER_BYTES)),
        }
    ]
    return RunRequest.model_validate(data)


async def test_final_answer_uses_durable_consumer_lease_and_exact_bytes_without_storage() -> (
    None
):
    consuming = answer_request()
    repository = FakeRunRepository()
    lease = await repository.try_claim(consuming)
    assert lease is not None
    restored = RunRequest.model_validate_json(consuming.model_dump_json())
    route = InputResourceBackend(restored, lease, None, runs=repository)
    backend = CompositeBackend(default=StateBackend(), routes={INPUTS_ROOT: route})
    receipt = await backend.adownload_files(["/.inputs/0"])
    assert receipt[0].content == _ANSWER_BYTES
    assert "原样𠮷" in str((await backend.aread("/.inputs/0")).file_data)
    assert (await backend.awrite("/.inputs/0", "changed")).error == PERMISSION_DENIED
    prompt = input_resource_prompt(restored.selected_resource_refs)
    assert (
        '"/.inputs/0"' in prompt and "原样" not in prompt and "seg-source" not in prompt
    )


@pytest.mark.parametrize("case", ["stale", "not-selected", "digest", "size"])
async def test_final_answer_fails_closed_if_consumer_binding_or_bytes_change(
    case: str,
) -> None:
    consuming = answer_request()
    selected = consuming.selected_resource_refs[0]
    if case in {"digest", "size"}:
        changed = (
            {"content_sha256": "0" * 64} if case == "digest" else {"size_bytes": "1"}
        )
        consuming = consuming.model_copy(
            update={"selected_resource_refs": (selected.model_copy(update=changed),)}
        )
    repository = FakeRunRepository()
    lease = await repository.try_claim(consuming)
    assert lease is not None
    expected = "INPUT_READ_CONTENT_MISMATCH"
    if case == "stale":
        lease = LeaseFence(owner=lease.owner, generation=lease.generation + 1)
        expected = "INPUT_LEASE_STALE"
    elif case == "not-selected":
        consuming = consuming.model_copy(
            update={
                "selected_resource_refs": (
                    selected.model_copy(update={"source_revision": "5"}),
                )
            }
        )
        expected = "INPUT_RESOURCE_NOT_SELECTED"
    backend = InputResourceBackend(consuming, lease, None, runs=repository)
    with pytest.raises(StorageClientError, match=expected):
        await backend.adownload_files(["/0"])
