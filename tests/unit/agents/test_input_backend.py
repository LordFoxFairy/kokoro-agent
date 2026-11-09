"""Selected inputs expose immutable bytes through native file routes only."""

from __future__ import annotations

import hashlib

from deepagents.backends import CompositeBackend, StateBackend
from deepagents.backends.protocol import PERMISSION_DENIED
from kokoro_agent.clients.storage import (
    InputResourceReadReceipt,
    InputResourceReadRequest,
)
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.execution.input_backend import InputResourceBackend, INPUTS_ROOT
from kokoro_agent.protocol.control import AssetResourceRef, AssetSourceScope
from kokoro_agent.protocol import RunRequest, ExecutionIdentity, IdentityRef, RunInput


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
