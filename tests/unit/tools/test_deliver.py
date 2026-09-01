"""Delivery reads the native workspace and calls only Storage's public facade."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

import hashlib
from pathlib import Path

from deepagents.backends.local_shell import LocalShellBackend

from kokoro_agent.clients.storage import (
    DeliveryReceipt,
    DeliveryRequest,
    StorageClientError,
)
from kokoro_agent.contract import ExecutionIdentity, IdentityRef
from kokoro_agent.tools.deliver import DeliverResult, make_deliver_tool

_NS = "local:s1"
_RUN = "run-1"
_IDENTITY = ExecutionIdentity(
    tenant_ref="tenant",
    actor=IdentityRef(kind="user", opaque_ref="actor"),
    subject=IdentityRef(kind="project", opaque_ref="project"),
    identity_assertion_ref="assertion",
)


class FakeDeliveryClient:
    def __init__(self) -> None:
        self.requests: list[DeliveryRequest] = []
        self.failure: StorageClientError | None = None

    async def publish(self, request: DeliveryRequest) -> DeliveryReceipt:
        if self.failure is not None:
            raise self.failure
        self.requests.append(request)
        return DeliveryReceipt(
            artifact_id=f"artifact-{request.content_sha256}",
            asset_id=f"asset-{request.content_sha256}",
            content_sha256=request.content_sha256,
            size_bytes=len(request.content),
            mime_type=request.mime_type,
            replayed=len(self.requests) > 1,
        )


def _backend(tmp_path: Path) -> LocalShellBackend:
    return LocalShellBackend(root_dir=tmp_path, virtual_mode=True)


def _tool(tmp_path: Path, client: FakeDeliveryClient):
    return make_deliver_tool(
        _backend(tmp_path),
        client,
        namespace=_NS,
        run_id=_RUN,
        identity=_IDENTITY,
    )


async def test_deliver_publishes_workspace_bytes_through_public_client(tmp_path: Path) -> None:
    (tmp_path / "report.pdf").write_bytes(b"final report")
    client = FakeDeliveryClient()

    raw = await _tool(tmp_path, client).ainvoke(
        {"path": "/report.pdf", "title": "Report", "note": "v1"}
    )

    result = DeliverResult.model_validate_json(raw)
    request = client.requests[0]
    assert result.status == "delivered"
    assert result.content_hash == hashlib.sha256(b"final report").hexdigest()
    assert request.namespace == _NS
    assert request.run_id == _RUN
    assert request.identity == _IDENTITY
    assert request.content == b"final report"
    assert request.content_sha256 == result.content_hash
    assert request.mime_type == "application/pdf"


async def test_deliver_request_is_idempotent_for_same_run_path_and_bytes(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_bytes(b"same")
    client = FakeDeliveryClient()
    tool = _tool(tmp_path, client)

    await tool.ainvoke({"path": "/a.txt", "title": "A"})
    await tool.ainvoke({"path": "/a.txt", "title": "A"})

    assert client.requests[0].request_id == client.requests[1].request_id


async def test_deliver_captures_each_source_revision(tmp_path: Path) -> None:
    source = tmp_path / "a.txt"
    source.write_bytes(b"original")
    client = FakeDeliveryClient()
    tool = _tool(tmp_path, client)

    first = DeliverResult.model_validate_json(
        await tool.ainvoke({"path": "/a.txt", "title": "A"})
    )
    source.write_bytes(b"mutated later")
    second = DeliverResult.model_validate_json(
        await tool.ainvoke({"path": "/a.txt", "title": "A"})
    )

    assert first.content_hash != second.content_hash
    assert client.requests[0].content == b"original"
    assert client.requests[1].content == b"mutated later"


async def test_deliver_rejects_invalid_or_reserved_paths(tmp_path: Path) -> None:
    client = FakeDeliveryClient()
    tool = _tool(tmp_path, client)

    traversal = await tool.ainvoke({"path": "../secret.txt", "title": "X"})
    skill = await tool.ainvoke({"path": "/.skills/music/SKILL.md", "title": "X"})

    assert "合法的工作区绝对路径" in traversal
    assert "合法的工作区绝对路径" in skill
    assert client.requests == []


async def test_deliver_missing_file_returns_error(tmp_path: Path) -> None:
    client = FakeDeliveryClient()
    out = await _tool(tmp_path, client).ainvoke({"path": "/nope.txt", "title": "X"})
    assert "无法读取" in out
    assert client.requests == []


async def test_storage_client_failure_is_readable(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("hello")
    client = FakeDeliveryClient()
    client.failure = StorageClientError("timeout")

    out = await _tool(tmp_path, client).ainvoke({"path": "/a.txt", "title": "A"})

    assert "交付存储暂不可用" in out
