"""Delivery reads the native workspace and calls only Storage's public facade."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

import hashlib
import asyncio
from pathlib import Path

import pytest

from support.file_egress import Environment

from deepagents.backends.local_shell import LocalShellBackend

from kokoro_agent.clients.storage import (
    DeliveryReceipt,
    DeliveryRecoveryRequest,
    DeliveryRequest,
    StorageClientError,
)
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef
from kokoro_agent.sandbox.docker_backend import DockerShellBackend
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
        self.recovery: DeliveryReceipt | None = None

    async def recover(self, request: DeliveryRecoveryRequest) -> DeliveryReceipt | None:
        return self.recovery

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
            artifact_kind="document",
            replayed=len(self.requests) > 1,
            display_path=request.display_path,
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


async def test_recovered_final_receipt_does_not_read_missing_workspace_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def tool_call_id(_run_id: str) -> str:
        return "tool-1"

    monkeypatch.setattr(
        "kokoro_agent.tools.deliver.current_delivery_tool_call_id",
        tool_call_id,
    )
    client = FakeDeliveryClient()
    client.recovery = DeliveryReceipt(
        artifact_id="artifact-1",
        asset_id="asset-1",
        content_sha256="a" * 64,
        size_bytes=6,
        mime_type="application/pdf",
        artifact_kind="document",
        replayed=True,
    )
    result = DeliverResult.model_validate_json(
        await _tool(tmp_path, client).ainvoke(
            {"path": "/missing.pdf", "title": "Report"}
        )
    )
    assert result.artifact_id == "artifact-1"
    assert result.asset_id == "asset-1"
    assert result.artifact_kind == "document"
    assert client.requests == []


async def test_deliver_publishes_workspace_bytes_through_public_client(
    tmp_path: Path,
) -> None:
    (tmp_path / "report.pdf").write_bytes(b"final report")
    client = FakeDeliveryClient()

    raw = await _tool(tmp_path, client).ainvoke(
        {"path": "/report.pdf", "title": "Report", "note": "v1"}
    )

    result = DeliverResult.model_validate_json(raw)
    request = client.requests[0]
    assert result.status == "delivered"
    assert result.content_hash == hashlib.sha256(b"final report").hexdigest()
    assert result.artifact_id == f"artifact-{result.content_hash}"
    assert result.asset_id == f"asset-{result.content_hash}"
    assert result.artifact_kind == "document"
    assert request.namespace == _NS
    assert request.run_id == _RUN
    assert request.identity == _IDENTITY
    assert request.content == b"final report"
    assert request.content_sha256 == result.content_hash
    assert request.mime_type == "application/pdf"


@pytest.mark.parametrize(
    ("path", "proven", "display_path"),
    [
        ("/reports/report.txt", True, "/reports/report.txt"),
        ("/reports/report.txt", False, None),
        ("/reports//report.txt", True, None),
        ("/reports/./report.txt", True, None),
        ("/.inputs/0", True, None),
        ("/reports/.internal/report.txt", True, None),
        ("/reports/service-account.json", True, None),
    ],
)
async def test_delivery_only_emits_proven_exact_display_paths(
    tmp_path: Path, path: str, proven: bool, display_path: str | None
) -> None:
    source = tmp_path / path[1:]
    source.parent.mkdir(parents=True)
    source.write_bytes(b"same authorized bytes")
    client = FakeDeliveryClient()
    tool = make_deliver_tool(
        _backend(tmp_path),
        client,
        namespace=_NS,
        run_id=_RUN,
        identity=_IDENTITY,
        virtual_workspace=proven,
    )
    result = DeliverResult.model_validate_json(
        await tool.ainvoke({"path": path, "title": "Same delivery"})
    )
    assert result.display_path == client.requests[0].display_path == display_path
    assert ("display_path" in result.model_dump()) == (display_path is not None)
    assert client.requests[0].content == b"same authorized bytes"


async def test_deliver_request_is_idempotent_for_same_run_path_and_bytes(
    tmp_path: Path,
) -> None:
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


async def test_custom_api_deliver_denies_literal_secret_before_publish(
    tmp_path: Path,
) -> None:
    source = tmp_path / "report.pdf"
    source.write_bytes(b"binary\x00" + "private-原值-marker".encode())
    client = FakeDeliveryClient()
    backend = DockerShellBackend(
        root=tmp_path,
        container_id="unused",
        timeout=5,
        max_output_bytes=1024,
        environment_provider=Environment(),
        environment_loop=asyncio.get_running_loop(),
    )
    tool = make_deliver_tool(
        backend, client, namespace=_NS, run_id=_RUN, identity=_IDENTITY
    )
    denied = await tool.ainvoke({"path": "/report.pdf", "title": "Report"})
    assert "无法读取" in denied
    assert "private-原值-marker" not in denied
    assert client.requests == []
    source.write_bytes(b"binary\x00clean")
    result = DeliverResult.model_validate_json(
        await tool.ainvoke({"path": "/report.pdf", "title": "Report"})
    )
    assert result.status == "delivered"
    assert client.requests[0].content == b"binary\x00clean"


async def test_storage_client_failure_is_readable(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("hello")
    client = FakeDeliveryClient()
    client.failure = StorageClientError("timeout")

    out = await _tool(tmp_path, client).ainvoke({"path": "/a.txt", "title": "A"})

    assert "交付存储暂不可用" in out


async def test_retryable_storage_failure_preserves_journal_intent(
    tmp_path: Path,
) -> None:
    (tmp_path / "a.txt").write_text("hello")
    client = FakeDeliveryClient()
    client.failure = StorageClientError("network", retryable=True)

    with pytest.raises(StorageClientError, match="network"):
        await _tool(tmp_path, client).ainvoke({"path": "/a.txt", "title": "A"})
