"""Per-call binding/proof isolation at the generated Connect boundary."""

from __future__ import annotations

import asyncio
from typing import Any
from collections.abc import AsyncGenerator

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from kokoro_agent.clients.platform_transport import PlatformTransport, RunPlatformClient
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb
from kokoro_agent.generated.kokoro.platform.v1.platform_runtime_connect import (
    McpConnectorServiceClient,
    SkillSourceServiceClient,
)


@pytest.mark.parametrize("read", [False, True], ids=["mcp", "skill-discovery"])
async def test_snapshot_fresh_proof_bearer_and_request_id(
    read: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, dict[str, str]]] = []
    order: list[str] = []

    class Tokens:
        async def token(self, tenant_id: str) -> str:
            assert tenant_id == "tenant"
            order.append("token")
            return "TOKEN"

    class Proof:
        async def issue(self, operation: str, request_binding_sha256: str) -> str:
            assert operation == (
                "skill.discover_visible_skills" if read else "mcp.get_connector"
            )
            assert len(request_binding_sha256) == 64
            order.append("proof")
            return f"proof-{len(order)}"

    async def send(
        self: object,
        request: pb.GetMcpConnectorRequest | pb.DiscoverVisibleSkillsRequest,
        **kwargs: Any,
    ) -> pb.GetMcpConnectorResponse | pb.DiscoverVisibleSkillsResponse:
        order.append("send")
        calls.append((request.request_id, request.execution_proof, kwargs["headers"]))
        return (
            pb.DiscoverVisibleSkillsResponse() if read else pb.GetMcpConnectorResponse()
        )

    monkeypatch.setattr(McpConnectorServiceClient, "get_mcp_connector", send)
    monkeypatch.setattr(SkillSourceServiceClient, "discover_visible_skills", send)
    async with PlatformTransport("http://127.0.0.1:1") as transport:
        client = RunPlatformClient(
            tenant_id="tenant", tokens=Tokens(), proofs=Proof(), transport=transport
        )
        request = (
            pb.DiscoverVisibleSkillsRequest(request_id="logical", query="", tags=[])
            if read
            else pb.GetMcpConnectorRequest(
                request_id="logical", connector_id=pb.McpConnectorId(value="id")
            )
        )
        await client.send(request)
        await client.send(request)
        assert request.execution_proof == ""
        assert calls[0][0] == calls[1][0] == "logical"
        assert calls[0][1] != calls[1][1]
        assert calls[0][2] == {"authorization": "Bearer TOKEN"}
        assert order == ["token", "proof", "send"] * 2


async def test_connect_cancel_restores_python_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def send(*args: Any, **kwargs: Any) -> pb.GetMcpConnectorResponse:
        raise ConnectError(Code.CANCELED, "upstream secret")

    monkeypatch.setattr(McpConnectorServiceClient, "get_mcp_connector", send)
    async with PlatformTransport("http://127.0.0.1:1") as transport:
        with pytest.raises(asyncio.CancelledError):
            await transport.send(pb.GetMcpConnectorRequest(), "TOKEN", 50)


async def test_response_limit_stops_reading_before_unbounded_buffer() -> None:
    from pyqwest import Request, Response
    from kokoro_agent.clients.platform_transport import BoundedConnectTransport

    consumed: list[int] = []

    async def chunks() -> AsyncGenerator[bytes, None]:
        for number in range(100):
            consumed.append(number)
            yield b"x" * 65_536

    class Transport:
        async def execute(self, request: Request) -> Response:
            return Response(status=200, content=chunks())

    with pytest.raises(ConnectError) as caught:
        await BoundedConnectTransport(Transport()).execute(
            Request("POST", "http://loopback")
        )
    assert caught.value.code == Code.RESOURCE_EXHAUSTED
    assert len(consumed) == 17


async def test_runtime_calls_share_fresh_proof_snapshot_and_exact_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from protobuf import Oneof
    from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as runtime
    from kokoro_agent.generated.kokoro.platform.v1.mcp_runtime_connect import (
        McpRuntimeServiceClient,
    )

    calls: list[tuple[str, str, str, int]] = []
    bindings: list[tuple[str, str]] = []

    class Tokens:
        async def token(self, tenant_id: str) -> str:
            assert tenant_id == "tenant"
            # Caller edits while token I/O is pending must not change the signed request.
            original.tool_selector = "changed.by.caller"
            return "TOKEN"

    class Proof:
        async def issue(self, operation: str, request_binding_sha256: str) -> str:
            bindings.append((operation, request_binding_sha256))
            return f"proof-{len(bindings)}"

    async def resolve(
        self: object,
        request: runtime.ResolveMcpRuntimeConnectionRequest,
        *,
        headers: dict[str, str],
        timeout_ms: int,
    ) -> runtime.ResolveMcpRuntimeConnectionResponse:
        assert headers == {"authorization": "Bearer TOKEN"}
        calls.append(
            (
                request.request_id,
                request.tool_selector,
                request.execution_proof,
                timeout_ms,
            )
        )
        return runtime.ResolveMcpRuntimeConnectionResponse()

    async def acquire(
        self: object,
        request: runtime.AcquireMcpRuntimeCredentialRequest,
        *,
        headers: dict[str, str],
        timeout_ms: int,
    ) -> runtime.AcquireMcpRuntimeCredentialResponse:
        assert headers == {"authorization": "Bearer TOKEN"}
        calls.append(
            (
                request.request_id,
                request.tool_selector,
                request.execution_proof,
                timeout_ms,
            )
        )
        return runtime.AcquireMcpRuntimeCredentialResponse()

    monkeypatch.setattr(
        McpRuntimeServiceClient, "resolve_mcp_runtime_connection", resolve
    )
    monkeypatch.setattr(
        McpRuntimeServiceClient, "acquire_mcp_runtime_credential", acquire
    )
    original = runtime.ResolveMcpRuntimeConnectionRequest(
        request_id="logical",
        connector_id=pb.McpConnectorId(value="connector"),
        connection_id=pb.McpConnectionId(value="connection"),
        server_id=pb.McpServerId(value="server"),
        tool_selector="issues.search",
        expected_declaration_digest="a" * 64,
    )
    credential = runtime.AcquireMcpRuntimeCredentialRequest(
        request_id="logical",
        connector_id=original.connector_id,
        connection_id=original.connection_id,
        server_id=original.server_id,
        tool_selector=original.tool_selector,
        expected_declaration_digest=original.expected_declaration_digest,
        resolution_ref="resolution",
        credential_use=Oneof("discovery", runtime.McpRuntimeDiscoveryCredentialUse()),
    )
    async with PlatformTransport("http://127.0.0.1:1") as transport:
        client = RunPlatformClient(
            tenant_id="tenant", tokens=Tokens(), proofs=Proof(), transport=transport
        )
        for request in (original, credential, credential):
            await client.send(request, timeout_s=1)
        assert original.execution_proof == credential.execution_proof == ""
        assert [row[1] for row in calls] == ["issues.search"] * 3
        assert [row[2] for row in calls] == ["proof-1", "proof-2", "proof-3"]
        assert all(0 < row[3] <= 1000 for row in calls)
        assert [entry[0] for entry in bindings] == [
            "mcp.resolve_runtime_connection",
            "mcp.acquire_runtime_credential",
            "mcp.acquire_runtime_credential",
        ]
        assert bindings[1] == bindings[2]


async def test_current_declarations_use_existing_connection_rpc_and_fresh_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.generated.kokoro.platform.v1.platform_runtime_connect import (
        McpConnectionServiceClient,
    )
    from kokoro_agent.generated.kokoro.common.v1 import common_pb

    proofs: list[str] = []

    class Tokens:
        async def token(self, tenant_id: str) -> str:
            return "TOKEN"

    class Proofs:
        async def issue(self, operation: str, request_binding_sha256: str) -> str:
            assert operation == "mcp.list_server_declarations"
            proofs.append(request_binding_sha256)
            return "fresh"

    async def declarations(
        self: object, request: pb.ListMcpServerDeclarationsRequest, **kwargs: Any
    ) -> pb.ListMcpServerDeclarationsResponse:
        assert (
            request.execution_proof == "fresh"
            and request.page is not None
            and request.page.limit == 1
        )
        assert kwargs["headers"] == {"authorization": "Bearer TOKEN"}
        return pb.ListMcpServerDeclarationsResponse()

    monkeypatch.setattr(
        McpConnectionServiceClient, "list_mcp_server_declarations", declarations
    )
    request = pb.ListMcpServerDeclarationsRequest(
        request_id="read",
        connector_id=pb.McpConnectorId(value="connector"),
        page=common_pb.PageRequest(limit=1),
    )
    async with PlatformTransport("http://127.0.0.1:1") as transport:
        client = RunPlatformClient(
            tenant_id="tenant", tokens=Tokens(), proofs=Proofs(), transport=transport
        )
        await client.send(request)
    assert len(proofs) == 1 and request.execution_proof == ""
