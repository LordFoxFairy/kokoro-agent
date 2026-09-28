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
)


async def test_snapshot_fresh_proof_bearer_and_request_id(
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
            assert operation == "mcp.get_connector"
            assert len(request_binding_sha256) == 64
            order.append("proof")
            return f"proof-{len(order)}"

    async def send(
        self: object, request: pb.GetMcpConnectorRequest, **kwargs: Any
    ) -> pb.GetMcpConnectorResponse:
        order.append("send")
        calls.append((request.request_id, request.execution_proof, kwargs["headers"]))
        return pb.GetMcpConnectorResponse()

    monkeypatch.setattr(McpConnectorServiceClient, "get_mcp_connector", send)
    async with PlatformTransport("http://127.0.0.1:1") as transport:
        client = RunPlatformClient(
            tenant_id="tenant", tokens=Tokens(), proofs=Proof(), transport=transport
        )
        request = pb.GetMcpConnectorRequest(
            request_id="logical", connector_id=pb.McpConnectorId(value="id")
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
