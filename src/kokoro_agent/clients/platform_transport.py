"""Run/fence-scoped authenticated calls through pinned generated Connect clients."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from time import monotonic
from types import TracebackType
from typing import Protocol, Self, TypeAlias, TypedDict

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from pyqwest import Client, HTTPTransport, Request, Response, Transport

from kokoro_agent.clients.platform_tokens import validate_owner_url
from kokoro_agent.execution.platform_request_binding import project_request_binding
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb
from kokoro_agent.generated.kokoro.platform.v1.platform_runtime_connect import (
    McpAuthorizationServiceClient,
    McpConnectionServiceClient,
    McpConnectorServiceClient,
    SkillSourceServiceClient,
)

PlatformRequest: TypeAlias = (
    pb.ResolveVisibleSkillRequest
    | pb.GetApprovedSkillPackageReferenceRequest
    | pb.GetMcpConnectorRequest
    | pb.GetMcpConnectionRequest
    | pb.ListMcpConnectorCapabilitiesRequest
    | pb.AuthorizeMcpToolRequest
)
PlatformResponse: TypeAlias = (
    pb.ResolveVisibleSkillResponse
    | pb.GetApprovedSkillPackageReferenceResponse
    | pb.GetMcpConnectorResponse
    | pb.GetMcpConnectionResponse
    | pb.ListMcpConnectorCapabilitiesResponse
    | pb.AuthorizeMcpToolResponse
)
_ALLOWED = (
    pb.ResolveVisibleSkillRequest,
    pb.GetApprovedSkillPackageReferenceRequest,
    pb.GetMcpConnectorRequest,
    pb.GetMcpConnectionRequest,
    pb.ListMcpConnectorCapabilitiesRequest,
    pb.AuthorizeMcpToolRequest,
)


class TenantTokenProvider(Protocol):
    async def token(self, tenant_id: str) -> str: ...


class CallProofSupplier(Protocol):
    async def issue(self, operation: str, request_binding_sha256: str) -> str: ...


class PlatformCallError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class RunPlatformClient:
    """Immutable tenant and supplier; no shared mutable bearer or proof fields."""

    tenant_id: str = field(repr=False)
    tokens: TenantTokenProvider = field(repr=False)
    proofs: CallProofSupplier = field(repr=False)
    transport: PlatformTransport = field(repr=False)

    async def send(
        self, request: PlatformRequest, *, timeout_s: float = 10
    ) -> PlatformResponse:
        if type(request) not in _ALLOWED or not 0 < timeout_s <= 60:
            raise PlatformCallError("PLATFORM_REQUEST_INVALID")
        snapshot = deepcopy(request)
        # Validate before I/O; project again immediately before signing, never reuse proof.
        project_request_binding(tenant_ref=self.tenant_id, request=snapshot)
        deadline = monotonic() + timeout_s
        try:
            async with asyncio.timeout(timeout_s):
                token = await self.tokens.token(self.tenant_id)
                binding = project_request_binding(
                    tenant_ref=self.tenant_id, request=snapshot
                )
                snapshot.execution_proof = await self.proofs.issue(
                    binding.operation, binding.sha256
                )
                remaining_ms = int((deadline - monotonic()) * 1000)
                if remaining_ms <= 0:
                    raise TimeoutError
                return await self.transport.send(snapshot, token, remaining_ms)
        except TimeoutError:
            raise PlatformCallError("PLATFORM_DEADLINE_EXCEEDED") from None


class _ClientOptions(TypedDict):
    http_client: Client
    read_max_bytes: int
    timeout_ms: int
    send_compression: None


class BoundedConnectTransport:
    """Bound decoded HTTP bytes before pyqwest's unary client buffers them.

    Connect's read_max_bytes alone runs after full buffering. This wraps only
    the public HTTP Transport, not Connect serialization, framing, or routing.
    """

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    async def execute(self, request: Request) -> Response:
        response = await self._transport.execute(request)
        async with response:
            body = bytearray()
            async for chunk in response.content:
                if len(body) + len(chunk) > 1_048_576:
                    raise ConnectError(
                        Code.RESOURCE_EXHAUSTED, "response size limit exceeded"
                    )
                body.extend(chunk)
            return Response(
                status=response.status,
                http_version=response.http_version,
                headers=response.headers,
                content=bytes(body),
                trailers=response.trailers,
            )


class PlatformTransport:
    """Process-owned pool; the generated stub owns all wire/framing and RPC paths."""

    def __init__(self, base_url: str) -> None:
        self._url = validate_owner_url(base_url)
        self._pool = HTTPTransport(
            tls_include_system_certs=True,
            follow_redirects=False,
            connect_timeout=3,
            read_timeout=10,
            enable_otel=False,
        )
        self._http = Client(BoundedConnectTransport(self._pool))
        options: _ClientOptions = {
            "http_client": self._http,
            "read_max_bytes": 1_048_576,
            "timeout_ms": 10_000,
            "send_compression": None,
        }
        self._skills = SkillSourceServiceClient(self._url, **options)
        self._connectors = McpConnectorServiceClient(self._url, **options)
        self._connections = McpConnectionServiceClient(self._url, **options)
        self._authorization = McpAuthorizationServiceClient(self._url, **options)
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        for client in (
            self._skills,
            self._connectors,
            self._connections,
            self._authorization,
        ):
            await client.close()
        await self._pool.aclose()

    async def send(
        self, request: PlatformRequest, token: str, timeout_ms: int
    ) -> PlatformResponse:
        if self._closed:
            raise PlatformCallError("PLATFORM_TRANSPORT_CLOSED")
        headers = {"authorization": f"Bearer {token}"}
        try:
            match request:
                case pb.ResolveVisibleSkillRequest():
                    return await self._skills.resolve_visible_skill(
                        request, headers=headers, timeout_ms=timeout_ms
                    )
                case pb.GetApprovedSkillPackageReferenceRequest():
                    return await self._skills.get_approved_skill_package_reference(
                        request, headers=headers, timeout_ms=timeout_ms
                    )
                case pb.GetMcpConnectorRequest():
                    return await self._connectors.get_mcp_connector(
                        request, headers=headers, timeout_ms=timeout_ms
                    )
                case pb.GetMcpConnectionRequest():
                    return await self._connections.get_mcp_connection(
                        request, headers=headers, timeout_ms=timeout_ms
                    )
                case pb.ListMcpConnectorCapabilitiesRequest():
                    return await self._authorization.list_mcp_connector_capabilities(
                        request, headers=headers, timeout_ms=timeout_ms
                    )
                case pb.AuthorizeMcpToolRequest():
                    return await self._authorization.authorize_mcp_tool(
                        request, headers=headers, timeout_ms=timeout_ms
                    )
        except ConnectError as error:
            code = error.code
        else:
            raise PlatformCallError("PLATFORM_REQUEST_INVALID")
        if code == Code.CANCELED:
            raise asyncio.CancelledError from None
        # Raise outside the except block so even __context__ retains no upstream data.
        raise PlatformCallError("PLATFORM_" + code.name) from None
