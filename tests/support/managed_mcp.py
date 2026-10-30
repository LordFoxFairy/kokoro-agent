"""Controlled current owner responses for real SDK tests; never production auth."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from collections.abc import Callable, AsyncGenerator
from contextlib import asynccontextmanager
import hashlib
import time
from uuid import uuid4
from types import CoroutineType, FunctionType
from typing import Literal, cast

from protobuf import Oneof
from pydantic import JsonValue
from kokoro_agent.clients.platform_transport import PlatformRequest
from kokoro_agent.generated.kokoro.common.v1 import common_pb as c
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as p
from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r
from kokoro_agent.mcp.schema import schema_digest


@asynccontextmanager
async def isolated_mcp_server_state() -> AsyncGenerator[None]:
    """Own the test-provider process shutdown state, not client or production state."""
    from sse_starlette import sse

    previous = sse.AppStatus.should_exit
    baseline = asyncio.all_tasks()
    watcher_code = cast(FunctionType, getattr(sse, "_shutdown_watcher")).__code__

    def is_watcher(task: asyncio.Task[object]) -> bool:
        coro = task.get_coro()
        return isinstance(coro, CoroutineType) and coro.cr_code is watcher_code

    try:
        yield
    finally:
        # The caller's existing finally has already stopped and joined its
        # provider Server. Only its newly spawned SDK watchdog is ours to retire;
        # never touch pre-existing tasks, disable SDK drain or clear its state.
        owned = [task for task in asyncio.all_tasks() - baseline if is_watcher(task)]
        for task in owned:
            task.cancel()
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*owned, return_exceptions=True), 2
            )
        finally:
            if all(task.done() for task in owned):
                sse.AppStatus.should_exit = previous
        for result in results:
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                raise result


class ManagedMcpOwner:
    def __init__(
        self,
        *,
        endpoint: str,
        schemas: dict[str, dict[str, JsonValue]],
        transport: Literal["streamable_http", "sse"] = "streamable_http",
    ) -> None:
        self.on_authorize: Callable[[], None] | None = None
        self.tool_calls = 0
        self.endpoint = endpoint
        self.transport = transport
        self.schemas = schemas
        self.requests: list[PlatformRequest] = []
        self.revoked = False
        self.bad_grant = False
        self.bad_schema = False
        self.bad_resolution = False
        self.resolutions: dict[str, str] = {}

    async def send(self, request: PlatformRequest, *, timeout_s: float = 10) -> object:
        self.requests.append(deepcopy(request))
        expiry = int(time.time()) + 30
        if isinstance(request, p.GetMcpConnectionRequest):
            return p.GetMcpConnectionResponse(
                connection=p.McpConnection(
                    connection_id=p.McpConnectionId(value="connection-1"),
                    connector_id=p.McpConnectorId(value="connector-1"),
                    server_id=p.McpServerId(value="server-1"),
                    allowed_selectors=list(self.schemas),
                    policy_digest="b" * 64,
                    status="revoked" if self.revoked else "active",
                )
            )
        if isinstance(request, p.GetMcpConnectorRequest):
            return p.GetMcpConnectorResponse(
                mcp_connector=p.McpConnector(
                    connector_id=p.McpConnectorId(value="connector-1"),
                    provider_key="owned",
                    status=p.McpConnectorStatus.ACTIVE,
                )
            )
        if isinstance(request, p.ListMcpServerDeclarationsRequest):
            return p.ListMcpServerDeclarationsResponse(
                servers=[
                    p.McpServer(
                        server_id=p.McpServerId(value="server-1"),
                        provider_key="owned",
                        transport=p.McpTransport.SSE_COMPAT
                        if self.transport == "sse"
                        else p.McpTransport.STREAMABLE_HTTP,
                        declaration_digest="a" * 64,
                        status=p.McpServerStatus.REGISTERED,
                    )
                ],
                capabilities=[
                    p.McpCapabilitySummary(
                        selector=name,
                        kind=p.McpPrimitiveKind.TOOL,
                        display_name=name,
                        input_schema_digest="0" * 64
                        if self.bad_schema
                        else schema_digest(schema),
                    )
                    for name, schema in self.schemas.items()
                ],
                page=c.PageResult(),
            )
        if isinstance(request, p.AuthorizeMcpToolRequest):
            if self.on_authorize is not None:
                self.on_authorize()
            return p.AuthorizeMcpToolResponse(
                connector_id=p.McpConnectorId(value="connector-1"),
                connection_id=p.McpConnectionId(
                    value="other" if self.bad_grant else "connection-1"
                ),
                server_id=p.McpServerId(value="server-1"),
                tool_ref=request.tool_selector,
                typed_arguments_digest=hashlib.sha256(
                    request.typed_arguments_json
                ).hexdigest(),
                invocation_grant=p.McpInvocationGrant(
                    value="mcp-grant:11111111-1111-4111-8111-111111111111"
                ),
                audit_ref="audit-1",
                expires_at_unix_seconds=expiry,
            )
        if isinstance(request, r.ResolveMcpRuntimeConnectionRequest):
            reference = str(uuid4())
            self.resolutions[reference] = request.tool_selector
            return r.ResolveMcpRuntimeConnectionResponse(
                resolution=r.McpRuntimeResolution(
                    connector_id=request.connector_id,
                    connection_id=p.McpConnectionId(value="other")
                    if self.bad_resolution
                    else request.connection_id,
                    server_id=request.server_id,
                    tool_selector=request.tool_selector,
                    declaration_digest=request.expected_declaration_digest,
                    resolution_ref=reference,
                    expires_at_unix_seconds=expiry,
                    transport=Oneof(
                        "sse",
                        r.McpRuntimeSseConnection(canonical_endpoint=self.endpoint),
                    )
                    if self.transport == "sse"
                    else Oneof(
                        "streamable_http",
                        r.McpRuntimeHttpConnection(canonical_endpoint=self.endpoint),
                    ),
                )
            )
        if isinstance(request, r.AcquireMcpRuntimeCredentialRequest):
            assert request.resolution_ref in self.resolutions
            assert request.credential_use is not None
            if request.credential_use.field == "invocation":
                use = request.credential_use.value
                assert (
                    use.invocation_grant is not None
                    and use.idempotency_key
                    and use.typed_arguments_json
                )
            return r.AcquireMcpRuntimeCredentialResponse(
                resolution_ref_sha256=hashlib.sha256(
                    request.resolution_ref.encode()
                ).hexdigest(),
                expires_at_unix_seconds=expiry,
                transport=Oneof("sse", r.McpRuntimeSseCredential())
                if self.transport == "sse"
                else Oneof("streamable_http", r.McpRuntimeHttpCredential()),
            )
        raise AssertionError(type(request))
