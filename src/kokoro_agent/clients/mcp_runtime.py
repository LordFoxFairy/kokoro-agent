"""Current Platform authorization followed by one standard MCP SDK session."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
import hashlib
import json
import re
import time
from typing import Protocol
from uuid import uuid4

from langchain_mcp_adapters.callbacks import Callbacks
from mcp import ClientSession
from mcp.types import CallToolResult, Tool, PaginatedRequestParams
from protobuf import Oneof
from pydantic import JsonValue, TypeAdapter

from kokoro_agent.clients.mcp import (
    McpClientError,
    McpConnectionRef,
    McpTool,
    McpDisplayDeclaration,
)
from kokoro_agent.domain.chat.projection import ToolDisplayDetail
from kokoro_agent.clients.mcp_package_transport import McpPackageTransport
from kokoro_agent.clients.platform_transport import PlatformRequest
from kokoro_agent.execution.platform_request_binding_values import (
    canonical_binding_bytes,
)
from kokoro_agent.generated.kokoro.common.v1 import common_pb as common
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as p
from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r
from kokoro_agent.mcp.managed_session import managed_mcp_session
from kokoro_agent.mcp.schema import schema_digest

_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


class McpSender(Protocol):
    async def send(
        self, request: PlatformRequest, *, timeout_s: float = 10
    ) -> object: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class _Binding:
    connection: p.McpConnection
    connector: p.McpConnector
    server: p.McpServer
    capabilities: tuple[p.McpCapabilitySummary, ...]


def _id(value: p.McpConnectorId | p.McpConnectionId | p.McpServerId | None) -> str:
    if (
        value is None
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,190}", value.value) is None
    ):
        raise McpClientError("MCP_BINDING_INVALID")
    return value.value


def _digest(value: str) -> bool:
    return re.fullmatch(r"[a-f0-9]{64}", value) is not None


class PlatformMcpClient:
    def __init__(
        self,
        sender: McpSender,
        *,
        packages: McpPackageTransport | None,
        run_id: str,
        egress_mode: str = "strict",
    ) -> None:
        if not run_id:
            raise ValueError("MCP_RUN_REQUIRED")
        self._run_id = run_id
        self._sender = sender
        self._packages = packages
        self._egress_mode = egress_mode

    async def _send(self, request: PlatformRequest) -> object:
        failed = False
        try:
            response = await self._sender.send(request)
            if not isinstance(
                response,
                (
                    p.GetMcpConnectionResponse,
                    p.GetMcpConnectorResponse,
                    p.ListMcpServerDeclarationsResponse,
                    p.AuthorizeMcpToolResponse,
                    r.ResolveMcpRuntimeConnectionResponse,
                    r.AcquireMcpRuntimeCredentialResponse,
                ),
            ) or response.to_binary() != response.to_binary(write_unknown_fields=False):
                raise ValueError("invalid response")
            return response
        except asyncio.CancelledError:
            raise
        except Exception:
            failed = True
        if failed:
            raise McpClientError("MCP_OWNER_UNAVAILABLE") from None
        raise McpClientError("MCP_OWNER_INVALID")

    async def _binding(self, reference: McpConnectionRef) -> _Binding:
        response = await self._send(
            p.GetMcpConnectionRequest(
                request_id=str(uuid4()),
                connection_id=p.McpConnectionId(value=reference.connection_id),
            )
        )
        if (
            not isinstance(response, p.GetMcpConnectionResponse)
            or response.connection is None
        ):
            raise McpClientError("MCP_CONNECTION_INVALID")
        connection = response.connection
        if (
            _id(connection.connection_id) != reference.connection_id
            or connection.status != "active"
            or not _digest(connection.policy_digest)
        ):
            raise McpClientError("MCP_CONNECTION_INVALID")
        connector_id, server_id = (
            _id(connection.connector_id),
            _id(connection.server_id),
        )
        connector_response = await self._send(
            p.GetMcpConnectorRequest(
                request_id=str(uuid4()),
                connector_id=p.McpConnectorId(value=connector_id),
            )
        )
        if (
            not isinstance(connector_response, p.GetMcpConnectorResponse)
            or connector_response.mcp_connector is None
        ):
            raise McpClientError("MCP_CONNECTOR_INVALID")
        connector = connector_response.mcp_connector
        if (
            _id(connector.connector_id) != connector_id
            or connector.status != p.McpConnectorStatus.ACTIVE
        ):
            raise McpClientError("MCP_CONNECTOR_INVALID")
        # limit=1 preserves the owner response's Server/capability association.
        # The owner admits at most 32 current connections; cursors never grant access.
        cursor: str | None = None
        cursors: set[str] = set()
        for _ in range(32):
            declarations = await self._send(
                p.ListMcpServerDeclarationsRequest(
                    request_id=str(uuid4()),
                    connector_id=p.McpConnectorId(value=connector_id),
                    page=common.PageRequest(limit=1, cursor=cursor),
                )
            )
            if (
                not isinstance(declarations, p.ListMcpServerDeclarationsResponse)
                or len(declarations.servers) > 1
                or len(declarations.capabilities) > 2000
            ):
                raise McpClientError("MCP_DECLARATION_INVALID")
            if declarations.servers:
                server = declarations.servers[0]
                if _id(server.server_id) == server_id:
                    if (
                        server.status != p.McpServerStatus.REGISTERED
                        or server.provider_key != connector.provider_key
                        or not _digest(server.declaration_digest)
                    ):
                        raise McpClientError("MCP_DECLARATION_INVALID")
                    caps = tuple(
                        cap
                        for cap in declarations.capabilities
                        if cap.kind == p.McpPrimitiveKind.TOOL
                        and (
                            not connection.allowed_selectors
                            or cap.selector in connection.allowed_selectors
                        )
                    )
                    if len({cap.selector for cap in caps}) != len(caps) or any(
                        not cap.selector
                        or not _digest(cap.input_schema_digest)
                        or cap.output_schema_digest != ""
                        and not _digest(cap.output_schema_digest)
                        for cap in caps
                    ):
                        raise McpClientError("MCP_DECLARATION_INVALID")
                    return _Binding(
                        connection=connection,
                        connector=connector,
                        server=server,
                        capabilities=tuple(
                            cap
                            for cap in caps
                            if set(cap.required_scopes).issubset(
                                connector.approved_scopes
                            )
                        ),
                    )
            cursor = (
                declarations.page.next_cursor if declarations.page is not None else ""
            )
            if not cursor or cursor in cursors:
                break
            cursors.add(cursor)
        raise McpClientError("MCP_DECLARATION_UNAVAILABLE")

    async def check(
        self, references: Sequence[McpConnectionRef]
    ) -> tuple[McpDisplayDeclaration, ...]:
        declarations: list[McpDisplayDeclaration] = []
        async with asyncio.timeout(30):
            for reference in references:
                binding = await self._binding(reference)
                for cap in binding.capabilities:
                    try:
                        detail = ToolDisplayDetail(
                            kind="mcp_tool", name=cap.display_name
                        )
                    except ValueError:
                        # Selector is the owner's public tool declaration name;
                        # _tools requires exact equality with native tool.name.
                        # Never interpret a private registry key or raw args.
                        try:
                            detail = ToolDisplayDetail(
                                kind="mcp_tool", name=cap.selector
                            )
                        except ValueError:
                            continue  # Unsafe optional labels never grant execution.

                    declarations.append(
                        McpDisplayDeclaration(
                            connection_id=reference.connection_id,
                            selector=cap.selector,
                            display_name=detail.name,
                        )
                    )
        return tuple(declarations)

    async def _material(
        self,
        binding: _Binding,
        selector: str,
        invocation: r.McpRuntimeInvocationCredentialUse | None = None,
    ) -> tuple[r.McpRuntimeResolution, r.AcquireMcpRuntimeCredentialResponse]:
        connector_id = _id(binding.connector.connector_id)
        connection_id = _id(binding.connection.connection_id)
        server_id = _id(binding.server.server_id)
        response = await self._send(
            r.ResolveMcpRuntimeConnectionRequest(
                request_id=str(uuid4()),
                connector_id=p.McpConnectorId(value=connector_id),
                connection_id=p.McpConnectionId(value=connection_id),
                server_id=p.McpServerId(value=server_id),
                tool_selector=selector,
                expected_declaration_digest=binding.server.declaration_digest,
            )
        )
        if (
            not isinstance(response, r.ResolveMcpRuntimeConnectionResponse)
            or response.resolution is None
        ):
            raise McpClientError("MCP_RESOLUTION_INVALID")
        resolution = response.resolution
        if (
            _id(resolution.connector_id) != connector_id
            or _id(resolution.connection_id) != connection_id
            or _id(resolution.server_id) != server_id
            or resolution.tool_selector != selector
            or resolution.declaration_digest != binding.server.declaration_digest
            or resolution.transport is None
            or resolution.transport.field
            != {
                p.McpTransport.STDIO: "stdio",
                p.McpTransport.STREAMABLE_HTTP: "streamable_http",
                p.McpTransport.SSE_COMPAT: "sse",
            }.get(binding.server.transport)
            or not 0 < resolution.expires_at_unix_seconds - time.time() <= 30
        ):
            raise McpClientError("MCP_RESOLUTION_INVALID")
        material = await self._send(
            r.AcquireMcpRuntimeCredentialRequest(
                request_id=str(uuid4()),
                connector_id=p.McpConnectorId(value=connector_id),
                connection_id=p.McpConnectionId(value=connection_id),
                server_id=p.McpServerId(value=server_id),
                tool_selector=selector,
                expected_declaration_digest=binding.server.declaration_digest,
                resolution_ref=resolution.resolution_ref,
                credential_use=Oneof("discovery", r.McpRuntimeDiscoveryCredentialUse())
                if invocation is None
                else Oneof("invocation", invocation),
            )
        )
        if not isinstance(material, r.AcquireMcpRuntimeCredentialResponse):
            raise McpClientError("MCP_MATERIAL_INVALID")
        return resolution, material

    async def _tools(
        self, session: ClientSession, binding: _Binding
    ) -> dict[str, Tool]:
        capabilities = {cap.selector: cap for cap in binding.capabilities}
        tools: dict[str, Tool] = {}
        seen: set[str] = set()
        cursors: set[str] = set()
        cursor: str | None = None
        for _ in range(100):
            response = await session.list_tools(
                params=PaginatedRequestParams(cursor=cursor)
            )
            for tool in response.tools:
                if tool.name in seen or len(seen) >= 2000:
                    raise McpClientError("MCP_TOOLS_INVALID")
                seen.add(tool.name)
                cap = capabilities.get(tool.name)
                if cap is None:
                    continue
                schema = _JSON_OBJECT.validate_python(tool.inputSchema)
                if schema_digest(schema) != cap.input_schema_digest:
                    raise McpClientError("MCP_SCHEMA_CHANGED")
                if cap.output_schema_digest and (
                    tool.outputSchema is None
                    or schema_digest(_JSON_OBJECT.validate_python(tool.outputSchema))
                    != cap.output_schema_digest
                ):
                    raise McpClientError("MCP_SCHEMA_CHANGED")
                tools[tool.name] = tool
            cursor = response.nextCursor
            if not cursor:
                return tools
            if cursor in cursors:
                raise McpClientError("MCP_TOOLS_INVALID")
            cursors.add(cursor)
        raise McpClientError("MCP_TOOLS_INVALID")

    async def list_tools(self, reference: McpConnectionRef) -> tuple[McpTool, ...]:
        async with asyncio.timeout(30):
            binding = await self._binding(reference)
            if not binding.capabilities:
                return ()
            resolution, material = await self._material(
                binding, binding.capabilities[0].selector
            )
            async with managed_mcp_session(
                resolution,
                material,
                packages=self._packages,
                egress_mode=self._egress_mode,
            ) as session:
                tools = await self._tools(session, binding)
                return tuple(
                    McpTool(
                        name=tool.name,
                        description=tool.description or "",
                        input_schema=_JSON_OBJECT.validate_python(tool.inputSchema),
                    )
                    for tool in tools.values()
                )

    async def call_tool(
        self,
        reference: McpConnectionRef,
        tool: str,
        arguments: dict[str, JsonValue],
        *,
        tool_call_id: str,
        callbacks: Callbacks | None = None,
    ) -> CallToolResult:
        if not tool_call_id or len(tool_call_id) > 512:
            raise McpClientError("MCP_CALL_ID_REQUIRED")
        # Freeze before the first owner await. The same JSON semantics go to the
        # grant digest, Acquire, and the actual SDK tools/call; caller mutation is irrelevant.
        arguments_bytes = json.dumps(
            arguments,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(arguments_bytes) > 1_048_576:
            raise McpClientError("MCP_ARGUMENTS_INVALID")
        snapshot = _JSON_OBJECT.validate_json(arguments_bytes)
        async with asyncio.timeout(30):
            binding = await self._binding(reference)
            if tool not in {cap.selector for cap in binding.capabilities}:
                raise McpClientError("MCP_TOOL_NOT_ALLOWED")
            key = (
                "mcp-call:"
                + hashlib.sha256(
                    canonical_binding_bytes(
                        {"run_id": self._run_id, "tool_call_id": tool_call_id}
                    )
                ).hexdigest()
            )
            response = await self._send(
                p.AuthorizeMcpToolRequest(
                    request_id=str(uuid4()),
                    connector_id=binding.connector.connector_id,
                    tool_selector=tool,
                    typed_arguments_json=arguments_bytes,
                    idempotency_key=key,
                )
            )
            if (
                not isinstance(response, p.AuthorizeMcpToolResponse)
                or _id(response.connector_id) != _id(binding.connector.connector_id)
                or _id(response.connection_id) != reference.connection_id
                or _id(response.server_id) != _id(binding.server.server_id)
                or response.tool_ref != tool
                or response.typed_arguments_digest
                != hashlib.sha256(arguments_bytes).hexdigest()
                or response.invocation_grant is None
                or not response.invocation_grant.value
                or not response.audit_ref
                or response.expires_at_unix_seconds <= time.time()
            ):
                raise McpClientError("MCP_GRANT_INVALID")
            resolution, material = await self._material(
                binding,
                tool,
                r.McpRuntimeInvocationCredentialUse(
                    invocation_grant=response.invocation_grant,
                    audit_ref=response.audit_ref,
                    typed_arguments_json=arguments_bytes,
                    idempotency_key=key,
                ),
            )
            async with managed_mcp_session(
                resolution,
                material,
                packages=self._packages,
                callbacks=callbacks,
                egress_mode=self._egress_mode,
            ) as session:
                if tool not in await self._tools(session, binding):
                    raise McpClientError("MCP_TOOL_UNAVAILABLE")
                # No retry/reconnect on timeout or unknown tools/call outcome.
                return await session.call_tool(tool, snapshot)
