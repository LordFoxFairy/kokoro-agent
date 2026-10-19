"""Exact decoded Runtime requests under the pinned owner's binding vectors."""

from __future__ import annotations

import hashlib
import re
from pydantic import JsonValue
from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
    ProjectedPlatformRequest,
    project_bounded_text,
    project_identity,
)
from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r

RuntimeRequest = (
    r.ResolveMcpRuntimeConnectionRequest | r.AcquireMcpRuntimeCredentialRequest
)


def project_runtime_request(request: RuntimeRequest) -> ProjectedPlatformRequest:
    """No identity from a body, no parsed/reserialized arguments, no credential lookup."""
    if type(request) not in (
        r.ResolveMcpRuntimeConnectionRequest,
        r.AcquireMcpRuntimeCredentialRequest,
    ):
        raise PlatformRequestBindingError("MCP runtime request type is invalid")
    if request.to_binary() != request.to_binary(write_unknown_fields=False):
        raise PlatformRequestBindingError("MCP runtime unknown fields")
    if (
        request.connector_id is None
        or request.connection_id is None
        or request.server_id is None
    ):
        raise PlatformRequestBindingError("MCP runtime target is incomplete")
    request_id = project_bounded_text(
        request.request_id, label="request_id", maximum=191
    )
    digest = project_bounded_text(
        request.expected_declaration_digest, label="declaration_digest", maximum=64
    )
    if re.fullmatch(r"[a-f0-9]{64}", digest) is None:
        raise PlatformRequestBindingError("MCP runtime declaration digest invalid")
    projected: dict[str, JsonValue] = {
        "connector_id": project_identity(
            request.connector_id, wrapper="McpConnectorId", label="connector_id"
        ),
        "connection_id": project_identity(
            request.connection_id, wrapper="McpConnectionId", label="connection_id"
        ),
        "server_id": project_identity(
            request.server_id, wrapper="McpServerId", label="server_id"
        ),
        "tool_selector": project_bounded_text(
            request.tool_selector, label="tool_selector", maximum=512
        ),
        "expected_declaration_digest": digest,
    }
    if isinstance(request, r.ResolveMcpRuntimeConnectionRequest):
        return ProjectedPlatformRequest(
            operation="mcp.resolve_runtime_connection",
            fq_method="kokoro.platform.v1.McpRuntimeService/ResolveMcpRuntimeConnection",
            request_id=request_id,
            request=projected,
        )
    reference = project_bounded_text(
        request.resolution_ref, label="resolution_ref", maximum=8192
    )
    projected["resolution_ref_sha256"] = hashlib.sha256(
        reference.encode("utf-8")
    ).hexdigest()
    use = request.credential_use
    if use is None:
        raise PlatformRequestBindingError("MCP runtime credential purpose required")
    if (
        use.field == "discovery"
        and type(use.value) is r.McpRuntimeDiscoveryCredentialUse
    ):
        projected["credential_use"] = {"kind": "discovery"}
    elif (
        use.field == "invocation"
        and type(use.value) is r.McpRuntimeInvocationCredentialUse
    ):
        value = use.value
        if value.invocation_grant is None:
            raise PlatformRequestBindingError("MCP runtime grant is missing")
        project_identity(
            value.invocation_grant,
            wrapper="McpInvocationGrant",
            label="invocation_grant",
        )
        arguments = value.typed_arguments_json
        if type(arguments) is not bytes or not 0 < len(arguments) <= 1048576:
            raise PlatformRequestBindingError("MCP runtime arguments are invalid")
        projected["credential_use"] = {
            "kind": "invocation",
            "invocation_grant_sha256": hashlib.sha256(
                value.invocation_grant.value.encode("utf-8")
            ).hexdigest(),
            "audit_ref": project_bounded_text(
                value.audit_ref, label="audit_ref", maximum=191
            ),
            "typed_arguments_sha256": hashlib.sha256(arguments).hexdigest(),
            "approval_ref": {"present": False}
            if not value.has_field("approval_ref")
            else {
                "present": True,
                "value": project_bounded_text(
                    value.approval_ref, label="approval_ref", maximum=191
                ),
            },
            "idempotency_key": project_bounded_text(
                value.idempotency_key, label="idempotency_key", maximum=191
            ),
        }
    else:
        raise PlatformRequestBindingError("MCP runtime credential purpose invalid")
    return ProjectedPlatformRequest(
        operation="mcp.acquire_runtime_credential",
        fq_method="kokoro.platform.v1.McpRuntimeService/AcquireMcpRuntimeCredential",
        request_id=request_id,
        request=projected,
    )
