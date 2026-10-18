"""The five original owner vectors and unique Runtime binding risks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal
from copy import deepcopy

import pytest
from protobuf import Oneof
from kokoro_agent.execution.platform_request_binding import project_request_binding
from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
)
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as p
from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as r

ROOT = Path(__file__).parents[2]


def target() -> r.ResolveMcpRuntimeConnectionRequest:
    return r.ResolveMcpRuntimeConnectionRequest(
        request_id="request-runtime",
        connector_id=p.McpConnectorId(value="connector-runtime"),
        connection_id=p.McpConnectionId(value="connection-runtime"),
        server_id=p.McpServerId(value="server-runtime"),
        tool_selector="issues.search",
        expected_declaration_digest="a" * 64,
    )


def acquire(
    approval: str | None = None, *, discovery: bool = False
) -> r.AcquireMcpRuntimeCredentialRequest:
    base = target()
    use: (
        Oneof[Literal["discovery"], r.McpRuntimeDiscoveryCredentialUse]
        | Oneof[Literal["invocation"], r.McpRuntimeInvocationCredentialUse]
    ) = (
        Oneof("discovery", r.McpRuntimeDiscoveryCredentialUse())
        if discovery
        else Oneof(
            "invocation",
            r.McpRuntimeInvocationCredentialUse(
                invocation_grant=p.McpInvocationGrant(
                    value="mcp-grant:00000000-0000-4000-8000-000000000001"
                ),
                audit_ref="audit-fixture",
                typed_arguments_json=b'{"q":"term"}',
                approval_ref=approval,
                idempotency_key="tool-call-fixture",
            ),
        )
    )
    return r.AcquireMcpRuntimeCredentialRequest(
        request_id=base.request_id,
        connector_id=base.connector_id,
        connection_id=base.connection_id,
        server_id=base.server_id,
        tool_selector=base.tool_selector,
        expected_declaration_digest=base.expected_declaration_digest,
        resolution_ref="resolution-fixture",
        credential_use=use,
    )


def test_runtime_matches_owner_original_bytes_and_presence_vectors() -> None:
    document = json.loads(
        (
            ROOT
            / "contract/platform/v1/mcp-runtime/mcp-runtime-request-vectors-v1.json"
        ).read_bytes()
    )
    requests = [
        target(),
        acquire(discovery=True),
        acquire(),
        acquire("approval-one"),
        acquire("approval-two"),
    ]
    assert len(requests) == len(document["vectors"]) == 5
    for request, vector in zip(requests, document["vectors"], strict=True):
        decoded = type(request).from_binary(request.to_binary())
        result = project_request_binding(tenant_ref="tenant-runtime", request=decoded)
        assert json.loads(result.canonical_bytes) == vector["canonical"]
        assert result.sha256 == vector["sha256"]
        request.execution_proof = "proof-is-not-a-binding-input"
        assert (
            project_request_binding(tenant_ref="tenant-runtime", request=request).sha256
            == result.sha256
        )
    changed = acquire()
    assert changed.credential_use and changed.credential_use.field == "invocation"
    changed.credential_use.value.typed_arguments_json = b'{ "q": "term" }'
    assert (
        project_request_binding(tenant_ref="tenant-runtime", request=changed).sha256
        != document["vectors"][2]["sha256"]
    )


def test_runtime_closed_target_purpose_unknown_fields_and_text_boundaries() -> None:
    invalid: list[
        r.ResolveMcpRuntimeConnectionRequest | r.AcquireMcpRuntimeCredentialRequest
    ] = []
    for field, value in [
        ("request_id", ""),
        ("tool_selector", "x" * 513),
        ("tool_selector", "bad\nvalue"),
        ("expected_declaration_digest", "A" * 64),
    ]:
        request = deepcopy(target())
        setattr(request, field, value)
        invalid.append(request)
    missing = target()
    missing.connector_id = None
    invalid.append(missing)
    invalid.append(
        r.ResolveMcpRuntimeConnectionRequest.from_binary(
            target().to_binary() + b"\xc2\x0c\x01x"
        )
    )
    nested = target()
    nested.connector_id = p.McpConnectorId.from_binary(
        p.McpConnectorId(value="connector-runtime").to_binary() + b"\x10\x01"
    )
    invalid.append(nested)
    no_purpose = acquire()
    no_purpose.credential_use = None
    invalid.append(no_purpose)
    for approval in ["", "bad\x00value"]:
        invalid.append(acquire(approval))
    for request in invalid:
        with pytest.raises(PlatformRequestBindingError):
            project_request_binding(tenant_ref="tenant-runtime", request=request)
    for tenant in [" leading", "x" * 256, "\ud800"]:
        with pytest.raises(PlatformRequestBindingError):
            project_request_binding(tenant_ref=tenant, request=target())
    # JS strings count UTF-16 units; astral text must not be silently shortened or rejected by UTF-8 byte length.
    positive = target()
    positive.tool_selector = "🦊" * 256
    project_request_binding(tenant_ref="tenant-runtime", request=positive)
    positive.tool_selector += "🦊"
    with pytest.raises(PlatformRequestBindingError):
        project_request_binding(tenant_ref="tenant-runtime", request=positive)
