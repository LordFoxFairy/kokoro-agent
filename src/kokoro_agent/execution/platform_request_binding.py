"""Canonical binding for pinned tenant-execution Platform requests."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re

from pydantic import JsonValue

from kokoro_agent.execution.platform_request_binding_values import (
    BINDING_VERSION,
    PlatformRequestBindingError,
    canonical_binding_bytes,
    project_nonempty_string,
    project_bounded_text,
)
from kokoro_agent.execution.custom_api_request_binding import project_custom_api_request
from kokoro_agent.generated.kokoro.platform.v1 import (
    custom_api_runtime_pb as custom_api_pb,
)
from kokoro_agent.generated.platform_request_projector import project_request
from kokoro_agent.execution.mcp_runtime_request_binding import project_runtime_request
from kokoro_agent.generated.kokoro.platform.v1 import mcp_runtime_pb as runtime_pb


@dataclass(frozen=True, slots=True, kw_only=True)
class PlatformRequestBinding:
    operation: str
    fq_method: str
    canonical_bytes: bytes
    sha256: str


def project_request_binding(
    *, tenant_ref: str, request: object
) -> PlatformRequestBinding:
    """Project one exact generated request; workload/global/unknown types fail closed."""

    tenant = project_nonempty_string(tenant_ref, label="tenant_ref")
    if isinstance(
        request,
        (
            custom_api_pb.DescribeCustomApiEnvironmentRequest,
            custom_api_pb.AcquireCustomApiEnvironmentRequest,
        ),
    ):
        if re.fullmatch(r"[\x21-\x7e]{1,255}", tenant) is None:
            raise PlatformRequestBindingError("CustomAPI tenant invalid")
        projected = project_custom_api_request(request)
    elif isinstance(
        request,
        (
            runtime_pb.ResolveMcpRuntimeConnectionRequest,
            runtime_pb.AcquireMcpRuntimeCredentialRequest,
        ),
    ):
        tenant = project_bounded_text(tenant_ref, label="tenant_ref", maximum=255)
        projected = project_runtime_request(request)
    else:
        projected = project_request(request)
    root: dict[str, JsonValue] = {
        "binding_version": BINDING_VERSION,
        "fq_method": projected.fq_method,
        "tenant_ref": tenant,
        "request_id": project_nonempty_string(projected.request_id, label="request_id"),
        "request": projected.request,
    }
    canonical = canonical_binding_bytes(root)
    return PlatformRequestBinding(
        operation=projected.operation,
        fq_method=projected.fq_method,
        canonical_bytes=canonical,
        sha256=hashlib.sha256(canonical).hexdigest(),
    )


__all__ = [
    "PlatformRequestBinding",
    "PlatformRequestBindingError",
    "project_request_binding",
]
