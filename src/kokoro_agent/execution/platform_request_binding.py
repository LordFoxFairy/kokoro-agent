"""Canonical binding for the 24 pinned tenant-execution Platform requests."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

from pydantic import JsonValue

from kokoro_agent.execution.platform_request_binding_values import (
    BINDING_VERSION,
    PlatformRequestBindingError,
    canonical_binding_bytes,
    project_nonempty_string,
)
from kokoro_agent.generated.platform_request_projector import project_request


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
