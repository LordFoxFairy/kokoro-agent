"""HTTP projection of current preparation; frozen bytes are never reconstructed."""

from __future__ import annotations

import base64
from datetime import UTC

from kokoro_agent.model.call_models import PreparedModelCallSnapshot


def prepared_snapshot_payload(snapshot: PreparedModelCallSnapshot) -> dict[str, object]:
    payload: dict[str, object] = {
        "binding_version": snapshot.binding_version,
        "attempt_id": snapshot.attempt_id,
        "status": snapshot.status,
        "final_request_binding_version": snapshot.final_request_binding_version,
        "final_request_digest": snapshot.final_request_digest,
    }
    for name, raw, digest in (
        (
            "attempt_binding",
            snapshot.attempt_binding_bytes,
            snapshot.attempt_binding_digest,
        ),
        ("planned", snapshot.planned_bytes, snapshot.planned_digest),
        ("authorization", snapshot.authorization_bytes, snapshot.authorization_digest),
    ):
        payload[f"{name}_bytes_base64url"] = (
            base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
        )
        payload[f"{name}_digest"] = digest
    for name, value in (
        ("prepared_at", snapshot.prepared_at),
        ("observed_at", snapshot.observed_at),
        ("lease_expires_at", snapshot.lease_expires_at),
    ):
        payload[name] = (
            value.astimezone(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
    return payload
