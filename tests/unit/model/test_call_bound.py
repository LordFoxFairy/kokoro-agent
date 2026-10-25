"""Execution-bound parsing and immutable clock rules, without external resources."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path

import pytest
import rfc8785
from pydantic import JsonValue, TypeAdapter

from support.model_call_bound import supported_material, supported_final_digest
from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.protocol.control import (
    RunRequest,
    RunInput,
    ExecutionIdentity,
    IdentityRef,
)
from kokoro_agent.model.call_bound import parse_bound, build_bound
from kokoro_agent.model.capacity_guarantee import CapacityGuaranteeInvalid
from kokoro_agent.model.call_models import ModelCallInputInvalid, PrepareModelCall
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding

ROOT = Path(__file__).resolve().parents[3]


def materials() -> tuple[dict[str, JsonValue], bytes]:
    planned = PlannedBinding(
        binding_version="2.0.0",
        model="m",
        provider="p",
        revision_id="r",
        provider_model_name="gpt-4o-2024-08-06",
        request_model="gpt-4o-2024-08-06",
        feature_key="chat",
        revision="1",
        generation="1",
        tenant_generation="1",
        digest="a" * 64,
        label="qa",
    )
    pb = rfc8785.dumps(planned.model_dump(mode="json"))
    value = TypeAdapter(dict[str, JsonValue]).validate_python(
        json.loads((ROOT / "contract/model-call-bound/v1/vectors.json").read_bytes())[
            "valid"
        ][0],
        strict=True,
    )
    value["planned_binding_digest"] = hashlib.sha256(pb).hexdigest()
    return value, pb


@pytest.mark.parametrize(
    ("field", "invalid"), [("final_request_binding_version", True), ("sealed", 1)]
)
def test_bound_literal_types_never_coerce_json_bool_and_number(
    field: str, invalid: JsonValue
) -> None:
    value, planned = materials()
    value[field] = invalid
    with pytest.raises(ModelCallInputInvalid):
        parse_bound(rfc8785.dumps(value), planned_bytes=planned)


def test_formal_bound_preserves_exact_canonical_bytes_and_identity() -> None:
    value, planned = materials()
    raw = rfc8785.dumps(value)
    parsed = parse_bound(
        raw, planned_bytes=planned, expected_digest=hashlib.sha256(raw).hexdigest()
    )
    assert rfc8785.dumps(parsed.model_dump(mode="json")) == raw


@pytest.mark.parametrize(
    "change",
    [
        "unknown",
        "noncanonical",
        "duplicate",
        "bom",
        "bad_utf8",
        "digest",
        "planned",
        "total",
        "expiry",
        "ref",
    ],
)
def test_bound_rejects_raw_or_joint_identity_drift(change: str) -> None:
    value, planned = materials()
    if change == "unknown":
        value["extra"] = True
    elif change == "total":
        value["total_tokens_max"] = "1"
    elif change == "expiry":
        value["valid_until"] = value["sealed_at"]
    elif change == "ref":
        value["planned_binding_ref"] = "mpb_00000000-0000-5000-8000-000000000099"
    raw = rfc8785.dumps(value)
    if change == "noncanonical":
        raw = b" " + raw
    elif change == "duplicate":
        raw = raw[:-1] + b',"sealed":true}'
    elif change == "bom":
        raw = b"\xef\xbb\xbf" + raw
    elif change == "bad_utf8":
        raw = b"\xff" + raw
    elif change == "planned":
        planned = planned + b" "
    with pytest.raises(ModelCallInputInvalid):
        parse_bound(
            raw,
            planned_bytes=planned,
            expected_digest="0" * 64 if change == "digest" else None,
        )


def _time_bound_command() -> PrepareModelCall:
    value, planned_raw = materials()
    planned = PlannedBinding.model_validate_json(planned_raw, strict=True)
    request = RunRequest(
        kind="run.request",
        run_id=str(value["run_id"]),
        session_id="time-bound-session",
        feature_key="chat",
        project_ref=None,
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref=str(value["tenant_ref"]),
            actor=IdentityRef(kind="user", opaque_ref="time-actor"),
            subject=IdentityRef(kind="user", opaque_ref="time-subject"),
            identity_assertion_ref="time-assertion",
        ),
        input=RunInput(message_id="time-message", content="time-bound"),
    )
    recipe = b"{}"
    return PrepareModelCall(
        request=request,
        lease=LeaseFence(owner="time-worker", generation=1),
        call_id=str(value["call_id"]),
        attempt_id=str(value["attempt_id"]),
        planned=planned,
        peer_path=("assistant",),
        recipe=StaticRecipeBinding(
            canonical_bytes=recipe, fingerprint=hashlib.sha256(recipe).hexdigest()
        ),
        capacity_request=supported_material(planned, request.input.content),
        final_request_binding_version=1,
        final_request_digest=supported_final_digest(planned, request.input.content),
    )


@pytest.mark.parametrize("lease_first", [True, False])
def test_bound_freezes_minimum_of_lease_and_profile_sunset_with_floor(
    lease_first: bool,
) -> None:
    command = _time_bound_command()
    sunset = datetime(2027, 1, 14, tzinfo=UTC)
    sealed = sunset - timedelta(seconds=10, microseconds=876001)
    lease = (
        sunset - timedelta(seconds=1, microseconds=123001)
        if lease_first
        else sunset + timedelta(hours=1)
    )
    raw, digest, expiry = build_bound(
        command, ordinal=1, prepared_at=sealed, lease_expires_at=lease
    )
    bound = parse_bound(raw, planned_bytes=materials()[1], expected_digest=digest)
    assert bound.sealed_at == "2027-01-13T23:59:49.123Z"
    assert expiry == (
        datetime(2027, 1, 13, 23, 59, 58, 876000, tzinfo=UTC) if lease_first else sunset
    )
    assert bound.valid_until == (
        "2027-01-13T23:59:58.876Z" if lease_first else "2027-01-14T00:00:00.000Z"
    )


@pytest.mark.parametrize(
    "boundary", ["lease_equal", "lease_earlier", "sunset_equal", "after_sunset"]
)
def test_bound_rejects_empty_lifetime_or_new_seal_at_sunset(boundary: str) -> None:
    command = _time_bound_command()
    sunset = datetime(2027, 1, 14, tzinfo=UTC)
    sealed = sunset - timedelta(seconds=1)
    lease = sealed if boundary == "lease_equal" else sealed - timedelta(milliseconds=1)
    if boundary in {"sunset_equal", "after_sunset"}:
        sealed = sunset + (
            timedelta(milliseconds=1) if boundary == "after_sunset" else timedelta()
        )
        lease = sunset + timedelta(hours=1)
    with pytest.raises(
        CapacityGuaranteeInvalid
        if boundary in {"sunset_equal", "after_sunset"}
        else ModelCallInputInvalid
    ):
        build_bound(command, ordinal=1, prepared_at=sealed, lease_expires_at=lease)
