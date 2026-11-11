"""Consumer binding agrees with the independently pinned CustomAPI owner vectors."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kokoro_agent.execution.platform_request_binding import project_request_binding
from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
)
from kokoro_agent.generated.kokoro.platform.v1 import custom_api_runtime_pb as pb

ROOT = Path(__file__).parents[2]


def test_owner_vectors_and_order_independence() -> None:
    vectors = json.loads(
        (
            ROOT
            / "contract/platform/v1/custom-api-runtime/custom-api-runtime-request-vectors-v1.json"
        ).read_text()
    )
    for vector in vectors:
        data = vector["input"]
        cls = (
            pb.DescribeCustomApiEnvironmentRequest
            if data["kind"] == "describe"
            else pb.AcquireCustomApiEnvironmentRequest
        )
        request = cls(
            request_id=data["requestId"],
            invocation_id=data["invocationId"],
            profiles=[
                pb.CustomApiRuntimeProfileRef(
                    profile_id=p["profileId"],
                    expected_state_version=p["expectedStateVersion"],
                )
                for p in reversed(data["profiles"])
            ],
        )
        assert (
            project_request_binding(
                tenant_ref=vector["tenant_id"], request=request
            ).sha256
            == vector["sha256"]
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "empty",
        "duplicate",
        "zero",
        "overflow",
        "id",
        "invocation",
        "request",
        "unknown",
        "nested_unknown",
        "tenant",
    ],
)
def test_invalid_input_fails_closed(mutation: str) -> None:
    request = pb.AcquireCustomApiEnvironmentRequest(
        request_id="request",
        invocation_id="invocation",
        profiles=[
            pb.CustomApiRuntimeProfileRef(
                profile_id="profile", expected_state_version=1
            )
        ],
    )
    tenant = "tenant"
    if mutation == "empty":
        request.profiles.clear()
    elif mutation == "duplicate":
        request.profiles.append(request.profiles[0])
    elif mutation == "zero":
        request.profiles[0].expected_state_version = 0
    elif mutation == "overflow":
        request.profiles[0].expected_state_version = 2147483648
    elif mutation == "id":
        request.profiles[0].profile_id = "profile\n"
    elif mutation == "invocation":
        request.invocation_id = ""
    elif mutation == "request":
        request.request_id = "a b"
    elif mutation == "tenant":
        tenant = "ténant"
    elif mutation == "unknown":
        request = pb.AcquireCustomApiEnvironmentRequest.from_binary(
            request.to_binary() + b"\x98\x06\x01"
        )
    else:
        request.profiles[0] = pb.CustomApiRuntimeProfileRef.from_binary(
            request.profiles[0].to_binary() + b"\x98\x06\x01"
        )
    with pytest.raises(PlatformRequestBindingError):
        project_request_binding(tenant_ref=tenant, request=request)


@pytest.mark.parametrize("mutation", ["bytes", "repin", "extra", "symlink", "commit"])
def test_custom_api_publication_is_fixed_and_closed(
    tmp_path: Path, mutation: str
) -> None:
    import hashlib
    import shutil
    from kokoro_agent.custom_api_runtime_contract import (
        validate_custom_api_runtime_artifact,
    )

    shutil.copytree(ROOT / "contract/platform", tmp_path / "contract/platform")
    provenance = json.loads(
        (tmp_path / "contract/platform/v1/provenance.json").read_text()
    )
    record = provenance["custom_api_runtime"]["sources"][0]
    source = tmp_path / record["path"]
    if mutation in {"bytes", "repin"}:
        source.write_bytes(source.read_bytes() + b" ")
        if mutation == "repin":
            record["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    elif mutation == "extra":
        (source.parent / "extra.json").write_text("{}")
    elif mutation == "symlink":
        data = source.read_bytes()
        source.unlink()
        target = tmp_path / "original.json"
        target.write_bytes(data)
        source.symlink_to(target)
    else:
        provenance["custom_api_runtime"]["owner_commit"] = "0" * 40
    with pytest.raises(PlatformRequestBindingError):
        validate_custom_api_runtime_artifact(tmp_path, provenance)
