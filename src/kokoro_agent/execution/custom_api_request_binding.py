"""Strict CustomAPI projection from the independently pinned owner contract."""

from __future__ import annotations

import re

from pydantic import JsonValue

from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
    ProjectedPlatformRequest,
)
from kokoro_agent.generated.kokoro.platform.v1 import custom_api_runtime_pb as pb

CustomApiRequest = (
    pb.DescribeCustomApiEnvironmentRequest | pb.AcquireCustomApiEnvironmentRequest
)
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,190}\Z")


def custom_api_identity(value: str) -> str:
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise PlatformRequestBindingError("CustomAPI identity invalid")
    return value


def project_custom_api_request(request: CustomApiRequest) -> ProjectedPlatformRequest:
    if type(request) not in (
        pb.DescribeCustomApiEnvironmentRequest,
        pb.AcquireCustomApiEnvironmentRequest,
    ):
        raise PlatformRequestBindingError("CustomAPI request type invalid")
    if request.to_binary() != request.to_binary(write_unknown_fields=False):
        raise PlatformRequestBindingError("CustomAPI unknown fields")
    request_id = custom_api_identity(request.request_id)
    invocation_id = custom_api_identity(request.invocation_id)
    if not 1 <= len(request.profiles) <= 32:
        raise PlatformRequestBindingError("CustomAPI profile selection invalid")
    profiles: list[JsonValue] = []
    seen: set[str] = set()
    for ref in request.profiles:
        if type(
            ref
        ) is not pb.CustomApiRuntimeProfileRef or ref.to_binary() != ref.to_binary(
            write_unknown_fields=False
        ):
            raise PlatformRequestBindingError("CustomAPI profile fields invalid")
        profile_id = custom_api_identity(ref.profile_id)
        if (
            profile_id in seen
            or type(ref.expected_state_version) is not int
            or not 1 <= ref.expected_state_version <= 2147483647
        ):
            raise PlatformRequestBindingError("CustomAPI profile version invalid")
        seen.add(profile_id)
    for ref in sorted(request.profiles, key=lambda ref: ref.profile_id.encode("utf-8")):
        profiles.append(
            {
                "profile_id": ref.profile_id,
                "expected_state_version": ref.expected_state_version,
            }
        )
    describe = type(request) is pb.DescribeCustomApiEnvironmentRequest
    return ProjectedPlatformRequest(
        operation="custom_api.describe_environment"
        if describe
        else "custom_api.acquire_environment",
        fq_method="kokoro.platform.v1.CustomApiRuntimeService/"
        + (
            "DescribeCustomApiEnvironment"
            if describe
            else "AcquireCustomApiEnvironment"
        ),
        request_id=request_id,
        request={"invocation_id": invocation_id, "profiles": profiles},
    )
