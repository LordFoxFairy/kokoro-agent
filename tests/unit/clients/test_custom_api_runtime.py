"""Ephemeral credentials remain outside client state, descriptions and exceptions."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

import pytest

from kokoro_agent.clients.custom_api_runtime import (
    CustomApiClientError,
    CustomApiConfigurationError,
    CustomApiDependencyError,
    CustomApiEnvironmentProvider,
    CustomApiProfileRef,
    PlatformCustomApiClient,
)
from kokoro_agent.clients.platform_transport import PlatformCallError, PlatformRequest
from kokoro_agent.execution.platform_request_binding import project_request_binding
from kokoro_agent.generated.kokoro.platform.v1 import custom_api_runtime_pb as pb

REFS = (CustomApiProfileRef(profile_id="profile", expected_state_version=3),)
Response = (
    pb.DescribeCustomApiEnvironmentResponse | pb.AcquireCustomApiEnvironmentResponse
)


class Sender:
    def __init__(self, mutate: Callable[[Response], None] | None = None) -> None:
        self.requests: list[PlatformRequest] = []
        self.mutate = mutate

    async def send(self, request: PlatformRequest, *, timeout_s: float = 10) -> object:
        assert 0 < timeout_s <= 10
        self.requests.append(request)
        binding = project_request_binding(tenant_ref="tenant", request=request)
        if isinstance(request, pb.DescribeCustomApiEnvironmentRequest):
            response: Response = pb.DescribeCustomApiEnvironmentResponse(
                request_binding_sha256=binding.sha256,
                run_id="run",
                execution_session_id="session",
                profiles=[
                    pb.CustomApiRuntimeProfileDescription(
                        profile_id="profile",
                        state_version=3,
                        name="API",
                        instructions="Use API_KEY",
                        environment_names=["API_KEY"],
                    )
                ],
            )
        else:
            response = pb.AcquireCustomApiEnvironmentResponse(
                request_binding_sha256=binding.sha256,
                run_id="run",
                execution_session_id="session",
                environment=[
                    pb.CustomApiRuntimeEnvironmentValue(
                        name="API_KEY", value=b"ephemeral-secret"
                    )
                ],
            )
        if self.mutate:
            self.mutate(response)
        return response


def client(sender: Sender) -> PlatformCustomApiClient:
    return PlatformCustomApiClient(
        sender=sender, tenant_id="tenant", run_id="run", execution_session_id="session"
    )


async def test_describe_is_secret_free_and_each_acquire_is_fresh() -> None:
    sender = Sender()
    api = client(sender)
    descriptions = await api.describe(REFS, deadline_monotonic=time.monotonic() + 5)
    assert descriptions[0].environment_names == ("API_KEY",)
    assert descriptions[0].instructions == "Use API_KEY"
    provider = CustomApiEnvironmentProvider(client=api, profiles=REFS)
    first = await provider.acquire(deadline_monotonic=time.monotonic() + 5)
    second = await provider.acquire(deadline_monotonic=time.monotonic() + 5)
    assert first[0][1].get_secret_value() == "ephemeral-secret"
    assert second[0][1].get_secret_value() == "ephemeral-secret"
    requests = sender.requests
    assert len({r.request_id for r in requests}) == 3
    custom_requests = [
        r
        for r in requests
        if isinstance(
            r,
            (
                pb.DescribeCustomApiEnvironmentRequest,
                pb.AcquireCustomApiEnvironmentRequest,
            ),
        )
    ]
    assert len({r.invocation_id for r in custom_requests}) == 3
    assert "ephemeral-secret" not in repr((api, provider, first, descriptions))


@pytest.mark.parametrize(
    ("code", "error_type"),
    [
        ("PLATFORM_FAILED_PRECONDITION", CustomApiConfigurationError),
        ("PLATFORM_NOT_FOUND", CustomApiConfigurationError),
        ("PLATFORM_UNAVAILABLE", CustomApiDependencyError),
        ("PLATFORM_DEADLINE_EXCEEDED", CustomApiDependencyError),
    ],
)
async def test_structured_owner_error_is_classified_without_message_matching(
    code: str, error_type: type[CustomApiClientError]
) -> None:
    class FailingSender(Sender):
        async def send(
            self, request: PlatformRequest, *, timeout_s: float = 10
        ) -> object:
            raise PlatformCallError(code)

    with pytest.raises(error_type) as raised:
        await client(FailingSender()).describe(
            REFS, deadline_monotonic=time.monotonic() + 5
        )
    assert str(raised.value) in {
        "CUSTOM_API_CONFIGURATION_INVALID",
        "CUSTOM_API_OWNER_UNAVAILABLE",
    }


@pytest.mark.parametrize(
    "field", ["run_id", "execution_session_id", "request_binding_sha256"]
)
async def test_exact_response_binding_required(field: str) -> None:
    def mutate(response: Response) -> None:
        setattr(response, field, "other")

    api = client(Sender(mutate))
    with pytest.raises(CustomApiClientError):
        await api.acquire(REFS, deadline_monotonic=time.monotonic() + 5)
    with pytest.raises(CustomApiClientError):
        await api.describe(REFS, deadline_monotonic=time.monotonic() + 5)


@pytest.mark.parametrize(
    "mutation",
    [
        "duplicate",
        "reserved",
        "prefix",
        "empty",
        "newline",
        "utf8",
        "oversize",
        "unknown",
        "total",
        "count",
    ],
)
async def test_invalid_environment_is_rejected_without_leaking(mutation: str) -> None:
    def mutate(response: Response) -> None:
        assert isinstance(response, pb.AcquireCustomApiEnvironmentResponse)
        value = response.environment[0]
        if mutation == "duplicate":
            response.environment.append(value)
        elif mutation == "reserved":
            value.name = "PATH"
        elif mutation == "prefix":
            value.name = "PYTHONPATH"
        elif mutation == "empty":
            value.value = b""
        elif mutation == "newline":
            value.value = b"secret\n"
        elif mutation == "utf8":
            value.value = b"\xff"
        elif mutation == "oversize":
            value.value = b"s" * 4097
        elif mutation == "unknown":
            response.environment[0] = pb.CustomApiRuntimeEnvironmentValue.from_binary(
                value.to_binary() + b"\x98\x06\x01"
            )
        elif mutation == "total":
            response.environment = [
                pb.CustomApiRuntimeEnvironmentValue(name=f"API_{i}", value=b"s" * 4096)
                for i in range(17)
            ]
        else:
            response.environment = [
                pb.CustomApiRuntimeEnvironmentValue(name=f"API_{i}", value=b"s")
                for i in range(129)
            ]

    with pytest.raises(CustomApiClientError) as caught:
        await client(Sender(mutate)).acquire(
            REFS, deadline_monotonic=time.monotonic() + 5
        )
    assert "ephemeral-secret" not in repr(caught.value)
    assert caught.value.__context__ is None


@pytest.mark.parametrize(
    "mutation", ["missing", "extra", "version", "duplicate", "duplicate_env", "unknown"]
)
async def test_describe_requires_exact_versions_and_complete_set(mutation: str) -> None:
    def mutate(response: Response) -> None:
        assert isinstance(response, pb.DescribeCustomApiEnvironmentResponse)
        ref = response.profiles[0]
        if mutation == "missing":
            response.profiles.clear()
        elif mutation == "extra":
            ref.profile_id = "extra"
        elif mutation == "version":
            ref.state_version = 4
        elif mutation == "duplicate":
            response.profiles.append(ref)
        elif mutation == "duplicate_env":
            ref.environment_names.append("API_KEY")
        else:
            response.profiles[0] = pb.CustomApiRuntimeProfileDescription.from_binary(
                ref.to_binary() + b"\x98\x06\x01"
            )

    with pytest.raises(CustomApiClientError):
        await client(Sender(mutate)).describe(
            REFS, deadline_monotonic=time.monotonic() + 5
        )


async def test_expired_deadline_is_no_io() -> None:
    sender = Sender()
    with pytest.raises(CustomApiClientError):
        await client(sender).acquire(REFS, deadline_monotonic=time.monotonic() - 1)
    assert not sender.requests


async def test_failure_and_cancellation_drop_upstream_exception_context() -> None:
    class Failing(Sender):
        async def send(
            self, request: PlatformRequest, *, timeout_s: float = 10
        ) -> object:
            raise RuntimeError("ephemeral-secret")

    with pytest.raises(CustomApiClientError) as caught:
        await client(Failing()).acquire(REFS, deadline_monotonic=time.monotonic() + 5)
    assert caught.value.__context__ is None

    class Cancelled(Sender):
        async def send(
            self, request: PlatformRequest, *, timeout_s: float = 10
        ) -> object:
            raise asyncio.CancelledError("ephemeral-secret")

    with pytest.raises(asyncio.CancelledError) as cancelled:
        await client(Cancelled()).acquire(REFS, deadline_monotonic=time.monotonic() + 5)
    assert not cancelled.value.args
    assert cancelled.value.__context__ is None


@pytest.mark.parametrize("describe", [False, True])
async def test_run_transport_uses_generated_connect_and_fresh_proof(
    describe: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typing import Any
    from kokoro_agent.clients.platform_transport import (
        PlatformTransport,
        RunPlatformClient,
    )
    from kokoro_agent.generated.kokoro.platform.v1.custom_api_runtime_connect import (
        CustomApiRuntimeServiceClient,
    )

    calls: list[tuple[str, str]] = []
    operations: list[str] = []

    class Credentials:
        async def credential(self, tenant_id: str) -> str:
            assert tenant_id == "tenant"
            return "TOKEN"

    class Proofs:
        async def issue(self, operation: str, request_binding_sha256: str) -> str:
            operations.append(operation)
            return f"proof-{len(operations)}"

    async def send(self: object, request: PlatformRequest, **kwargs: Any) -> Response:
        assert kwargs["headers"] == {"authorization": "Bearer TOKEN"}
        assert kwargs["timeout_ms"] > 0
        calls.append((request.request_id, request.execution_proof))
        response = await Sender().send(request)
        assert isinstance(
            response,
            (
                pb.DescribeCustomApiEnvironmentResponse,
                pb.AcquireCustomApiEnvironmentResponse,
            ),
        )
        return response

    monkeypatch.setattr(
        CustomApiRuntimeServiceClient,
        "describe_custom_api_environment"
        if describe
        else "acquire_custom_api_environment",
        send,
    )
    async with PlatformTransport("http://127.0.0.1:1") as transport:
        sender = RunPlatformClient(
            tenant_id="tenant",
            credentials=Credentials(),
            proofs=Proofs(),
            transport=transport,
            frozen_custom_api_profiles=(("profile", 3),),
        )
        api = PlatformCustomApiClient(
            sender=sender,
            tenant_id="tenant",
            run_id="run",
            execution_session_id="session",
        )
        method = api.describe if describe else api.acquire
        await method(REFS, deadline_monotonic=time.monotonic() + 5)
        await method(REFS, deadline_monotonic=time.monotonic() + 5)
    assert calls[0][0] != calls[1][0]
    assert calls[0][1] != calls[1][1]
    assert (
        operations
        == [
            "custom_api.describe_environment"
            if describe
            else "custom_api.acquire_environment"
        ]
        * 2
    )


@pytest.mark.parametrize(
    "mutation",
    ["name_bytes", "name_blank", "instructions_bytes", "nul", "names", "reserved"],
)
async def test_describe_metadata_matches_owner_utf8_limits(mutation: str) -> None:
    def mutate(response: Response) -> None:
        assert isinstance(response, pb.DescribeCustomApiEnvironmentResponse)
        profile = response.profiles[0]
        if mutation == "name_bytes":
            profile.name = "界" * 134
        elif mutation == "name_blank":
            profile.name = "  "
        elif mutation == "instructions_bytes":
            profile.instructions = "界" * 1334
        elif mutation == "nul":
            profile.instructions = "text\x00"
        elif mutation == "reserved":
            profile.environment_names = ["PATH"]
        else:
            profile.environment_names = [f"API_{i}" for i in range(129)]

    with pytest.raises(CustomApiClientError):
        await client(Sender(mutate)).describe(
            REFS, deadline_monotonic=time.monotonic() + 5
        )


@pytest.mark.parametrize("mutation", ["binding", "unknown", "late", "count"])
async def test_failed_material_is_cleared_before_exception(mutation: str) -> None:
    responses: list[pb.AcquireCustomApiEnvironmentResponse] = []

    class MaterialSender(Sender):
        async def send(
            self, request: PlatformRequest, *, timeout_s: float = 10
        ) -> object:
            response = await super().send(request, timeout_s=timeout_s)
            assert isinstance(response, pb.AcquireCustomApiEnvironmentResponse)
            if mutation == "binding":
                response.run_id = "wrong"
            elif mutation == "unknown":
                response = pb.AcquireCustomApiEnvironmentResponse.from_binary(
                    response.to_binary() + b"\x98\x06\x01"
                )
            elif mutation == "count":
                response.environment *= 129
            else:
                # A sender can swallow cancellation; the boundary must reject late data.
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    pass
            responses.append(response)
            return response

    with pytest.raises(CustomApiClientError) as caught:
        await client(MaterialSender()).acquire(
            REFS, deadline_monotonic=time.monotonic() + 0.01
        )
    assert responses and not responses[0].environment
    assert caught.value.__context__ is None
    trace = caught.value.__traceback__
    while trace:
        if trace.tb_frame.f_code.co_name == "_send":
            assert trace.tb_frame.f_locals["response"] is None
        trace = trace.tb_next


@pytest.mark.parametrize("describe", [False, True])
@pytest.mark.parametrize(
    "selection",
    ["unknown", "extra", "missing", "version", "empty", "unfrozen", "duplicate_frozen"],
)
async def test_transport_custom_api_requests_are_exact_frozen_run_selection_before_any_io(
    describe: bool, selection: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kokoro_agent.clients.platform_transport import (
        PlatformCallError,
        PlatformTransport,
        RunPlatformClient,
    )

    calls: list[str] = []

    class Credentials:
        async def credential(self, tenant_id: str) -> str:
            calls.append("credential")
            return "TOKEN"

    class Proofs:
        async def issue(self, operation: str, request_binding_sha256: str) -> str:
            calls.append("proof")
            return "proof"

    async def send(
        *args: object, **kwargs: object
    ) -> pb.AcquireCustomApiEnvironmentResponse:
        calls.append("send")
        return pb.AcquireCustomApiEnvironmentResponse()

    monkeypatch.setattr(PlatformTransport, "send", send)
    profiles = [("p1", 1), ("p2", 2)]
    frozen = tuple(profiles)
    if selection == "unknown":
        profiles = [("unknown", 1), ("p2", 2)]
    elif selection == "extra":
        profiles.append(("p3", 1))
    elif selection == "missing":
        profiles.pop()
    elif selection == "version":
        profiles[0] = ("p1", 2)
    elif selection == "empty":
        profiles = []
    elif selection == "unfrozen":
        frozen = ()
    else:
        frozen = (("p1", 1), ("p1", 1), ("p2", 2))
    cls = (
        pb.DescribeCustomApiEnvironmentRequest
        if describe
        else pb.AcquireCustomApiEnvironmentRequest
    )
    request = cls(
        request_id="request",
        invocation_id="call",
        profiles=[
            pb.CustomApiRuntimeProfileRef(
                profile_id=pid, expected_state_version=version
            )
            for pid, version in profiles
        ],
    )
    async with PlatformTransport("http://127.0.0.1:1") as transport:
        sender = RunPlatformClient(
            tenant_id="tenant",
            credentials=Credentials(),
            proofs=Proofs(),
            transport=transport,
            frozen_custom_api_profiles=frozen,
        )
        with pytest.raises((PlatformCallError, ValueError)):
            await sender.send(request)
    assert calls == []


@pytest.mark.parametrize("describe", [False, True])
async def test_transport_exact_frozen_profile_set_allows_order_independence(
    describe: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kokoro_agent.clients.platform_transport import (
        PlatformTransport,
        RunPlatformClient,
    )

    calls: list[str] = []

    class Credentials:
        async def credential(self, tenant_id: str) -> str:
            calls.append("credential")
            return "TOKEN"

    class Proofs:
        async def issue(self, operation: str, request_binding_sha256: str) -> str:
            calls.append("proof")
            return "proof"

    async def send(
        *args: object, **kwargs: object
    ) -> pb.AcquireCustomApiEnvironmentResponse:
        calls.append("send")
        return pb.AcquireCustomApiEnvironmentResponse()

    monkeypatch.setattr(PlatformTransport, "send", send)
    cls = (
        pb.DescribeCustomApiEnvironmentRequest
        if describe
        else pb.AcquireCustomApiEnvironmentRequest
    )
    request = cls(
        request_id="request",
        invocation_id="call",
        profiles=[
            pb.CustomApiRuntimeProfileRef(profile_id="p2", expected_state_version=2),
            pb.CustomApiRuntimeProfileRef(profile_id="p1", expected_state_version=1),
        ],
    )
    async with PlatformTransport("http://127.0.0.1:1") as transport:
        sender = RunPlatformClient(
            tenant_id="tenant",
            credentials=Credentials(),
            proofs=Proofs(),
            transport=transport,
            frozen_custom_api_profiles=(("p1", 1), ("p2", 2)),
        )
        await sender.send(request)
    assert calls == ["proof", "credential", "send"]
