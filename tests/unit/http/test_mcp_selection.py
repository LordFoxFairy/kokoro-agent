"""Exact MCP choice is immutable intent, never a trace or an authorization grant."""

import json

import pytest
from pydantic import ValidationError

from kokoro_agent.interfaces.http.ingress import (
    AgentIngress,
    IngressError,
)
from kokoro_agent.application.chat.service import ChatService
from kokoro_agent.protocol import RunRequest, inbound_adapter
from support.chat import FakeChatRepository
from support.fakes import FakeBus, FakeRunRepository
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef


def identity() -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref="subject"),
        identity_assertion_ref="assertion",
    )


def _body() -> dict[str, object]:
    return {
        "request_id": "request-legacy",
        "run_id": "legacy",
        "session_id": "session",
        "feature_key": "chat",
        "project_ref": None,
        "selected_skill_source_refs": [],
        "message_id": "message",
        "content": "hello",
    }


async def _admit(body: dict[str, object]) -> tuple[RunRequest, str]:
    repository = FakeRunRepository()
    ingress = AgentIngress(
        bus=FakeBus(),
        run_repository=repository,
        chat_service=ChatService(FakeChatRepository()),
    )
    await ingress.launch(body, execution_identity=identity())
    return repository.dispatch_requests["legacy"], repository.dispatch_fences["legacy"]


LEGACY_JSON = '{"kind":"run.request","request_id":"request-legacy","run_id":"legacy","session_id":"session","feature_key":"chat","project_ref":null,"selected_skill_source_refs":[],"execution_identity":{"tenant_ref":"tenant","actor":{"kind":"user","opaque_ref":"actor"},"subject":{"kind":"user","opaque_ref":"subject"},"identity_assertion_ref":"assertion"},"input":{"message_id":"message","content":"hello"},"requested_model_label":null,"trace":null}'
LEGACY_FENCE = "sha256:e2f309d47b13c7c0f9b1d92797d1f15a79cdf282bcc9c0d6ec7876fcccc15fd2"


@pytest.mark.parametrize("explicit", [False, True])
async def test_empty_choice_preserves_published_request_bytes_and_fence(
    explicit: bool,
) -> None:
    body = _body()
    if explicit:
        body["selected_mcp_connection_ids"] = []
    request, fence = await _admit(body)
    assert request.selected_mcp_connection_ids == ()
    assert request.model_dump_json() == LEGACY_JSON
    assert fence == LEGACY_FENCE
    assert RunRequest.model_validate_json(LEGACY_JSON).model_dump_json() == LEGACY_JSON
    assert inbound_adapter.validate_json(LEGACY_JSON) == request


async def test_nonempty_choice_roundtrips_in_original_order_and_changes_fence() -> None:
    body = {**_body(), "selected_mcp_connection_ids": ["connection-b", "connection-a"]}
    request, fence = await _admit(body)
    assert request.selected_mcp_connection_ids == ("connection-b", "connection-a")
    assert (
        json.loads(request.model_dump_json())["selected_mcp_connection_ids"]
        == body["selected_mcp_connection_ids"]
    )
    assert inbound_adapter.validate_json(request.model_dump_json()) == request
    assert fence != LEGACY_FENCE
    with pytest.raises(ValidationError):
        request.selected_mcp_connection_ids = ()


@pytest.mark.parametrize(
    "value",
    [
        None,
        "connection-a",
        1,
        {},
        [1],
        [True],
        [None],
        [""],
        [" a"],
        ["a\n"],
        ["https://host/mcp"],
        [{"connection_id": "a"}],
        ["a", "a"],
        ["a" * 192],
        [f"c{i}" for i in range(17)],
    ],
)
async def test_malformed_choices_fail_at_admission(value: object) -> None:
    with pytest.raises(IngressError) as error:
        await _admit({**_body(), "selected_mcp_connection_ids": value})
    assert (error.value.status, error.value.code) == (400, "invalid_launch_request")


def test_direct_worker_choice_uses_same_strict_boundary() -> None:
    raw = json.loads(LEGACY_JSON)
    raw["selected_mcp_connection_ids"] = ["connection-1"]
    request = inbound_adapter.validate_python(raw)
    assert isinstance(request, RunRequest)
    assert request.selected_mcp_connection_ids == ("connection-1",)
    for invalid in (["x", "x"], [1], {"connection-1"}, ["a\n"]):
        with pytest.raises(ValidationError):
            inbound_adapter.validate_python(
                {**raw, "selected_mcp_connection_ids": invalid}
            )


async def test_exact_choice_is_durable_intent_and_same_run_drift_conflicts() -> None:
    repository, bus = FakeRunRepository(), FakeBus()
    ingress = AgentIngress(
        bus=bus,
        run_repository=repository,
        chat_service=ChatService(FakeChatRepository()),
    )
    body = {**_body(), "selected_mcp_connection_ids": ["connection-b", "connection-a"]}
    assert not (await ingress.launch(body, execution_identity=identity())).replayed
    assert (
        await repository.claim_dispatch(
            repository.dispatch_requests["legacy"], "worker"
        )
        is not None
    )
    assert (await ingress.launch(body, execution_identity=identity())).replayed
    assert repository.dispatch_requests["legacy"].selected_mcp_connection_ids == (
        "connection-b",
        "connection-a",
    )
    assert len(bus.published) == 1
    assert (
        bus.published[0][1]["selected_mcp_connection_ids"]
        == body["selected_mcp_connection_ids"]
    )
    for changed in ([], ["connection-a", "connection-b"], ["connection-c"]):
        with pytest.raises(IngressError) as error:
            await ingress.launch(
                {**body, "selected_mcp_connection_ids": changed},
                execution_identity=identity(),
            )
        assert error.value.status == 409
    assert len(bus.published) == 1
