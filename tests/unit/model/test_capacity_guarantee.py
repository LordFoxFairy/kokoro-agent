"""Actual finite guarantee bytes and final-wire rejection, with no provider I/O."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest
import rfc8785
from pydantic import JsonValue
from typing import cast

from kokoro_agent.clients.system import ModelConnection, ResolvedModel
from kokoro_agent.model.call_models import ModelCallInputInvalid
from kokoro_agent.model.capacity_guarantee import (
    ENDPOINT,
    MODEL,
    CapacityGuaranteeInvalid,
    load_capacity_guarantee,
    matches_capacity_route,
    parse_capacity_guarantee,
    require_capacity_time,
    validate_capacity_request,
)

ROOT = Path(__file__).resolve().parents[3]


def route() -> ResolvedModel:
    return ResolvedModel(
        model_id="m",
        provider_id="provider-id",
        revision_id="r",
        revision=1,
        digest="a" * 64,
        generation="1",
        tenant_generation="1",
        provider_model_name=MODEL,
        request_model=MODEL,
        feature_key="chat",
        label_key="qa",
        context_window=128000,
        max_output_tokens=128,
        connection=ModelConnection(
            protocol="openai_chat_completions",
            connection_type="direct",
            base_url="https://api.openai.com/v1",
            secret_handle_ref="qa",
        ),
    )


def body() -> dict[str, JsonValue]:
    return {
        "model": MODEL,
        "messages": [{"role": "user", "content": "你好"}],
        "n": 1,
        "max_completion_tokens": 128,
        "stream": False,
        "store": False,
        "service_tier": "default",
    }


def validate(value: JsonValue, selected: ResolvedModel | None = None) -> int:
    raw = rfc8785.dumps(value)
    return validate_capacity_request(
        route=selected or route(),
        raw=raw,
        method="POST",
        endpoint=ENDPOINT,
        headers=(
            ("Host", "api.openai.com"),
            ("Content-Length", str(len(raw))),
            ("Content-Type", "application/json"),
            ("Accept", "application/json"),
        ),
        guarantee=load_capacity_guarantee(),
    )


def test_formal_canonical_bytes_load_and_exact_digest() -> None:
    g = load_capacity_guarantee()
    assert parse_capacity_guarantee(rfc8785.dumps(g.model_dump(mode="json"))) == g
    assert g.context_window == 128000 and g.max_output_tokens == 16384
    assert validate(body()) == 128


@pytest.mark.parametrize(
    "mutation",
    [
        {"owner": "caller"},
        {"artifact_version": "2.0.0"},
        {"context_window": True},
        {"context_window": 128001},
        {"max_output_tokens": 16385},
        {"service_tier": "auto"},
        {"stream": True},
        {"extra": None},
        {"sunset": "2028-01-01T00:00:00Z"},
        {"evidence_urls": ["https://caller.invalid"]},
        {"trust_class": "mock"},
    ],
)
def test_guarantee_identity_cannot_be_self_reported(
    mutation: dict[str, JsonValue],
) -> None:
    g = load_capacity_guarantee().model_dump(mode="json")
    with pytest.raises(
        CapacityGuaranteeInvalid, match="^capacity guarantee is invalid$"
    ):
        parse_capacity_guarantee(rfc8785.dumps({**g, **mutation}))


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"x" * 65537,
        b"\xff",
        b'{"owner":"kokoro-agent","owner":"kokoro-agent"}',
        b"[]",
        b"\xef\xbb\xbf{}",
        b"{" * 1000,
    ],
)
def test_guarantee_raw_rejects_budget_encoding_duplicates(raw: bytes) -> None:
    with pytest.raises(CapacityGuaranteeInvalid):
        parse_capacity_guarantee(raw)


@pytest.mark.parametrize(
    "mutation",
    cast(
        list[dict[str, JsonValue]],
        [
            {"tools": []},
            {"functions": []},
            {"prediction": {}},
            {"modalities": ["text"]},
            {"reasoning_effort": "minimal"},
            {"stream_options": {"include_usage": True}},
            {"service_tier": "auto"},
            {"store": True},
            {"stream": True},
            {"n": 2},
            {"n": True},
            {"max_completion_tokens": 129},
            {"max_completion_tokens": 0},
            {"max_completion_tokens": True},
            {"model": "gpt-4o"},
            {"messages": []},
            {"messages": [{"role": "tool", "content": "x"}]},
            {
                "messages": [
                    {"role": "user", "content": [{"type": "text", "text": "x"}]}
                ]
            },
            {"messages": [{"role": "assistant", "content": "x", "tool_calls": []}]},
            {"messages": [{"role": "user", "content": "x", "name": "extra"}]},
        ],
    ),
)
def test_closed_cost_profile_rejects_unsupported_final_wire(
    mutation: dict[str, JsonValue],
) -> None:
    with pytest.raises(
        ModelCallInputInvalid, match="^model capacity request is invalid$"
    ):
        validate({**body(), **mutation})


@pytest.mark.parametrize(
    "changes",
    [
        {"request_model": "gpt-4o"},
        {"provider_model_name": "other"},
        {"context_window": 128001},
        {"max_output_tokens": 16385},
    ],
)
def test_route_requires_exact_fixed_binding(changes: dict[str, JsonValue]) -> None:
    # Dataclass replacement here deliberately models a different authenticated route.
    assert not matches_capacity_route(replace(route(), **changes))


@pytest.mark.parametrize(
    "base,kind",
    [
        ("https://other.invalid/v1", "direct"),
        ("https://api.openai.com/v1/", "direct"),
        ("https://api.openai.com/v1", "litellm"),
    ],
)
def test_gateway_or_endpoint_is_not_official_profile(base: str, kind: str) -> None:
    selected = replace(
        route(),
        connection=ModelConnection.model_validate(
            {
                "protocol": "openai_chat_completions",
                "connection_type": kind,
                "base_url": base,
                "secret_handle_ref": "qa",
            }
        ),
    )
    assert not matches_capacity_route(selected)


def test_profile_sunset_is_not_renewable_and_no_implicit_clock() -> None:
    g = load_capacity_guarantee()
    require_capacity_time(g, datetime(2027, 1, 13, 23, 59, 59, tzinfo=timezone.utc))
    for value in [datetime(2027, 1, 14, tzinfo=timezone.utc), datetime(2026, 1, 1)]:
        with pytest.raises(CapacityGuaranteeInvalid):
            require_capacity_time(g, value)


def test_constructed_guarantee_cannot_override_the_formal_identity() -> None:
    genuine = load_capacity_guarantee()
    forged = genuine.model_copy(update={"max_output_tokens": 999999})
    with pytest.raises(ModelCallInputInvalid):
        raw = rfc8785.dumps(body())
        validate_capacity_request(
            route=route(),
            raw=raw,
            method="POST",
            endpoint=ENDPOINT,
            headers=(("Content-Type", "application/json"),),
            guarantee=forged,
        )


async def test_matching_cost_profile_real_owned_hook_still_harddenies_without_scope() -> (
    None
):
    """No freeze stub: the production scope gate stops before controlled transport."""
    import httpx
    import openai
    from pydantic import SecretStr
    from kokoro_agent.model.credentials import ModelCredential
    from kokoro_agent.model.call_models import ModelCallAuthorizationUnavailable
    from kokoro_agent.model.factory import (
        ChatModelSettings,
        ModelHttpClients,
        make_chat_model,
        model_from_route,
    )

    selected = route()
    settings = ChatModelSettings(
        disable_streaming=False,
        openai_api_key=None,
        openai_base_url=None,
        openai_reasoning=False,
        anthropic_api_key=None,
        anthropic_base_url=None,
        model_credentials=(
            ModelCredential(
                provider_id=selected.provider_id,
                secret_handle_ref="qa",
                api_key=SecretStr("not-a-real-key"),
            ),
        ),
    )
    sent: list[httpx.Request] = []

    async def peer(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        raise AssertionError("harddeny must precede transport")

    clients = ModelHttpClients(settings, selected)
    await clients.async_client.aclose()
    object.__setattr__(
        clients,
        "async_client",
        httpx.AsyncClient(
            transport=httpx.MockTransport(peer),
            event_hooks={"request": [getattr(clients, "_validate_async")]},
            trust_env=False,
            follow_redirects=False,
        ),
    )
    try:
        model = make_chat_model(
            settings, model_from_route(selected, None), model_clients=clients
        )
        with pytest.raises(openai.APIConnectionError) as failure:
            await model.ainvoke("hello")
        assert isinstance(failure.value.__cause__, ModelCallAuthorizationUnavailable)
        assert sent == []
    finally:
        await clients.aclose()


@pytest.mark.parametrize("invalid", ["tools", "evidence-load"])
@pytest.mark.parametrize("swallow", [False, True])
async def test_real_owned_sdk_capacity_denial_survives_wrapping_or_swallow(
    monkeypatch: pytest.MonkeyPatch,
    invalid: str,
    swallow: bool,
) -> None:
    """Real middleware/final hook; fake repository must never be prepared or sent."""
    import httpx
    from pydantic import SecretStr
    from langchain.agents.middleware import ModelRequest, ModelResponse
    from langgraph.runtime import ExecutionInfo, Runtime
    from kokoro_agent.model.credentials import ModelCredential
    from kokoro_agent.model.call_models import (
        ModelCallRepository,
        PrepareModelCall,
        PreparedModelCall,
    )
    from kokoro_agent.model.call_runtime import ModelCallRuntimeMiddleware
    from kokoro_agent.model.call_scope import ModelCallScope, bind_model_call_scope
    from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
    from kokoro_agent.model.factory import (
        ChatModelSettings,
        ModelHttpClients,
        make_chat_model,
        model_from_route,
    )
    from kokoro_agent.protocol import (
        RunRequest,
        ExecutionIdentity,
        IdentityRef,
        RunInput,
    )
    from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding
    import kokoro_agent.model.factory as factory

    from typing import Never

    class UnusedRepository(ModelCallRepository):
        prepared = 0

        async def _unexpected(self, *args: object, **kwargs: object) -> Never:
            raise AssertionError("invalid capacity cannot invoke repository")

        begin_authorization = _unexpected
        grant_authorization = _unexpected
        start_dispatch = _unexpected
        append_evidence = _unexpected
        read_final_evidence = _unexpected

        async def prepare(self, command: PrepareModelCall) -> PreparedModelCall:
            self.prepared += 1
            raise AssertionError("invalid capacity cannot prepare")

    selected = route()
    settings = ChatModelSettings(
        disable_streaming=False,
        openai_api_key=None,
        openai_base_url=None,
        openai_reasoning=False,
        anthropic_api_key=None,
        anthropic_base_url=None,
        model_credentials=(
            ModelCredential(
                provider_id=selected.provider_id,
                secret_handle_ref="qa",
                api_key=SecretStr("not-a-real-key"),
            ),
        ),
    )
    sent: list[httpx.Request] = []

    async def peer(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        raise AssertionError("invalid capacity must stop before transport")

    def unavailable():
        raise CapacityGuaranteeInvalid("capacity guarantee is invalid")

    clients = ModelHttpClients(settings, selected)
    await clients.async_client.aclose()
    object.__setattr__(
        clients,
        "async_client",
        httpx.AsyncClient(
            transport=httpx.MockTransport(peer),
            event_hooks={"request": [getattr(clients, "_validate_async")]},
            trust_env=False,
            follow_redirects=False,
        ),
    )
    try:
        model = make_chat_model(
            settings, model_from_route(selected, None), model_clients=clients
        )
        request = RunRequest(
            kind="run.request",
            run_id="capacity-run",
            session_id="capacity-session",
            feature_key="chat",
            project_ref=None,
            selected_skill_source_refs=(),
            execution_identity=ExecutionIdentity(
                tenant_ref="capacity-tenant",
                actor=IdentityRef(kind="user", opaque_ref="actor"),
                subject=IdentityRef(kind="user", opaque_ref="subject"),
                identity_assertion_ref="assertion",
            ),
            input=RunInput(message_id="capacity-message", content="hello"),
        )
        planned = PlannedBinding(
            binding_version="2.0.0",
            model=selected.model_id,
            provider=selected.provider_id,
            revision_id=selected.revision_id,
            revision=str(selected.revision),
            digest=selected.digest,
            generation=selected.generation,
            tenant_generation=selected.tenant_generation,
            provider_model_name=selected.provider_model_name,
            request_model=selected.request_model,
            feature_key=selected.feature_key,
            label=selected.label_key,
        )
        repository = UnusedRepository()
        middleware = ModelCallRuntimeMiddleware(
            repository=repository,
            request=request,
            lease=LeaseFence(owner="owned-worker", generation=1),
            planned=planned,
            recipe=StaticRecipeBinding(canonical_bytes=b"{}", fingerprint="a" * 64),
            peer_path=("main",),
        )
        model_request = ModelRequest(
            model=model,
            messages=[],
            runtime=Runtime(
                execution_info=ExecutionInfo(
                    checkpoint_id="checkpoint", checkpoint_ns="", task_id="task"
                )
            ),
        )
        wrapped: list[Exception] = []
        if invalid == "evidence-load":
            monkeypatch.setattr(factory, "load_capacity_guarantee", unavailable)

        async def handler(current: ModelRequest) -> ModelResponse:
            try:
                if invalid == "tools":
                    await current.model.ainvoke("hello", tools=[])
                else:
                    await current.model.ainvoke("hello")
            except Exception as error:
                wrapped.append(error)
                if not swallow:
                    raise
            return ModelResponse(result=[])

        with bind_model_call_scope(ModelCallScope(timeout_s=1)):
            with pytest.raises(
                ModelCallInputInvalid,
                match="^model capacity (request is invalid|guarantee is unavailable)$",
            ):
                await middleware.awrap_model_call(model_request, handler)
        assert len(wrapped) == 1
        assert repository.prepared == 0 and sent == []
    finally:
        await clients.aclose()


@pytest.mark.parametrize("asset", ["evidence.json", "normalization-manifest.json"])
def test_runtime_load_rejects_coordinated_reference_identity_drift(
    monkeypatch: pytest.MonkeyPatch, asset: str
) -> None:
    import json
    import hashlib
    import kokoro_agent.model.capacity_guarantee as capacity
    from kokoro_agent.distribution_assets import read_capacity_guarantee_assets

    assets = read_capacity_guarantee_assets()
    changed = json.loads(assets[asset])
    changed["owner"] = "self-reported"
    assets[asset] = json.dumps(changed).encode()
    manifest = json.loads(assets["manifest.json"])
    relative = (
        "contract/usage/v1/manifest.json"
        if asset == "normalization-manifest.json"
        else f"contract/model-call-capacity-guarantee/v1/{asset}"
    )
    manifest["source_sha256"][relative] = hashlib.sha256(assets[asset]).hexdigest()
    assets["manifest.json"] = json.dumps(manifest).encode()
    monkeypatch.setattr(capacity, "read_capacity_guarantee_assets", lambda: assets)
    with pytest.raises(
        CapacityGuaranteeInvalid, match="^capacity guarantee is invalid$"
    ):
        capacity.load_capacity_guarantee()
