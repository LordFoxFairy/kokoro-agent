"""AgentFactory 在无外部 owner client 时仍构造真实 DeepAgents Agent。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable

import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.language_models import BaseChatModel
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.stream import CustomTransformer
from langgraph.store.memory import InMemoryStore

import kokoro_agent.agent_factory as agent_factory_module
from kokoro_agent.agent_factory import AgentFactory
from kokoro_agent.features.catalog import FeatureCatalog, FEATURE_CATALOG
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.config import AppConfig
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.policy import ModelConfig
from kokoro_agent.tools.toolbox import ProcessToolbox
from kokoro_agent.worker.dependencies import WorkerClients, WorkerDependencies
from support.fakes import FakeRunRepository
from support.local_fake import LocalFakeChatModel
from kokoro_agent.clients.storage import (
    DeliveryClient,
    DeliveryReceipt,
    DeliveryRequest,
    DeliveryRecoveryRequest,
)
from kokoro_agent.clients.system import (
    ModelResolutionError,
    ModelResolver,
    ResolvedModel,
)
from kokoro_agent.clients.skills import SkillClientError


class RouteResolver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None, str | None]] = []

    async def resolve(
        self,
        *,
        tenant_id: str,
        feature_key: str,
        label: str | None,
        request_id: str | None,
    ) -> ResolvedModel:
        self.calls.append((tenant_id, feature_key, label, request_id))
        return ResolvedModel(
            model_id="model",
            provider_id="provider",
            revision_id="revision",
            revision=1,
            digest="a" * 64,
            generation="1",
            tenant_generation="1",
            provider_model_name="provider-name",
            gateway_model_name="gateway-name",
            feature_key=feature_key,
            label_key=label or "default",
        )


def _request(feature_key: str) -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id=f"run-{feature_key}",
        session_id="session",
        feature_key=feature_key,
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant",
            actor=IdentityRef(kind="user", opaque_ref="actor"),
            subject=IdentityRef(kind="user", opaque_ref="subject"),
            identity_assertion_ref="assertion",
        ),
        input=RunInput(message_id="message", content="hello"),
    )


def _factory(
    monkeypatch: pytest.MonkeyPatch,
    resolver: ModelResolver | None,
    catalog: FeatureCatalog = FEATURE_CATALOG,
    *,
    delivery: DeliveryClient | None = None,
) -> tuple[AgentFactory, FakeRunRepository]:
    # The deterministic model is a test driver, not a production configuration option.
    # Inject it at the test boundary while exercising the real AgentFactory/DeepAgents path.
    def test_model(_settings: ChatModelSettings, _model: ModelConfig) -> BaseChatModel:
        assert _model.provider == "litellm"
        assert _model.name == "gateway-name"
        return LocalFakeChatModel()

    monkeypatch.setattr(agent_factory_module, "make_chat_model", test_model)
    config = AppConfig.from_env({})
    clients = WorkerClients()
    repository = FakeRunRepository()
    return AgentFactory(
        WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            run_token_budget=config.run_token_budget,
            subagent_catalog=build_subagent_catalog(None),
            toolbox=ProcessToolbox(configured=()),
            checkpointer=InMemorySaver(),
            run_repository=repository,
            memory_store=InMemoryStore(),
            skill_client=clients.skill_client,
            skill_reader=clients.skill_reader,
            mcp_client=clients.mcp,
            delivery=delivery or clients.delivery,
            model_resolver=resolver,
        ),
        catalog,
    ), repository


@pytest.mark.parametrize("feature_key", ["chat"])
async def test_builds_native_agent_without_external_clients(
    feature_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    run_request = _request(feature_key)
    lease = await repository.try_claim(run_request)
    assert lease is not None
    handle = await factory.build(run_request, lease)

    assert callable(handle.runnable.astream_events)
    assert callable(handle.runnable.aget_state)
    assert "deliver" not in handle.tool_descriptions
    assert resolver.calls
    assert all(call[:3] == ("tenant", feature_key, None) for call in resolver.calls)


async def test_selected_skill_source_fails_before_any_external_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    request = _request("chat").model_copy(
        update={"selected_skill_source_refs": ("skill:exact-revision",)}
    )
    lease = await repository.try_claim(request)
    assert lease is not None
    called: list[str] = []

    async def forbidden(*args: object, **kwargs: object) -> None:
        called.append("backend")
        raise AssertionError("backend must not be built for unreadable Skill selection")

    monkeypatch.setattr(agent_factory_module, "make_backend_for_run", forbidden)
    monkeypatch.setattr(agent_factory_module, "build_toolset", forbidden)
    with pytest.raises(SkillClientError, match="typed skill source reader unavailable"):
        await factory.build(request, lease)
    assert resolver.calls == []
    assert called == []


async def test_invokes_native_agent_with_model_resolver_without_optional_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The local path exercises the actual DeepAgents loop, not just construction."""
    factory, repository = _factory(monkeypatch, RouteResolver())
    run_request = _request("chat")
    lease = await repository.try_claim(run_request)
    assert lease is not None
    handle = await factory.build(run_request, lease)

    run = await handle.runnable.astream_events(
        {"messages": [HumanMessage(content="hello")]},
        version="v3",
        config={"configurable": {"thread_id": "session"}},
        transformers=[CustomTransformer],
    )
    async with run:
        outputs = await asyncio.gather(
            _collect_messages(run.messages),
            _drain(run.tool_calls),
            _drain(run.subagents),
            _drain(run.custom),
        )
        assert await run.interrupted() is False

    assert any(
        output.startswith("本地预览：DeepAgents 活动流已接通") for output in outputs[0]
    )


async def _collect_messages(messages: AsyncIterable[object]) -> list[str]:
    outputs: list[str] = []
    async for model in messages:
        text = getattr(model, "text")
        reasoning = getattr(model, "reasoning")
        await asyncio.gather(_drain(text), _drain(reasoning))
        output_message = getattr(model, "output_message")
        if output_message is not None:
            outputs.append(str(output_message.text))
    return outputs


async def test_missing_resolver_fails_before_backend_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, repository = _factory(monkeypatch, None)
    request = _request("chat")
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(ModelResolutionError, match="MODEL_RESOLVER_NOT_CONFIGURED"):
        await factory.build(request, lease)


async def test_requested_label_is_resolved_and_not_interpreted_locally(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    request = _request("chat").model_copy(
        update={"requested_model_label": "opaque-label", "request_id": "request-1"}
    )
    lease = await repository.try_claim(request)
    assert lease is not None
    with caplog.at_level("INFO", logger="kokoro_agent.agent_factory"):
        await factory.build(request, lease)
    assert resolver.calls == [("tenant", "chat", "opaque-label", "request-1")]
    record = next(
        record for record in caplog.records if record.message == "model route resolved"
    )
    assert getattr(record, "revision_id") == "revision"
    assert getattr(record, "digest") == "a" * 64


async def test_owner_failure_is_not_replaced_by_a_local_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnavailableResolver(RouteResolver):
        async def resolve(
            self,
            *,
            tenant_id: str,
            feature_key: str,
            label: str | None,
            request_id: str | None,
        ) -> ResolvedModel:
            raise ModelResolutionError("MODEL_UNAVAILABLE", retryable=True)

    factory, repository = _factory(monkeypatch, UnavailableResolver())
    request = _request("chat")
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(ModelResolutionError, match="MODEL_UNAVAILABLE"):
        await factory.build(request, lease)


async def _drain(values: AsyncIterable[object]) -> None:
    async for _ in values:
        pass


async def test_all_peers_preflight_before_any_backend_or_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.features.definition import Feature
    from kokoro_agent.features.catalog import FeatureCatalog
    from kokoro_agent.clients.skills import SkillClientError

    feature = Feature(
        key="peer_gate",
        agents=(
            Agent(key="first", prompt="first"),
            Agent(key="second", prompt="second", skills=("required",)),
        ),
        entry_agent="first",
        handoffs=(("first", "second"),),
    )
    factory, repository = _factory(
        monkeypatch, RouteResolver(), FeatureCatalog((feature,))
    )
    calls: list[str] = []

    async def forbidden(*args: object, **kwargs: object) -> None:
        calls.append("backend")
        raise AssertionError("sandbox called before all peers passed")

    monkeypatch.setattr(agent_factory_module, "make_backend_for_run", forbidden)
    request = _request("peer_gate")
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(SkillClientError):
        await factory.build(request, lease)
    assert calls == []


@pytest.mark.parametrize("feature_key", ["music", "music_chat"])
async def test_declared_features_require_external_clients(
    feature_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kokoro_agent.clients.skills import SkillClientError

    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    request = _request(feature_key)
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(SkillClientError):
        await factory.build(request, lease)
    assert resolver.calls == []


class RecordingDelivery:
    """Storage boundary double; native workspace and tool loop remain real."""

    def __init__(self) -> None:
        self.requests: list[DeliveryRequest] = []

    async def recover(self, request: DeliveryRecoveryRequest) -> DeliveryReceipt | None:
        return None

    async def publish(self, request: DeliveryRequest) -> DeliveryReceipt:
        self.requests.append(request)
        return DeliveryReceipt(
            artifact_id="artifact-native",
            asset_id="asset-native",
            artifact_kind="document",
            content_sha256=request.content_sha256,
            size_bytes=len(request.content),
            mime_type=request.mime_type,
        )


async def _native_script(
    monkeypatch: pytest.MonkeyPatch,
    script: list[AIMessage],
    delivery: DeliveryClient,
    *,
    catalog: FeatureCatalog = FEATURE_CATALOG,
) -> tuple[list[BaseMessage], FakeRunRepository]:
    factory, repository = _factory(
        monkeypatch, RouteResolver(), catalog, delivery=delivery
    )

    def scripted_model(
        _settings: ChatModelSettings, _model: ModelConfig
    ) -> BaseChatModel:
        return LocalFakeChatModel.with_script(script)

    monkeypatch.setattr(agent_factory_module, "make_chat_model", scripted_model)
    request = _request("chat")
    lease = await repository.try_claim(request)
    assert lease is not None
    handle = await factory.build(request, lease)
    config: RunnableConfig = {"configurable": {"thread_id": request.session_id}}
    run = await handle.runnable.astream_events(
        {"messages": [HumanMessage(content="Create and deliver a workspace file.")]},
        version="v3",
        config=config,
        transformers=[CustomTransformer],
    )
    async with run:
        await asyncio.gather(
            _collect_messages(run.messages),
            _drain(run.tool_calls),
            _drain(run.subagents),
            _drain(run.custom),
        )
        assert await run.interrupted() is False
    state = await handle.runnable.aget_state(config)
    messages: list[BaseMessage] = state.values["messages"]
    return messages, repository


async def test_general_native_write_read_and_deliver_share_state_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    from kokoro_agent.tools.deliver import DeliverResult

    client = RecordingDelivery()
    messages, _repository = await _native_script(
        monkeypatch,
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_file",
                        "id": "write-1",
                        "args": {
                            "file_path": "/report.txt",
                            "content": "native workspace bytes",
                        },
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "read_file",
                        "id": "read-1",
                        "args": {"file_path": "/report.txt"},
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "deliver",
                        "id": "deliver-1",
                        "args": {"path": "/report.txt", "title": "Native report"},
                    }
                ],
            ),
            AIMessage(content="done"),
        ],
        client,
    )
    tools = {
        message.name: message
        for message in messages
        if isinstance(message, ToolMessage)
    }
    assert len(client.requests) == 1, {
        name: message.text for name, message in tools.items()
    }
    request = client.requests[0]
    assert request.content == b"native workspace bytes"
    assert request.tool_call_id == "deliver-1"
    assert request.lease is not None
    assert request.run_id == "run-chat"
    assert request.identity == _request("chat").execution_identity
    assert "native workspace bytes" in tools["read_file"].text
    result = DeliverResult.model_validate_json(tools["deliver"].text)
    assert result.artifact_id == "artifact-native"
    assert result.content_hash == hashlib.sha256(request.content).hexdigest()


async def test_general_workspace_write_does_not_make_skill_packages_writable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingDelivery()
    messages, _repository = await _native_script(
        monkeypatch,
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_file",
                        "id": "skill-write",
                        "args": {
                            "file_path": "/.skills/music/SKILL.md",
                            "content": "replace skill",
                        },
                    }
                ],
            ),
            AIMessage(content="done"),
        ],
        client,
    )
    writes = [
        message
        for message in messages
        if isinstance(message, ToolMessage) and message.name == "write_file"
    ]
    assert len(writes) == 1
    assert "permission" in writes[0].text.lower()
    assert client.requests == []


async def test_other_agent_native_write_is_still_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.features.definition import Feature

    restricted = Agent(key="restricted", prompt="Keep the default read-only policy.")
    catalog = FeatureCatalog(
        (Feature(key="chat", agents=(restricted,), entry_agent="restricted"),)
    )
    messages, _repository = await _native_script(
        monkeypatch,
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_file",
                        "id": "denied-write",
                        "args": {"file_path": "/report.txt", "content": "denied"},
                    }
                ],
            ),
            AIMessage(content="done"),
        ],
        RecordingDelivery(),
        catalog=catalog,
    )
    writes = [
        message
        for message in messages
        if isinstance(message, ToolMessage) and message.name == "write_file"
    ]
    assert len(writes) == 1
    assert "permission denied" in writes[0].text.lower()


@pytest.mark.parametrize("field", ["permissions", "filesystem", "agent", "backend"])
def test_run_wire_cannot_override_workspace_policy(field: str) -> None:
    from pydantic import ValidationError

    payload = _request("chat").model_dump()
    payload[field] = {"filesystem": "workspace_write"}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        RunRequest.model_validate(payload)
