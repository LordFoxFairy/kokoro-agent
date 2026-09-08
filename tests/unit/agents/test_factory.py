"""AgentFactory 在无外部 owner client 时仍构造真实 DeepAgents Agent。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable

import pytest
from langchain_core.messages import HumanMessage
from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.stream import CustomTransformer
from langgraph.store.memory import InMemoryStore

import kokoro_agent.agent_factory as agent_factory_module
from kokoro_agent.agent_factory import AgentFactory
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.config import AppConfig
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.policy import ModelConfig
from kokoro_agent.tools.toolbox import ProcessToolbox
from kokoro_agent.worker.dependencies import WorkerClients, WorkerDependencies
from support.fakes import FakeRunRepository
from support.local_fake import LocalFakeChatModel
from kokoro_agent.clients.system import (
    ModelResolutionError,
    ModelResolver,
    ResolvedModel,
)


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
            delivery=clients.delivery,
            model_resolver=resolver,
        )
    ), repository


@pytest.mark.parametrize("feature_key", ["chat", "music", "music_chat"])
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
