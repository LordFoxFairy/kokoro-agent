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
from kokoro_agent.contract import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.policy import ModelConfig
from kokoro_agent.tools.toolbox import ProcessToolbox
from kokoro_agent.worker.dependencies import WorkerClients, WorkerDependencies
from support.fakes import FakeRunRepository
from support.local_fake import LocalFakeChatModel


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


def _factory(monkeypatch: pytest.MonkeyPatch) -> AgentFactory:
    # The deterministic model is a test driver, not a production configuration option.
    # Inject it at the test boundary while exercising the real AgentFactory/DeepAgents path.
    def test_model(_settings: ChatModelSettings, _model: ModelConfig) -> BaseChatModel:
        return LocalFakeChatModel()

    monkeypatch.setattr(agent_factory_module, "make_chat_model", test_model)
    config = AppConfig.from_env({})
    clients = WorkerClients()
    return AgentFactory(
        WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            run_token_budget=config.run_token_budget,
            subagent_catalog=build_subagent_catalog(None),
            toolbox=ProcessToolbox(configured=()),
            checkpointer=InMemorySaver(),
            run_repository=FakeRunRepository(),
            memory_store=InMemoryStore(),
            skill_client=clients.skill_client,
            skill_reader=clients.skill_reader,
            mcp_client=clients.mcp,
            delivery=clients.delivery,
        )
    )


@pytest.mark.parametrize("feature_key", ["chat", "music", "music_chat"])
async def test_builds_native_agent_without_external_clients(
    feature_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle = await _factory(monkeypatch).build(_request(feature_key))

    assert callable(handle.runnable.astream_events)
    assert callable(handle.runnable.aget_state)
    assert "deliver" not in handle.tool_descriptions


async def test_invokes_native_agent_without_external_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The local path exercises the actual DeepAgents loop, not just construction."""
    handle = await _factory(monkeypatch).build(_request("chat"))

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
        output.startswith("本地预览：DeepAgents 活动流已接通")
        for output in outputs[0]
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


async def _drain(values: AsyncIterable[object]) -> None:
    async for _ in values:
        pass
