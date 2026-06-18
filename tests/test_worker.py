from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, TypeAlias

import pytest
from _pytest.monkeypatch import MonkeyPatch
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from kokoro_agent.infrastructure.stream_port import JsonObject, JsonValue, MemoryStreamPort, StreamPort
from kokoro_agent.infrastructure.subagent_registry import RuntimeSubagentRegistry

from kokoro_agent.domain.agent_event import AgentEvent
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.chat_model import LOCAL_FAKE_MODEL_FLAG, make_chat_model
from kokoro_agent.infrastructure.local_fake_model import make_local_fake_chat_model
from kokoro_agent.interfaces.worker import (
    MAX_PROCESSED_RUN_IDS,
    REQUESTS_STREAM,
    ProcessedRunIds,
    events_stream,
    run_once,
    serve,
)


def _payload_text(value: JsonValue) -> str:
    assert isinstance(value, Mapping)
    text = value.get("text")
    assert isinstance(text, str)
    return text


def _payload_field(event: Mapping[str, JsonValue], key: str) -> str:
    payload = event["payload"]
    assert isinstance(payload, Mapping)
    value = payload.get(key)
    assert isinstance(value, str)
    return value


def _fake_model(*replies: str) -> BaseChatModel:
    return GenericFakeChatModel(
        messages=iter([AIMessage(content=text) for text in replies])
    )


def _request(run_id: str = "run_01") -> JsonObject:
    request = RunRequest(
        kind="run.request",
        run_id=run_id,
        session_id="ses_01",
        conversation_id="conv_01",
        input="hello kokoro",
    )
    return request.model_dump()


def test_processed_run_id_cache_is_bounded_fifo() -> None:
    processed = ProcessedRunIds()
    for i in range(MAX_PROCESSED_RUN_IDS + 1):
        processed.add(f"run_{i}")
    assert len(processed) == MAX_PROCESSED_RUN_IDS
    assert ("run_0" in processed) is False
    assert (f"run_{MAX_PROCESSED_RUN_IDS}" in processed) is True


async def test_run_once_streams_with_injected_model() -> None:
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request())

    processed = ProcessedRunIds()
    await run_once(port, processed, make_local_fake_chat_model())

    items = await port.read_all(events_stream("run_01"))
    kinds = [item.event["kind"] for item in items]
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert "todo.updated" in kinds
    assert "text.completed" in kinds

    seqs = [item.event["seq"] for item in items]
    assert seqs == list(range(1, len(seqs) + 1))


async def test_run_once_executes_the_built_in_now_tool() -> None:
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request("run_tool"))

    processed = ProcessedRunIds()
    await run_once(port, processed, make_local_fake_chat_model())

    items = await port.read_all(events_stream("run_tool"))
    kinds = [item.event["kind"] for item in items]
    assert "tool.invoked" in kinds
    invoked = next(item.event for item in items if item.event["kind"] == "tool.invoked")
    returned = next(item.event for item in items if item.event["kind"] == "tool.returned")
    assert _payload_field(invoked, "name") == "now"
    assert _payload_field(returned, "name") == "now"
    assert datetime.fromisoformat(_payload_field(returned, "result")).tzinfo is not None


async def test_run_once_is_idempotent_per_run_id() -> None:
    port = MemoryStreamPort()
    model = make_local_fake_chat_model()
    processed = ProcessedRunIds()

    await port.publish(REQUESTS_STREAM, _request())
    await run_once(port, processed, model)
    await port.publish(REQUESTS_STREAM, _request())
    await run_once(port, processed, model)

    items = await port.read_all(events_stream("run_01"))
    kinds = [item.event["kind"] for item in items]
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert kinds.count("run.started") == 1


async def test_run_once_rejects_malformed_request() -> None:
    port = MemoryStreamPort()
    await port.publish(
        REQUESTS_STREAM,
        {
            "kind": "run.request",
            "run_id": "run_bad",
            "session_id": "ses_01",
            "conversation_id": "conv_01",
        },
    )

    processed = ProcessedRunIds()
    await run_once(port, processed, _fake_model("unused"))

    items = await port.read_all(events_stream("run_bad"))
    assert [item.event["kind"] for item in items] == ["run.failed"]
    assert ("run_bad" in processed) is False


@pytest.mark.asyncio
async def test_run_once_streams_with_local_fake_model(
    monkeypatch: MonkeyPatch,
) -> None:
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request("run_local_fake"))

    monkeypatch.setenv(LOCAL_FAKE_MODEL_FLAG, "1")
    model = make_chat_model()

    processed = ProcessedRunIds()
    await run_once(port, processed, model)

    items = await port.read_all(events_stream("run_local_fake"))
    kinds = [item.event["kind"] for item in items]
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert "todo.updated" in kinds
    assert "text.completed" in kinds

    completed = next(item for item in items if item.event["kind"] == "text.completed")
    assert "本地预览" in _payload_text(completed.event["payload"])


@pytest.mark.asyncio
async def test_model_resolution_failure_emits_run_failed_and_loop_survives(
    monkeypatch: MonkeyPatch,
) -> None:
    port = MemoryStreamPort()
    processed = ProcessedRunIds()

    def broken_make_chat_model(execution_style: str = "fast") -> BaseChatModel:
        raise ValueError("Invalid KOKORO_MODEL spec: 'plainstring'")

    monkeypatch.setattr(
        "kokoro_agent.interfaces.worker.make_chat_model", broken_make_chat_model
    )

    await port.publish(REQUESTS_STREAM, _request("run_broken"))
    await run_once(port, processed, None)

    items = await port.read_all(events_stream("run_broken"))
    assert [item.event["kind"] for item in items] == ["run.failed"]
    payload = items[-1].event["payload"]
    assert isinstance(payload, Mapping)
    assert payload["error_kind"] == "ValueError"

    await port.publish(REQUESTS_STREAM, _request("run_after"))
    await run_once(port, processed, make_local_fake_chat_model())
    after = await port.read_all(events_stream("run_after"))
    assert [item.event["kind"] for item in after][-1] == "run.completed"


@pytest.mark.asyncio
async def test_run_once_resolves_model_from_request_execution_style(
    monkeypatch: MonkeyPatch,
) -> None:
    port = MemoryStreamPort()
    processed = ProcessedRunIds()
    seen_styles: list[str] = []
    await port.publish(
        REQUESTS_STREAM,
        {**_request(), "execution_style": "thinking"},
    )

    def fake_make_chat_model(execution_style: str = "fast") -> BaseChatModel:
        seen_styles.append(execution_style)
        return make_local_fake_chat_model()

    async def fake_run_agent(
        request: RunRequest, model: BaseChatModel, control_port: StreamPort | None = None, runtime_registry: RuntimeSubagentRegistry | None = None, checkpointer: BaseCheckpointSaver[str] | None = None
    ):
        yield AgentEvent(
            kind="run.completed",
            run_id="run_01",
            seq=1,
            payload={"status": "completed"},
        )

    monkeypatch.setattr("kokoro_agent.interfaces.worker.make_chat_model", fake_make_chat_model)
    monkeypatch.setattr("kokoro_agent.interfaces.worker.run_agent", fake_run_agent)

    await run_once(port, processed, None)

    assert seen_styles == ["thinking"]


@pytest.mark.asyncio
async def test_serve_runs_concurrently_a_blocked_run_does_not_freeze_others(
    monkeypatch: MonkeyPatch,
) -> None:
    port = MemoryStreamPort()
    release = asyncio.Event()

    async def fake_run_agent(
        request: RunRequest, model: BaseChatModel, control_port: StreamPort | None = None, runtime_registry: RuntimeSubagentRegistry | None = None, checkpointer: BaseCheckpointSaver[str] | None = None
    ):
        if request.run_id == "run_block":
            await release.wait()
        yield AgentEvent(kind="run.started", run_id=request.run_id, seq=1, payload={})
        yield AgentEvent(
            kind="run.completed",
            run_id=request.run_id,
            seq=2,
            payload={"status": "completed"},
        )

    def fake_make_chat_model(execution_style: str = "fast") -> BaseChatModel:
        return make_local_fake_chat_model()

    monkeypatch.setattr(
        "kokoro_agent.interfaces.worker.make_chat_model", fake_make_chat_model
    )
    monkeypatch.setattr("kokoro_agent.interfaces.worker.run_agent", fake_run_agent)

    async def completed(run_id: str) -> bool:
        items = await port.read_all(events_stream(run_id))
        return any(i.event.get("kind") == "run.completed" for i in items)

    serve_task = asyncio.create_task(serve(port))
    try:
        await port.publish(REQUESTS_STREAM, _request("run_block"))
        await port.publish(REQUESTS_STREAM, _request("run_fast"))
        async with asyncio.timeout(2):
            while not await completed("run_fast"):
                await asyncio.sleep(0.01)
        assert not await completed("run_block")
        release.set()
        async with asyncio.timeout(2):
            while not await completed("run_block"):
                await asyncio.sleep(0.01)
    finally:
        serve_task.cancel()


@pytest.mark.asyncio
async def test_serve_cancels_a_run_on_control_cancel(
    monkeypatch: MonkeyPatch,
) -> None:
    from kokoro_agent.infrastructure.control import control_stream

    port = MemoryStreamPort()
    hang = asyncio.Event()

    async def fake_run_agent(
        request: RunRequest, model: BaseChatModel, control_port: StreamPort | None = None, runtime_registry: RuntimeSubagentRegistry | None = None, checkpointer: BaseCheckpointSaver[str] | None = None
    ):
        yield AgentEvent(kind="run.started", run_id=request.run_id, seq=1, payload={})
        await hang.wait()
        yield AgentEvent(
            kind="run.completed",
            run_id=request.run_id,
            seq=2,
            payload={"status": "completed"},
        )

    def fake_make_chat_model(execution_style: str = "fast") -> BaseChatModel:
        return make_local_fake_chat_model()

    monkeypatch.setattr(
        "kokoro_agent.interfaces.worker.make_chat_model", fake_make_chat_model
    )
    monkeypatch.setattr("kokoro_agent.interfaces.worker.run_agent", fake_run_agent)

    async def has_kind(run_id: str, kind: str) -> bool:
        items = await port.read_all(events_stream(run_id))
        return any(i.event.get("kind") == kind for i in items)

    async def cancelled(run_id: str) -> bool:
        items = await port.read_all(events_stream(run_id))
        for i in items:
            if i.event.get("kind") == "run.completed":
                payload = i.event.get("payload")
                if not isinstance(payload, Mapping):
                    continue
                if payload.get("status") == "cancelled":
                    return True
        return False

    serve_task = asyncio.create_task(serve(port))
    try:
        await port.publish(REQUESTS_STREAM, _request("run_cancel"))
        async with asyncio.timeout(2):
            while not await has_kind("run_cancel", "run.started"):
                await asyncio.sleep(0.01)
        await port.publish(
            control_stream("run_cancel"), {"kind": "control", "decision": "cancel"}
        )
        async with asyncio.timeout(2):
            while not await cancelled("run_cancel"):
                await asyncio.sleep(0.01)
        items = await port.read_all(events_stream("run_cancel"))
        statuses = [
            _payload_field(i.event, "status")
            for i in items
            if i.event.get("kind") == "run.completed"
        ]
        assert statuses == ["cancelled"]
    finally:
        serve_task.cancel()


_MEMORY_PROBE_SEEN: list[list[str]] = []
_ToolLike: TypeAlias = dict[str, Any] | type | Callable[..., Any] | BaseTool


class _MemoryProbeModel(BaseChatModel):
    @property
    def _llm_type(self) -> str:
        return "memory-probe"

    def bind_tools(
        self,
        tools: Sequence[_ToolLike],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        return self.with_types(output_type=AIMessage)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        contents = [str(message.text) for message in messages]
        _MEMORY_PROBE_SEEN.append(contents)
        last = str(contents[-1]) if contents else ""
        if "我叫什么" in last:
            remembered = any("我的名字是 Nako" in str(item) for item in contents[:-1])
            reply = "Nako" if remembered else "我不知道"
        else:
            reply = "记住了"
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=reply))])


async def test_run_once_same_conversation_remembers_prior_turn_without_transport_replay(
    monkeypatch: MonkeyPatch,
) -> None:
    port = MemoryStreamPort()
    processed = ProcessedRunIds()
    _MEMORY_PROBE_SEEN.clear()

    monkeypatch.setattr(
        "kokoro_agent.interfaces.worker.make_chat_model", lambda execution_style="fast": _MemoryProbeModel()
    )

    await port.publish(
        REQUESTS_STREAM,
            {
                **_request("run_mem_1"),
                "conversation_id": "conv_mem",
                "input": "记住：我的名字是 Nako",
            },
    )
    await run_once(port, processed, None)

    await port.publish(
        REQUESTS_STREAM,
            {
                **_request("run_mem_2"),
                "conversation_id": "conv_mem",
                "input": "我叫什么？",
            },
    )
    await run_once(port, processed, None)

    items = await port.read_all(events_stream("run_mem_2"))
    completed = [item.event for item in items if item.event["kind"] == "text.completed"]
    assert completed, "expected a final text.completed on turn 2"
    assert _payload_field(completed[-1], "text") == "Nako"
