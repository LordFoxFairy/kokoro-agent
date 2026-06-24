from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

import pytest
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Interrupt
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamItem, StreamProtocol
from kokoro_agent.application.run.invoke import events_stream, invoke_once

_T = TypeVar("_T")


async def _aiter(items: Sequence[_T]) -> AsyncIterator[_T]:
    for item in items:
        yield item


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, JsonValue]]] = []

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        self.published.append((stream, dict(event)))
        return StreamItem(cursor=str(len(self.published)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return []

    def subscribe(self, stream: str, from_cursor: str | None = None) -> AsyncIterator[StreamItem]:
        return _aiter([])


def _empty_ns() -> list[str]:
    return []


@dataclass
class _Model:
    text_deltas: Sequence[str] = ()
    reasoning_deltas: Sequence[str] = ()
    output_message: AIMessage | None = None
    message_id: str | None = "seg"
    namespace: list[str] = field(default_factory=_empty_ns)
    node: str | None = "model"

    @property
    def text(self) -> AsyncIterator[str]:
        return _aiter(self.text_deltas)

    @property
    def reasoning(self) -> AsyncIterator[str]:
        return _aiter(self.reasoning_deltas)


@dataclass
class _RunStream:
    models: Sequence[_Model] = ()
    is_interrupted: bool = False

    @property
    def messages(self) -> AsyncIterator[_Model]:
        return _aiter(self.models)

    @property
    def tool_calls(self) -> AsyncIterator[object]:
        return _aiter([])

    @property
    def subagents(self) -> AsyncIterator[object]:
        return _aiter([])

    @property
    def custom(self) -> AsyncIterator[object]:
        return _aiter([])

    async def interrupted(self) -> bool:
        return self.is_interrupted

    async def __aenter__(self) -> "_RunStream":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


@dataclass
class _State:
    values: Mapping[str, object] = field(default_factory=lambda: {})
    interrupts: tuple[Interrupt, ...] = ()


class _UsageFake(BaseChatModel):
    # 吐 usage_metadata + model_name 的真 model：被调用时触发 on_llm_end，供 usage callback 聚合。
    tokens: tuple[int, int, int] = (0, 0, 0)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        i, o, t = self.tokens
        msg = AIMessage(
            content="u",
            usage_metadata={"input_tokens": i, "output_tokens": o, "total_tokens": t},
            response_metadata={"model_name": "fake-model"},
        )
        return ChatResult(generations=[ChatGeneration(message=msg)])

    @property
    def _llm_type(self) -> str:
        return "usage-fake"


@dataclass
class _FakeAgent:
    run: _RunStream = field(default_factory=_RunStream)
    state: _State = field(default_factory=_State)
    raise_on_stream: Exception | None = None
    seen_config: dict[str, object] = field(default_factory=lambda: {})
    # 每元素触发一次吐 usage 的 model 调用（在 invoke_once 的 usage callback 上下文内）。
    model_usages: Sequence[tuple[int, int, int]] = ()

    async def astream_events(
        self,
        payload: object,
        *,
        version: str,
        config: RunnableConfig,
        transformers: Sequence[object],
    ) -> _RunStream:
        self.seen_config.update(config)
        if self.raise_on_stream is not None:
            raise self.raise_on_stream
        for tokens in self.model_usages:
            await _UsageFake(tokens=tokens).ainvoke("x")
        return self.run

    async def aget_state(self, config: RunnableConfig) -> _State:
        return self.state


def _text_model(text: str, *, msg_id: str = "seg") -> _Model:
    return _Model(text_deltas=(text,), output_message=AIMessage(content=text, id=msg_id), message_id=msg_id)


def _events(published: list[tuple[str, dict[str, JsonValue]]]) -> list[JsonValue]:
    return [event["event"] for _, event in published]


def _data(event: dict[str, JsonValue]) -> Mapping[str, JsonValue]:
    data = event["data"]
    assert isinstance(data, Mapping)
    return data


@pytest.mark.asyncio
async def test_first_event_is_agent_status_started() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(run=_RunStream(models=(_text_model("hi"),)))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    stream, first = bus.published[0]
    assert stream == events_stream("r1")
    assert first["event"] == "agent_status"
    assert first["request_id"] == "r1"
    assert _data(first) == {"status": "started"}


@pytest.mark.asyncio
async def test_order_started_text_done() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(run=_RunStream(models=(_text_model("hello"),)))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    events = _events(bus.published)
    assert events[0] == "agent_status"
    assert events[-1] == "agent_done"
    assert "text_chunk" in events
    assert agent.seen_config == {"configurable": {"thread_id": "c1"}}
    assert _data(bus.published[-1][1]) == {"status": "completed", "usage": {}}


@pytest.mark.asyncio
async def test_text_and_reasoning_channels() -> None:
    model = _Model(
        text_deltas=("hel", "lo"),
        reasoning_deltas=("think",),
        output_message=AIMessage(content="hello", id="seg"),
        message_id="seg",
    )
    bus = _FakeBus()
    await invoke_once(bus, _FakeAgent(run=_RunStream(models=(model,))), "r1", "c1", {"messages": []})
    by_event = [(e["event"], _data(e)) for _, e in bus.published]
    assert ("reasoning_chunk", {"segment_id": "seg", "text": "think", "final": False}) in by_event
    # 两通道对称：text 与 reasoning 都发终态帧。
    text_finals = [d for ev, d in by_event if ev == "text_chunk" and d.get("final")]
    reasoning_finals = [d for ev, d in by_event if ev == "reasoning_chunk" and d.get("final")]
    assert text_finals and text_finals[-1]["text"] == "hello"
    assert reasoning_finals and reasoning_finals[-1]["text"] == "think"


@pytest.mark.asyncio
async def test_usage_aggregated_into_done() -> None:
    # 两次 model 调用(主 + 模拟子代理)经 usage callback 跨调用聚合，扁平 total 进 agent_done。
    bus = _FakeBus()
    agent = _FakeAgent(
        run=_RunStream(models=(_text_model("x"),)),
        model_usages=((3, 5, 8), (1, 2, 3)),
    )
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    assert _data(bus.published[-1][1]) == {
        "status": "completed",
        "usage": {"input_tokens": 4, "output_tokens": 7, "total_tokens": 11},
    }


def _interrupt_state(
    action_requests: list[dict[str, JsonValue]], tool_calls: list[dict[str, object]]
) -> _State:
    messages = [HumanMessage(content="go"), AIMessage(content="", tool_calls=tool_calls, id="seg-1")]
    interrupt = Interrupt(value={"action_requests": list(action_requests)})
    return _State(values={"messages": messages}, interrupts=(interrupt,))


@pytest.mark.asyncio
async def test_pending_interrupt_emits_awaiting_no_done() -> None:
    agent = _FakeAgent(
        run=_RunStream(is_interrupted=True),
        state=_interrupt_state(
            [{"name": "danger", "args": {"x": 1}, "description": "do danger"}],
            [{"name": "danger", "args": {"x": 1}, "id": "call-A"}],
        ),
    )
    bus = _FakeBus()
    result = await invoke_once(bus, agent, "r1", "c1", {"messages": []}, frozenset({"danger"}))
    assert result is False
    assert "agent_done" not in _events(bus.published)
    last_event, last_data = bus.published[-1][1]["event"], _data(bus.published[-1][1])
    assert last_event == "tool_call_awaiting"
    assert last_data == {"segment_id": "seg-1", "tool_id": "call-A", "name": "danger", "args": {"x": 1}}


@pytest.mark.asyncio
async def test_no_pending_interrupt_ends_with_done() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(run=_RunStream(models=(_text_model("hi"),)))
    result = await invoke_once(bus, agent, "r1", "c1", {"messages": []}, frozenset({"danger"}))
    assert result is True
    assert _events(bus.published)[-1] == "agent_done"


@pytest.mark.asyncio
async def test_exception_emits_agent_error() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(raise_on_stream=ValueError("boom"))
    result = await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    assert result is True
    assert _events(bus.published) == ["agent_status", "agent_error"]
    assert _data(bus.published[-1][1]) == {"error_kind": "ValueError", "message": "boom"}


def test_events_stream_format() -> None:
    assert events_stream("abc") == "kokoro:run:abc:events"


def test_fake_bus_is_stream_protocol() -> None:
    assert isinstance(_FakeBus(), StreamProtocol)


@pytest.mark.asyncio
async def test_trace_config_merged() -> None:
    from langchain_core.callbacks import BaseCallbackHandler

    class _Handler(BaseCallbackHandler):
        pass

    handler = _Handler()
    trace: RunnableConfig = {
        "callbacks": [handler],
        "metadata": {"langfuse_session_id": "s1", "kokoro_run_id": "r1"},
    }
    bus = _FakeBus()
    agent = _FakeAgent(run=_RunStream(models=(_text_model("hi"),)))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []}, trace=trace)
    assert agent.seen_config.get("configurable") == {"thread_id": "c1"}
    assert agent.seen_config.get("callbacks") == [handler]
    assert agent.seen_config.get("metadata") == {"langfuse_session_id": "s1", "kokoro_run_id": "r1"}


@pytest.mark.asyncio
async def test_trace_none_config_only_configurable() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(run=_RunStream(models=(_text_model("hi"),)))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []}, trace=None)
    assert agent.seen_config.get("configurable") == {"thread_id": "c1"}
    assert "callbacks" not in agent.seen_config
    assert "metadata" not in agent.seen_config
