from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeVar

import pytest
from langchain_core.messages import AIMessage, HumanMessage
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
    blocks: Sequence[Mapping[str, object]]
    output_message: AIMessage | None
    namespace: list[str] = field(default_factory=_empty_ns)
    node: str | None = "model"

    def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
        return _aiter(self.blocks)


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


@dataclass
class _FakeAgent:
    run: _RunStream = field(default_factory=_RunStream)
    state: _State = field(default_factory=_State)
    raise_on_stream: Exception | None = None
    seen_config: dict[str, object] = field(default_factory=lambda: {})

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
        return self.run

    async def aget_state(self, config: RunnableConfig) -> _State:
        return self.state


def _text_model(text: str, *, msg_id: str = "seg") -> _Model:
    return _Model(blocks=(), output_message=AIMessage(content=text, id=msg_id))


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
    assert _events(bus.published) == ["agent_status", "text_chunk", "agent_done"]
    assert agent.seen_config == {"configurable": {"thread_id": "c1"}}
    assert _data(bus.published[-1][1]) == {"status": "completed", "usage": {}}


@pytest.mark.asyncio
async def test_usage_aggregated_into_done() -> None:
    model = _Model(
        blocks=(),
        output_message=AIMessage(
            content="x",
            id="seg",
            usage_metadata={"input_tokens": 3, "output_tokens": 5, "total_tokens": 8},
        ),
    )
    bus = _FakeBus()
    await invoke_once(bus, _FakeAgent(run=_RunStream(models=(model,))), "r1", "c1", {"messages": []})
    assert _data(bus.published[-1][1]) == {
        "status": "completed",
        "usage": {"input_tokens": 3, "output_tokens": 5, "total_tokens": 8},
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
    assert _data(bus.published[-1][1]) == {
        "status": "awaiting_approval",
        "segment_id": "seg-1",
        "pending": [{"tool_id": "call-A", "name": "danger", "args": {"x": 1}}],
    }


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
