from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field

import pytest
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StandardStreamEvent, StreamEvent
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamItem, StreamProtocol
from kokoro_agent.application.run.invoke import events_stream, invoke_once


class _FakeBus:
    """记录 publish 的 (stream, event)；read_all/subscribe 不在 invoke 路径，留空实现。"""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, JsonValue]]] = []

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        self.published.append((stream, dict(event)))
        return StreamItem(cursor=str(len(self.published)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return []

    def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
        return _empty()


async def _empty() -> AsyncIterator[StreamItem]:
    return
    yield  # pragma: no cover - 使函数成为 async generator


@dataclass(frozen=True)
class _FakeInterrupt:
    value: Mapping[str, JsonValue]


@dataclass(frozen=True)
class _FakeTask:
    interrupts: tuple[_FakeInterrupt, ...] = ()


@dataclass(frozen=True)
class _FakeState:
    tasks: tuple[_FakeTask, ...] = ()
    values: Mapping[str, object] = field(default_factory=lambda: {})


@dataclass
class _FakeAgent:
    events: Sequence[StreamEvent] = field(default_factory=tuple)
    state: _FakeState = field(default_factory=_FakeState)
    raise_on_stream: Exception | None = None
    seen_config: dict[str, object] = field(default_factory=lambda: {})

    async def astream_events(
        self, payload: object, *, version: str, config: RunnableConfig
    ) -> AsyncIterator[StreamEvent]:
        self.seen_config.update(config)
        if self.raise_on_stream is not None:
            raise self.raise_on_stream
        for event in self.events:
            yield event

    async def aget_state(self, config: RunnableConfig) -> _FakeState:
        return self.state


def _chat_stream_event(run_id: str, text: str) -> StandardStreamEvent:
    from langchain_core.messages import AIMessageChunk

    return {
        "event": "on_chat_model_stream",
        "name": "model",
        "run_id": run_id,
        "data": {"chunk": AIMessageChunk(content=text)},
        "metadata": {},
        "tags": [],
        "parent_ids": [],
    }


def _events(published: list[tuple[str, dict[str, JsonValue]]]) -> list[JsonValue]:
    return [event["event"] for _, event in published]


def _data(event: dict[str, JsonValue]) -> Mapping[str, JsonValue]:
    data = event["data"]
    assert isinstance(data, Mapping)
    return data


@pytest.mark.asyncio
async def test_first_event_is_agent_status_started() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hi"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    stream, first = bus.published[0]
    assert stream == events_stream("r1")
    assert first["event"] == "agent_status"
    assert first["request_id"] == "r1"
    assert _data(first) == {"status": "started"}


@pytest.mark.asyncio
async def test_normal_terminal_ends_with_agent_done() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hello"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    assert _events(bus.published)[0] == "agent_status"
    assert _events(bus.published)[-1] == "agent_done"
    assert _data(bus.published[-1][1]) == {"status": "completed", "usage": {}}


@pytest.mark.asyncio
async def test_projected_events_published_between_started_and_done() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hi"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    assert _events(bus.published) == ["agent_status", "text_chunk", "agent_done"]
    assert agent.seen_config == {"configurable": {"thread_id": "c1"}}


def _interrupt_state(
    action_requests: list[dict[str, JsonValue]], tool_calls: list[dict[str, object]]
) -> _FakeState:
    from langchain_core.messages import AIMessage, HumanMessage

    value: dict[str, JsonValue] = {"action_requests": list(action_requests)}
    interrupt = _FakeInterrupt(value=value)
    messages = [HumanMessage(content="go"), AIMessage(content="", tool_calls=tool_calls)]
    return _FakeState(
        tasks=(_FakeTask(interrupts=(interrupt,)),), values={"messages": messages}
    )


@pytest.mark.asyncio
async def test_pending_interrupt_emits_awaiting_status_no_done() -> None:
    state = _interrupt_state(
        action_requests=[{"name": "danger", "args": {"x": 1}, "description": "do danger"}],
        tool_calls=[{"name": "danger", "args": {"x": 1}, "id": "call-A"}],
    )
    bus = _FakeBus()
    # 缓存的 on_chat_model_stream run_id（"seg-r1"）即 segment_id。
    agent = _FakeAgent(events=(_chat_stream_event("seg-r1", "hi"),), state=state)
    await invoke_once(bus, agent, "r1", "c1", {"messages": []}, frozenset({"danger"}))
    assert "agent_done" not in _events(bus.published)
    assert _data(bus.published[-1][1]) == {
        "status": "awaiting_approval",
        "segment_id": "seg-r1",
        "pending": [{"tool_id": "call-A", "name": "danger", "args": {"x": 1}}],
    }


@pytest.mark.asyncio
async def test_pending_interrupt_filters_auto_approved_and_aligns() -> None:
    # 三个 tool_call，safe 未进 interrupt_on_names；action_requests 是命中同序子序列。
    state = _interrupt_state(
        action_requests=[
            {"name": "danger1", "args": {"a": 1}, "description": ""},
            {"name": "danger2", "args": {"c": 3}, "description": ""},
        ],
        tool_calls=[
            {"name": "danger1", "args": {"a": 1}, "id": "call-1"},
            {"name": "safe", "args": {"b": 2}, "id": "call-2"},
            {"name": "danger2", "args": {"c": 3}, "id": "call-3"},
        ],
    )
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("seg-r1", "hi"),), state=state)
    await invoke_once(bus, agent, "r1", "c1", {"messages": []}, frozenset({"danger1", "danger2"}))
    assert "agent_done" not in _events(bus.published)
    assert _data(bus.published[-1][1]) == {
        "status": "awaiting_approval",
        "segment_id": "seg-r1",
        "pending": [
            {"tool_id": "call-1", "name": "danger1", "args": {"a": 1}},
            {"tool_id": "call-3", "name": "danger2", "args": {"c": 3}},
        ],
    }


@pytest.mark.asyncio
async def test_no_pending_interrupt_no_awaiting() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("seg-r1", "hi"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []}, frozenset({"danger"}))
    assert all(_data(e).get("status") != "awaiting_approval" for _, e in bus.published)
    assert _events(bus.published)[-1] == "agent_done"


@pytest.mark.asyncio
async def test_exception_emits_agent_error() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(raise_on_stream=ValueError("boom"))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    assert _events(bus.published) == ["agent_status", "agent_error"]
    assert _data(bus.published[-1][1]) == {"error_kind": "ValueError", "message": "boom"}


def test_events_stream_format() -> None:
    assert events_stream("abc") == "kokoro:run:abc:events"


def _assert_protocol(bus: StreamProtocol) -> None:
    # 静态确认 _FakeBus 结构化满足 StreamProtocol
    assert isinstance(bus, StreamProtocol)


def test_fake_bus_is_stream_protocol() -> None:
    _assert_protocol(_FakeBus())


@pytest.mark.asyncio
async def test_trace_config_merged_into_astream_config() -> None:
    """trace 非 None 时，astream 收到的 config 含 callbacks+metadata，且 configurable.thread_id 保留。"""
    from langchain_core.callbacks import BaseCallbackHandler

    class _DummyHandler(BaseCallbackHandler):
        pass

    handler = _DummyHandler()
    trace: RunnableConfig = {
        "callbacks": [handler],
        "metadata": {"langfuse_session_id": "s1", "kokoro_run_id": "r1"},
    }
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hi"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []}, trace=trace)
    assert agent.seen_config.get("configurable") == {"thread_id": "c1"}
    assert agent.seen_config.get("callbacks") == [handler]
    assert agent.seen_config.get("metadata") == {"langfuse_session_id": "s1", "kokoro_run_id": "r1"}


@pytest.mark.asyncio
async def test_trace_none_config_only_configurable() -> None:
    """trace=None 时，config 只含 configurable，不崩，无 callbacks/metadata。"""
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hi"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []}, trace=None)
    assert agent.seen_config.get("configurable") == {"thread_id": "c1"}
    assert "callbacks" not in agent.seen_config
    assert "metadata" not in agent.seen_config


@pytest.mark.asyncio
async def test_invoke_once_returns_true_on_normal_completion() -> None:
    """正常完成(发 agent_done)分支返回 True：已发终态。"""
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hi"),))
    result = await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    assert result is True


@pytest.mark.asyncio
async def test_invoke_once_returns_false_on_interrupt_pause() -> None:
    """interrupt 暂停分支(发 awaiting 后 return)返回 False：未发终态。"""
    from langchain_core.messages import AIMessage, HumanMessage as LCHumanMessage

    value: dict[str, JsonValue] = {
        "action_requests": [{"name": "tool", "args": {}, "description": ""}]
    }
    interrupt = _FakeInterrupt(value=value)
    tool_calls: list[dict[str, object]] = [{"name": "tool", "args": {}, "id": "call-X"}]
    messages = [LCHumanMessage(content="go"), AIMessage(content="", tool_calls=tool_calls)]
    state = _FakeState(
        tasks=(_FakeTask(interrupts=(interrupt,)),),
        values={"messages": messages},
    )
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("seg", "hi"),), state=state)
    result = await invoke_once(bus, agent, "r2", "c2", {"messages": []}, frozenset({"tool"}))
    assert result is False
