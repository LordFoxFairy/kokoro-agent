from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field

import pytest
from langchain_core.runnables.schema import StandardStreamEvent, StreamEvent
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamItem, StreamProtocol
from kokoro_agent.run.invoke import events_stream, invoke_once


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


@dataclass
class _FakeAgent:
    events: Sequence[StreamEvent] = field(default_factory=tuple)
    state: _FakeState = field(default_factory=_FakeState)
    raise_on_stream: Exception | None = None
    seen_config: dict[str, object] = field(default_factory=lambda: {})

    async def astream_events(
        self, payload: object, *, version: str, config: dict[str, JsonValue]
    ) -> AsyncIterator[StreamEvent]:
        self.seen_config.update(config)
        if self.raise_on_stream is not None:
            raise self.raise_on_stream
        for event in self.events:
            yield event

    async def aget_state(self, config: dict[str, JsonValue]) -> _FakeState:
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


def _kinds(published: list[tuple[str, dict[str, JsonValue]]]) -> list[JsonValue]:
    return [event["kind"] for _, event in published]


@pytest.mark.asyncio
async def test_first_event_is_run_started() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hi"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    assert bus.published[0] == (events_stream("r1"), {"kind": "run.started", "run_id": "r1", "payload": {}})


@pytest.mark.asyncio
async def test_normal_terminal_ends_with_run_completed() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hello"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    assert _kinds(bus.published)[0] == "run.started"
    assert _kinds(bus.published)[-1] == "run.completed"
    last = bus.published[-1][1]
    assert last["payload"] == {"status": "completed"}
    assert "tool.awaiting_approval" not in _kinds(bus.published)


@pytest.mark.asyncio
async def test_projected_events_published_between_started_and_completed() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hi"),))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    # project 真实投影 on_chat_model_stream → text.delta
    assert _kinds(bus.published) == ["run.started", "text.delta", "run.completed"]
    assert agent.seen_config == {"configurable": {"thread_id": "c1"}}


@pytest.mark.asyncio
async def test_pending_interrupt_emits_awaiting_approval_no_completed() -> None:
    interrupt = _FakeInterrupt(
        value={
            "action_requests": [
                {"name": "danger", "args": {"x": 1}, "description": "do danger"}
            ],
            "review_configs": [
                {"action_name": "danger", "allowed_decisions": ["approve", "edit", "reject"]}
            ],
        }
    )
    state = _FakeState(tasks=(_FakeTask(interrupts=(interrupt,)),))
    bus = _FakeBus()
    agent = _FakeAgent(events=(_chat_stream_event("r1", "hi"),), state=state)
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    kinds = _kinds(bus.published)
    assert kinds[0] == "run.started"
    assert kinds[-1] == "tool.awaiting_approval"
    assert "run.completed" not in kinds
    payload = bus.published[-1][1]["payload"]
    assert payload == {
        "name": "danger",
        "args": {"x": 1},
        "description": "do danger",
        "allowed_decisions": ["approve", "edit", "reject"],
    }


@pytest.mark.asyncio
async def test_exception_emits_run_failed() -> None:
    bus = _FakeBus()
    agent = _FakeAgent(raise_on_stream=ValueError("boom"))
    await invoke_once(bus, agent, "r1", "c1", {"messages": []})
    kinds = _kinds(bus.published)
    assert kinds == ["run.started", "run.failed"]
    assert bus.published[-1][1]["payload"] == {"error_kind": "ValueError", "message": "boom"}


def test_events_stream_format() -> None:
    assert events_stream("abc") == "kokoro:run:abc:events"


def _assert_protocol(bus: StreamProtocol) -> None:
    # 静态确认 _FakeBus 结构化满足 StreamProtocol
    assert isinstance(bus, StreamProtocol)


def test_fake_bus_is_stream_protocol() -> None:
    _assert_protocol(_FakeBus())
