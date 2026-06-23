from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeGuard

import pytest

from langchain_core.messages import HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StandardStreamEvent, StreamEvent
from langgraph.types import Command
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamItem
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.run.invoke import events_stream
from kokoro_agent.run.supervisor import REQUESTS_STREAM, RunSupervisor
from kokoro_agent.wire.run_request import InboundMessage, parse_inbound


class _FakeBus:
    """记录 publish 的 (stream, event)；subscribe 喂入预置消息序列。"""

    def __init__(self, items: Sequence[StreamItem] = ()) -> None:
        self.published: list[tuple[str, dict[str, JsonValue]]] = []
        self._items = tuple(items)

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        self.published.append((stream, dict(event)))
        return StreamItem(cursor=str(len(self.published)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return list(self._items)

    async def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
        for item in self._items:
            yield item


@dataclass(frozen=True)
class _FakeInterrupt:
    value: Mapping[str, JsonValue]


@dataclass(frozen=True)
class _FakeTask:
    interrupts: tuple[_FakeInterrupt, ...] = ()


@dataclass(frozen=True)
class _FakeState:
    tasks: tuple[_FakeTask, ...] = ()


_PENDING_STATE = _FakeState(tasks=(_FakeTask(interrupts=(_FakeInterrupt(value={"x": 1}),)),))
_EMPTY_STATE = _FakeState()


@dataclass
class _FakeAgent:
    events: Sequence[StreamEvent] = field(default_factory=tuple)
    state: _FakeState = field(default_factory=_FakeState)
    seen_payloads: list[object] = field(default_factory=lambda: [])
    block: asyncio.Event | None = None

    async def astream_events(
        self, payload: object, *, version: str, config: RunnableConfig
    ) -> AsyncIterator[StreamEvent]:
        self.seen_payloads.append(payload)
        if self.block is not None:
            await self.block.wait()
        for event in self.events:
            yield event

    async def aget_state(self, config: RunnableConfig) -> _FakeState:
        return self.state


def _chat_event(text: str) -> StandardStreamEvent:
    from langchain_core.messages import AIMessageChunk

    return {
        "event": "on_chat_model_stream",
        "name": "model",
        "run_id": "r",
        "data": {"chunk": AIMessageChunk(content=text)},
        "metadata": {},
        "tags": [],
        "parent_ids": [],
    }


def _request_item(run_id: str, conversation_id: str = "c1") -> StreamItem:
    return StreamItem(
        cursor="0",
        event={
            "kind": "run.request",
            "run_id": run_id,
            "session_id": "s1",
            "conversation_id": conversation_id,
            "input": "hello",
        },
    )


def _request(run_id: str, conversation_id: str = "c1") -> RunRequest:
    return RunRequest.model_validate(_request_item(run_id, conversation_id).event)


def _kinds(published: list[tuple[str, dict[str, JsonValue]]]) -> list[JsonValue]:
    return [event["kind"] for _, event in published]


def _is_obj_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _mapping_get(mapping: Mapping[object, object], key: str) -> object:
    return mapping.get(key)


def _is_obj_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _builder(agent: _FakeAgent) -> Callable[[RunRequest], _FakeAgent]:
    def build(request: RunRequest) -> _FakeAgent:
        return agent

    return build


async def _drain(sup: RunSupervisor) -> None:
    # 等待 supervisor spawn 的 invoke task 结束，再断言发布序列。
    for task in tuple(sup.tasks.values()):
        await task


def _inbound(raw: dict[str, JsonValue]) -> InboundMessage:
    parsed = parse_inbound(raw)
    assert parsed is not None
    return parsed


# ① RunRequest → invoke_once 初始 payload（HumanMessage(input)）。
async def test_request_dispatches_initial_invoke() -> None:
    agent = _FakeAgent(events=(_chat_event("hi"),), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r1"))
    await _drain(sup)

    kinds = _kinds(bus.published)
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert len(agent.seen_payloads) == 1
    initial = agent.seen_payloads[0]
    assert _is_obj_mapping(initial)
    messages: object = _mapping_get(initial, "messages")
    assert _is_obj_list(messages)
    first: object = messages[0]
    assert isinstance(first, HumanMessage)
    assert first.text == "hello"


# ④ 重复 run_id → 去重跳过，不再二次 invoke。
async def test_duplicate_run_id_skipped() -> None:
    agent = _FakeAgent(events=(_chat_event("hi"),), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("dup"))
    await _drain(sup)
    first_count = len(bus.published)
    await sup.dispatch(bus, _request("dup"))
    await _drain(sup)
    assert len(bus.published) == first_count
    assert len(agent.seen_payloads) == 1


# ② resume：有 pending interrupt → invoke_once(Command(resume))。
async def test_resume_with_pending_invokes_command() -> None:
    agent = _FakeAgent(events=(_chat_event("done"),), state=_PENDING_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r2"))
    await _drain(sup)
    agent.seen_payloads.clear()

    resume = _inbound(
        {"kind": "run.resume", "run_id": "r2", "decision": {"type": "approve"}}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert len(agent.seen_payloads) == 1
    payload = agent.seen_payloads[0]
    assert isinstance(payload, Command)
    assert payload.resume == {"decisions": [{"type": "approve"}]}


# ② resume：无 pending → 不调 invoke_once（幂等护栏）。
async def test_resume_without_pending_is_dropped() -> None:
    agent = _FakeAgent(events=(_chat_event("hi"),), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r3"))
    await _drain(sup)
    before = len(bus.published)
    agent.seen_payloads.clear()

    resume = _inbound(
        {"kind": "run.resume", "run_id": "r3", "decision": {"type": "approve"}}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert len(agent.seen_payloads) == 0
    assert len(bus.published) == before


# resume edit/reject decision dict 组装按 spec §9.1。
async def test_resume_edit_and_reject_decision_shapes() -> None:
    agent = _FakeAgent(events=(_chat_event("ok"),), state=_PENDING_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r4"))
    await _drain(sup)

    agent.seen_payloads.clear()
    edit = _inbound(
        {
            "kind": "run.resume",
            "run_id": "r4",
            "decision": {"type": "edit", "edited_action": {"name": "tool", "args": {}}},
        }
    )
    await sup.dispatch(bus, edit)
    await _drain(sup)
    edit_payload = agent.seen_payloads[0]
    assert isinstance(edit_payload, Command)
    assert edit_payload.resume == {
        "decisions": [{"type": "edit", "edited_action": {"name": "tool", "args": {}}}]
    }

    agent.seen_payloads.clear()
    reject = _inbound(
        {
            "kind": "run.resume",
            "run_id": "r4",
            "decision": {"type": "reject", "message": "no"},
        }
    )
    await sup.dispatch(bus, reject)
    await _drain(sup)
    reject_payload = agent.seen_payloads[0]
    assert isinstance(reject_payload, Command)
    assert reject_payload.resume == {"decisions": [{"type": "reject", "message": "no"}]}


# resume 未知 run_id → warn+drop，不调 invoke。
async def test_resume_unknown_run_dropped() -> None:
    agent = _FakeAgent(events=(_chat_event("hi"),), state=_PENDING_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    resume = _inbound(
        {"kind": "run.resume", "run_id": "ghost", "decision": {"type": "approve"}}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)
    assert len(agent.seen_payloads) == 0
    assert bus.published == []


# ③ cancel 运行中 → task.cancel + run.completed{status:cancelled}。
async def test_cancel_running_cancels_task_and_emits_cancelled() -> None:
    gate = asyncio.Event()
    agent = _FakeAgent(events=(_chat_event("x"),), state=_EMPTY_STATE, block=gate)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r5"))
    # invoke task 已 spawn 并阻塞在 astream_events；让出一拍确保进入流。
    await asyncio.sleep(0)

    cancel = _inbound({"kind": "run.cancel", "run_id": "r5"})
    await sup.dispatch(bus, cancel)
    await _drain(sup)

    last = bus.published[-1]
    assert last[0] == events_stream("r5")
    assert last[1]["kind"] == "run.completed"
    assert last[1]["payload"] == {"status": "cancelled"}


# cancel 未知/已结束 run → 仍补发 cancelled 终态。
async def test_cancel_unknown_run_still_emits_cancelled() -> None:
    agent = _FakeAgent(events=(_chat_event("hi"),), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    cancel = _inbound({"kind": "run.cancel", "run_id": "gone"})
    await sup.dispatch(bus, cancel)
    last = bus.published[-1]
    assert last[1]["kind"] == "run.completed"
    assert last[1]["payload"] == {"status": "cancelled"}


# agent_builder 抛异常 → run.failed{error_kind,message}。
async def test_builder_failure_emits_run_failed() -> None:
    def build(request: RunRequest) -> _FakeAgent:
        raise ValueError("bad model")

    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=build)
    await sup.dispatch(bus, _request("r6"))
    await _drain(sup)
    last = bus.published[-1]
    assert last[1]["kind"] == "run.failed"
    assert last[1]["payload"] == {"error_kind": "ValueError", "message": "bad model"}


# serve 订阅循环 → 对每条 request 派发。
async def test_serve_dispatches_subscribed_requests() -> None:
    agent = _FakeAgent(events=(_chat_event("hi"),), state=_EMPTY_STATE)
    bus = _FakeBus(items=(_request_item("sv1"),))
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.serve(bus)
    await _drain(sup)
    assert REQUESTS_STREAM == "kokoro:runs:requests"
    assert _kinds(bus.published)[0] == "run.started"


# supervisor 调 invoke_once 时传了 trace kwarg（langfuse 未配时为 None 也不崩）。
@pytest.mark.asyncio
async def test_supervisor_passes_trace_to_invoke_once() -> None:
    from unittest.mock import patch

    captured: list[dict[str, object]] = []

    async def spy_invoke(*args: object, **kwargs: object) -> None:
        captured.append({"args": args, "kwargs": kwargs})

    with patch("kokoro_agent.run.supervisor.invoke_once", spy_invoke):
        request = RunRequest(
            kind="run.request",
            run_id="r1",
            conversation_id="c1",
            session_id="s1",
            input="hello",
        )
        supervisor = RunSupervisor(agent_builder=lambda req: _FakeAgent())
        bus = _FakeBus()
        await supervisor.dispatch(bus, request)
        for task in list(supervisor.tasks.values()):
            await task

    assert len(captured) == 1
    # trace kwarg 被传入（None or RunnableConfig，langfuse 未配 → None）
    kwargs = captured[0]["kwargs"]
    assert isinstance(kwargs, dict)
    assert "trace" in kwargs


# Task-6: 终态单发保证测试 ─────────────────────────────────────────────


# T6-①: run 自然完成后再发 cancel → 不双发 run.completed{cancelled}。
@pytest.mark.asyncio
async def test_cancel_after_natural_completion_no_duplicate_terminal() -> None:
    """run 自然完成(invoke_once 返回 True) → _terminal 记录 run_id；
    再 dispatch cancel → _on_cancel 检查 _terminal 跳过补发，只有自然那条终态。"""
    agent = _FakeAgent(events=(_chat_event("hi"),), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("rc1"))
    await _drain(sup)

    # 此时 invoke_once 已返回 True，_terminal 应含 "rc1"。
    cancel = _inbound({"kind": "run.cancel", "run_id": "rc1"})
    await sup.dispatch(bus, cancel)

    completed_events = [
        e for _, e in bus.published
        if e.get("kind") == "run.completed"
    ]
    # 只允许自然完成那条，不补发 cancelled。
    assert len(completed_events) == 1
    payload = completed_events[0].get("payload")
    assert isinstance(payload, dict)
    assert payload.get("status") == "completed"


# T6-②: 暂停态(invoke_once 返回 False)cancel → 补发 cancelled。
@pytest.mark.asyncio
async def test_cancel_after_pause_emits_cancelled() -> None:
    """run invoke_once 因 interrupt 返回 False → _terminal 不记录；
    cancel → _on_cancel 无 running task 且不在 _terminal → 补发 cancelled。"""
    state = _FakeState(tasks=(_FakeTask(interrupts=(_FakeInterrupt(value={"x": 1}),)),))
    agent = _FakeAgent(events=(), state=state)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("rc2"))
    await _drain(sup)

    cancel = _inbound({"kind": "run.cancel", "run_id": "rc2"})
    await sup.dispatch(bus, cancel)

    last = bus.published[-1]
    assert last[1].get("kind") == "run.completed"
    payload = last[1].get("payload")
    assert isinstance(payload, dict)
    assert payload.get("status") == "cancelled"


# T6-③: 运行中 cancel(task 未完成,CancelledError) → 补发 cancelled。
@pytest.mark.asyncio
async def test_cancel_mid_run_emits_cancelled() -> None:
    """task 被 cancel 时 invoke_once 抛 CancelledError 不返回 → 不入 _terminal；
    _on_cancel 正常补发 cancelled。"""
    gate = asyncio.Event()
    agent = _FakeAgent(events=(_chat_event("x"),), state=_EMPTY_STATE, block=gate)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("rc3"))
    await asyncio.sleep(0)  # 让 task 开始阻塞在 block.wait()

    cancel = _inbound({"kind": "run.cancel", "run_id": "rc3"})
    await sup.dispatch(bus, cancel)
    await _drain(sup)

    last = bus.published[-1]
    assert last[1].get("kind") == "run.completed"
    payload = last[1].get("payload")
    assert isinstance(payload, dict)
    assert payload.get("status") == "cancelled"
