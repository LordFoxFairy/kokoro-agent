from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeGuard, TypeVar

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Command, Interrupt
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamItem
from kokoro_agent.application.run.invoke import events_stream
from kokoro_agent.application.run.supervisor import REQUESTS_STREAM, RunSupervisor
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.run_state import MemoryRunStateStore
from kokoro_agent.interfaces.inbound import InboundMessage, parse_inbound

_T = TypeVar("_T")


async def _aiter(items: Sequence[_T]) -> AsyncIterator[_T]:
    for item in items:
        yield item


class _FakeBus:
    def __init__(self, items: Sequence[StreamItem] = ()) -> None:
        self.published: list[tuple[str, dict[str, JsonValue]]] = []
        self._items = tuple(items)

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        self.published.append((stream, dict(event)))
        return StreamItem(cursor=str(len(self.published)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return list(self._items)

    def subscribe(self, stream: str, from_cursor: str | None = None) -> AsyncIterator[StreamItem]:
        return _aiter(self._items)


@dataclass
class _Model:
    text_deltas: Sequence[str] = ()
    reasoning_deltas: Sequence[str] = ()
    output_message: AIMessage | None = None
    message_id: str | None = "seg"
    namespace: list[str] = field(default_factory=lambda: [])
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
    interrupts: tuple[Interrupt, ...] = ()
    values: Mapping[str, object] = field(default_factory=lambda: {})


@dataclass
class _FakeAgent:
    run: _RunStream = field(default_factory=_RunStream)
    state: _State = field(default_factory=_State)
    seen_payloads: list[object] = field(default_factory=lambda: [])
    block: asyncio.Event | None = None

    async def astream_events(
        self,
        payload: object,
        *,
        version: str,
        config: RunnableConfig,
        transformers: Sequence[object],
    ) -> _RunStream:
        self.seen_payloads.append(payload)
        if self.block is not None:
            await self.block.wait()
        return self.run

    async def aget_state(self, config: RunnableConfig) -> _State:
        return self.state


def _text_run(text: str = "done") -> _RunStream:
    return _RunStream(
        models=(_Model(text_deltas=(text,), output_message=AIMessage(content=text, id="seg")),)
    )


def _interrupt_run() -> _RunStream:
    # action_requests 空 + auto 档 interrupt_on_names 空 → awaiting 对齐 0==0，仅 invoke 返回 False（暂停）。
    return _RunStream(is_interrupted=True)


_PENDING_STATE = _State(interrupts=(Interrupt(value={"action_requests": []}),))
_EMPTY_STATE = _State()


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


def _events(published: list[tuple[str, dict[str, JsonValue]]]) -> list[JsonValue]:
    return [event["event"] for _, event in published]


def _is_obj_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _is_obj_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _builder(agent: _FakeAgent) -> Callable[[RunRequest], _FakeAgent]:
    def build(request: RunRequest) -> _FakeAgent:
        return agent

    return build


async def _drain(sup: RunSupervisor) -> None:
    for task in tuple(sup.tasks.values()):
        await task


def _inbound(raw: dict[str, JsonValue]) -> InboundMessage:
    parsed = parse_inbound(raw)
    assert parsed is not None
    return parsed


# ① RunRequest → invoke_once 初始 payload（HumanMessage(input)）。
async def test_request_dispatches_initial_invoke() -> None:
    agent = _FakeAgent(run=_text_run("hi"), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r1"))
    await _drain(sup)

    events = _events(bus.published)
    assert events[0] == "agent_status"
    assert events[-1] == "agent_done"
    assert len(agent.seen_payloads) == 1
    initial = agent.seen_payloads[0]
    assert _is_obj_mapping(initial)
    messages: object = initial.get("messages")
    assert _is_obj_list(messages)
    first: object = messages[0]
    assert isinstance(first, HumanMessage)
    assert first.text == "hello"


# ④ 重复 run_id → 去重跳过，不再二次 invoke。
async def test_duplicate_run_id_skipped() -> None:
    agent = _FakeAgent(run=_text_run("hi"), state=_EMPTY_STATE)
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
    agent = _FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r2"))
    await _drain(sup)
    agent.seen_payloads.clear()

    resume = _inbound({"kind": "run.resume", "run_id": "r2", "decision": {"type": "approve"}})
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert len(agent.seen_payloads) == 1
    payload = agent.seen_payloads[0]
    assert isinstance(payload, Command)
    assert payload.resume == {"decisions": [{"type": "approve"}]}


# ② resume：无 pending → 不调 invoke_once（幂等护栏）。
async def test_resume_without_pending_is_dropped() -> None:
    agent = _FakeAgent(run=_text_run("hi"), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r3"))
    await _drain(sup)
    before = len(bus.published)
    agent.seen_payloads.clear()

    resume = _inbound({"kind": "run.resume", "run_id": "r3", "decision": {"type": "approve"}})
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert len(agent.seen_payloads) == 0
    assert len(bus.published) == before


# resume edit/reject decision dict 组装按 spec §9.1。
async def test_resume_edit_and_reject_decision_shapes() -> None:
    agent = _FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
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
        {"kind": "run.resume", "run_id": "r4", "decision": {"type": "reject", "message": "no"}}
    )
    await sup.dispatch(bus, reject)
    await _drain(sup)
    reject_payload = agent.seen_payloads[0]
    assert isinstance(reject_payload, Command)
    assert reject_payload.resume == {"decisions": [{"type": "reject", "message": "no"}]}


# resume 未知 run_id → warn+drop，不调 invoke。
async def test_resume_unknown_run_dropped() -> None:
    agent = _FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    resume = _inbound({"kind": "run.resume", "run_id": "ghost", "decision": {"type": "approve"}})
    await sup.dispatch(bus, resume)
    await _drain(sup)
    assert len(agent.seen_payloads) == 0
    assert bus.published == []


# ③ cancel 运行中 → task.cancel + agent_done{status:cancelled}。
async def test_cancel_running_cancels_task_and_emits_cancelled() -> None:
    gate = asyncio.Event()
    agent = _FakeAgent(run=_text_run("x"), state=_EMPTY_STATE, block=gate)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("r5"))
    await asyncio.sleep(0)

    cancel = _inbound({"kind": "run.cancel", "run_id": "r5"})
    await sup.dispatch(bus, cancel)
    await _drain(sup)

    last = bus.published[-1]
    assert last[0] == events_stream("r5")
    assert last[1]["event"] == "agent_done"
    assert last[1]["data"] == {"status": "cancelled"}


# cancel 未知/已结束 run → 仍补发 cancelled 终态。
async def test_cancel_unknown_run_still_emits_cancelled() -> None:
    agent = _FakeAgent(run=_text_run("hi"), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    cancel = _inbound({"kind": "run.cancel", "run_id": "gone"})
    await sup.dispatch(bus, cancel)
    last = bus.published[-1]
    assert last[1]["event"] == "agent_done"
    assert last[1]["data"] == {"status": "cancelled"}


# agent_builder 抛异常 → agent_error{error_kind,message}。
async def test_builder_failure_emits_run_failed() -> None:
    def build(request: RunRequest) -> _FakeAgent:
        raise ValueError("bad model")

    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=build)
    await sup.dispatch(bus, _request("r6"))
    await _drain(sup)
    last = bus.published[-1]
    assert last[1]["event"] == "agent_error"
    assert last[1]["data"] == {"error_kind": "ValueError", "message": "bad model"}


# serve 订阅循环 → 对每条 request 派发。
async def test_serve_dispatches_subscribed_requests() -> None:
    agent = _FakeAgent(run=_text_run("hi"), state=_EMPTY_STATE)
    bus = _FakeBus(items=(_request_item("sv1"),))
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.serve(bus)
    await _drain(sup)
    assert REQUESTS_STREAM == "kokoro:runs:requests"
    assert _events(bus.published)[0] == "agent_status"


# supervisor 调 invoke_once 时传了 trace kwarg（langfuse 未配时为 None 也不崩）。
@pytest.mark.asyncio
async def test_supervisor_passes_trace_to_invoke_once() -> None:
    from unittest.mock import patch

    captured: list[dict[str, object]] = []

    async def spy_invoke(*args: object, **kwargs: object) -> bool:
        captured.append({"args": args, "kwargs": kwargs})
        return True

    with patch("kokoro_agent.application.run.supervisor.invoke_once", spy_invoke):
        request = RunRequest(
            kind="run.request", run_id="r1", conversation_id="c1", session_id="s1", input="hello"
        )
        supervisor = RunSupervisor(agent_builder=lambda req: _FakeAgent())
        bus = _FakeBus()
        await supervisor.dispatch(bus, request)
        for task in list(supervisor.tasks.values()):
            await task

    assert len(captured) == 1
    kwargs = captured[0]["kwargs"]
    assert isinstance(kwargs, dict)
    assert "trace" in kwargs


# T6-①: run 自然完成后再发 cancel → 不双发终态。
@pytest.mark.asyncio
async def test_cancel_after_natural_completion_no_duplicate_terminal() -> None:
    agent = _FakeAgent(run=_text_run("hi"), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("rc1"))
    await _drain(sup)

    cancel = _inbound({"kind": "run.cancel", "run_id": "rc1"})
    await sup.dispatch(bus, cancel)

    done_events = [e for _, e in bus.published if e.get("event") == "agent_done"]
    assert len(done_events) == 1
    data = done_events[0].get("data")
    assert isinstance(data, dict)
    assert data.get("status") == "completed"


# T6-②: 暂停态(invoke_once 返回 False)cancel → 补发 cancelled。
@pytest.mark.asyncio
async def test_cancel_after_pause_emits_cancelled() -> None:
    agent = _FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("rc2"))
    await _drain(sup)

    cancel = _inbound({"kind": "run.cancel", "run_id": "rc2"})
    await sup.dispatch(bus, cancel)

    last = bus.published[-1]
    assert last[1].get("event") == "agent_done"
    data = last[1].get("data")
    assert isinstance(data, dict)
    assert data.get("status") == "cancelled"


# T6-③: 运行中 cancel(task 未完成,CancelledError) → 补发 cancelled。
@pytest.mark.asyncio
async def test_cancel_mid_run_emits_cancelled() -> None:
    gate = asyncio.Event()
    agent = _FakeAgent(run=_text_run("x"), state=_EMPTY_STATE, block=gate)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("rc3"))
    await asyncio.sleep(0)

    cancel = _inbound({"kind": "run.cancel", "run_id": "rc3"})
    await sup.dispatch(bus, cancel)
    await _drain(sup)

    last = bus.published[-1]
    assert last[1].get("event") == "agent_done"
    data = last[1].get("data")
    assert isinstance(data, dict)
    assert data.get("status") == "cancelled"


# T6-④: 暂停态 cancel 后再来 resume → 被 _terminal 挡。
@pytest.mark.asyncio
async def test_resume_after_cancel_blocked_by_terminal() -> None:
    agent = _FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("rc4"))
    await _drain(sup)

    cancel = _inbound({"kind": "run.cancel", "run_id": "rc4"})
    await sup.dispatch(bus, cancel)
    before = len(bus.published)
    agent.seen_payloads.clear()

    resume = _inbound({"kind": "run.resume", "run_id": "rc4", "decision": {"type": "approve"}})
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert len(agent.seen_payloads) == 0
    assert len(bus.published) == before


# T6-⑤: 自然完成后再来 resume → 被 _terminal 挡。
@pytest.mark.asyncio
async def test_resume_after_natural_completion_blocked_by_terminal() -> None:
    agent = _FakeAgent(run=_text_run("hi"), state=_EMPTY_STATE)
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await sup.dispatch(bus, _request("rc5"))
    await _drain(sup)
    before = len(bus.published)
    agent.seen_payloads.clear()

    resume = _inbound({"kind": "run.resume", "run_id": "rc5", "decision": {"type": "approve"}})
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert len(agent.seen_payloads) == 0
    assert len(bus.published) == before


# T6-⑥: 构建失败发 agent_error 后再 cancel → 不补发第二终态。
@pytest.mark.asyncio
async def test_cancel_after_build_failure_no_duplicate_terminal() -> None:
    def build(request: RunRequest) -> _FakeAgent:
        raise ValueError("bad model")

    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=build)
    await sup.dispatch(bus, _request("rbf"))
    await _drain(sup)

    cancel = _inbound({"kind": "run.cancel", "run_id": "rbf"})
    await sup.dispatch(bus, cancel)

    terminals = [e for _, e in bus.published if e.get("event") in ("agent_done", "agent_error")]
    assert len(terminals) == 1
    assert terminals[0].get("event") == "agent_error"


# Layer 2: 全新 supervisor 实例(模拟重启/另一 pod)仅共享 store → 据持久化原 request 续接 resume。
@pytest.mark.asyncio
async def test_resume_on_fresh_supervisor_via_shared_store() -> None:
    store = MemoryRunStateStore()
    agent = _FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = _FakeBus()
    sup_a = RunSupervisor(agent_builder=_builder(agent), store=store)
    await sup_a.dispatch(bus, _request("rx"))
    await _drain(sup_a)

    # sup_b 无 sup_a 的进程内 map，仅共享 store；resume 须靠 store.get_request 重建 agent。
    sup_b = RunSupervisor(agent_builder=_builder(agent), store=store)
    agent.seen_payloads.clear()
    resume = _inbound({"kind": "run.resume", "run_id": "rx", "decision": {"type": "approve"}})
    await sup_b.dispatch(bus, resume)
    await _drain(sup_b)

    assert len(agent.seen_payloads) == 1
    payload = agent.seen_payloads[0]
    assert isinstance(payload, Command)
    assert payload.resume == {"decisions": [{"type": "approve"}]}
