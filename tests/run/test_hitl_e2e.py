"""HITL 端到端：fake agent 真走 interrupt→resume，四档决策经 supervisor 全链路。

每档：dispatch RunRequest → 收 tool.awaiting_approval(tool_id 对齐) →
dispatch RunResume(对应决定) → fake 收 Command(resume) 续跑 → 收终态。
fake 是真 fake（自带 interrupt/resume 状态机），非 mock。
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from typing import TypeGuard

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StreamEvent
from langgraph.types import Command
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamItem
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.application.run.invoke import events_stream
from kokoro_agent.application.run.supervisor import RunSupervisor
from kokoro_agent.interfaces.inbound import InboundMessage, parse_inbound

# 与 approval_policy.yaml 的 requires_approval_tools 同名：确保 supervisor 计算的
# interrupt_on_names 真命中本工具，端到端验证同源对齐而非旁路。
_TOOL_NAME = "fetch_url"
_TOOL_ID = "call-A"


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, JsonValue]]] = []

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        self.published.append((stream, dict(event)))
        return StreamItem(cursor=str(len(self.published)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return []

    async def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
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


def _tool_end_event(result: str) -> StreamEvent:
    return {
        "event": "on_tool_end",
        "name": _TOOL_NAME,
        "run_id": _TOOL_ID,
        "data": {"input": {}, "output": result},
        "metadata": {},
        "tags": [],
        "parent_ids": [],
    }


@dataclass
class _FakeHitlAgent:
    """interrupt→resume 状态机：首轮暂停带 pending；resume 据决策发 tool 结果后转终态。"""

    args: Mapping[str, JsonValue] = field(default_factory=lambda: {"x": 1})
    resumed: bool = False
    seen_resume: object = None

    async def astream_events(
        self, payload: object, *, version: str, config: RunnableConfig
    ) -> AsyncIterator[StreamEvent]:
        if isinstance(payload, Command):
            self.resumed = True
            self.seen_resume = payload.resume
            for event in self._resume_events(payload.resume):
                yield event
            return
        # 首轮 invoke：不发工具结果，直接暂停（astream 自然结束后 aget_state 暴露 pending）。
        return
        yield  # pragma: no cover

    def _resume_events(self, resume: object) -> list[StreamEvent]:
        decision = _first_decision(resume)
        dtype = _decision_type(decision)
        if dtype == "reject":
            # 拒绝：工具不真跑，发拒绝语义的 tool 结果。
            return [_tool_end_event("rejected by human")]
        if dtype == "respond":
            # 合成结果：人工 message 直接作为工具输出。
            return [_tool_end_event(f"synthetic: {_decision_message(decision)}")]
        if dtype == "edit":
            # 编辑：新 args 生效，工具据新参数真跑。
            return [_tool_end_event(f"ran with {_edited_args(decision)}")]
        # approve：工具据原 args 真跑出结果。
        return [_tool_end_event(f"ran with {dict(self.args)}")]

    async def aget_state(self, config: RunnableConfig) -> _FakeState:
        if self.resumed:
            return _FakeState()
        value: dict[str, JsonValue] = {
            "action_requests": [
                {"name": _TOOL_NAME, "args": dict(self.args), "description": "do danger"}
            ]
        }
        messages = [
            HumanMessage(content="go"),
            AIMessage(
                content="",
                tool_calls=[{"name": _TOOL_NAME, "args": dict(self.args), "id": _TOOL_ID}],
            ),
        ]
        return _FakeState(
            tasks=(_FakeTask(interrupts=(_FakeInterrupt(value=value),)),),
            values={"messages": messages},
        )


def _is_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _is_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _first_decision(resume: object) -> Mapping[object, object]:
    if not _is_mapping(resume):
        return {}
    decisions: object = resume.get("decisions")
    if not _is_list(decisions) or not decisions:
        return {}
    first = decisions[0]
    return first if _is_mapping(first) else {}


def _decision_type(decision: Mapping[object, object]) -> str:
    dtype: object = decision.get("type")
    return dtype if isinstance(dtype, str) else ""


def _decision_message(decision: Mapping[object, object]) -> str:
    message: object = decision.get("message")
    return message if isinstance(message, str) else ""


def _edited_args(decision: Mapping[object, object]) -> Mapping[object, object]:
    action: object = decision.get("edited_action")
    if not _is_mapping(action):
        return {}
    args: object = action.get("args")
    return args if _is_mapping(args) else {}


def _builder(agent: _FakeHitlAgent) -> Callable[[RunRequest], _FakeHitlAgent]:
    def build(request: RunRequest) -> _FakeHitlAgent:
        return agent

    return build


def _request(run_id: str) -> RunRequest:
    # permission_mode=default 让 supervisor 计算非空 interrupt_on_names。
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        session_id="s1",
        conversation_id="c1",
        input="hello",
        permission_mode="default",
    )


def _inbound(raw: dict[str, JsonValue]) -> InboundMessage:
    parsed = parse_inbound(raw)
    assert parsed is not None
    return parsed


async def _drain(sup: RunSupervisor) -> None:
    for task in tuple(sup.tasks.values()):
        await task


def _events_of(bus: _FakeBus, run_id: str, event_name: str) -> list[dict[str, JsonValue]]:
    return [
        e
        for s, e in bus.published
        if s == events_stream(run_id) and e.get("event") == event_name
    ]


def _data(event: dict[str, JsonValue]) -> Mapping[str, JsonValue]:
    data = event["data"]
    assert isinstance(data, Mapping)
    return data


async def _run_until_awaiting(sup: RunSupervisor, bus: _FakeBus, run_id: str) -> None:
    await sup.dispatch(bus, _request(run_id))
    await _drain(sup)
    awaiting = [
        e
        for e in _events_of(bus, run_id, "agent_status")
        if _data(e).get("status") == "awaiting_approval"
    ]
    assert len(awaiting) == 1
    pending: object = _data(awaiting[0]).get("pending")
    assert _is_list(pending) and len(pending) == 1
    first = pending[0]
    assert _is_mapping(first)
    assert first.get("tool_id") == _TOOL_ID
    assert "agent_done" not in [e.get("event") for _, e in bus.published]


async def _resume(
    sup: RunSupervisor, bus: _FakeBus, run_id: str, decision: dict[str, JsonValue]
) -> None:
    resume = _inbound({"kind": "run.resume", "run_id": run_id, "decision": decision})
    await sup.dispatch(bus, resume)
    await _drain(sup)


def _tool_result(bus: _FakeBus, run_id: str) -> str:
    returned = _events_of(bus, run_id, "tool_call_end")
    assert len(returned) == 1
    result: object = _data(returned[0]).get("result")
    assert isinstance(result, str)
    return result


def _assert_completed(bus: _FakeBus, run_id: str) -> None:
    done = _events_of(bus, run_id, "agent_done")
    assert len(done) == 1
    assert _data(done[0]).get("status") == "completed"


# ① approve → 工具据原 args 真跑出结果。
@pytest.mark.asyncio
async def test_approve_tool_actually_runs() -> None:
    agent = _FakeHitlAgent()
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await _run_until_awaiting(sup, bus, "ra")
    await _resume(sup, bus, "ra", {"type": "approve"})

    assert agent.seen_resume == {"decisions": [{"type": "approve"}]}
    assert _tool_result(bus, "ra") == "ran with {'x': 1}"
    _assert_completed(bus, "ra")


# ② edit → 新 args 生效，工具据编辑参数跑。
@pytest.mark.asyncio
async def test_edit_new_args_take_effect() -> None:
    agent = _FakeHitlAgent()
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await _run_until_awaiting(sup, bus, "re")
    await _resume(
        sup,
        bus,
        "re",
        {"type": "edit", "edited_action": {"name": _TOOL_NAME, "args": {"x": 99}}},
    )

    assert agent.seen_resume == {
        "decisions": [{"type": "edit", "edited_action": {"name": _TOOL_NAME, "args": {"x": 99}}}]
    }
    assert _tool_result(bus, "re") == "ran with {'x': 99}"
    _assert_completed(bus, "re")


# ③ reject → 工具不真跑，拒绝语义结果。
@pytest.mark.asyncio
async def test_reject_does_not_run_tool() -> None:
    agent = _FakeHitlAgent()
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await _run_until_awaiting(sup, bus, "rr")
    await _resume(sup, bus, "rr", {"type": "reject", "message": "no"})

    assert agent.seen_resume == {"decisions": [{"type": "reject", "message": "no"}]}
    assert _tool_result(bus, "rr") == "rejected by human"
    _assert_completed(bus, "rr")


# ④ respond → 合成结果回填。
@pytest.mark.asyncio
async def test_respond_synthesizes_result() -> None:
    agent = _FakeHitlAgent()
    bus = _FakeBus()
    sup = RunSupervisor(agent_builder=_builder(agent))
    await _run_until_awaiting(sup, bus, "rs")
    await _resume(sup, bus, "rs", {"type": "respond", "message": "use cache"})

    assert agent.seen_resume == {"decisions": [{"type": "respond", "message": "use cache"}]}
    assert _tool_result(bus, "rs") == "synthetic: use cache"
    _assert_completed(bus, "rs")
