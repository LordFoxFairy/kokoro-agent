"""R2 control command 与 receipt 闭环（agent 侧）：

- command ledger keep-first：重复 command_id 不双放。
- 两时点回执：persisted（落 command ledger）与 applied（apply 后）各发一次 run.control.receipt。
- 重启续办 scanner：persisted 未 applied 的 command——fingerprint 匹配当前 interrupt 才续 apply，
  不匹配/已终态=stale→superseded 不 apply（经 public serve() 启动路径驱动）。
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest
from langchain_core.messages import AIMessage
from langgraph.types import Interrupt
from pydantic import JsonValue

from support.fakes import (
    FakeAgent,
    FakeBus,
    FakeRunRepository,
    FakeRunStream,
    FakeState,
    find_event,
    find_events,
    request,
    text_run,
)
from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.contract import (
    InboundMessage,
    RunCompleted,
    RunControlReceipt,
    RunRequest,
    SubagentSource,
    RunSteer,
    inbound_adapter,
    run_control_stream,
)
from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.worker.supervisor import RunSupervisor

_GATED = "danger"
_TID = "call-A"

# 待审批暂停快照（与 test_supervisor._PENDING_STATE 同构）：resume 续跑需当前 interrupt 存在。
_PENDING_STATE = FakeState(
    interrupts=(
        Interrupt(
            value={
                "action_requests": [{"name": _GATED, "args": {}, "description": "d"}],
                "review_configs": [
                    {
                        "action_name": _GATED,
                        "allowed_decisions": ["approve", "edit", "reject"],
                    }
                ],
            }
        ),
    ),
    values={
        "messages": [
            AIMessage(
                content="",
                id="seg",
                tool_calls=[{"name": _GATED, "args": {}, "id": _TID}],
            )
        ]
    },
)


def _builder(agent: FakeAgent) -> Callable[[RunRequest], Awaitable[AgentHandle]]:
    async def _build(_request: RunRequest) -> AgentHandle:
        return AgentHandle(runnable=agent, tool_descriptions={})

    return _build


def _gated_names(_request: RunRequest) -> frozenset[str]:
    return frozenset({_GATED})


def _no_trace(_request: RunRequest) -> None:
    return None


def _source(_name: str) -> SubagentSource:
    return "runtime-custom"


def _supervisor(agent: FakeAgent, store: FakeRunRepository) -> RunSupervisor:
    return RunSupervisor(
        agent_builder=_builder(agent),
        store=store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="test-consumer",
    )


def _inbound(raw: dict[str, JsonValue]) -> InboundMessage:
    if raw.get("kind") in {"run.resume", "run.cancel"} and "session_id" not in raw:
        raw = {**raw, "session_id": "s1"}
    return inbound_adapter.validate_python(raw)


def _interrupt_run() -> FakeRunStream:
    return FakeRunStream(is_interrupted=True)


async def _drain(sup: RunSupervisor) -> None:
    for task in tuple(sup.tasks.values()):
        await task


def _fingerprint_of(state: FakeState) -> str:
    # 复刻 supervisor._interrupt_fingerprint 的指纹公式：稳定 interrupt.id 集合的 sha256。
    joined = ",".join(sorted(str(interrupt.id) for interrupt in state.interrupts))
    return hashlib.sha256(joined.encode()).hexdigest()


def _resume_body(run_id: str) -> str:
    return _inbound(
        {
            "kind": "run.resume",
            "command_id": "dec_1",
            "run_id": run_id,
            "decisions": [{"type": "approve", "tool_id": _TID}],
        }
    ).model_dump_json()


@pytest.mark.parametrize(
    ("kind", "body"),
    [
        (
            "run.resume",
            {
                "command_id": "dec_r",
                "decisions": [{"type": "approve", "tool_id": _TID}],
            },
        ),
        ("run.cancel", {"command_id": "dec_c"}),
        (
            "run.steer",
            {"command_id": "dec_s", "message_id": "m_foreign", "content": "keep out"},
        ),
    ],
)
async def test_foreign_session_control_frames_are_dropped_before_apply(
    kind: str, body: dict[str, JsonValue]
) -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    run_repository = FakeRunRepository()
    await run_repository.try_claim(request("fx", session_id="local-session"))
    bus = FakeBus()
    sup = _supervisor(agent, run_repository)
    await sup.dispatch(
        bus,
        _inbound(
            {
                "kind": kind,
                "run_id": "fx",
                "session_id": "foreign-session",
                **body,
            }
        ),
    )
    assert agent.seen_payloads == []
    assert run_repository.control_commands == {}
    assert run_repository.steers == {}
    assert bus.published == []


async def test_restart_scanner_supersedes_foreign_session_resume() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    run_repository = FakeRunRepository()
    await run_repository.try_claim(request("fs", session_id="local-session"))
    await run_repository.record_control_delivery(
        "fs",
        "dec_1",
        None,
        _fingerprint_of(_PENDING_STATE),
        _inbound(
            {
                "kind": "run.resume",
                "command_id": "dec_1",
                "run_id": "fs",
                "session_id": "foreign-session",
                "decisions": [{"type": "approve", "tool_id": _TID}],
            }
        ).model_dump_json(),
    )

    bus = FakeBus()
    sup = _supervisor(agent, run_repository)
    await sup.serve(bus)
    await _drain(sup)

    assert agent.seen_payloads == []
    assert run_repository.control_commands[("fs", "dec_1")]["status"] == "superseded"
    assert find_events(bus.run_events("fs"), RunControlReceipt) == []


async def test_control_commands_keep_first_dedup() -> None:
    run_repository = FakeRunRepository()
    await run_repository.try_claim(request("rd"))
    assert (
        await run_repository.record_control_delivery("rd", "dec_1", None, "fp", "{}")
        is True
    )
    # 重复 command_id（重发/重投）：命中既有条目 → False（丢弃不重放）。
    assert (
        await run_repository.record_control_delivery("rd", "dec_1", None, "fp", "{}")
        is False
    )
    # 不同 command_id 各自入账。
    assert (
        await run_repository.record_control_delivery("rd", "dec_2", None, None, "{}")
        is True
    )


async def test_cancel_via_control_loop_emits_two_receipts_and_applies() -> None:
    gate = asyncio.Event()  # 永不 set：run 只能被 cancel 结束
    agent = FakeAgent(run=text_run("x"), gates=[gate])
    bus = FakeBus(
        control={
            run_control_stream("cc"): (
                StreamItem(
                    cursor="1",
                    event={
                        "kind": "run.cancel",
                        "command_id": "dec_1",
                        "run_id": "cc",
                        "session_id": "s1",
                    },
                ),
            )
        }
    )
    run_repository = FakeRunRepository()
    sup = _supervisor(agent, run_repository)
    await sup.dispatch(bus, request("cc"))
    for _ in range(200):
        if run_control_stream("cc") in bus.deleted:
            break
        await asyncio.sleep(0.005)

    # 两时点回执按序上 run events 流（applied 先于 run.completed：cancel apply 即终态）。
    receipts = find_events(bus.run_events("cc"), RunControlReceipt)
    assert [r.payload.control_status for r in receipts] == ["persisted", "applied"]
    assert all(r.payload.command_id == "dec_1" for r in receipts)
    assert run_repository.control_commands[("cc", "dec_1")]["status"] == "succeeded"
    completed = find_event(bus.run_events("cc"), RunCompleted)
    assert completed.payload.status == "cancelled"
    assert run_control_stream("cc") in bus.deleted


async def test_steer_via_control_loop_uses_the_same_ledger_and_is_idempotent() -> None:
    run_repository = FakeRunRepository()
    await run_repository.try_claim(request("cs"))
    bus = FakeBus()
    sup = _supervisor(FakeAgent(run=text_run("x")), run_repository)
    steer = _inbound(
        {
            "kind": "run.steer",
            "command_id": "steer-1",
            "run_id": "cs",
            "session_id": "s1",
            "message_id": "message-1",
            "content": "continue with the API contract",
        }
    )
    assert isinstance(steer, RunSteer)

    consumer = cast(Any, sup)._consume_control_frame
    await consumer(bus, "cs", steer, run_control_stream("cs"), "1")
    await consumer(bus, "cs", steer, run_control_stream("cs"), "2")

    assert run_repository.control_commands[("cs", "steer-1")]["status"] == "succeeded"
    assert run_repository.steers["cs"] == [
        ("message-1", "continue with the API contract")
    ]
    receipts = find_events(bus.run_events("cs"), RunControlReceipt)
    assert [receipt.payload.control_status for receipt in receipts] == [
        "persisted",
        "applied",
    ]
    assert bus.acked == ["1", "2"]


async def test_restart_scanner_reapplies_on_fingerprint_match() -> None:
    # 崩溃前：command ledger persisted 已落，fingerprint=当时 interrupt 指纹，apply 未跑。serve() 启动续办。
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    run_repository = FakeRunRepository()
    await run_repository.try_claim(request("rf"))
    await run_repository.record_control_delivery(
        "rf", "dec_1", None, _fingerprint_of(_PENDING_STATE), _resume_body("rf")
    )

    bus = FakeBus()  # 空请求流：serve 跑完 startup 续办即收束
    sup = _supervisor(agent, run_repository)
    await sup.serve(bus)
    await _drain(sup)

    # 指纹匹配 → 续 apply：agent 收到 resume Command。
    assert len(agent.seen_payloads) >= 1
    assert run_repository.control_commands[("rf", "dec_1")]["status"] == "succeeded"
    # restart 续办只补 applied（persisted 已在崩溃前发过，不重发）。
    receipts = find_events(bus.run_events("rf"), RunControlReceipt)
    assert [r.payload.control_status for r in receipts] == ["applied"]


async def test_restart_scanner_supersedes_on_fingerprint_mismatch() -> None:
    # 崩溃前记录的 fingerprint 与当前 interrupt 不符（interrupt 已变/run 已推进）。
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    run_repository = FakeRunRepository()
    await run_repository.try_claim(request("rm"))
    await run_repository.record_control_delivery(
        "rm", "dec_1", None, "stale-fingerprint-mismatch", _resume_body("rm")
    )

    bus = FakeBus()
    sup = _supervisor(agent, run_repository)
    await sup.serve(bus)
    await _drain(sup)

    # 不匹配 → 不 apply，标 superseded；无 apply、无 applied 回执。
    assert agent.seen_payloads == []
    assert run_repository.control_commands[("rm", "dec_1")]["status"] == "superseded"
    assert find_events(bus.run_events("rm"), RunControlReceipt) == []


async def test_terminal_run_control_excluded_from_reapply() -> None:
    # 已终态的 run：其 persisted control 条目不入续办扫描（随 run purge 清理），绝不重放 apply。
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    run_repository = FakeRunRepository()
    await run_repository.try_claim(request("rt"))
    await run_repository.record_control_delivery(
        "rt",
        "dec_1",
        None,
        None,
        _inbound(
            {"kind": "run.cancel", "command_id": "dec_1", "run_id": "rt"}
        ).model_dump_json(),
    )
    await run_repository.try_mark_terminal("rt")

    # 终态 run 不进 pending 列表。
    assert await run_repository.list_pending_control_delivery() == []

    bus = FakeBus()
    sup = _supervisor(agent, run_repository)
    await sup.serve(bus)

    # 未重放 apply；条目留 persisted 待 purge_terminal 清理。
    assert agent.seen_payloads == []
    assert run_repository.control_commands[("rt", "dec_1")]["status"] == "persisted"
