"""R2 control inbox 与 receipt 闭环（agent 侧）：

- inbox keep-first：重复 decision_id 不双放。
- 两时点回执：persisted（落 inbox）与 applied（apply 后）各发一次 run.control.receipt。
- 重启续办 scanner：persisted 未 applied 的 command——fingerprint 匹配当前 interrupt 才续 apply，
  不匹配/已终态=stale→superseded 不 apply（经 public serve() 启动路径驱动）。
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable

from langchain_core.messages import AIMessage
from langgraph.types import Interrupt
from pydantic import JsonValue

from fakes import (
    FakeAgent,
    FakeBus,
    FakeLedger,
    FakeRunStream,
    FakeState,
    find_event,
    find_events,
    request,
    text_run,
)
from kokoro_agent.agents.base import AssembledAgent
from kokoro_agent.contract import (
    InboundMessage,
    RunCompleted,
    RunControlReceipt,
    RunRequest,
    SubagentSource,
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
                    {"action_name": _GATED, "allowed_decisions": ["approve", "edit", "reject"]}
                ],
            }
        ),
    ),
    values={
        "messages": [
            AIMessage(content="", id="seg", tool_calls=[{"name": _GATED, "args": {}, "id": _TID}])
        ]
    },
)


def _builder(agent: FakeAgent) -> Callable[[RunRequest], Awaitable[AssembledAgent]]:
    async def _build(_request: RunRequest) -> AssembledAgent:
        return AssembledAgent(agent=agent, tool_descriptions={})

    return _build


def _gated_names(_request: RunRequest) -> frozenset[str]:
    return frozenset({_GATED})


def _no_trace(_request: RunRequest) -> None:
    return None


def _source(_name: str) -> SubagentSource:
    return "runtime-custom"


def _supervisor(agent: FakeAgent, store: FakeLedger) -> RunSupervisor:
    return RunSupervisor(
        agent_builder=_builder(agent),
        store=store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="test-consumer",
    )


def _inbound(raw: dict[str, JsonValue]) -> InboundMessage:
    if raw.get("kind") in {"run.resume", "run.cancel"} and "thread_id" not in raw:
        raw = {**raw, "thread_id": "c1"}
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
        {"kind": "run.resume", "decision_id": "dec_1", "run_id": run_id, "decisions": [{"type": "approve", "tool_id": _TID}]}
    ).model_dump_json()


async def test_control_inbox_keep_first_dedup() -> None:
    ledger = FakeLedger()
    await ledger.try_claim(request("rd"))
    assert await ledger.record_control_inbox("rd", "dec_1", "fp", "{}") is True
    # 重复 decision_id（重发/重投）：命中既有条目 → False（丢弃不重放）。
    assert await ledger.record_control_inbox("rd", "dec_1", "fp", "{}") is False
    # 不同 decision_id 各自入账。
    assert await ledger.record_control_inbox("rd", "dec_2", None, "{}") is True


async def test_cancel_via_control_loop_emits_two_receipts_and_applies() -> None:
    gate = asyncio.Event()  # 永不 set：run 只能被 cancel 结束
    agent = FakeAgent(run=text_run("x"), gates=[gate])
    bus = FakeBus(
        control={
            run_control_stream("cc"): (
                StreamItem(
                    cursor="1",
                    event={"kind": "run.cancel", "decision_id": "dec_1", "run_id": "cc", "thread_id": "c1"},
                ),
            )
        }
    )
    ledger = FakeLedger()
    sup = _supervisor(agent, ledger)
    await sup.dispatch(bus, request("cc"))
    for _ in range(200):
        if run_control_stream("cc") in bus.deleted:
            break
        await asyncio.sleep(0.005)

    # 两时点回执按序上 run events 流（applied 先于 run.completed：cancel apply 即终态）。
    receipts = find_events(bus.run_events("cc"), RunControlReceipt)
    assert [r.payload.control_status for r in receipts] == ["persisted", "applied"]
    assert all(r.payload.decision_id == "dec_1" for r in receipts)
    assert ledger.control_inbox["cc"][0]["status"] == "applied"
    completed = find_event(bus.run_events("cc"), RunCompleted)
    assert completed.payload.status == "cancelled"
    assert run_control_stream("cc") in bus.deleted


async def test_restart_scanner_reapplies_on_fingerprint_match() -> None:
    # 崩溃前：inbox persisted 已落，fingerprint=当时 interrupt 指纹，apply 未跑。serve() 启动续办。
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    ledger = FakeLedger()
    await ledger.try_claim(request("rf"))
    await ledger.record_control_inbox("rf", "dec_1", _fingerprint_of(_PENDING_STATE), _resume_body("rf"))

    bus = FakeBus()  # 空请求流：serve 跑完 startup 续办即收束
    sup = _supervisor(agent, ledger)
    await sup.serve(bus)
    await _drain(sup)

    # 指纹匹配 → 续 apply：agent 收到 resume Command。
    assert len(agent.seen_payloads) >= 1
    assert ledger.control_inbox["rf"][0]["status"] == "applied"
    # restart 续办只补 applied（persisted 已在崩溃前发过，不重发）。
    receipts = find_events(bus.run_events("rf"), RunControlReceipt)
    assert [r.payload.control_status for r in receipts] == ["applied"]


async def test_restart_scanner_supersedes_on_fingerprint_mismatch() -> None:
    # 崩溃前记录的 fingerprint 与当前 interrupt 不符（interrupt 已变/run 已推进）。
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    ledger = FakeLedger()
    await ledger.try_claim(request("rm"))
    await ledger.record_control_inbox("rm", "dec_1", "stale-fingerprint-mismatch", _resume_body("rm"))

    bus = FakeBus()
    sup = _supervisor(agent, ledger)
    await sup.serve(bus)
    await _drain(sup)

    # 不匹配 → 不 apply，标 superseded；无 apply、无 applied 回执。
    assert agent.seen_payloads == []
    assert ledger.control_inbox["rm"][0]["status"] == "superseded"
    assert find_events(bus.run_events("rm"), RunControlReceipt) == []


async def test_terminal_run_control_excluded_from_reapply() -> None:
    # 已终态的 run：其 persisted control 条目不入续办扫描（随 run purge 清理），绝不重放 apply。
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    ledger = FakeLedger()
    await ledger.try_claim(request("rt"))
    await ledger.record_control_inbox(
        "rt", "dec_1", None, _inbound({"kind": "run.cancel", "decision_id": "dec_1", "run_id": "rt"}).model_dump_json()
    )
    await ledger.try_mark_terminal("rt")

    # 终态 run 不进 pending 列表。
    assert await ledger.list_pending_control_inbox() == []

    bus = FakeBus()
    sup = _supervisor(agent, ledger)
    await sup.serve(bus)

    # 未重放 apply；条目留 persisted 待 purge_terminal 清理。
    assert agent.seen_payloads == []
    assert ledger.control_inbox["rt"][0]["status"] == "persisted"
