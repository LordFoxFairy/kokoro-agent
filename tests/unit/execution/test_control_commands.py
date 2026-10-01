"""R2 control command 与 receipt 闭环（agent 侧）：

- command ledger keep-first：重复 command_id 不双放。
- 两时点回执：persisted（落 command ledger）与 applied（apply 后）各发一次 run.control.receipt。
- 重启续办 scanner：以持久 collection revision/ref 与原 command 判定，
  unknown 不重投，stale 整批拒绝（经 public serve() 启动路径驱动）。
"""

from __future__ import annotations

from support.fakes import (
    read_unpaused_interaction,
    InitialPauseThenUnknownReader,
    interaction_pause_fixture,
    admit_resume_fixture,
    admit_control_fixture,
)

from support.fakes import finish_run

import asyncio
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
from kokoro_agent.protocol import (
    InboundMessage,
    RunCompleted,
    RunControlReceipt,
    RunRequest,
    SubagentSource,
    RunSteer,
    RunCancel,
    RunResume,
    inbound_adapter,
    run_control_stream,
)
from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.worker.supervisor import RunSupervisor
from kokoro_agent.domain.run.repository import LeaseFence

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


def _builder(
    agent: FakeAgent,
) -> Callable[[RunRequest, LeaseFence], Awaitable[AgentHandle]]:
    async def _build(_request: RunRequest, _lease: LeaseFence) -> AgentHandle:
        return AgentHandle(runnable=agent, tool_descriptions={})

    return _build


def _gated_names(_request: RunRequest) -> frozenset[str]:
    return frozenset({_GATED})


def _no_trace(_request: RunRequest) -> None:
    return None


def _source(_name: str) -> SubagentSource:
    return "runtime-custom"


def _supervisor(
    agent: FakeAgent, store: FakeRunRepository, *, native_resume: JsonValue = None
) -> RunSupervisor:
    return RunSupervisor(
        interaction_reader=(
            InitialPauseThenUnknownReader(store, native_value=native_resume)
            if native_resume is not None
            else read_unpaused_interaction
        ),
        agent_builder=_builder(agent),
        run_repository=store,
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


@pytest.mark.parametrize(
    ("kind", "body"),
    [
        (
            "run.resume",
            {
                "command_id": "dec_r",
                "expected_pause_revision": 1,
                "pause_ref": "pause-1",
                "decisions": [{"type": "approve", "item_id": "item-A"}],
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
    command = await admit_control_fixture(
        run_repository,
        RunResume.model_validate(
            {
                "kind": "run.resume",
                "run_id": "fs",
                "session_id": "foreign-session",
                "command_id": "dec_1",
                "expected_pause_revision": 1,
                "pause_ref": "pause-1",
                "decisions": [{"type": "approve", "item_id": "item-A"}],
            }
        ),
    )
    await run_repository.record_control_delivery(
        "fs", "dec_1", command.request_digest, None, command.model_dump_json()
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
    for command_id in ("dec_1", "dec_2"):
        await admit_control_fixture(
            run_repository,
            RunCancel(
                kind="run.cancel", run_id="rd", session_id="s1", command_id=command_id
            ),
        )
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
    run = request("cc")
    run_repository.dispatches[run.run_id] = "pending"
    run_repository.dispatch_requests[run.run_id] = run
    await sup.dispatch(bus, run)
    await admit_control_fixture(
        run_repository,
        RunCancel(kind="run.cancel", run_id="cc", session_id="s1", command_id="dec_1"),
    )
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
    steer = await admit_control_fixture(run_repository, steer)
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


async def test_restart_scanner_dispatches_accepted_once_then_unknown_never_reinvokes() -> (
    None
):
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    repository = FakeRunRepository()
    run = request("rf")
    lease = await repository.try_claim(run)
    assert lease is not None
    await repository.record_pause(run, lease, interaction_pause_fixture(run))
    command = await admit_resume_fixture(
        repository,
        run,
        command_id="dec_1",
        decisions=[{"type": "approve", "item_id": "item-A"}],
    )
    await repository.record_control_delivery(
        run.run_id,
        command.command_id,
        command.request_digest,
        None,
        command.model_dump_json(),
    )
    bus = FakeBus()
    native: JsonValue = {"decisions": [{"type": "approve"}]}
    sup = _supervisor(agent, repository, native_resume=native)
    await sup.serve(bus)
    await _drain(sup)
    assert len(agent.seen_payloads) == 1
    assert repository.control_commands[("rf", "dec_1")]["status"] == "succeeded"
    receipts = find_events(bus.run_events("rf"), RunControlReceipt)
    assert [r.payload.control_status for r in receipts] == ["applied"]
    context = await repository.read_resume_context(run, "dec_1")
    assert context is not None and context.intent.status.value == "unknown"
    resumed = _supervisor(agent, repository, native_resume=native)
    await resumed.serve(bus)
    await _drain(resumed)
    assert len(agent.seen_payloads) == 1


async def test_restart_scanner_rejects_stale_collection_without_native_effect() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    repository = FakeRunRepository()
    run = request("rm")
    lease = await repository.try_claim(run)
    assert lease is not None
    await repository.record_pause(run, lease, interaction_pause_fixture(run))
    before = await repository.read_interaction(run)
    command = await admit_resume_fixture(
        repository,
        run,
        command_id="dec_1",
        revision=2,
        decisions=[{"type": "approve", "item_id": "item-A"}],
    )
    await repository.record_control_delivery(
        run.run_id,
        command.command_id,
        command.request_digest,
        None,
        command.model_dump_json(),
    )
    bus = FakeBus()
    sup = _supervisor(agent, repository)
    await sup.serve(bus)
    await _drain(sup)
    assert agent.seen_payloads == []
    assert repository.control_commands[("rm", "dec_1")]["status"] == "failed"
    assert (
        repository.control_commands[("rm", "dec_1")]["error_code"]
        == "interaction_conflict"
    )
    assert await repository.read_interaction(run) == before
    assert find_events(bus.run_events("rm"), RunControlReceipt) == []


async def test_terminal_run_control_excluded_from_reapply() -> None:
    # 已终态的 run：其 persisted control 条目不入续办扫描（随 run purge 清理），绝不重放 apply。
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    run_repository = FakeRunRepository()
    await run_repository.try_claim(request("rt"))
    await admit_control_fixture(
        run_repository,
        RunCancel(kind="run.cancel", run_id="rt", session_id="s1", command_id="dec_1"),
    )
    await run_repository.record_control_delivery(
        "rt",
        "dec_1",
        None,
        None,
        _inbound(
            {"kind": "run.cancel", "command_id": "dec_1", "run_id": "rt"}
        ).model_dump_json(),
    )
    terminal_lease = run_repository.current_lease("rt")
    assert terminal_lease is not None
    await finish_run(run_repository, "rt", terminal_lease)

    # 终态 run 不进 pending 列表。
    assert await run_repository.list_pending_control_delivery() == []

    bus = FakeBus()
    sup = _supervisor(agent, run_repository)
    await sup.serve(bus)

    # 未重放 apply；条目留 persisted 待 purge_terminal 清理。
    assert agent.seen_payloads == []
    assert run_repository.control_commands[("rt", "dec_1")]["status"] == "persisted"


@pytest.mark.parametrize(
    ("method", "parameters"),
    [
        ("record_checkpoint_observation", ("self", "request", "lease", "observation")),
        ("read_checkpoint_observations", ("self", "request", "target")),
        ("reconcile_resume", ("self", "request", "lease", "evidence")),
        ("record_reconcile_probe", ("self", "request", "lease", "probe")),
        (
            "reset_reconcile_probe",
            ("self", "request", "lease", "command_id", "attempt_id"),
        ),
        ("list_unsettled_interactions", ("self", "limit")),
        ("read_resume_context", ("self", "request", "command_id")),
    ],
)
def test_bridge_r35_exact_run_port_is_present(
    method: str, parameters: tuple[str, ...]
) -> None:
    """Declaration RED only; transaction/call-gate proof is in real-PG tests."""
    import inspect

    from kokoro_agent.domain.run.repositories import RunInteractionPort

    operation = getattr(RunInteractionPort, method, None)
    assert callable(operation), f"Approved RunInteractionPort.{method} is missing"
    assert tuple(inspect.signature(operation).parameters) == parameters


@pytest.mark.parametrize("failure_kind", ["read", "authority", "missing"])
async def test_resume_operational_failures_are_not_interaction_conflicts(
    failure_kind: str,
) -> None:
    from kokoro_agent.domain.run.interactions import (
        AcceptedResume,
        ReplayedResume,
        InteractionAuthorityLost,
        InteractionRunMissing,
    )

    failure = {
        "read": RuntimeError("read unavailable"),
        "authority": InteractionAuthorityLost(),
        "missing": InteractionRunMissing(),
    }[failure_kind]

    class FailingAcceptance(FakeRunRepository):
        async def accept_resume(
            self, request: RunRequest, command_id: str, owner: str
        ) -> AcceptedResume | ReplayedResume:
            raise failure

    repository = FailingAcceptance()
    run = request("read-failure")
    lease = await repository.try_claim(run)
    assert lease is not None
    await repository.record_pause(run, lease, interaction_pause_fixture(run))
    before = await repository.read_interaction(run)
    command = await admit_resume_fixture(
        repository,
        run,
        command_id="command",
        decisions=[{"type": "approve", "item_id": "item-A"}],
    )
    await repository.record_control_delivery(
        run.run_id,
        command.command_id,
        command.request_digest,
        None,
        command.model_dump_json(),
    )
    agent, bus = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE), FakeBus()
    supervisor = _supervisor(agent, repository)
    with pytest.raises(type(failure)) as caught:
        await supervisor.serve(bus)
    assert caught.value is failure
    assert repository.control_commands[(run.run_id, "command")]["status"] == "persisted"
    assert repository.control_commands[(run.run_id, "command")]["error_code"] is None
    assert await repository.read_interaction(run) == before
    assert not await repository.is_terminal(run.run_id)
    assert agent.seen_payloads == []
    assert bus.run_events(run.run_id) == []
