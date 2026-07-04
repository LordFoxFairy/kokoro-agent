"""supervisor 规格：三路分发、单终态、租约心跳/重拾、control 流传输、任务句柄竞态、serve 隔离。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.types import Command, Interrupt
from pydantic import JsonValue

from fakes import (
    FakeAgent,
    FakeBus,
    FakeRunStateStore,
    FakeRunStream,
    FakeState,
    find_event,
    request,
    text_run,
)
from kokoro_agent.contract import (
    InboundMessage,
    RunCompleted,
    RunFailed,
    RunRequest,
    SubagentSource,
    inbound_adapter,
    run_control_stream,
)
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.worker.messages import parse_inbound
from kokoro_agent.worker.supervisor import RunSupervisor

_GATED = "danger"
_TID = "call-A"


def _builder(agent: FakeAgent) -> Callable[[RunRequest], Awaitable[InvokableAgent]]:
    async def _build(_request: RunRequest) -> InvokableAgent:
        return agent

    return _build


def _gated_names(_request: RunRequest) -> frozenset[str]:
    return frozenset({_GATED})


def _no_trace(_request: RunRequest) -> None:
    return None


def _source(_name: str) -> SubagentSource:
    return "runtime-custom"


def _supervisor(
    agent: FakeAgent, store: FakeRunStateStore | None = None, heartbeat_s: float = 30.0
) -> tuple[RunSupervisor, FakeRunStateStore]:
    state_store = store if store is not None else FakeRunStateStore()
    sup = RunSupervisor(
        agent_builder=_builder(agent),
        store=state_store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="test-consumer",
        heartbeat_s=heartbeat_s,
    )
    return sup, state_store


async def _drain(sup: RunSupervisor) -> None:
    for task in tuple(sup.tasks.values()):
        await task


def _inbound(raw: dict[str, JsonValue]) -> InboundMessage:
    # resume/cancel 契约要求 thread_id；测试省略时补默认。
    if raw.get("kind") in {"run.resume", "run.cancel"} and "thread_id" not in raw:
        raw = {**raw, "thread_id": "c1"}
    return inbound_adapter.validate_python(raw)


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
            AIMessage(content="", id="seg", tool_calls=[{"name": _GATED, "args": {}, "id": _TID}])
        ]
    },
)


def _interrupt_run() -> FakeRunStream:
    return FakeRunStream(is_interrupted=True)


# ① request → 初始 invoke（HumanMessage(content)）+ 单终态。
async def test_request_dispatches_initial_invoke() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("r1"))
    await _drain(sup)

    kinds = bus.kinds("r1")
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert len(agent.seen_payloads) == 1
    initial = agent.seen_payloads[0]
    assert initial == {"messages": [HumanMessage(content="hello")]}


# namespace：checkpoint thread_id 带 namespace 前缀，双 namespace 互不可见。
async def test_thread_id_scoped_by_namespace() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rn", namespace="tenant-a", thread_id="c1"))
    await _drain(sup)
    assert agent.seen_config.get("configurable") == {"thread_id": "tenant-a:c1"}


# ② 重复 run_id → 租约认领去重，不二次 invoke。
async def test_duplicate_run_id_skipped() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("dup"))
    await _drain(sup)
    before = len(bus.published)
    await sup.dispatch(bus, request("dup"))
    await _drain(sup)
    assert len(bus.published) == before
    assert len(agent.seen_payloads) == 1


# ③ resume：有 pending → Command(resume) 按契约翻译成框架决策。
async def test_resume_with_pending_invokes_command() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, store = _supervisor(agent)
    await sup.dispatch(bus, request("r2"))
    await _drain(sup)
    # interrupt 暂停：租约进入暂停哨兵。
    assert "r2" in store.paused_runs
    agent.seen_payloads.clear()

    resume = _inbound(
        {"kind": "run.resume", "run_id": "r2", "decisions": [{"type": "approve", "tool_id": _TID}]}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert len(agent.seen_payloads) == 1
    payload = agent.seen_payloads[0]
    assert isinstance(payload, Command)
    assert payload.resume == {"decisions": [{"type": "approve"}]}
    # 离开暂停：resume 前续租。
    assert "r2" in store.renewed


async def test_resume_edit_and_reject_decision_shapes() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("r4"))
    await _drain(sup)

    agent.seen_payloads.clear()
    edit = _inbound(
        {
            "kind": "run.resume",
            "run_id": "r4",
            "decisions": [{"type": "edit", "tool_id": _TID, "args": {"x": 1}}],
        }
    )
    await sup.dispatch(bus, edit)
    await _drain(sup)
    edit_payload = agent.seen_payloads[0]
    assert isinstance(edit_payload, Command)
    assert edit_payload.resume == {
        "decisions": [{"type": "edit", "edited_action": {"name": _GATED, "args": {"x": 1}}}]
    }

    agent.seen_payloads.clear()
    reject = _inbound(
        {
            "kind": "run.resume",
            "run_id": "r4",
            "decisions": [{"type": "reject", "tool_id": _TID, "reason": "no"}],
        }
    )
    await sup.dispatch(bus, reject)
    await _drain(sup)
    reject_payload = agent.seen_payloads[0]
    assert isinstance(reject_payload, Command)
    assert reject_payload.resume == {"decisions": [{"type": "reject", "message": "no"}]}
    # reject 快照直发 tool.returned{rejected}。
    returned = [e for e in bus.run_events("r4") if e.kind == "tool.returned"]
    assert returned, "reject must emit snapshot tool.returned"


# ④ resume：无 pending → 幂等护栏丢弃。
async def test_resume_without_pending_is_dropped() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("r3"))
    await _drain(sup)
    before = len(bus.published)
    agent.seen_payloads.clear()

    resume = _inbound(
        {"kind": "run.resume", "run_id": "r3", "decisions": [{"type": "approve", "tool_id": _TID}]}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)
    assert agent.seen_payloads == []
    assert len(bus.published) == before


async def test_resume_unknown_run_dropped() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    resume = _inbound(
        {"kind": "run.resume", "run_id": "ghost", "decisions": [{"type": "approve", "tool_id": _TID}]}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)
    assert agent.seen_payloads == []
    assert bus.published == []


# ⑤ cancel：运行中 → task.cancel + run.completed{cancelled} 恰一次。
async def test_cancel_running_emits_cancelled() -> None:
    gate = asyncio.Event()
    agent = FakeAgent(run=text_run("x"), gates=[gate])
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("r5"))
    await asyncio.sleep(0)

    await sup.dispatch(bus, _inbound({"kind": "run.cancel", "run_id": "r5"}))
    await _drain(sup)

    completed = find_event(bus.run_events("r5"), RunCompleted)
    assert completed.payload.status == "cancelled"
    terminals = [e for e in bus.run_events("r5") if e.kind in {"run.completed", "run.failed"}]
    assert len(terminals) == 1


async def test_cancel_unknown_run_dropped() -> None:
    bus = FakeBus()
    sup, _store = _supervisor(FakeAgent(run=text_run("hi")))
    await sup.dispatch(bus, _inbound({"kind": "run.cancel", "run_id": "gone"}))
    assert bus.published == []


async def test_cancel_after_natural_completion_no_duplicate_terminal() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rc1"))
    await _drain(sup)
    await sup.dispatch(bus, _inbound({"kind": "run.cancel", "run_id": "rc1"}))

    terminals = [e for e in bus.run_events("rc1") if e.kind in {"run.completed", "run.failed"}]
    assert len(terminals) == 1
    completed = find_event(terminals, RunCompleted)
    assert completed.payload.status == "completed"


async def test_cancel_after_pause_emits_cancelled() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rc2"))
    await _drain(sup)
    await sup.dispatch(bus, _inbound({"kind": "run.cancel", "run_id": "rc2"}))
    completed = find_event(bus.run_events("rc2"), RunCompleted)
    assert completed.payload.status == "cancelled"


async def test_resume_after_cancel_blocked_by_terminal() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rc4"))
    await _drain(sup)
    await sup.dispatch(bus, _inbound({"kind": "run.cancel", "run_id": "rc4"}))
    before = len(bus.published)
    agent.seen_payloads.clear()

    resume = _inbound(
        {"kind": "run.resume", "run_id": "rc4", "decisions": [{"type": "approve", "tool_id": _TID}]}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)
    assert agent.seen_payloads == []
    assert len(bus.published) == before


async def test_builder_failure_emits_run_failed_once() -> None:
    async def boom(_request: RunRequest) -> InvokableAgent:
        raise ValueError("bad model")

    bus = FakeBus()
    store = FakeRunStateStore()
    sup = RunSupervisor(
        agent_builder=boom,
        store=store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="t",
    )
    await sup.dispatch(bus, request("rbf"))
    await _drain(sup)
    failed = find_event(bus.run_events("rbf"), RunFailed)
    assert failed.payload.error_kind == "ValueError"
    assert failed.payload.message == "bad model"

    # 构建失败已认领终态：cancel 不补发第二终态。
    await sup.dispatch(bus, _inbound({"kind": "run.cancel", "run_id": "rbf"}))
    terminals = [e for e in bus.run_events("rbf") if e.kind in {"run.completed", "run.failed"}]
    assert len(terminals) == 1


# control 流传输：cancel 从 per-run control 流进来，路由到 _on_cancel 并终态删流。
async def test_control_stream_delivers_cancel() -> None:
    gate = asyncio.Event()  # 永不 set：run 只能被 cancel 结束
    agent = FakeAgent(run=text_run("x"), gates=[gate])
    bus = FakeBus(
        control={
            run_control_stream("cx"): (
                StreamItem(
                    cursor="1",
                    event={"kind": "run.cancel", "run_id": "cx", "thread_id": "c1"},
                ),
            )
        }
    )
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("cx"))
    # 轮询等待 control 监听消费 cancel 并终态删流。
    for _ in range(200):
        if run_control_stream("cx") in bus.deleted:
            break
        await asyncio.sleep(0.005)

    completed = find_event(bus.run_events("cx"), RunCompleted)
    assert completed.payload.status == "cancelled"
    assert run_control_stream("cx") in bus.deleted


# ⑥ 任务句柄按身份弹出：旧任务完成回调不误删新任务句柄。
async def test_task_handle_popped_by_identity_not_run_id() -> None:
    gate1 = asyncio.Event()
    gate2 = asyncio.Event()
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE, gates=[gate1, gate2])
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("race"))
    await asyncio.sleep(0)  # task1 阻塞在 gate1

    resume = _inbound(
        {"kind": "run.resume", "run_id": "race", "decisions": [{"type": "approve", "tool_id": _TID}]}
    )
    await sup.dispatch(bus, resume)  # task2 覆盖同 run_id 句柄，阻塞在 gate2
    await asyncio.sleep(0)
    task2 = sup.tasks.get("race")
    assert task2 is not None

    gate1.set()  # 旧任务完成 → 其回调按身份比对，不得弹掉 task2
    await asyncio.sleep(0.01)
    assert sup.tasks.get("race") is task2

    gate2.set()
    await _drain(sup)
    assert "race" not in sup.tasks


# ⑦ serve：consumer-group 消费 + parse 后 ack（坏帧也 ack）+ 单消息失败隔离。
async def test_serve_acks_and_isolates_failures() -> None:
    class _BoomStore(FakeRunStateStore):
        async def is_terminal(self, run_id: str) -> bool:
            raise RuntimeError("store boom")

    good = StreamItem(cursor="1", event=dict(request("sv1").model_dump()))
    malformed = StreamItem(cursor="2", event={"kind": "run.request", "run_id": ""})
    resume_boom = StreamItem(
        cursor="3",
        event={
            "kind": "run.resume",
            "run_id": "rx",
            "thread_id": "c1",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        },
    )
    bus = FakeBus(inbound=(good, malformed, resume_boom))
    store = _BoomStore()
    sup = RunSupervisor(
        agent_builder=_builder(FakeAgent(run=text_run("hi"))),
        store=store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="t",
    )
    await sup.serve(bus)  # 必须正常返回，而非异常冒泡杀掉 worker
    await _drain(sup)

    # 三条消息（含坏帧）全部 ack：坏帧不重投，崩溃恢复权在租约。
    assert bus.acked == ["1", "2", "3"]
    # 好请求照常跑完；resume 的存储抛错收口为该 run 的 run.failed。
    assert bus.kinds("sv1")[-1] == "run.completed"
    failed = find_event(bus.run_events("rx"), RunFailed)
    assert failed.payload.error_kind == "RuntimeError"


def test_parse_inbound_malformed_returns_none() -> None:
    assert parse_inbound({"kind": "run.request"}) is None
    assert parse_inbound({"kind": "mystery"}) is None


# ⑧ 心跳：活跃 run 续租；过期 run 重拾再执行；自己在跑的不双起。
async def test_heartbeat_renews_and_reclaims() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, store = _supervisor(agent)

    gate = asyncio.Event()
    running_agent = FakeAgent(run=text_run("slow"), gates=[gate])
    sup_running, store_running = _supervisor(running_agent)
    await sup_running.dispatch(bus, request("active"))
    await asyncio.sleep(0)
    await sup_running.heartbeat_once(bus)
    assert "active" in store_running.renewed
    gate.set()
    await _drain(sup_running)

    # 过期重拾：store 吐出他处遗留的 request → 重新执行到终态（index 续接不回卷）。
    store.expired = [request("orphan")]
    await sup.heartbeat_once(bus)
    await _drain(sup)
    assert bus.kinds("orphan")[-1] == "run.completed"


async def test_heartbeat_skips_reclaim_of_own_running_task() -> None:
    gate = asyncio.Event()
    agent = FakeAgent(run=text_run("hi"), gates=[gate])
    bus = FakeBus()
    sup, store = _supervisor(agent)
    await sup.dispatch(bus, request("mine"))
    await asyncio.sleep(0)

    store.expired = [request("mine")]
    await sup.heartbeat_once(bus)
    # 未双起：仍只有最初那次 invoke。
    assert len(agent.seen_payloads) == 1
    gate.set()
    await _drain(sup)


# ⑨ 跨 supervisor（模拟另一 pod / 重启）：共享 store + 总线续接，index 不回卷。
async def test_resume_on_fresh_supervisor_via_shared_store() -> None:
    store = FakeRunStateStore()
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup_a, _ = _supervisor(agent, store=store)
    await sup_a.dispatch(bus, request("rx2"))
    await _drain(sup_a)

    sup_b, _ = _supervisor(agent, store=store)
    agent.seen_payloads.clear()
    resume = _inbound(
        {"kind": "run.resume", "run_id": "rx2", "decisions": [{"type": "approve", "tool_id": _TID}]}
    )
    await sup_b.dispatch(bus, resume)
    await _drain(sup_b)

    assert len(agent.seen_payloads) == 1
    assert isinstance(agent.seen_payloads[0], Command)
    indexes = [e.index for e in bus.run_events("rx2")]
    assert indexes == sorted(indexes)
    assert len(set(indexes)) == len(indexes), "index 不得回卷复用"


async def test_pause_recorded_on_interrupt() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, store = _supervisor(agent)
    await sup.dispatch(bus, request("rp"))
    await _drain(sup)
    assert store.paused_runs == ["rp"]
    assert store.leases["rp"] is None


# ⑨ control 监听收养：认领 worker 崩溃后，暂停 run 的 resume/cancel 由任意存活 worker 心跳接管。
async def test_heartbeat_adopts_control_listener_for_paused_run() -> None:
    agent = FakeAgent(run=text_run("unused"))
    bus = FakeBus(
        control={
            run_control_stream("orphan-hitl"): (
                StreamItem(
                    cursor="1",
                    event={"kind": "run.cancel", "run_id": "orphan-hitl", "thread_id": "t1"},
                ),
            )
        }
    )
    sup, store = _supervisor(agent)
    # 模拟他处 worker 崩溃遗留：run 已认领并暂停（哨兵），本 supervisor 从未 dispatch 过它。
    await store.try_claim(request("orphan-hitl"))
    await store.pause("orphan-hitl")

    await sup.heartbeat_once(bus)
    for _ in range(200):
        if run_control_stream("orphan-hitl") in bus.deleted:
            break
        await asyncio.sleep(0.005)

    completed = find_event(bus.run_events("orphan-hitl"), RunCompleted)
    assert completed.payload.status == "cancelled"
    assert run_control_stream("orphan-hitl") in bus.deleted


# ⑩ 优雅停机：drain 等活跃 run 收尾（限时），暂停 run 不阻塞退出。
async def test_drain_waits_active_runs_until_deadline() -> None:
    gate = asyncio.Event()
    agent = FakeAgent(run=text_run("slow"), gates=[gate])
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("draining"))
    await asyncio.sleep(0)

    async def release() -> None:
        await asyncio.sleep(0.05)
        gate.set()

    releaser = asyncio.create_task(release())
    drained = await sup.drain(timeout_s=5.0)
    await releaser
    assert drained is True
    assert bus.kinds("draining")[-1] == "run.completed"


async def test_drain_times_out_on_stuck_run() -> None:
    gate = asyncio.Event()  # 永不 set：模拟卡死 run
    agent = FakeAgent(run=text_run("stuck"), gates=[gate])
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("stuck"))
    await asyncio.sleep(0)
    drained = await sup.drain(timeout_s=0.1)
    assert drained is False  # 超时如实上报；剩余恢复权归 TTL 租约重拾
    gate.set()
    await _drain(sup)


# ⑪ 收养监听自退出必须出表：他处终态删流后 NOGROUP 收束，_control 不得无界泄漏。
async def test_adopted_listener_pops_after_remote_teardown() -> None:
    agent = FakeAgent(run=text_run("unused"))

    class _ClosingBus(FakeBus):
        async def subscribe(
            self, stream: str, *, group: str, consumer: str
        ) -> AsyncIterator[StreamItem]:
            raise RuntimeError("NOGROUP no such stream")
            yield StreamItem(cursor="0", event={})  # pragma: no cover — 使其成为异步生成器

    closing = _ClosingBus()
    sup, store = _supervisor(agent)
    await store.try_claim(request("gone"))
    await store.pause("gone")
    await store.try_mark_terminal("gone")  # 他处已终态
    await sup.heartbeat_once(closing)  # 收养入口＝心跳（公开面）
    for _ in range(200):
        if not sup.control_listeners:
            break
        await asyncio.sleep(0.005)
    assert sup.control_listeners == {}


# ⑫ 复审 #1 竞态：多 worker 收养后 resume/cancel 分投两处——终态后绝不 spawn。
async def test_resume_lost_to_concurrent_cancel_does_not_spawn() -> None:
    class _CancelInWindowStore(FakeRunStateStore):
        async def renew(self, run_id: str) -> None:
            await super().renew(run_id)
            # 模拟他处 cancel 恰在 resume 长窗（build/aget_state/renew 之后）完成终态。
            self.terminals.add(run_id)

    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, store = _supervisor(agent, store=_CancelInWindowStore())
    await sup.dispatch(bus, request("rc"))
    await _drain(sup)
    assert "rc" in store.paused_runs
    agent.seen_payloads.clear()

    resume = _inbound(
        {"kind": "run.resume", "run_id": "rc", "decisions": [{"type": "approve", "tool_id": _TID}]}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)
    # 终态复检收手：不 spawn、无新 invoke。
    assert agent.seen_payloads == []
