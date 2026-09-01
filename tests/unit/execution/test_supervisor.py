"""supervisor 规格：三路分发、单终态、租约心跳/重拾、control 流传输、任务句柄竞态、serve 隔离。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.types import Command, Interrupt
from pydantic import JsonValue, TypeAdapter

from support.fakes import (
    FakeAgent,
    FakeBus,
    FakeLedger,
    FakeRunStream,
    FakeState,
    find_event,
    request,
    text_run,
)
from support.chat import FakeChatStore
from kokoro_agent.chat.models import ChatMessageDraft, ChatMessageRecord
from kokoro_agent.contract import (
    InboundMessage,
    REQUESTS_STREAM,
    RunCompleted,
    RunFailed,
    RunRequest,
    RunSteer,
    SubagentSource,
    inbound_adapter,
    run_control_stream,
)
from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.worker.messages import parse_inbound
from kokoro_agent.worker.supervisor import RunSupervisor
from kokoro_agent.execution.scope import RunScope

_GATED = "danger"
_TID = "call-A"
_CHAT_NS = RunScope.of(request("scope")).namespace


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


def _supervisor(
    agent: FakeAgent,
    store: FakeLedger | None = None,
    heartbeat_s: float = 30.0,
    chat_store: FakeChatStore | None = None,
) -> tuple[RunSupervisor, FakeLedger]:
    state_store = store if store is not None else FakeLedger()
    sup = RunSupervisor(
        agent_builder=_builder(agent),
        store=state_store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="test-consumer",
        heartbeat_s=heartbeat_s,
        chat_store=chat_store,
    )
    return sup, state_store


async def _drain(sup: RunSupervisor) -> None:
    for task in tuple(sup.tasks.values()):
        await task


def _inbound(raw: dict[str, JsonValue]) -> InboundMessage:
    # resume/cancel 契约要求 thread_id；测试省略时补默认。
    if raw.get("kind") in {"run.resume", "run.cancel"} and "session_id" not in raw:
        raw = {**raw, "session_id": "s1"}
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
            AIMessage(
                content="",
                id="seg",
                tool_calls=[{"name": _GATED, "args": {}, "id": _TID}],
            )
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
    # State 保持 DeepAgents 原生形状；身份与隔离信息只进入装配和 RunnableConfig。
    assert initial == {
        "messages": [HumanMessage(content="hello", id="native-input:r1")]
    }


async def test_request_consumer_persists_user_message_and_safe_chat_events() -> None:
    chat_store = FakeChatStore()
    item = StreamItem(cursor="1", event=request("chat-1").model_dump())
    bus = FakeBus(inbound=(item,))
    supervisor, _store = _supervisor(
        FakeAgent(run=text_run("answer")), chat_store=chat_store
    )

    await supervisor.serve(bus)
    await _drain(supervisor)

    history = await chat_store.history(_CHAT_NS, "s1")
    assert history[0].role == "user"
    assert history[0].chat_message_id == "chat-1-m"
    assert [event.event_type for event in await chat_store.replay(_CHAT_NS, "s1")] == [
        "run.started",
        "assistant.delta",
        "assistant.completed",
        "run.completed",
    ]


async def test_chat_message_failure_happens_before_dispatch_claim_and_ack() -> None:
    class _FailingChatStore(FakeChatStore):
        async def save_message(self, message: ChatMessageDraft) -> ChatMessageRecord:
            raise RuntimeError("chat unavailable")

    item = StreamItem(cursor="1", event=request("chat-fail").model_dump())
    bus = FakeBus(inbound=(item,))
    agent = FakeAgent(run=text_run("unreachable"))
    supervisor, ledger = _supervisor(agent, chat_store=_FailingChatStore())

    await supervisor.serve(bus)

    assert bus.acked == []
    assert "chat-fail" not in ledger.requests
    assert agent.seen_payloads == []


# LangGraph checkpoint 只使用 GA session_id；namespace 留在 GA 资源隔离边界。
async def test_checkpoint_thread_id_uses_session_id() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rn", namespace="tenant-a", thread_id="c1"))
    await _drain(sup)
    assert agent.seen_config.get("configurable") == {"thread_id": "s1"}


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
        {
            "kind": "run.resume",
            "decision_id": "dec_wire",
            "run_id": "r2",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        }
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert len(agent.seen_payloads) == 1
    payload = agent.seen_payloads[0]
    assert isinstance(payload, Command)
    assert payload.resume == {"decisions": [{"type": "approve"}]}
    # 离开暂停：resume 前完成所有权交接（fencing 属主随收养更新）；
    # 段末再次 interrupt 会重回暂停哨兵，故不断言租约数值。
    assert store.owners.get("r2") == "test-consumer"


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
            "decision_id": "dec_wire",
            "run_id": "r4",
            "decisions": [{"type": "edit", "tool_id": _TID, "args": {"x": 1}}],
        }
    )
    await sup.dispatch(bus, edit)
    await _drain(sup)
    edit_payload = agent.seen_payloads[0]
    assert isinstance(edit_payload, Command)
    assert edit_payload.resume == {
        "decisions": [
            {"type": "edit", "edited_action": {"name": _GATED, "args": {"x": 1}}}
        ]
    }

    agent.seen_payloads.clear()
    reject = _inbound(
        {
            "kind": "run.resume",
            "decision_id": "dec_wire",
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
        {
            "kind": "run.resume",
            "decision_id": "dec_wire",
            "run_id": "r3",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        }
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
        {
            "kind": "run.resume",
            "decision_id": "dec_wire",
            "run_id": "ghost",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        }
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)
    assert agent.seen_payloads == []
    assert bus.published == []


# steer：入账信箱（keep-first），不打断运行、不发事件；终态后到达安全丢弃。
async def test_steer_lands_in_mailbox_without_interrupting() -> None:
    gate = asyncio.Event()
    agent = FakeAgent(run=text_run("x"), gates=[gate])
    bus = FakeBus()
    sup, store = _supervisor(agent)
    await sup.dispatch(bus, request("rs1"))
    await asyncio.sleep(0)

    steer = _inbound(
        {
            "kind": "run.steer",
            "run_id": "rs1",
            "session_id": "s1",
            "message_id": "m9",
            "content": "改方向",
        }
    )
    await sup.dispatch(bus, steer)
    await sup.dispatch(bus, steer)  # 重放幂等
    assert store.steers["rs1"] == [("m9", "改方向")]

    gate.set()
    await _drain(sup)
    kinds = bus.kinds("rs1")
    assert kinds[-1] == "run.completed"  # steer 不产生额外 wire 事件、不打断 run


async def test_steer_after_terminal_dropped() -> None:
    agent = FakeAgent(run=text_run("x"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rs2"))
    await _drain(sup)
    await sup.dispatch(
        bus,
        _inbound(
            {
                "kind": "run.steer",
                "run_id": "rs2",
                "session_id": "s1",
                "message_id": "m1",
                "content": "太迟了",
            }
        ),
    )
    # run 已终态：信箱入账与否无消费者，但绝不抛错、绝不产生新事件。
    terminals = [
        e for e in bus.run_events("rs2") if e.kind in {"run.completed", "run.failed"}
    ]
    assert len(terminals) == 1


# ⑤ cancel：运行中 → task.cancel + run.completed{cancelled} 恰一次。
async def test_cancel_running_emits_cancelled() -> None:
    gate = asyncio.Event()
    agent = FakeAgent(run=text_run("x"), gates=[gate])
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("r5"))
    await asyncio.sleep(0)

    await sup.dispatch(
        bus, _inbound({"kind": "run.cancel", "decision_id": "dec_wire", "run_id": "r5"})
    )
    await _drain(sup)

    completed = find_event(bus.run_events("r5"), RunCompleted)
    assert completed.payload.status == "cancelled"
    terminals = [
        e for e in bus.run_events("r5") if e.kind in {"run.completed", "run.failed"}
    ]
    assert len(terminals) == 1


async def test_cancel_unknown_run_dropped() -> None:
    bus = FakeBus()
    sup, _store = _supervisor(FakeAgent(run=text_run("hi")))
    await sup.dispatch(
        bus,
        _inbound({"kind": "run.cancel", "decision_id": "dec_wire", "run_id": "gone"}),
    )
    assert bus.published == []


async def test_cancel_after_natural_completion_no_duplicate_terminal() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rc1"))
    await _drain(sup)
    await sup.dispatch(
        bus,
        _inbound({"kind": "run.cancel", "decision_id": "dec_wire", "run_id": "rc1"}),
    )

    terminals = [
        e for e in bus.run_events("rc1") if e.kind in {"run.completed", "run.failed"}
    ]
    assert len(terminals) == 1
    completed = find_event(terminals, RunCompleted)
    assert completed.payload.status == "completed"


async def test_cancel_after_pause_emits_cancelled() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rc2"))
    await _drain(sup)
    await sup.dispatch(
        bus,
        _inbound({"kind": "run.cancel", "decision_id": "dec_wire", "run_id": "rc2"}),
    )
    completed = find_event(bus.run_events("rc2"), RunCompleted)
    assert completed.payload.status == "cancelled"


async def test_resume_after_cancel_blocked_by_terminal() -> None:
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rc4"))
    await _drain(sup)
    await sup.dispatch(
        bus,
        _inbound({"kind": "run.cancel", "decision_id": "dec_wire", "run_id": "rc4"}),
    )
    before = len(bus.published)
    agent.seen_payloads.clear()

    resume = _inbound(
        {
            "kind": "run.resume",
            "decision_id": "dec_wire",
            "run_id": "rc4",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        }
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)
    assert agent.seen_payloads == []
    assert len(bus.published) == before


async def test_builder_failure_emits_run_failed_once() -> None:
    async def boom(_request: RunRequest) -> AgentHandle:
        raise ValueError("bad model")

    bus = FakeBus()
    store = FakeLedger()
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
    await sup.dispatch(
        bus,
        _inbound({"kind": "run.cancel", "decision_id": "dec_wire", "run_id": "rbf"}),
    )
    terminals = [
        e for e in bus.run_events("rbf") if e.kind in {"run.completed", "run.failed"}
    ]
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
                    event={
                        "kind": "run.cancel",
                        "decision_id": "dec_wire",
                        "run_id": "cx",
                        "session_id": "s1",
                    },
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
        {
            "kind": "run.resume",
            "decision_id": "dec_wire",
            "run_id": "race",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        }
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
    class _BoomStore(FakeLedger):
        async def is_terminal(self, run_id: str) -> bool:
            raise RuntimeError("store boom")

    good = StreamItem(cursor="1", event=dict(request("sv1").model_dump()))
    malformed = StreamItem(cursor="2", event={"kind": "run.request", "run_id": ""})
    resume_boom = StreamItem(
        cursor="3",
        event={
            "kind": "run.resume",
            "decision_id": "dec_wire",
            "run_id": "rx",
            "session_id": "s1",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        },
    )
    bus = FakeBus(inbound=(good, malformed, resume_boom))
    store = _BoomStore()
    # The control frame must refer to a durable run so the failure path reaches
    # the terminal-state guard (rather than being discarded as an unknown run).
    store.requests["rx"] = request("rx")
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


class _FailingOutboxBus(FakeBus):
    def __init__(self, *, fail: bool) -> None:
        super().__init__()
        self._fail = fail

    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        if self._fail:
            raise RuntimeError("outbox publish failed")
        return await super().publish(stream, event, maxlen=maxlen)


async def test_heartbeat_republishes_queued_outbox_and_dedupes_after_success() -> None:
    store = FakeLedger()
    await store.try_claim(request("queued-outbox"))
    await store.stage_critical_frame(
        "queued-outbox",
        "run.started",
        0,
        111,
        "{}",
        terminal=False,
    )
    bus = _FailingOutboxBus(fail=False)
    sup, _ = _supervisor(FakeAgent(run=text_run("hi")), store=store)

    await sup.heartbeat_once(bus)
    assert [event.kind for event in bus.run_events("queued-outbox")] == ["run.started"]
    assert await store.list_unpublished_outbox() == []

    await sup.heartbeat_once(bus)
    assert [event.kind for event in bus.run_events("queued-outbox")] == ["run.started"]


async def test_heartbeat_keeps_queued_outbox_recoverable_on_publish_failure() -> None:
    store = FakeLedger()
    await store.try_claim(request("queued-fail"))
    await store.stage_critical_frame(
        "queued-fail",
        "run.started",
        0,
        111,
        "{}",
        terminal=False,
    )
    bus = _FailingOutboxBus(fail=True)
    sup, _ = _supervisor(FakeAgent(run=text_run("hi")), store=store)

    await sup.heartbeat_once(bus)
    assert bus.run_events("queued-fail") == []
    assert [frame.kind for frame in await store.list_unpublished_outbox()] == ["run.started"]


# ⑨ 跨 supervisor（模拟另一 pod / 重启）：共享 store + 总线续接，index 不回卷。
async def test_resume_on_fresh_supervisor_via_shared_store() -> None:
    store = FakeLedger()
    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup_a, _ = _supervisor(agent, store=store)
    await sup_a.dispatch(bus, request("rx2"))
    await _drain(sup_a)

    sup_b, _ = _supervisor(agent, store=store)
    agent.seen_payloads.clear()
    resume = _inbound(
        {
            "kind": "run.resume",
            "decision_id": "dec_wire",
            "run_id": "rx2",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        }
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
                    event={
                        "kind": "run.cancel",
                        "decision_id": "dec_wire",
                        "run_id": "orphan-hitl",
                        "session_id": "s1",
                    },
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
            yield StreamItem(
                cursor="0", event={}
            )  # pragma: no cover — 使其成为异步生成器

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
    class _CancelInWindowStore(FakeLedger):
        async def adopt(self, run_id: str, owner: str = "test-consumer") -> None:
            await super().adopt(run_id, owner)
            # 模拟他处 cancel 恰在 resume 长窗（build/aget_state/adopt 之后）完成终态。
            self.terminals.add(run_id)

    agent = FakeAgent(run=_interrupt_run(), state=_PENDING_STATE)
    bus = FakeBus()
    sup, store = _supervisor(agent, store=_CancelInWindowStore())
    await sup.dispatch(bus, request("rc"))
    await _drain(sup)
    assert "rc" in store.paused_runs
    agent.seen_payloads.clear()

    resume = _inbound(
        {
            "kind": "run.resume",
            "decision_id": "dec_wire",
            "run_id": "rc",
            "decisions": [{"type": "approve", "tool_id": _TID}],
        }
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)
    # 终态复检收手：不 spawn、无新 invoke。
    assert agent.seen_payloads == []


# ⑬ 崩溃重拾幂等：原始 input 重放（TTL 重拾语义）不得复制用户消息——稳定 id=message_id。
async def test_initial_payload_carries_stable_message_id() -> None:
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, _store = _supervisor(agent)
    await sup.dispatch(bus, request("rid"))
    await _drain(sup)
    payload = agent.seen_payloads[0]
    assert isinstance(payload, dict)
    messages = TypeAdapter(list[HumanMessage]).validate_python(payload["messages"])
    assert messages[0].id == "native-input:rid"
    assert messages[0].id != "rid-m"  # GA chat ID 不进入 LangGraph native state。


# retention：终态收口给事件流设存活期；心跳清扫超龄终态行（0=全关，默认无副作用）。
async def test_retention_expires_events_stream_on_terminal() -> None:
    agent = FakeAgent(run=text_run("x"))
    bus = FakeBus()
    store = FakeLedger()
    sup = RunSupervisor(
        agent_builder=_builder(agent),
        store=store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="t",
        events_ttl_s=3600,
    )
    await sup.dispatch(bus, request("rr1"))
    await _drain(sup)
    assert ("kokoro:run:rr1:events", 3600) in bus.expired_streams


async def test_retention_heartbeat_purges_terminal_runs() -> None:
    agent = FakeAgent(run=text_run("x"))
    bus = FakeBus()
    store = FakeLedger()
    sup = RunSupervisor(
        agent_builder=_builder(agent),
        store=store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="t",
        run_ttl_s=1,
    )
    await sup.dispatch(bus, request("rr2"))
    await _drain(sup)
    store.clock_ms = 10_000  # 终态已超龄
    await sup.heartbeat_once(bus)
    assert await store.is_terminal("rr2") is False  # 已被清扫


async def test_retention_off_by_default_no_side_effects() -> None:
    agent = FakeAgent(run=text_run("x"))
    bus = FakeBus()
    store = FakeLedger()
    sup, _ = _supervisor(agent, store)
    await sup.dispatch(bus, request("rr3"))
    await _drain(sup)
    await sup.heartbeat_once(bus)
    assert bus.expired_streams == []
    assert await store.is_terminal("rr3") is True


# --- 审计修复回归钉（2026-07-05 链路审计） ---


class _ExplodingMailboxLedger(FakeLedger):
    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        raise RuntimeError("mailbox down")


async def test_steer_persist_failure_does_not_kill_healthy_run() -> None:
    # steer 只是插话信箱：入账失败可由用户重发，绝不判死健康 run（审计缺口①）。
    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    sup, store = _supervisor(agent, store=_ExplodingMailboxLedger())
    await sup.dispatch(
        bus,
        RunSteer(
            kind="run.steer",
            run_id="r-any",
            session_id="s1",
            message_id="m1",
            content="嘿",
        ),
    )
    assert "r-any" not in store.terminals
    assert bus.kinds("r-any") == []


async def test_terminal_funnel_triggers_sandbox_teardown() -> None:
    # 审计缺口③：终态统一漏斗回收沙箱——自然完成与 cancel 两路都要触发（kind+sandbox_id 透传）。
    torn: list[tuple[str, str | None]] = []

    async def teardown(kind: str, sandbox_id: str | None) -> None:
        torn.append((kind, sandbox_id))

    agent = FakeAgent(run=text_run("hi"))
    bus = FakeBus()
    store = FakeLedger()
    sup = RunSupervisor(
        agent_builder=_builder(agent),
        store=store,
        approval_tool_names=_gated_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="test-consumer",
        heartbeat_s=30.0,
        sandbox_teardown=teardown,
    )
    store.sandbox_ids["t1"] = "sbx_123"
    await sup.dispatch(bus, request("t1"))
    await _drain(sup)
    assert torn == [("state", "sbx_123")]


async def test_fencing_yields_local_task_when_ownership_lost() -> None:
    # 裂脑 fencing：心跳发现所有权被他处夺走 → 取消本地任务、不由本 worker 发终态。
    gate = asyncio.Event()
    agent = FakeAgent(run=text_run("slow"), gates=[gate])
    bus = FakeBus()
    sup, store = _supervisor(agent)
    await sup.dispatch(bus, request("split"))
    await asyncio.sleep(0)
    assert "split" in store.owners
    # 模拟他处重拾：所有权易主。
    store.owners["split"] = "another-pod"
    await sup.heartbeat_once(bus)
    gate.set()
    # 被 fencing 取消的任务以 CancelledError 收束：用生产 drain 面（asyncio.wait 不上抛）。
    assert await sup.drain(timeout_s=2) is True
    # 本 worker 让渡：未发任何终态事件（终态权归新属主）；run 也未被本地标终态。
    assert all(
        kind != "run.completed" and kind != "run.failed" for kind in bus.kinds("split")
    )
    assert "split" not in store.terminals


# --- Wave2 R1：serve dispatch CAS 序（claim→ACK 后置）+ 迟到/重复帧丢弃 + DLQ + outbox ---


async def test_dispatch_win_executes_and_acks_after_claim() -> None:
    # pending intent → CAS 赢 → 执行到终态 → ACK（ACK 后置于 durable claim 之后）。
    store = FakeLedger()
    store.dispatches["r-go"] = "pending"
    store.dispatch_deadlines["r-go"] = 10**15
    frame = StreamItem(cursor="1", event=dict(request("r-go").model_dump()))
    bus = FakeBus(inbound=(frame,))
    sup, _ = _supervisor(FakeAgent(run=text_run("hi")), store=store)
    await sup.serve(bus)
    await _drain(sup)
    assert bus.acked == ["1"]
    assert bus.kinds("r-go")[-1] == "run.completed"
    assert store.dispatches["r-go"] == "claimed"


async def test_redelivered_dispatch_after_claim_is_discarded_not_double_executed() -> (
    None
):
    # §8.3「claim 后 ACK 前崩溃」：重投同帧 CAS 输（已 claimed）→ ACK 丢弃，不二次执行。
    store = FakeLedger()
    store.dispatches["r-dup"] = "pending"
    store.dispatch_deadlines["r-dup"] = 10**15
    frame = StreamItem(cursor="1", event=dict(request("r-dup").model_dump()))
    sup, _ = _supervisor(FakeAgent(run=text_run("hi")), store=store)
    await sup.serve(FakeBus(inbound=(frame,)))
    await _drain(sup)
    # 重投：dispatch 已 claimed，第二次 serve 丢弃不执行。
    bus2 = FakeBus(inbound=(frame,))
    await sup.serve(bus2)
    await _drain(sup)
    assert bus2.acked == ["1"]  # 迟到帧仍 ACK（不再重投）
    assert bus2.kinds("r-dup") == []  # 未二次执行（bus2 上无新事件）


async def test_expired_dispatch_frame_never_executes() -> None:
    # session reconciler 已转 expired：迟到帧永不执行，仅 ACK 丢弃。
    store = FakeLedger()
    store.dispatches["r-exp"] = "expired"
    frame = StreamItem(cursor="1", event=dict(request("r-exp").model_dump()))
    bus = FakeBus(inbound=(frame,))
    sup, _ = _supervisor(FakeAgent(run=text_run("hi")), store=store)
    await sup.serve(bus)
    await _drain(sup)
    assert bus.acked == ["1"]
    assert bus.kinds("r-exp") == []


async def test_crash_before_durable_claim_leaves_frame_unacked() -> None:
    # §8.3「request 读出后 claim 前崩溃」：durable claim 未落地 → 不 ACK，留 PEL 重投。
    class _CrashClaim(FakeLedger):
        async def try_claim(
            self, request: RunRequest, owner: str = "test-consumer"
        ) -> bool:
            raise RuntimeError("crash before durable claim")

    store = _CrashClaim()
    store.dispatches["r-crash"] = "pending"
    store.dispatch_deadlines["r-crash"] = 10**15
    frame = StreamItem(cursor="1", event=dict(request("r-crash").model_dump()))
    bus = FakeBus(inbound=(frame,))
    sup, _ = _supervisor(FakeAgent(run=text_run("hi")), store=store)
    await sup.serve(bus)  # 不冒泡杀循环
    assert bus.acked == []  # 未 ACK
    # 崩溃前不合成终态（留重投，而非误判失败）。
    assert bus.kinds("r-crash") == []


async def test_malformed_frame_quarantined_to_dlq_and_acked() -> None:
    # 不可解析帧：DLQ 记录后 ACK（坏帧无 identity 不重投）。
    store = FakeLedger()
    malformed = StreamItem(cursor="1", event={"kind": "run.request", "run_id": ""})
    bus = FakeBus(inbound=(malformed,))
    sup, _ = _supervisor(FakeAgent(), store=store)
    await sup.serve(bus)
    assert bus.acked == ["1"]
    assert len(store.dlq) == 1
    _raw, source, reason = store.dlq[0]
    assert source == REQUESTS_STREAM and reason == "unparseable"


async def test_serve_republishes_queued_outbox() -> None:
    # R4 critical outbox：stage 落库但发布未确认（崩在 publish 前）→ 启动 scanner 补发（幂等）。
    store = FakeLedger()
    store.outbox["r-orphan"] = [
        {
            "durable_seq": 1,
            "event_id": "evt_orphan_1",
            "kind": "run.started",
            "index": 0,
            "timestamp": 5,
            "payload_json": "{}",
            "status": "queued",
        }
    ]
    bus = FakeBus(inbound=())
    sup, _ = _supervisor(FakeAgent(), store=store)
    await sup.serve(bus)
    # 补发到事件流，复用固定 event_id/durable_seq；补发后置 published，不再补发。
    started = [e for e in bus.run_events("r-orphan") if e.kind == "run.started"]
    assert (
        len(started) == 1
        and started[0].durable_seq == 1
        and started[0].event_id == "evt_orphan_1"
    )
    assert store.outbox["r-orphan"][0]["status"] == "published"


async def test_heartbeat_reconciles_receipt_nack_terminates_contract_incompatible() -> (
    None
):
    # session NACK（rejected 回执）：心跳对账 → 同步 fence + 原子认领终态（停止执行与分配）。
    store = FakeLedger()
    await store.try_claim(request("r-nack"))
    store.outbox["r-nack"] = [
        {
            "durable_seq": 1,
            "event_id": "e1",
            "kind": "run.started",
            "index": 0,
            "timestamp": 0,
            "payload_json": "{}",
            "status": "published",
        }
    ]
    store.receipts["r-nack"] = [
        {"durable_seq": 1, "event_id": "e1", "status": "rejected"}
    ]
    bus = FakeBus()
    sup, _ = _supervisor(FakeAgent(), store=store)
    await sup.heartbeat_once(bus)
    # 终态认领 + fence 同步到 rejected_seq（其后 critical 帧一律 superseded）。
    assert await store.is_terminal("r-nack") is True
    assert store.terminal_fence["r-nack"] == 1


async def test_heartbeat_republishes_stale_published_outbox() -> None:
    # published 后回执一直不来、超宽限期（events 流被修剪/丢失）→ 心跳重发（复用固定身份）。
    store = FakeLedger()
    await store.try_claim(request("r-stale"))
    store.outbox["r-stale"] = [
        {
            "durable_seq": 1,
            "event_id": "e1",
            "kind": "run.started",
            "index": 0,
            "timestamp": 0,
            "payload_json": "{}",
            "status": "published",
            "published_at": 0,
        }
    ]
    store.clock_ms = 60_000  # 远超默认宽限期 30s；无回执、无 manifest。
    bus = FakeBus()
    sup, _ = _supervisor(FakeAgent(), store=store)
    await sup.heartbeat_once(bus)
    started = [e for e in bus.run_events("r-stale") if e.kind == "run.started"]
    assert (
        len(started) == 1
        and started[0].durable_seq == 1
        and started[0].event_id == "e1"
    )
