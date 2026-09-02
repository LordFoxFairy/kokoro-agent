"""Wave 2 R0 故障注入护栏（先红后绿钉）。

每个钉注入总设计稿 §2.3 已证实的当前真实缺陷，断言的是**修复后应成立的语义**，
以 `xfail(strict=True)` 钉住：当前套件保持绿（assertion 现在必然失败→xfail），
缺陷修复后测试转 XPASS，strict 使其 fail-loud，天然提醒回来收口本钉并去标。

不改任何 src。注入点与归属 Wave 见 tests/R0-FAULT-MATRIX.md。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from pydantic import JsonValue

from support.fakes import FakeAgent, FakeBus, FakeRunRepository, request, text_run, usage_recorder
from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.contract import RUN_EVENTS_MAXLEN, RunRequest, SubagentSource, run_events_stream
from kokoro_agent.execution.events import RunEmitter, outbox_wire_event
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.worker.supervisor import RunSupervisor


def _builder(agent: FakeAgent) -> Callable[[RunRequest], Awaitable[AgentHandle]]:
    async def _build(_request: RunRequest) -> AgentHandle:
        return AgentHandle(runnable=agent, tool_descriptions={})

    return _build


def _no_names(_request: RunRequest) -> frozenset[str]:
    return frozenset()


def _no_trace(_request: RunRequest) -> None:
    return None


def _source(_name: str) -> SubagentSource:
    return "runtime-custom"


# ---------------------------------------------------------------------------
# 钉 1（归属 R1，已收口·绿钉）：request 必须在 durable claim 落地后才 ACK。
#   R1 实现：`serve` 对 RunRequest 走 CAS claim→try_claim(durable)→ACK 后置序
#   （worker/supervisor.py `_consume_request`）；claim 持久化前崩溃 → 不 ACK，留 PEL 重投。
#   纲领 §2.3「request/control 在 durable claim/inbox 前 ACK」、§8.3「request 读出后 claim 前…ACK 前」。
#   本钉由 R0 的 strict xfail（红）收口为正式绿钉：注入 durable claim 崩溃，断言消息未 ACK。
# ---------------------------------------------------------------------------
async def test_request_not_acked_before_durable_claim_persists() -> None:
    class _CrashBeforeClaimRepository(FakeRunRepository):
        async def try_claim(self, request: RunRequest, owner: str = "test-consumer") -> bool:
            # 注入：durable claim 于持久化前崩溃（claim 未落库）。
            raise RuntimeError("crash before durable claim persists")

    good = StreamItem(cursor="req-1", event=dict(request("req-crash").model_dump()))
    bus = FakeBus(inbound=(good,))
    sup = RunSupervisor(
        agent_builder=_builder(FakeAgent(run=text_run("hi"))),
        run_repository=_CrashBeforeClaimRepository(),
        approval_tool_names=_no_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="t",
    )
    await sup.serve(bus)
    # 期望：claim 未落地 → 请求消息未 ACK，仍在 PEL 可被重投。
    assert "req-1" not in bus.acked


# ---------------------------------------------------------------------------
# 钉 2（归属 R4，已收口·绿钉）：agent terminal publish 瞬时失败不得静默丢弃。
#   R4 实现：critical 帧（run.completed 等）经 durable outbox——emitter 先 stage_critical_frame
#   落 queued 行（固定 durable_seq/event_id），再 publish；publish 抛错被 invoke_once 顶层 except
#   吞掉后，outbox 行仍留 queued，scanner（supervisor._republish_outbox）按 seq 序幂等补发到事件流。
#   纲领 §2.3 / §7「critical publish 失败 → agent outbox pending 自动补发；固定 event_id/durable_seq」。
#   本钉由 R0 的 strict xfail（红）收口为正式绿钉：注入首次终态 publish 故障，断言终态帧经 outbox
#   补发落 wire 且身份（durable_seq/event_id）不漂移。
# ---------------------------------------------------------------------------
class _FlakyTerminalBus(FakeBus):
    """终态帧首次 publish 抛错（注入关键状态帧发布瞬时故障）；其余帧正常。"""

    def __init__(self) -> None:
        super().__init__()
        self._failed_terminal = False

    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        if not self._failed_terminal and event.get("kind") == "run.completed":
            self._failed_terminal = True
            raise RuntimeError("transient terminal publish failure")
        return await super().publish(stream, event, maxlen=maxlen)


async def test_terminal_frame_republished_from_outbox_on_publish_failure() -> None:
    bus = _FlakyTerminalBus()
    store = FakeRunRepository()
    await store.try_claim(request("term-drop"))  # 建 run 文档：stage 落 outbox 行的前提
    emitter = await RunEmitter.attach(bus, "term-drop", frozenset(), store)
    await invoke_once(
        emitter,
        FakeAgent(run=text_run("hi")),
        "c1",
        {"messages": []},
        approval_tool_names=frozenset(),
        source_for=_source,
        claim_terminal=lambda: store.try_mark_terminal("term-drop"),
        record_usage=usage_recorder()[0],
    )
    # 首次 publish 失败被顶层 except 吞掉 → run.completed 未上 wire，但 outbox 行留 queued。
    on_wire = [e for e in bus.run_events("term-drop") if e.kind in {"run.completed", "run.failed"}]
    assert on_wire == []
    queued = [f for f in await store.list_unpublished_outbox() if f.kind == "run.completed"]
    assert len(queued) == 1
    seq, event_id = queued[0].durable_seq, queued[0].event_id

    # scanner 补发（FlakyBus 只失败一次）：终态帧落 wire，复用固定 durable_seq/event_id（不漂移）。
    for frame in await store.list_unpublished_outbox():
        await bus.publish(
            run_events_stream(frame.run_id), outbox_wire_event(frame), maxlen=RUN_EVENTS_MAXLEN
        )
        await store.mark_critical_published(frame.run_id, frame.durable_seq)
    republished = [e for e in bus.run_events("term-drop") if e.kind == "run.completed"]
    assert len(republished) == 1
    assert republished[0].durable_seq == seq and republished[0].event_id == event_id
    # 补发后无残留 queued（幂等收敛）。
    assert await store.list_unpublished_outbox() == []


class _FlakyFailureBus(FakeBus):
    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        if event.get("kind") == "run.failed":
            raise RuntimeError("terminal failure publish failure")
        return await super().publish(stream, event, maxlen=maxlen)


async def test_terminal_failure_publish_failure_leaves_recoverable_outbox() -> None:
    bus = _FlakyFailureBus()
    store = FakeRunRepository()
    await store.try_claim(request("term-fail"))
    emitter = await RunEmitter.attach(bus, "term-fail", frozenset(), store)
    handled = await invoke_once(
        emitter,
        FakeAgent(raise_on_stream=RuntimeError("boom")),
        "c1",
        {"messages": []},
        approval_tool_names=frozenset(),
        source_for=_source,
        claim_terminal=lambda: store.try_mark_terminal("term-fail"),
        record_usage=usage_recorder()[0],
    )
    assert handled is True
    assert [e.kind for e in bus.run_events("term-fail")] == ["run.started"]
    queued = [f for f in await store.list_unpublished_outbox() if f.kind == "run.failed"]
    assert len(queued) == 1
