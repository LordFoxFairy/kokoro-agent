"""Wave 2 R0 故障注入护栏（先红后绿钉）。

每个钉注入总设计稿 §2.3 已证实的当前真实缺陷，断言的是**修复后应成立的语义**，
以 `xfail(strict=True)` 钉住：当前套件保持绿（assertion 现在必然失败→xfail），
缺陷修复后测试转 XPASS，strict 使其 fail-loud，天然提醒回来收口本钉并去标。

不改任何 src。注入点与归属 Wave 见 tests/R0-FAULT-MATRIX.md。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

import pytest
from pydantic import JsonValue

from fakes import FakeAgent, FakeBus, FakeLedger, request, text_run, usage_recorder
from kokoro_agent.agents.base import AssembledAgent
from kokoro_agent.contract import RunRequest, SubagentSource
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.worker.supervisor import RunSupervisor


def _builder(agent: FakeAgent) -> Callable[[RunRequest], Awaitable[AssembledAgent]]:
    async def _build(_request: RunRequest) -> AssembledAgent:
        return AssembledAgent(agent=agent, tool_descriptions={})

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
    class _CrashBeforeClaimLedger(FakeLedger):
        async def try_claim(self, request: RunRequest, owner: str = "test-consumer") -> bool:
            # 注入：durable claim 于持久化前崩溃（claim 未落库）。
            raise RuntimeError("crash before durable claim persists")

    good = StreamItem(cursor="req-1", event=dict(request("req-crash").model_dump()))
    bus = FakeBus(inbound=(good,))
    sup = RunSupervisor(
        agent_builder=_builder(FakeAgent(run=text_run("hi"))),
        store=_CrashBeforeClaimLedger(),
        approval_tool_names=_no_names,
        trace_factory=_no_trace,
        source_for=_source,
        consumer="t",
    )
    await sup.serve(bus)
    # 期望：claim 未落地 → 请求消息未 ACK，仍在 PEL 可被重投。
    assert "req-1" not in bus.acked


# ---------------------------------------------------------------------------
# 钉 2（归属 R4）：agent terminal 在事件发布前消耗终态权，发布失败即丢弃。
#   注入点：execution/run_agent.py:62-76 `invoke_once` —— claim_terminal()（line 62）
#   先消耗终态权，emitter.emit(RunCompletedPayload)（line 69）后发布；publish 抛错被
#   line 73 顶层 except 吞掉（再 claim 得 False → 什么都不发），终态帧永失、无补发。
#   纲领 §2.3「agent terminal 在事件发布前消耗终态权；关键状态帧发布失败可直接丢弃」、
#         §7「critical publish 失败 → agent outbox pending 自动补发；固定 event_id/durable_seq」。
#   期望语义（修复后成立）：终态 publish 瞬时失败不得静默丢弃——要么经 durable outbox 补发到
#   事件流，要么上抛触发重投；二者居一即可。
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


def _one_shot_claim() -> Callable[[], Awaitable[bool]]:
    # 忠实复刻 supervisor 的 try_mark_terminal：终态权一次性，第二次认领即 False。
    consumed = {"v": False}

    async def claim() -> bool:
        if consumed["v"]:
            return False
        consumed["v"] = True
        return True

    return claim


@pytest.mark.xfail(
    strict=True,
    reason="Wave2 R0/R4: agent terminal 在发布前消耗终态权,关键帧 publish 失败被 run_agent.py:73 "
    "broad except 吞掉即丢(无 durable outbox/补发)。期望:终态 publish 故障经补发投递或上抛,不静默丢弃。",
)
async def test_terminal_frame_not_silently_dropped_on_publish_failure() -> None:
    bus = _FlakyTerminalBus()
    emitter = await RunEmitter.attach(bus, "term-drop")
    raised = False
    try:
        await invoke_once(
            emitter,
            FakeAgent(run=text_run("hi")),
            "c1",
            {"messages": []},
            approval_tool_names=frozenset(),
            source_for=_source,
            claim_terminal=_one_shot_claim(),
            record_usage=usage_recorder()[0],
        )
    except Exception:  # noqa: BLE001 — 上抛也是「不静默丢弃」的一种合规收口
        raised = True
    terminal = [e for e in bus.run_events("term-drop") if e.kind in {"run.completed", "run.failed"}]
    # 期望：终态帧未被静默丢弃——补发到事件流，或上抛触发重投。
    assert raised or len(terminal) >= 1
