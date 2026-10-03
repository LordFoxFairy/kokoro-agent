"""Worker process shutdown outcome tests; all owner services stay injected."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from support.fakes import (
    FakeAgent,
    FakeBus,
    FakeRunRepository,
    read_unpaused_interaction,
    request,
    text_run,
)

from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.protocol import RunRequest, SkillProgressSink, SubagentSource
from kokoro_agent.sandbox.archive import ArchivingWritesMixin
from kokoro_agent.worker.main import (
    ShutdownBudget,
    require_worker_drain,
)
from kokoro_agent.worker.supervisor import RunSupervisor


class DrainProbe:
    def __init__(self, result: bool) -> None:
        self.result = result
        self.timeouts: list[float] = []

    async def drain(self, *, timeout_s: float) -> bool:
        self.timeouts.append(timeout_s)
        return self.result


async def test_worker_shutdown_accepts_a_fully_drained_supervisor() -> None:
    supervisor = DrainProbe(True)

    await require_worker_drain(supervisor, timeout_s=60)

    assert supervisor.timeouts == [60]


async def test_worker_shutdown_is_nonzero_when_resource_drain_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    supervisor = DrainProbe(False)

    with caplog.at_level("ERROR", logger="kokoro_agent.worker.main"):
        with pytest.raises(RuntimeError, match="worker shutdown drain failed"):
            await require_worker_drain(supervisor, timeout_s=7.5)

    assert supervisor.timeouts == [7.5]
    assert "resource cleanup did not drain" in caplog.text


def test_worker_shutdown_budget_starts_once_and_deducts_cancellation_time() -> None:
    budget = ShutdownBudget(10)

    budget.begin(100)
    budget.begin(104)

    assert budget.remaining(107.5) == 2.5
    assert budget.remaining(111) == 0


async def test_worker_shutdown_budget_owns_cancelled_construction_until_cleanup() -> (
    None
):
    construction_started = asyncio.Event()
    construction_release = asyncio.Event()
    close_calls = 0

    class Resource:
        async def aclose_resources(self) -> None:
            nonlocal close_calls
            close_calls += 1

    resource = Resource()
    agent = FakeAgent(run=text_run("must-not-start"))

    async def build(
        _request: RunRequest, _lease: LeaseFence, _progress: SkillProgressSink
    ) -> AgentHandle:
        construction_started.set()
        while not construction_release.is_set():
            try:
                await asyncio.shield(construction_release.wait())
            except asyncio.CancelledError:
                # A real sync connector cannot be killed with its awaiting task.
                # It may still return a resource that the supervisor must own.
                continue
        return AgentHandle(
            runnable=agent,
            tool_descriptions={},
            resources=(cast(ArchivingWritesMixin, resource),),
        )

    store = FakeRunRepository()
    supervisor = RunSupervisor(
        interaction_reader=read_unpaused_interaction,
        agent_builder=build,
        run_repository=store,
        approval_tool_names=lambda _request: frozenset(),
        trace_factory=lambda _request: None,
        source_for=lambda _name: cast(SubagentSource, "runtime-custom"),
        consumer="worker-construction-shutdown",
    )
    bus = FakeBus()
    run = request("worker-construction-shutdown")
    store.dispatches[run.run_id] = "pending"
    store.dispatch_requests[run.run_id] = run
    serving = asyncio.create_task(supervisor.dispatch(bus, run))
    await asyncio.wait_for(construction_started.wait(), 1)

    serving.cancel()
    await asyncio.sleep(0)
    drained_while_blocked = await supervisor.drain(timeout_s=0.01)
    construction_release.set()
    serving_result = await asyncio.gather(serving, return_exceptions=True)
    drained_after_release = await supervisor.drain(timeout_s=1)

    assert drained_while_blocked is False
    assert isinstance(serving_result[0], asyncio.CancelledError)
    assert drained_after_release is True
    assert agent.seen_payloads == []
    assert close_calls == 1


async def test_worker_shutdown_observes_late_construction_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    construction_started = asyncio.Event()
    construction_release = asyncio.Event()
    primary = RuntimeError("late-construction-private-detail")

    async def build(
        _request: RunRequest, _lease: LeaseFence, _progress: SkillProgressSink
    ) -> AgentHandle:
        construction_started.set()
        await construction_release.wait()
        raise primary

    store = FakeRunRepository()
    supervisor = RunSupervisor(
        interaction_reader=read_unpaused_interaction,
        agent_builder=build,
        run_repository=store,
        approval_tool_names=lambda _request: frozenset(),
        trace_factory=lambda _request: None,
        source_for=lambda _name: cast(SubagentSource, "runtime-custom"),
        consumer="worker-late-assembly-error",
    )
    bus = FakeBus()
    run = request("worker-late-assembly-error")
    store.dispatches[run.run_id] = "pending"
    store.dispatch_requests[run.run_id] = run
    serving = asyncio.create_task(supervisor.dispatch(bus, run))
    await asyncio.wait_for(construction_started.wait(), 1)

    serving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await serving
    assert await supervisor.drain(timeout_s=0.01) is False
    with caplog.at_level("ERROR", logger="kokoro_agent.worker.supervisor"):
        construction_release.set()
        assert await supervisor.drain(timeout_s=1) is False

    assert supervisor.assembly_tasks == ()
    assert "abandoned agent assembly failed" in caplog.text
    assert "late-construction-private-detail" not in caplog.text
