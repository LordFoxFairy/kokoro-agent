"""Worker process shutdown outcome tests; all owner services stay injected."""

from __future__ import annotations

import asyncio
from typing import cast
from collections.abc import AsyncIterator

import pytest

from support.fakes import (
    FakeAgent,
    FakeBus,
    FakeRunRepository,
    read_unpaused_interaction,
    request,
    text_run,
    admit_control_fixture,
)

from kokoro_agent.domain.chat.action_intent import ActionObservationSink
from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.protocol import (
    RunCancel,
    RunRequest,
    SkillProgressSink,
    SubagentSource,
    run_control_stream,
)
from kokoro_agent.streams.protocol import StreamItem
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
        _request: RunRequest,
        _lease: LeaseFence,
        _progress: SkillProgressSink,
        _action_observer: ActionObservationSink,
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
        _request: RunRequest,
        _lease: LeaseFence,
        _progress: SkillProgressSink,
        _action_observer: ActionObservationSink,
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


@pytest.mark.parametrize("reclaimed", [False, True], ids=["initial", "reclaim"])
async def test_owned_construction_renews_its_captured_lease_while_builder_waits(
    reclaimed: bool,
) -> None:
    entered, release, renewed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    observed: list[LeaseFence] = []

    class Store(FakeRunRepository):
        async def renew(self, run_id: str, lease: LeaseFence) -> bool:
            observed.append(lease)
            result = await super().renew(run_id, lease)
            renewed.set()
            return result

    store = Store()
    agent = FakeAgent(run=text_run("assembled"))
    captured: list[LeaseFence] = []

    async def build(
        _request: RunRequest,
        lease: LeaseFence,
        _progress: SkillProgressSink,
        _observer: ActionObservationSink,
    ) -> AgentHandle:
        captured.append(lease)
        entered.set()
        await release.wait()
        return AgentHandle(runnable=agent, tool_descriptions={})

    supervisor = RunSupervisor(
        interaction_reader=read_unpaused_interaction,
        agent_builder=build,
        run_repository=store,
        approval_tool_names=lambda _request: frozenset(),
        trace_factory=lambda _request: None,
        source_for=lambda _name: cast(SubagentSource, "runtime-custom"),
        consumer="construction-owner",
        heartbeat_s=0.01,
    )
    bus, run = FakeBus(), request("construction-keepalive")
    if reclaimed:
        await store.try_claim(run, "expired-owner")
        store.expired = [run]
        starting = asyncio.create_task(supervisor.heartbeat_once(bus))
    else:
        store.dispatches[run.run_id] = "pending"
        store.dispatch_requests[run.run_id] = run
        starting = asyncio.create_task(supervisor.dispatch(bus, run))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.04)
        assert renewed.is_set(), (
            "an awaiting constructor must renew even inside reclaim's heartbeat"
        )
        assert observed and all(lease == captured[0] for lease in observed)
        assert not starting.done()
    finally:
        release.set()
        await starting
        assert await supervisor.drain(timeout_s=1)


async def test_control_cancel_during_construction_owns_late_handle_without_invoking() -> (
    None
):
    entered, release = asyncio.Event(), asyncio.Event()
    control: asyncio.Queue[StreamItem] = asyncio.Queue()
    close_calls = 0
    run = request("construction-native-cancel")

    class Bus(FakeBus):
        async def subscribe(
            self, stream: str, *, group: str, consumer: str
        ) -> AsyncIterator[StreamItem]:
            assert stream == run_control_stream(run.run_id)
            yield await control.get()

    class Resource:
        async def aclose_resources(self) -> None:
            nonlocal close_calls
            close_calls += 1

    agent, resource, store, bus = (
        FakeAgent(run=text_run("must-not-invoke")),
        Resource(),
        FakeRunRepository(),
        Bus(),
    )

    async def build(
        _request: RunRequest,
        _lease: LeaseFence,
        _progress: SkillProgressSink,
        _observer: ActionObservationSink,
    ) -> AgentHandle:
        entered.set()
        await release.wait()
        return AgentHandle(
            runnable=agent,
            tool_descriptions={},
            resources=(cast(ArchivingWritesMixin, resource),),
        )

    supervisor = RunSupervisor(
        interaction_reader=read_unpaused_interaction,
        agent_builder=build,
        run_repository=store,
        approval_tool_names=lambda _request: frozenset(),
        trace_factory=lambda _request: None,
        source_for=lambda _name: cast(SubagentSource, "runtime-custom"),
        consumer="construction-cancel-owner",
        heartbeat_s=0.01,
    )
    store.dispatches[run.run_id] = "pending"
    store.dispatch_requests[run.run_id] = run
    starting = asyncio.create_task(supervisor.dispatch(bus, run))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        command = await admit_control_fixture(
            store,
            RunCancel(
                kind="run.cancel",
                run_id=run.run_id,
                session_id=run.session_id,
                command_id="stop-construction",
            ),
        )
        await control.put(
            StreamItem(
                cursor="construction-cancel",
                event=command.model_dump(mode="json", exclude_none=True),
            )
        )
        for _ in range(100):
            if await store.is_terminal(run.run_id):
                break
            await asyncio.sleep(0.001)
        assert await store.is_terminal(run.run_id), (
            "the original control listener must deliver cancel before construction returns"
        )
        assert bus.kinds(run.run_id).count("run.completed") == 1
        assert agent.seen_payloads == []
        assert await supervisor.drain(timeout_s=0.01) is False
    finally:
        release.set()
        await starting
        assert await supervisor.drain(timeout_s=1)
    assert close_calls == 1
    assert agent.seen_payloads == []
    assert bus.kinds(run.run_id).count("run.completed") == 1
    assert "run.failed" not in bus.kinds(run.run_id)


async def test_superseded_construction_never_renews_successor_or_transfers_late_handle() -> (
    None
):
    entered, release = asyncio.Event(), asyncio.Event()
    observed: list[LeaseFence] = []
    close_calls = 0

    class Store(FakeRunRepository):
        async def renew(self, run_id: str, lease: LeaseFence) -> bool:
            observed.append(lease)
            return await super().renew(run_id, lease)

    class Resource:
        async def aclose_resources(self) -> None:
            nonlocal close_calls
            close_calls += 1

    store, resource, agent = (
        Store(),
        Resource(),
        FakeAgent(run=text_run("stale-must-not-invoke")),
    )
    captured: list[LeaseFence] = []

    async def build(
        _request: RunRequest,
        lease: LeaseFence,
        _progress: SkillProgressSink,
        _observer: ActionObservationSink,
    ) -> AgentHandle:
        captured.append(lease)
        entered.set()
        await release.wait()
        return AgentHandle(
            runnable=agent,
            tool_descriptions={},
            resources=(cast(ArchivingWritesMixin, resource),),
        )

    supervisor = RunSupervisor(
        interaction_reader=read_unpaused_interaction,
        agent_builder=build,
        run_repository=store,
        approval_tool_names=lambda _request: frozenset(),
        trace_factory=lambda _request: None,
        source_for=lambda _name: cast(SubagentSource, "runtime-custom"),
        consumer="construction-old-owner",
        heartbeat_s=0.01,
    )
    run, bus = request("construction-takeover"), FakeBus()
    store.dispatches[run.run_id] = "pending"
    store.dispatch_requests[run.run_id] = run
    starting = asyncio.create_task(supervisor.dispatch(bus, run))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        store.expired = [run]
        successor = (await store.reclaim_expired("construction-successor"))[0].lease
        await asyncio.sleep(0.04)
        assert observed and all(lease == captured[0] for lease in observed)
        assert starting.done(), (
            "lost assembly ownership retires its waiter without abandoning physical construction"
        )
        assert await store.is_lease_current(run.run_id, successor)
    finally:
        release.set()
        await starting
        assert await supervisor.drain(timeout_s=1)
    assert close_calls == 1
    assert agent.seen_payloads == []
    assert not await store.is_terminal(run.run_id)
