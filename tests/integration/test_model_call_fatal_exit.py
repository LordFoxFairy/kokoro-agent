from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
from typing import Never

from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.protocol import (
    CONSUMER_GROUP,
    REQUESTS_STREAM,
    InboundMessage,
    RunCancel,
    RunRequest,
    SkillProgressSink,
)
from kokoro_agent.streams.protocol import StreamItem, StreamProtocol
from kokoro_agent.worker import main as worker_main
from kokoro_agent.worker.supervisor import RunSupervisor
from support.fakes import (
    FakeAgent,
    FakeBus,
    FakeRunRepository,
    FakeRunStream,
    admit_control_fixture,
    read_unpaused_interaction,
    request,
)


async def _fatal_chain(write_fd: int, *, illegal_fatal_return: bool) -> None:
    entered = asyncio.Event()
    never = asyncio.Event()

    async def held_custom() -> AsyncIterator[Never]:
        entered.set()
        try:
            await never.wait()
        except asyncio.CancelledError:
            await never.wait()
        if False:
            yield

    class HeldRun(FakeRunStream):
        @property
        def custom(self) -> AsyncIterator[Never]:
            return held_custom()

    async def build(
        _request: RunRequest, _lease: LeaseFence, _progress: SkillProgressSink
    ) -> AgentHandle:
        return AgentHandle(
            runnable=FakeAgent(run=HeldRun()),
            tool_descriptions={},
        )

    first = request("fatal-first")
    second = request("fatal-second")
    cancel = RunCancel(
        kind="run.cancel",
        run_id=first.run_id,
        session_id=first.session_id,
        command_id="fatal-cancel",
    )
    store = FakeRunRepository()
    for pending in (first, second):
        store.dispatches[pending.run_id] = "pending"
        store.dispatch_requests[pending.run_id] = pending

    class FatalBus(FakeBus):
        async def subscribe(
            self, stream: str, *, group: str, consumer: str
        ) -> AsyncIterator[StreamItem]:
            assert group == CONSUMER_GROUP
            del consumer
            if stream != REQUESTS_STREAM:
                if False:
                    yield StreamItem(cursor="never", event={})
                return
            yield StreamItem(cursor="first", event=first.model_dump(mode="json"))
            await entered.wait()
            yield StreamItem(cursor="cancel", event=cancel.model_dump(mode="json"))
            await never.wait()
            yield StreamItem(cursor="second", event=second.model_dump(mode="json"))

    class AdmittedSupervisor(RunSupervisor):
        cleanup_called = False

        @property
        def fatal_safe(self) -> bool:
            return (
                self._intake_closed
                and self._model_call_fatal is not None
                and not self.cleanup_called
            )

        async def _retry_sandbox_cleanups(self, run_id: str | None = None) -> None:
            if run_id is None:
                await super()._retry_sandbox_cleanups(run_id=run_id)
                return
            self.cleanup_called = True
            await never.wait()

        def install_illegal_fatal_callback(self) -> None:
            def illegal_return(_exit_code: int) -> int:
                return os.write(write_fd, b"R")

            setattr(self, "_process_fatal", illegal_return)

        async def dispatch(self, bus: StreamProtocol, msg: InboundMessage) -> None:
            if isinstance(msg, RunCancel):
                await admit_control_fixture(store, msg)
                await store.record_control_delivery(
                    msg.run_id,
                    msg.command_id,
                    msg.request_digest,
                    None,
                    msg.model_dump_json(),
                )
            await super().dispatch(bus, msg)

    supervisor_ref: list[RunSupervisor] = []

    def fatal(exit_code: int) -> Never:
        supervisor = supervisor_ref[0]
        safe = (
            exit_code == 70
            and isinstance(supervisor, AdmittedSupervisor)
            and supervisor.fatal_safe
            and store.dispatches[second.run_id] == "pending"
            and second.run_id not in store.requests
        )
        os.write(write_fd, b"F" if safe else b"B")
        os._exit(exit_code)

    supervisor = AdmittedSupervisor(
        interaction_reader=read_unpaused_interaction,
        agent_builder=build,
        run_repository=store,
        approval_tool_names=lambda _request: frozenset(),
        trace_factory=lambda _request: None,
        source_for=lambda _name: "runtime-custom",
        consumer="fatal-system-worker",
        heartbeat_s=60.0,
        model_call_settle_timeout_s=0.02,
        process_fatal=fatal,
    )
    if illegal_fatal_return:
        supervisor.install_illegal_fatal_callback()
    supervisor_ref.append(supervisor)
    await supervisor.serve(FatalBus())
    raise AssertionError("fatal worker unexpectedly returned")


def fatal_chain_child(write_fd: int, illegal_fatal_return: bool) -> None:
    asyncio.run(_fatal_chain(write_fd, illegal_fatal_return=illegal_fatal_return))


async def _sigterm_chain(write_fd: int) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    never = asyncio.Event()
    run = FakeRunStream()

    async def cooperative_custom() -> AsyncIterator[Never]:
        entered.set()
        os.write(write_fd, b"S")
        await release.wait()
        if False:
            yield

    class CooperativeRun(FakeRunStream):
        @property
        def custom(self) -> AsyncIterator[Never]:
            return cooperative_custom()

    run = CooperativeRun()

    async def build(
        _request: RunRequest, _lease: LeaseFence, _progress: SkillProgressSink
    ) -> AgentHandle:
        return AgentHandle(runnable=FakeAgent(run=run), tool_descriptions={})

    pending = request("sigterm-cooperative")
    store = FakeRunRepository()
    store.dispatches[pending.run_id] = "pending"
    store.dispatch_requests[pending.run_id] = pending

    class SignalBus(FakeBus):
        async def subscribe(
            self, stream: str, *, group: str, consumer: str
        ) -> AsyncIterator[StreamItem]:
            assert group == CONSUMER_GROUP
            del consumer
            if stream != REQUESTS_STREAM:
                if False:
                    yield StreamItem(cursor="never", event={})
                return
            try:
                yield StreamItem(cursor="first", event=pending.model_dump(mode="json"))
                await never.wait()
            finally:
                release.set()

    def unexpected_fatal(_exit_code: int) -> Never:
        os.write(write_fd, b"X")
        os._exit(70)

    supervisor = RunSupervisor(
        interaction_reader=read_unpaused_interaction,
        agent_builder=build,
        run_repository=store,
        approval_tool_names=lambda _request: frozenset(),
        trace_factory=lambda _request: None,
        source_for=lambda _name: "runtime-custom",
        consumer="sigterm-system-worker",
        heartbeat_s=60.0,
        process_fatal=unexpected_fatal,
    )
    serve_until_shutdown = getattr(worker_main, "_serve_until_shutdown")
    await serve_until_shutdown(supervisor, SignalBus(), drain_timeout_s=1.0)
    assert entered.is_set()
    assert run.context_exited
    assert await supervisor.drain(timeout_s=0.0)
    os.write(write_fd, b"D")


def sigterm_chain_child(write_fd: int) -> None:
    asyncio.run(_sigterm_chain(write_fd))


def _run_fatal_chain(*, illegal_fatal_return: bool) -> tuple[int, bytes]:
    read_fd, write_fd = os.pipe()
    test_path = Path(__file__).resolve()
    script = (
        "import runpy, sys; "
        f"sys.path.insert(0, {str(test_path.parents[1])!r}); "
        f"ns=runpy.run_path({str(test_path)!r}); "
        f"ns['fatal_chain_child']({write_fd}, {illegal_fatal_return!r})"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        close_fds=True,
        pass_fds=(write_fd,),
    )
    os.close(write_fd)
    try:
        return_code = process.wait(timeout=5.0)
        marker = os.read(read_fd, 8)
    finally:
        os.close(read_fd)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=1.0)

    return return_code, marker


def test_model_call_settlement_timeout_stops_intake_and_exits_worker() -> None:
    assert _run_fatal_chain(illegal_fatal_return=False) == (70, b"F")


def test_illegal_fatal_callback_return_still_exits_worker() -> None:
    assert _run_fatal_chain(illegal_fatal_return=True) == (70, b"R")


def test_sigterm_uses_production_seam_and_gracefully_drains_worker() -> None:
    read_fd, write_fd = os.pipe()
    test_path = Path(__file__).resolve()
    script = (
        "import runpy, sys; "
        f"sys.path.insert(0, {str(test_path.parents[1])!r}); "
        f"ns=runpy.run_path({str(test_path)!r}); "
        f"ns['sigterm_chain_child']({write_fd})"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script], close_fds=True, pass_fds=(write_fd,)
    )
    os.close(write_fd)
    try:
        readable, _, _ = select.select((read_fd,), (), (), 3.0)
        assert readable == [read_fd]
        assert os.read(read_fd, 1) == b"S"
        process.send_signal(signal.SIGTERM)
        return_code = process.wait(timeout=3.0)
        marker = os.read(read_fd, 8)
    finally:
        os.close(read_fd)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=1.0)

    assert return_code == 0
    assert marker == b"D"
