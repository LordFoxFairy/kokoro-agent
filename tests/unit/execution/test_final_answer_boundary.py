"""Final identity comes from native completion AND the same Run's public fact."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Literal

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from pydantic import ValidationError

from kokoro_agent.execution.events import RunEmitter, ProgressPersistenceError
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.protocol import (
    MessageCompletedPayload,
    RunCompleted,
    RunCompletedPayload,
)
from support.chat import FakeChatRepository
from support.fakes import (
    FakeAgent,
    FakeBus,
    FakeRunStream,
    find_events,
    terminal_emitter,
    usage_recorder,
)


async def _claim() -> bool:
    return True


async def _finish(
    emitter: RunEmitter,
    run: FakeRunStream,
    phase: Literal["active", "waiting", "unknown"] = "active",
) -> bool:
    async def settle(
        seal: Callable[[], Awaitable[tuple[int, int]]],
    ) -> Literal["active", "waiting", "unknown"]:
        if phase != "active":
            await seal()
        return phase

    return await invoke_once(
        emitter,
        FakeAgent(run=run),
        "thread",
        None,
        initial=False,
        approval_tool_names=frozenset(),
        source_for=lambda _: "runtime-custom",
        finalize_terminal=terminal_emitter(emitter, _claim, usage_recorder()[0]),
        record_usage=usage_recorder()[0],
        on_native_settled=settle,
    )


@pytest.mark.parametrize(
    "case",
    [
        "same_run",
        "absent",
        "other_run",
        "other_session",
        "other_tenant",
        "other_namespace",
    ],
)
async def test_checkpoint_only_completion_requires_same_persisted_public_record(
    case: str,
) -> None:
    bus, chat = FakeBus(), FakeChatRepository()
    # Persist first, then create a fresh emitter with no incoming message stream.
    prior = await RunEmitter.attach(
        bus,
        "other" if case == "other_run" else "run",
        tenant_id="other" if case == "other_tenant" else "tenant",
        namespace="other" if case == "other_namespace" else "ns",
        session_id="other" if case == "other_session" else "session",
        chat_repository=chat,
    )
    if case != "absent":
        await prior.emit(MessageCompletedPayload(segment_id="final", content=""))
    emitter = await RunEmitter.attach(
        bus,
        "run",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=chat,
    )
    assert await _finish(
        emitter,
        FakeRunStream(output_state={"messages": [AIMessage(id="final", content="")]}),
    )
    terminal = find_events(bus.run_events("run"), RunCompleted)[-1]
    assert terminal.payload.final_segment_id == (
        "final" if case == "same_run" else None
    )


@pytest.mark.parametrize(
    "tail",
    [
        None,
        ToolMessage(content="direct", tool_call_id="call"),
        AIMessage(content="unstable"),
        AIMessage(
            id="final",
            content="handoff",
            tool_calls=[{"id": "call", "name": "handoff", "args": {}}],
        ),
        AIMessage(
            id="final",
            content="invalid",
            invalid_tool_calls=[
                {"id": "call", "name": "broken", "args": "bad", "error": "invalid"}
            ],
        ),
    ],
)
async def test_nonfinal_native_tail_never_promotes_preceding_public_text(
    tail: object,
) -> None:
    bus, chat = FakeBus(), FakeChatRepository()
    emitter = await RunEmitter.attach(
        bus,
        "run",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=chat,
    )
    await emitter.emit(
        MessageCompletedPayload(segment_id="progress", content="public progress")
    )
    await emitter.emit(
        MessageCompletedPayload(segment_id="final", content="public tool call")
    )
    assert await _finish(
        emitter,
        FakeRunStream(
            output_state={
                "messages": [AIMessage(id="progress", content="public progress"), tail]
            }
        ),
    )
    assert (
        find_events(bus.run_events("run"), RunCompleted)[0].payload.final_segment_id
        is None
    )


@pytest.mark.parametrize(
    "phase,interrupted", [("waiting", False), ("unknown", False), ("active", True)]
)
async def test_unsettled_or_interrupted_run_never_publishes_final_boundary(
    phase: Literal["active", "waiting", "unknown"], interrupted: bool
) -> None:
    bus, chat = FakeBus(), FakeChatRepository()
    emitter = await RunEmitter.attach(
        bus,
        "run",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=chat,
    )
    await emitter.emit(MessageCompletedPayload(segment_id="final", content="answer"))
    assert not await _finish(
        emitter,
        FakeRunStream(
            is_interrupted=interrupted,
            output_state={"messages": [AIMessage(id="final", content="answer")]},
        ),
        phase,
    )
    assert not find_events(bus.run_events("run"), RunCompleted)


async def test_proof_read_failure_does_not_invent_another_terminal_outcome() -> None:
    class BrokenRead(FakeChatRepository):
        async def completed_message_exists(
            self,
            tenant_id: str,
            namespace: str,
            session_id: str,
            run_id: str,
            chat_message_id: str,
        ) -> bool:
            raise OSError("store unavailable")

    bus = FakeBus()
    emitter = await RunEmitter.attach(
        bus,
        "run",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=BrokenRead(),
    )
    with pytest.raises(ProgressPersistenceError):
        await _finish(
            emitter,
            FakeRunStream(
                output_state={"messages": [AIMessage(id="final", content="answer")]}
            ),
        )
    assert not any(
        event.kind in {"run.completed", "run.failed"} for event in bus.run_events("run")
    )


def test_cancelled_terminal_cannot_bind_a_final_answer() -> None:
    with pytest.raises(ValidationError):
        RunCompletedPayload(status="cancelled", final_segment_id="final")


@pytest.mark.parametrize("mode", ["return_direct", "hitl_resume", "checkpoint_replay"])
async def test_native_graph_terminal_modes_keep_the_exact_answer(mode: str) -> None:
    """Actual SDK stream/output/checkpoint, deterministic local model, fake Chat I/O."""
    from langchain_core.messages import HumanMessage
    from langchain_core.tools import StructuredTool
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.types import Command
    from pydantic import BaseModel
    from support.deepagents import create_test_deep_agent
    from support.local_fake import LocalFakeChatModel
    from support.fakes import settled_state_callback

    class Args(BaseModel):
        pass

    noop = StructuredTool(
        name="noop",
        description="no-op",
        args_schema=Args,
        func=lambda: "ok",
        return_direct=mode == "return_direct",
    )
    checkpoint = InMemorySaver()
    replies = [
        AIMessage(
            id="stage",
            content="公开阶段说明",
            tool_calls=[{"id": "call", "name": "noop", "args": {}}],
        ),
        AIMessage(id="final", content="最终回答"),
    ]
    agent = create_test_deep_agent(
        model=LocalFakeChatModel.with_script(replies),
        tools=[noop],
        subagents=[],
        checkpointer=checkpoint,
        permissions=[],
        interrupt_on={"noop": True} if mode == "hitl_resume" else {},
    )
    bus, chat = FakeBus(), FakeChatRepository()

    async def invoke(payload: object, initial: bool) -> bool:
        emitter = await RunEmitter.attach(
            bus,
            "native",
            tenant_id="tenant",
            namespace="ns",
            session_id="session",
            chat_repository=chat,
        )
        return await invoke_once(
            emitter,
            agent,
            "native-thread",
            payload,
            initial=initial,
            approval_tool_names=frozenset({"noop"})
            if mode == "hitl_resume"
            else frozenset(),
            source_for=lambda _: "runtime-custom",
            finalize_terminal=terminal_emitter(emitter, _claim, usage_recorder()[0]),
            record_usage=usage_recorder()[0],
            on_native_settled=settled_state_callback(agent, "native-thread"),
        )

    completed = await invoke({"messages": [HumanMessage(content="go")]}, True)
    if mode == "hitl_resume":
        assert not completed and not find_events(bus.run_events("native"), RunCompleted)
        # A newly constructed native graph resumes the same checkpoint.
        agent = create_test_deep_agent(
            model=LocalFakeChatModel.with_script(replies),
            tools=[noop],
            subagents=[],
            checkpointer=checkpoint,
            permissions=[],
            interrupt_on={"noop": True},
        )
        assert await invoke(Command(resume={"decisions": [{"type": "approve"}]}), False)
    elif mode == "checkpoint_replay":
        count = len(
            [e for e in bus.run_events("native") if e.kind == "message.completed"]
        )
        agent = create_test_deep_agent(
            model=LocalFakeChatModel.with_script([]),
            tools=[noop],
            subagents=[],
            checkpointer=checkpoint,
            permissions=[],
            interrupt_on={},
        )
        assert await invoke(None, False)
        assert (
            len([e for e in bus.run_events("native") if e.kind == "message.completed"])
            == count
        )
    final = find_events(bus.run_events("native"), RunCompleted)[-1]
    assert final.payload.final_segment_id == (
        None if mode == "return_direct" else "final"
    )


async def test_native_swarm_handoff_binds_final_peer_not_handoff_prose() -> None:
    # RED witness: existing Swarm root omits peer stream transformers. Retain
    # this actual SDK failure until that separate execution gap is repaired.
    from langchain_core.messages import HumanMessage
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph_swarm import create_handoff_tool
    from kokoro_agent.swarm import create_swarm
    from support.deepagents import create_test_deep_agent
    from support.local_fake import LocalFakeChatModel
    from support.fakes import settled_state_callback

    handoff = create_handoff_tool(agent_name="peer")
    first = create_test_deep_agent(
        name="first",
        model=LocalFakeChatModel.with_script(
            [
                AIMessage(
                    id="handoff",
                    content="转交给协作助手。",
                    tool_calls=[{"id": "transfer", "name": handoff.name, "args": {}}],
                )
            ]
        ),
        tools=[handoff],
        subagents=[],
        permissions=[],
        interrupt_on={},
    )
    peer = create_test_deep_agent(
        name="peer",
        model=LocalFakeChatModel.with_script(
            [AIMessage(id="peer-answer", content="最终回答")] * 8
        ),
        tools=[],
        subagents=[],
        permissions=[],
        interrupt_on={},
    )
    agent = create_swarm(
        [first, peer], entry_agent="first", checkpointer=InMemorySaver()
    )
    bus, chat = FakeBus(), FakeChatRepository()
    emitter = await RunEmitter.attach(
        bus,
        "swarm-final",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=chat,
    )
    assert await invoke_once(
        emitter,
        agent,
        "swarm-thread",
        {"messages": [HumanMessage(content="go")]},
        initial=True,
        approval_tool_names=frozenset(),
        source_for=lambda _: "runtime-custom",
        finalize_terminal=terminal_emitter(emitter, _claim, usage_recorder()[0]),
        record_usage=usage_recorder()[0],
        on_native_settled=settled_state_callback(agent, "swarm-thread"),
    )
    completed = find_events(bus.run_events("swarm-final"), RunCompleted)
    assert completed, (
        "Swarm must expose peer streams before a final answer can be proved"
    )
    assert completed[-1].payload.final_segment_id == "peer-answer"
