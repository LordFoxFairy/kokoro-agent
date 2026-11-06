"""Real SDK Swarm peers are public; their declared native tasks remain children."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_swarm import create_handoff_tool

from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.protocols import AgentRunnable
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.swarm import create_swarm
from support.chat import FakeChatRepository
from support.deepagents import create_test_deep_agent
from support.fakes import (
    FakeBus,
    settled_state_callback,
    terminal_emitter,
    usage_recorder,
)


class PeerModel(GenericFakeChatModel):
    """Each native peer owns its script, independent of shared conversation length."""

    def bind_tools(self, tools: Sequence[Any], **kwargs: Any) -> PeerModel:
        return self


def reply(
    name: str, *, tool: str | None = None, args: dict[str, object] | None = None
) -> AIMessage:
    return AIMessage(
        id=name,
        content=name,
        tool_calls=(
            [{"id": f"call-{name}", "name": tool, "args": args or {}}] if tool else []
        ),
    )


def peer(
    name: str,
    messages: list[AIMessage],
    *,
    tools: list[Any],
    subagents: list[Any],
    interrupt_on: dict[str, bool] | None = None,
) -> AgentRunnable:
    return create_test_deep_agent(
        name=name,
        model=PeerModel(messages=iter(messages), disable_streaming=True),
        tools=tools,
        subagents=subagents,
        permissions=[],
        interrupt_on=interrupt_on or {},
    )


async def invoke(
    graph: AgentRunnable, emitter: RunEmitter, payload: object, *, initial: bool
) -> bool:
    async def claim() -> bool:
        return True

    return await invoke_once(
        emitter,
        graph,
        "scoped-peers",
        payload,
        initial=initial,
        approval_tool_names=frozenset(),
        source_for=lambda _: "runtime-custom",
        finalize_terminal=terminal_emitter(emitter, claim, usage_recorder()[0]),
        record_usage=usage_recorder()[0],
        on_native_settled=settled_state_callback(graph, "scoped-peers"),
    )


async def test_roundtrip_uses_each_peers_task_declarations_and_replays_without_duplication() -> (
    None
):
    to_beta, to_alpha = (
        create_handoff_tool(agent_name="beta"),
        create_handoff_tool(agent_name="alpha"),
    )

    def helper(name: str) -> dict[str, object]:
        return {
            "name": name,
            "description": name,
            "system_prompt": name,
            "model": PeerModel(
                messages=iter([reply(f"{name}-private")]), disable_streaming=True
            ),
            "tools": [],
        }

    alpha = peer(
        "alpha",
        [
            reply(
                "alpha-stage",
                tool="task",
                args={"subagent_type": "alpha-helper", "description": "look"},
            ),
            reply("alpha-handoff", tool=to_beta.name),
            reply("alpha-final"),
        ],
        tools=[to_beta],
        subagents=[helper("alpha-helper")],
    )
    beta = peer(
        "beta",
        [
            reply(
                "beta-stage",
                tool="task",
                args={"subagent_type": "beta-helper", "description": "look"},
            ),
            reply("beta-handoff", tool=to_alpha.name),
        ],
        tools=[to_alpha],
        subagents=[helper("beta-helper")],
    )
    graph = create_swarm(
        [alpha, beta], entry_agent="alpha", checkpointer=InMemorySaver()
    )
    bus, chat = FakeBus(), FakeChatRepository()
    emitter = await RunEmitter.attach(
        bus,
        "swarm-roundtrip",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=chat,
    )
    assert await invoke(
        graph, emitter, {"messages": [HumanMessage(content="go")]}, initial=True
    )
    events = bus.run_events("swarm-roundtrip")
    public = [e.payload.model_dump() for e in events if e.kind == "message.completed"]
    assert [p["segment_id"] for p in public] == [
        "alpha-stage",
        "alpha-handoff",
        "beta-stage",
        "beta-handoff",
        "alpha-final",
    ]
    children = [
        e.payload.model_dump() for e in events if e.kind == "subagent.text.completed"
    ]
    assert {p["segment_id"] for p in children} == {
        "alpha-helper-private",
        "beta-helper-private",
    }
    completed = [e.payload.model_dump() for e in events if e.kind == "run.completed"]
    assert completed[-1]["final_segment_id"] == "alpha-final"
    assert await invoke(graph, emitter, None, initial=False)
    assert len(
        [e for e in bus.run_events("swarm-roundtrip") if e.kind == "message.completed"]
    ) == len(public)


async def test_swarm_checkpoint_resume_keeps_peer_and_does_not_repeat_handoff() -> None:
    from langchain_core.tools import StructuredTool
    from langgraph.types import Command
    from pydantic import BaseModel

    class Args(BaseModel):
        pass

    executions: list[str] = []
    gate = StructuredTool(
        name="gate",
        description="gated operation",
        args_schema=Args,
        func=lambda: executions.append("gate") or "ok",
    )
    transfer = create_handoff_tool(agent_name="beta")
    saver = InMemorySaver()

    def build(resuming: bool) -> AgentRunnable:
        return create_swarm(
            [
                peer(
                    "alpha",
                    [] if resuming else [reply("handoff", tool=transfer.name)],
                    tools=[transfer],
                    subagents=[],
                ),
                peer(
                    "beta",
                    [reply("resumed-answer")]
                    if resuming
                    else [reply("approval-stage", tool="gate")],
                    tools=[gate],
                    subagents=[],
                    interrupt_on={"gate": True},
                ),
            ],
            entry_agent="alpha",
            checkpointer=saver,
        )

    graph = build(False)
    bus, chat = FakeBus(), FakeChatRepository()
    emitter = await RunEmitter.attach(
        bus,
        "swarm-resume",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=chat,
    )
    assert not await invoke(
        graph, emitter, {"messages": [HumanMessage(content="go")]}, initial=True
    )
    assert not executions
    assert not [e for e in bus.run_events("swarm-resume") if e.kind == "run.completed"]
    assert await invoke(
        build(True),
        emitter,
        Command(resume={"decisions": [{"type": "approve"}]}),
        initial=False,
    )
    assert executions == ["gate"]
    events = bus.run_events("swarm-resume")
    assert [
        e.payload.model_dump()["segment_id"]
        for e in events
        if e.kind == "message.completed"
    ] == ["handoff", "approval-stage", "resumed-answer"]
    assert [
        e.payload.model_dump()["final_segment_id"]
        for e in events
        if e.kind == "run.completed"
    ] == ["resumed-answer"]


@pytest.mark.parametrize("cause", ["cancel", "persistence"])
async def test_swarm_peer_failure_closes_native_tool_without_terminal_answer(
    cause: str,
) -> None:
    import asyncio
    from langchain_core.tools import StructuredTool
    from langgraph.stream import CustomTransformer
    from pydantic import BaseModel
    from kokoro_agent.execution.publish_agent_events import pump_run

    entered, closed = asyncio.Event(), asyncio.Event()

    class Args(BaseModel):
        pass

    async def wait_for_release() -> str:
        entered.set()
        try:
            await asyncio.Event().wait()
            return "unreachable"
        finally:
            closed.set()

    wait_tool = StructuredTool(
        name="wait_for_release",
        description="blocking tool",
        args_schema=Args,
        coroutine=wait_for_release,
    )
    transfer = create_handoff_tool(agent_name="beta")
    graph = create_swarm(
        [
            peer(
                "alpha",
                [reply("handoff", tool=transfer.name)],
                tools=[transfer],
                subagents=[],
            ),
            peer(
                "beta",
                [reply("waiting", tool="wait_for_release")],
                tools=[wait_tool],
                subagents=[],
            ),
        ],
        entry_agent="alpha",
        checkpointer=InMemorySaver(),
    )
    from kokoro_agent.domain.chat.models import ChatEventRecord, ChatProjection
    from kokoro_agent.execution.events import ProgressPersistenceError

    class FailingChat(FakeChatRepository):
        async def append(self, projection: ChatProjection) -> ChatEventRecord:
            if (
                cause == "persistence"
                and projection.message is not None
                and projection.message.content == "waiting"
            ):
                # Let the native beta tool enter, then fail its public progress
                # persistence while root and peer producers are still waiting.
                async with asyncio.timeout(2):
                    await entered.wait()
                raise OSError("progress write unavailable")
            return await super().append(projection)

    bus = FakeBus()
    emitter = await RunEmitter.attach(
        bus,
        "swarm-cancel",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=FailingChat(),
    )
    run = await graph.astream_events(
        {"messages": [HumanMessage(content="go")]},
        config={"configurable": {"thread_id": "cancel-peer"}},
        version="v3",
        transformers=[CustomTransformer],
    )
    baseline = asyncio.all_tasks()
    async with run:
        consumer = asyncio.create_task(
            pump_run(emitter, run, source_for=lambda _: "runtime-custom")
        )
        try:
            async with asyncio.timeout(2):
                await entered.wait()
            if cause == "cancel":
                consumer.cancel()
            done, _ = await asyncio.wait({consumer}, timeout=2)
            assert consumer in done, "cancel must join the peer consumers"
            if cause == "cancel":
                assert consumer.cancelled()
            else:
                with pytest.raises(ProgressPersistenceError):
                    await consumer
        finally:
            if not consumer.done():
                consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
    assert closed.is_set()
    assert not [e for e in bus.run_events("swarm-cancel") if e.kind == "run.completed"]
    assert not [task for task in asyncio.all_tasks() - baseline if not task.done()]
