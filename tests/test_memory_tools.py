"""非生产 legacy memory 实验规格；生产不可达性由 test_architecture.py 执法。"""

from __future__ import annotations

from uuid import uuid4

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.store.base import BaseStore
from pydantic import ValidationError
import pytest

from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.state import RunScope
from kokoro_agent.streams.redis import RedisStream
from fakes import FakeLedger, completed_execution_context, usage_recorder
from kokoro_agent.contract.streams import run_events_stream
from kokoro_agent.tools.memory import SaveMemoryArgs, make_memory_tools


def _context(namespace: str, run_id: str) -> RunScope:
    return RunScope(namespace=namespace, session_id="s1", run_id=run_id, thread_id="s1")


async def _run(
    script: list[AIMessage], store: BaseStore, context: RunScope, bus: RedisStream
) -> RedisStream:
    agent = build_agent(
        model=LocalFakeChatModel.with_script(script),
        tools=list(make_memory_tools(context.namespace)),
        system_prompt="x",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on={},
        store=store,
    )

    async def claim() -> bool:
        return True

    ledger = FakeLedger()
    terminal = await invoke_once(
        RunEmitter(bus, context.run_id, outbox=ledger),
        agent,
        {
            "configurable": {"thread_id": context.scoped_thread_id},
            "metadata": {"kokoro_run_id": context.run_id},
        },
        {"messages": [HumanMessage(content="hi")], "scope": context.as_state()},
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        claim_terminal=claim,
        prepare_completed=lambda: completed_execution_context(context.run_id),
        record_usage=usage_recorder()[0],
    )
    assert terminal is True
    return bus


async def _tool_result(bus: RedisStream, run_id: str, name: str) -> str:
    for item in await bus.read_all(run_events_stream(run_id)):
        event = item.event
        payload = event.get("payload")
        if (
            event.get("kind") == "tool.returned"
            and isinstance(payload, dict)
            and payload.get("name") == name
        ):
            return str(payload.get("result"))
    raise AssertionError(f"no tool.returned for {name!r}")


def _save_script(key: str, content: str) -> list[AIMessage]:
    return [
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "save_memory",
                    "args": {"key": key, "content": content},
                    "id": "m1",
                    "type": "tool_call",
                }
            ],
        ),
        AIMessage(content="saved"),
    ]


def _search_script(query: str) -> list[AIMessage]:
    return [
        AIMessage(
            content="",
            tool_calls=[
                {"name": "search_memory", "args": {"query": query}, "id": "m2", "type": "tool_call"}
            ],
        ),
        AIMessage(content="done"),
    ]


async def test_save_then_search_across_runs_same_namespace(
    stream: RedisStream, memory_store: BaseStore
) -> None:
    store = memory_store
    r1, r2 = f"r1-{uuid4().hex}", f"r2-{uuid4().hex}"
    await _run(_save_script("pref", "user likes dark mode"), store, _context("team-a", r1), stream)
    # 跨 run（新图、新 run_id、同 store）：记忆持久可读。
    bus = await _run(_search_script("dark"), store, _context("team-a", r2), stream)
    result = await _tool_result(bus, r2, "search_memory")
    assert "user likes dark mode" in result
    items = await store.asearch(("team-a", "memories"))
    assert [(i.key, i.value) for i in items] == [("pref", {"content": "user likes dark mode"})]


async def test_namespace_isolation_between_tenants(
    stream: RedisStream, memory_store: BaseStore
) -> None:
    store = memory_store
    r1, r2 = f"r1-{uuid4().hex}", f"r2-{uuid4().hex}"
    await _run(_save_script("k", "team-a secret"), store, _context("team-a", r1), stream)
    bus = await _run(_search_script("secret"), store, _context("team-b", r2), stream)
    assert await _tool_result(bus, r2, "search_memory") == "no memories found"
    assert await store.asearch(("team-b",)) == []
    assert len(await store.asearch(("team-a", "memories"))) == 1


async def test_save_memory_error_reaches_wire_as_tool_error(
    stream: RedisStream, memory_store: BaseStore
) -> None:
    store = memory_store
    r1 = f"r1-{uuid4().hex}"
    bus = await _run(_save_script("k", "   "), store, _context("team-a", r1), stream)
    result = await _tool_result(bus, r1, "save_memory")
    assert "non-empty" in result
    assert await store.asearch(("team-a", "memories")) == []


@pytest.mark.parametrize(
    "args",
    [
        {"key": "k", "content": ""},
        {"key": "", "content": "v"},
        {"key": "k"},
        {"content": "v"},
        {"key": "k", "content": "v", "extra": "x"},
    ],
)
def test_save_memory_args_schema_rejects_invalid(args: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        SaveMemoryArgs.model_validate(args)
