"""Native final recovery follows a preflighted lineage, not public text order."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langchain_core.tools import StructuredTool
from pydantic import BaseModel
from support.deepagents import create_test_deep_agent
from support.local_fake import LocalFakeChatModel

from kokoro_agent.infrastructure.checkpoint_final_answer import (
    final_lineage_tip,
    restore_native_final,
)


def test_lineage_requires_one_complete_same_run_chain() -> None:
    assert final_lineage_tip({"a": None, "b": "a", "c": "b"}) == "c"
    assert final_lineage_tip({}) is None
    assert final_lineage_tip({"b": "foreign-parent"}) is None
    assert final_lineage_tip({"a": None, "b": "a", "c": "a"}) is None
    assert final_lineage_tip({"a": "b", "b": "a"}) is None
    assert final_lineage_tip({"a": None, "b": None}) is None
    assert (
        final_lineage_tip({str(i): str(i - 1) if i else None for i in range(129)})
        is None
    )


@pytest.mark.asyncio
async def test_restores_native_delta_final_without_full_thread_history() -> None:
    saver = InMemorySaver()

    class Args(BaseModel):
        pass

    noop = StructuredTool(
        name="noop", description="no-op", args_schema=Args, func=lambda: "ok"
    )
    graph = create_test_deep_agent(
        model=LocalFakeChatModel.with_script(
            [
                AIMessage(
                    content="Checking documents",
                    id="progress",
                    tool_calls=[{"id": "call", "name": "noop", "args": {}}],
                ),
                AIMessage(content="Final answer", id="native-final"),
            ]
        ),
        tools=[noop],
        subagents=[],
        checkpointer=saver,
        permissions=[],
        interrupt_on={},
    )
    config: Any = {
        "configurable": {"thread_id": "approved-thread"},
        "metadata": {"kokoro_run_id": "approved-run"},
    }
    stream = await graph.astream_events(
        {"messages": [HumanMessage(content="question")]},
        version="v3",
        config=config,
        transformers=[],
    )
    async with stream:
        await stream.output()
    saved = await saver.aget_tuple(config)
    assert saved is not None
    tuples = {item.checkpoint["id"]: item async for item in saver.alist(config)}
    parents = {
        key: item.parent_config.get("configurable", {})["checkpoint_id"]
        if item.parent_config
        else None
        for key, item in tuples.items()
    }
    reads: list[str] = []

    async def get_tuple(lookup: Any) -> Any:
        key = lookup["configurable"]["checkpoint_id"]
        reads.append(key)
        return tuples[key]

    final = await restore_native_final(
        get_tuple=get_tuple,
        thread_id="approved-thread",
        run_id="approved-run",
        parents=parents,
        terminal_at=datetime.now(UTC) + timedelta(seconds=1),
    )
    assert final is not None
    assert (final.message_id, final.text) == ("native-final", "Final answer")
    assert set(reads) <= set(parents)
    assert len(reads) == len(set(reads))

    # Native completion must not be inferred from an AI message at an in-flight checkpoint.
    for item in tuples.values():
        if item.pending_writes:
            incomplete = {
                k: v for k, v in parents.items() if k != saved.checkpoint["id"]
            }
            assert (
                await restore_native_final(
                    get_tuple=get_tuple,
                    thread_id="approved-thread",
                    run_id="approved-run",
                    parents=incomplete,
                    terminal_at=datetime.now(UTC) + timedelta(seconds=1),
                )
                is None
            )
            break
    else:
        pytest.fail("native fixture must include pending work")

    with pytest.raises(ValueError, match="identity differs"):
        await restore_native_final(
            get_tuple=get_tuple,
            thread_id="approved-thread",
            run_id="other-run",
            parents=parents,
            terminal_at=datetime.now(UTC) + timedelta(seconds=1),
        )
    assert (
        await restore_native_final(
            get_tuple=get_tuple,
            thread_id="approved-thread",
            run_id="approved-run",
            parents=parents,
            terminal_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
        is None
    )


def test_read_compatibility_is_explicit_and_does_not_accept_unknown_assembly() -> None:
    from kokoro_agent.infrastructure.checkpoint_final_answer import (
        supports_final_messages,
    )

    def recipe(deepagents_version: str, graph_hash: str) -> dict[str, Any]:
        return {
            "feature": {"agents": [{}]},
            "implementations": {
                "dependencies": [
                    ["deepagents", deepagents_version],
                    ["langchain", "1.3.2"],
                    ["langchain-core", "1.4.0"],
                    ["langgraph", "1.2.2"],
                    ["langgraph-checkpoint", "4.1.1"],
                    ["langgraph-checkpoint-postgres", "3.1.2"],
                    ["ormsgpack", "1.12.2"],
                ],
                "sources": [
                    {
                        "source_id": "assembly",
                        "package": "kokoro_agent",
                        "files": [
                            {
                                "path": "agent_factory.py",
                                "sha256": "0742b5f34218d3b5fb77b14cefae391a3862d5059fdafa17fd4d37f757ddbe55",
                            }
                        ],
                    },
                    {
                        "source_id": "native",
                        "package": "deepagents",
                        "files": [
                            {"path": "graph.py", "sha256": graph_hash},
                            {
                                "path": "_messages_reducer.py",
                                "sha256": "4539763f8d47a4894fac9998d68b3fe0aa7eb369425f8dab08fc7cf9650aa681",
                            },
                        ],
                    },
                ],
            },
        }

    old = recipe(
        "0.6.6", "f342ed28dd9ed9768c8927848fd50bdb9e86145b33c2928120b4272cfdb2c6f7"
    )
    current = recipe(
        "0.6.7", "3f68528ca70489fe9a8ee1ed14095aa09648af62059188006b714a42b6308bb3"
    )
    old["implementations"]["sources"][0]["files"][0]["sha256"] = (
        "1f0bee0e5d5c10e2e8954179826d6391cb0ecf3d47afdba2ef00a1b6543263df"
    )
    assert supports_final_messages(old)
    assert supports_final_messages(current)
    assert not supports_final_messages(
        recipe(
            "0.6.8", "3f68528ca70489fe9a8ee1ed14095aa09648af62059188006b714a42b6308bb3"
        )
    )
    assert not supports_final_messages(
        recipe(
            "0.6.7", "f342ed28dd9ed9768c8927848fd50bdb9e86145b33c2928120b4272cfdb2c6f7"
        )
    )
    old["feature"]["agents"].append({})
    assert not supports_final_messages(old)
    current["implementations"]["sources"][0]["files"][0]["sha256"] = "0" * 64
    assert not supports_final_messages(current)
