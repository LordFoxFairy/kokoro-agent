# LangGraph's compiled generic surface currently exposes partially-unknown overloads in pyright.
# pyright: reportUnknownMemberType=false, reportArgumentType=false

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from fakes import request
from kokoro_agent.contract import ExecutionContextIntentContinue, ExecutionContextIntentFork
from kokoro_agent.storage.execution_context import (
    ClaimedCompletionFrames,
    CompletionEventDraft,
    CompletedExecutionContext,
    DurableCompletionFrame,
    ExecutionCheckpoint,
    ExecutionContextAuthority,
    ExecutionContextBinding,
    ExecutionContextConflict,
    ExecutionContextStore,
)


class _State(TypedDict):
    history: Annotated[list[str], operator.add]


class FakeExecutionContextStore(ExecutionContextStore):
    def __init__(self) -> None:
        self.bindings: dict[str, ExecutionContextBinding] = {}
        self.completed: dict[str, CompletedExecutionContext] = {}
        self.continuations: dict[str, str] = {}

    async def get_execution_context_binding(self, run_id: str) -> ExecutionContextBinding | None:
        return self.bindings.get(run_id)

    async def bind_execution_context(
        self, run_id: str, binding: ExecutionContextBinding
    ) -> ExecutionContextBinding:
        current = self.bindings.setdefault(run_id, binding)
        if current != binding:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_CONFLICT")
        return current

    async def update_execution_checkpoint(
        self, run_id: str, checkpoint: ExecutionCheckpoint
    ) -> None:
        current = self.bindings[run_id]
        self.bindings[run_id] = current.model_copy(update={"active_checkpoint": checkpoint})

    async def resolve_execution_parent(
        self,
        *,
        namespace: str,
        anchor: str,
        digest: str,
        continuation_run_id: str | None,
    ) -> ExecutionCheckpoint | None:
        parent = self.completed.get(anchor)
        if parent is None or parent.namespace != namespace or parent.digest != digest:
            return None
        if continuation_run_id is not None:
            current = self.continuations.setdefault(anchor, continuation_run_id)
            if current != continuation_run_id:
                return None
        return parent.checkpoint

    async def try_complete_execution_context(
        self,
        completion: CompletedExecutionContext,
        owner_event: CompletionEventDraft,
        terminal_event: CompletionEventDraft,
    ) -> ClaimedCompletionFrames | None:
        if completion.run_id in {item.run_id for item in self.completed.values()}:
            return None
        assert owner_event.kind == "run.owner.completed"
        assert terminal_event.kind == "run.completed"
        assert terminal_event.index == owner_event.index + 1
        self.completed[completion.anchor] = completion
        return ClaimedCompletionFrames(
            owner=DurableCompletionFrame(
                **owner_event.model_dump(), durable_seq=1, event_id="evt_owner"
            ),
            terminal=DurableCompletionFrame(
                **terminal_event.model_dump(), durable_seq=2, event_id="evt_terminal"
            ),
        )


async def _complete(
    authority: ExecutionContextAuthority,
    store: FakeExecutionContextStore,
    run_id: str,
) -> CompletedExecutionContext:
    completion = await authority.prepare_completion(run_id)
    claimed = await store.try_complete_execution_context(
        completion,
        CompletionEventDraft(
            kind="run.owner.completed", index=0, timestamp=1, payload_json="{}"
        ),
        CompletionEventDraft(kind="run.completed", index=1, timestamp=1, payload_json="{}"),
    )
    assert claimed is not None
    return completion


def _graph(saver: InMemorySaver):
    builder = StateGraph(_State)
    builder.add_node("checkpoint", lambda _state: {})
    builder.add_edge(START, "checkpoint")
    builder.add_edge("checkpoint", END)
    return builder.compile(checkpointer=saver)


async def test_exact_historical_continue_fork_and_new_root_are_isolated() -> None:
    saver = InMemorySaver()
    store = FakeExecutionContextStore()
    authority = ExecutionContextAuthority(store=store, checkpointer=saver)
    graph = _graph(saver)

    root = request("root", namespace="opaque-ns", thread_id="logical-thread")
    root_config = await authority.open(root)
    root_result = await graph.ainvoke({"history": ["root"]}, root_config)
    assert root_result["history"] == ["root"]
    root_completion = await _complete(authority, store, root.run_id)

    continuation = request("continue", namespace="opaque-ns", thread_id="logical-thread").model_copy(
        update={
            "execution_context": ExecutionContextIntentContinue(
                mode="continue",
                parent_anchor=root_completion.anchor,
                parent_digest=root_completion.digest,
            )
        }
    )
    continuation_config = await authority.open(continuation)
    continuation_result = await graph.ainvoke({"history": ["continue"]}, continuation_config)
    assert continuation_result["history"] == ["root", "continue"]
    await _complete(authority, store, continuation.run_id)

    fork = request("fork", namespace="opaque-ns", thread_id="logical-thread").model_copy(
        update={
            "execution_context": ExecutionContextIntentFork(
                mode="fork",
                parent_anchor=root_completion.anchor,
                parent_digest=root_completion.digest,
            )
        }
    )
    fork_result = await graph.ainvoke({"history": ["fork"]}, await authority.open(fork))
    assert fork_result["history"] == ["root", "fork"]

    fresh = request("fresh", namespace="opaque-ns", thread_id="logical-thread")
    fresh_result = await graph.ainvoke({"history": ["fresh"]}, await authority.open(fresh))
    assert fresh_result["history"] == ["fresh"]


async def test_continue_is_keep_first_but_fork_does_not_consume_parent() -> None:
    saver = InMemorySaver()
    store = FakeExecutionContextStore()
    authority = ExecutionContextAuthority(store=store, checkpointer=saver)
    graph = _graph(saver)
    root = request("root", namespace="ns")
    await graph.ainvoke({"history": ["root"]}, await authority.open(root))
    completed = await _complete(authority, store, root.run_id)

    first = request("child-1", namespace="ns").model_copy(
        update={
            "execution_context": ExecutionContextIntentContinue(
                mode="continue", parent_anchor=completed.anchor, parent_digest=completed.digest
            )
        }
    )
    await authority.open(first)
    assert await authority.open(first) == await authority.config_for_run(first.run_id)

    stale = first.model_copy(update={"run_id": "child-2"})
    with pytest.raises(ExecutionContextConflict, match="EXECUTION_CONTEXT_PARENT_UNAVAILABLE"):
        await authority.open(stale)

    fork = first.model_copy(
        update={
            "run_id": "fork-1",
            "execution_context": ExecutionContextIntentFork(
                mode="fork", parent_anchor=completed.anchor, parent_digest=completed.digest
            ),
        }
    )
    await authority.open(fork)


async def test_parent_pair_is_namespace_bound_and_checkpoint_digest_is_strict() -> None:
    saver = InMemorySaver()
    store = FakeExecutionContextStore()
    authority = ExecutionContextAuthority(store=store, checkpointer=saver)
    graph = _graph(saver)
    root = request("root", namespace="ns-a")
    await graph.ainvoke({"history": ["root"]}, await authority.open(root))
    completed = await _complete(authority, store, root.run_id)
    assert completed.anchor.startswith("ctx_") and len(completed.anchor) <= 256
    assert len(completed.digest) == 64

    cross_namespace = request("child", namespace="ns-b").model_copy(
        update={
            "execution_context": ExecutionContextIntentFork(
                mode="fork", parent_anchor=completed.anchor, parent_digest=completed.digest
            )
        }
    )
    with pytest.raises(ExecutionContextConflict, match="EXECUTION_CONTEXT_PARENT_UNAVAILABLE"):
        await authority.open(cross_namespace)
