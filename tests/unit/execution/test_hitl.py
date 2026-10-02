"""HITL full collections: the actual checkpoint adapter and pure domain rules.

Memory saver tests prove native shapes/mapping only, not durable PostgreSQL.
Legacy partial-frame selectors are intentionally absent.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypedDict, Protocol, runtime_checkable
from dataclasses import dataclass

import pytest
from langchain_core.runnables.config import RunnableConfig
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.types import interrupt
from pydantic import JsonValue, TypeAdapter, ValidationError

from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.domain.run.interactions import (
    InteractionConflict,
    InteractionCorrupt,
    AcceptedResume,
)
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.execution.protocols import require_agent_runnable
from kokoro_agent.infrastructure.checkpoint_interactions import (
    InteractionCheckpointer,
    NativePauseRead,
    PauseReadTarget,
    ResumeReadTarget,
    PreparedNativeResume,
    make_interaction_checkpointer,
)
from kokoro_agent.infrastructure.postgres_run_interactions import decode_pause_snapshot
from kokoro_agent.protocol import RunRequest, RunResume
from kokoro_agent.protocol.control import control_request_digest
from support.fakes import FakeRunRepository, request

_JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class _State(TypedDict):
    value: JsonValue


@dataclass
class _NativeFixture:
    bridge: InteractionCheckpointer
    repository: FakeRunRepository
    run: RunRequest
    lease: LeaseFence
    handle: AgentHandle
    paused: NativePauseRead
    effects: list[JsonValue]


def _approval(names: tuple[str, ...] = ("danger", "danger")) -> dict[str, JsonValue]:
    return {
        "action_requests": [
            {"name": name, "args": {"secret": "private"}, "description": "sdk-template"}
            for name in names
        ],
        "review_configs": [
            {"action_name": name, "allowed_decisions": ["approve", "edit", "reject"]}
            for name in dict.fromkeys(names)
        ],
    }


def _human(
    kind: str, request_id: str = "same-request", validation: JsonValue = None
) -> dict[str, JsonValue]:
    context: dict[str, JsonValue] = {
        "name": "input",
        "args": {"private": "secret"},
        "result": "private result",
    }
    if validation is not None:
        context["validation_error"] = validation
    return {
        "kokoro_human_request": {
            "request_id": request_id,
            "kind": kind,
            "response_schema": {"type": "object"},
            "context": context,
        }
    }


@runtime_checkable
class _CompiledGraph(Protocol):
    async def ainvoke(self, input: object, config: RunnableConfig) -> object: ...


@runtime_checkable
class _GraphBuilder(Protocol):
    def add_node(self, node: str, action: object) -> _GraphBuilder: ...
    def add_edge(self, start_key: str, end_key: str) -> _GraphBuilder: ...
    def compile(
        self, *, checkpointer: BaseCheckpointSaver[str] | None = None
    ) -> _CompiledGraph: ...


def _builder() -> _GraphBuilder:
    # Runtime-check the SDK boundary; pinned generic stubs have unbound cache
    # parameters. No copied scheduler, cast, Any, or checker suppression.
    graph: object = StateGraph(_State)
    return _checked_builder(graph)


def _checked_builder(graph: object) -> _GraphBuilder:
    assert isinstance(graph, _GraphBuilder)
    return graph


async def _fixture(values: tuple[JsonValue, ...]) -> _NativeFixture:
    repo = FakeRunRepository()
    run = request("native-collection")
    lease = await repo.try_claim(run)
    assert lease is not None
    saver, reader = InMemorySaver(), InMemorySaver()
    # Explicit memory-only shared backing, not independent PG evidence.
    reader.storage = saver.storage
    reader.writes = saver.writes
    reader.blobs = saver.blobs
    bridge = make_interaction_checkpointer(saver=saver, reader=reader, repository=repo)
    effects: list[JsonValue] = []

    def node(value: JsonValue) -> Callable[[_State], _State]:
        def execute(state: _State) -> _State:
            del state
            answer = _JSON.validate_python(interrupt(value))
            effects.append(answer)
            return {"value": answer}

        return execute

    # One child and successive root nodes avoid multiple writes to one state key.
    child = (
        _builder()
        .add_node("ask", node(values[0]))
        .add_edge(START, "ask")
        .add_edge("ask", END)
        .compile()
    )
    graph = _builder().add_node("child", child).add_edge(START, "child")
    # Mixed sets use parallel nodes with disjoint state writes (nodes return none).
    for index, value in enumerate(values[1:], 1):

        def pending(state: _State, value: JsonValue = value) -> None:
            del state
            effects.append(_JSON.validate_python(interrupt(value)))

        graph.add_node(f"root{index}", pending).add_edge(
            START, f"root{index}"
        ).add_edge(f"root{index}", END)
    graph.add_edge("child", END)
    compiled = graph.compile(checkpointer=bridge)
    handle = AgentHandle(
        runnable=require_agent_runnable(compiled),
        tool_descriptions={"danger": "approved description", "input": "Input"},
    )
    config: RunnableConfig = {
        "configurable": {"thread_id": RunScope.of(run).scoped_thread_id},
        "metadata": {
            "kokoro_run_id": run.run_id,
            "kokoro_generation": lease.generation,
        },
    }
    await compiled.ainvoke({"value": None}, config)
    read = await bridge.read_interaction(
        request=run,
        lease=lease,
        handle=handle,
        target=PauseReadTarget(command_id=None, attempt_id=None),
    )
    assert isinstance(read, NativePauseRead) and not effects
    await repo.record_pause(run, lease, read.pause)
    return _NativeFixture(bridge, repo, run, lease, handle, read, effects)


async def _prepare(
    fixture: _NativeFixture,
    decisions: list[dict[str, JsonValue]],
    *,
    revision: int = 1,
    ref: str | None = None,
) -> PreparedNativeResume:
    command = RunResume.model_validate(
        {
            "kind": "run.resume",
            "run_id": fixture.run.run_id,
            "session_id": fixture.run.session_id,
            "command_id": "decision",
            "request_digest": "placeholder",
            "expected_pause_revision": revision,
            "pause_ref": ref or fixture.paused.pause.pause_ref,
            "decisions": decisions,
        }
    )
    command = command.model_copy(
        update={"request_digest": control_request_digest(command)}
    )
    assert command.request_digest is not None
    await fixture.repository.admit_control(
        fixture.run.run_id,
        command.command_id,
        command.request_digest,
        command.model_dump_json(),
    )
    accepted = await fixture.repository.accept_resume(
        fixture.run, command.command_id, "test-consumer"
    )
    assert isinstance(accepted, AcceptedResume)
    read = await fixture.bridge.read_interaction(
        request=fixture.run,
        lease=accepted.lease,
        handle=fixture.handle,
        target=ResumeReadTarget(command_id=command.command_id),
    )
    assert isinstance(read, PreparedNativeResume)
    return read


async def test_complete_mixed_root_child_review_input_collection_not_partial_frame() -> (
    None
):
    fx = await _fixture((_approval(), _human("review"), _human("input")))
    pause = decode_pause_snapshot(fx.paused.pause)
    assert len(pause.groups) == 3
    assert sorted(len(g.items) for g in pause.groups) == [1, 1, 2]
    items = [i for g in pause.groups for i in g.items]
    assert len({i.item_id for i in items}) == 4
    assert {i.kind for i in items} == {"tool_approval", "result_review", "input"}
    assert any(g.checkpoint_ns for g in pause.locator.groups)
    assert any(not g.checkpoint_ns for g in pause.locator.groups)
    decisions: list[dict[str, JsonValue]] = [
        {"item_id": i.item_id, "type": "submit", "value": {"otp": "1"}}
        if i.kind == "input"
        else {"item_id": i.item_id, "type": "approve"}
        for i in reversed(items)
    ]
    prepared = await _prepare(fx, decisions)
    mapping = _JSON.validate_python(prepared.command.resume)
    assert isinstance(mapping, dict) and len(mapping) == 3
    assert fx.effects == []  # preparing a full map never executes native nodes.
    assert all(
        "secret" not in i.model_dump_json()
        and "private result" not in i.model_dump_json()
        for i in items
    )
    assert all(
        i.display.description == "approved description"
        for i in items
        if i.kind == "tool_approval"
    )


@pytest.mark.parametrize(
    "fault",
    ["missing", "extra", "duplicate", "stale_revision", "stale_ref", "disallowed"],
)
async def test_full_collection_rejects_entire_invalid_batch_before_native(
    fault: str,
) -> None:
    fx = await _fixture((_approval(),))
    items = decode_pause_snapshot(fx.paused.pause).groups[0].items
    decisions: list[dict[str, JsonValue]] = [
        {"type": "approve", "item_id": i.item_id} for i in items
    ]
    if fault == "missing":
        decisions.pop()
    elif fault == "extra":
        decisions.append({"type": "approve", "item_id": "foreign"})
    elif fault == "duplicate":
        decisions[1] = dict(decisions[0])
    elif fault == "disallowed":
        decisions[0] = {
            "type": "respond",
            "item_id": items[0].item_id,
            "response": "no",
        }
    with pytest.raises((InteractionConflict, ValidationError)):
        await _prepare(
            fx,
            decisions,
            revision=2 if fault == "stale_revision" else 1,
            ref="stale" if fault == "stale_ref" else None,
        )
    assert fx.effects == []
    state = await fx.repository.read_interaction(fx.run)
    assert state is not None and state.state.phase.value == "waiting"


@pytest.mark.parametrize(
    "kind,payload,expected",
    [
        ("approve", {}, {"type": "approve"}),
        (
            "approve",
            {"args": {"x": 2}},
            {"type": "edit", "edited_action": {"name": "danger", "args": {"x": 2}}},
        ),
        (
            "edit",
            {"args": {"x": 2}},
            {"type": "edit", "edited_action": {"name": "danger", "args": {"x": 2}}},
        ),
        ("reject", {"reason": "no"}, {"type": "reject", "message": "no"}),
    ],
)
async def test_native_approval_mapping_preserves_actual_decisions(
    kind: str, payload: dict[str, JsonValue], expected: dict[str, JsonValue]
) -> None:
    fx = await _fixture((_approval(("danger",)),))
    item = decode_pause_snapshot(fx.paused.pause).groups[0].items[0]
    prepared = await _prepare(fx, [{"item_id": item.item_id, "type": kind, **payload}])
    mapping = _JSON.validate_python(prepared.command.resume)
    assert isinstance(mapping, dict) and list(mapping.values()) == [
        {"decisions": [expected]}
    ]


@pytest.mark.parametrize(
    "kind,action,payload",
    [
        ("review", "respond", {"response": "curated"}),
        ("review", "reject", {"reason": "no"}),
        ("review", "approve", {}),
        ("input", "submit", {"value": {"otp": "1"}}),
        ("input", "reject", {"reason": "no"}),
        ("question", "respond", {"response": "Beijing"}),
    ],
)
async def test_human_native_mapping_uses_request_identity_only(
    kind: str, action: str, payload: dict[str, JsonValue]
) -> None:
    fx = await _fixture((_human(kind),))
    item = decode_pause_snapshot(fx.paused.pause).groups[0].items[0]
    prepared = await _prepare(
        fx, [{"item_id": item.item_id, "type": action, **payload}]
    )
    mapping = _JSON.validate_python(prepared.command.resume)
    assert isinstance(mapping, dict) and list(mapping.values()) == [
        [{"request_id": "same-request", "type": action, **payload}]
    ]


@pytest.mark.parametrize(
    "value",
    [
        {},
        "not a dict",
        {"action_requests": [], "review_configs": [], "extra": 1},
        {"action_requests": [{"name": "x"}], "review_configs": []},
    ],
)
async def test_malformed_native_approval_is_rejected(value: JsonValue) -> None:
    with pytest.raises((InteractionCorrupt, ValidationError)):
        await _fixture((value,))


async def test_safe_validation_code_path_and_no_raw_error_or_result() -> None:
    fx = await _fixture(
        (
            _human(
                "input",
                validation={"code": "json_schema_invalid", "instance_path": ["otp"]},
            ),
        )
    )
    items = decode_pause_snapshot(fx.paused.pause).groups[0].items
    assert items[0].validation is not None and items[0].validation.instance_path == [
        "otp"
    ]
    assert items[0].display.description == "Input"
    assert "secret" not in items[0].model_dump_json()
    with pytest.raises(ValidationError):
        await _fixture((_human("input", validation="secret raw error"),))


async def test_multiple_reviews_and_inputs_form_complete_set_not_old_single_frame_restriction() -> (
    None
):
    fx = await _fixture(
        (_human("review", "a"), _human("review", "b"), _human("input", "c"))
    )
    items = [i for g in decode_pause_snapshot(fx.paused.pause).groups for i in g.items]
    assert {i.request_id for i in items} == {"a", "b", "c"}
    assert len(items) == 3 and fx.effects == []


@pytest.mark.parametrize(
    ("phase", "result_kind", "pause_revision"),
    [
        ("waiting", None, 1),
        ("resuming", "accepted", 1),
        ("active", "native_consumed", 1),
        ("waiting", "validation_failed", 2),
        ("resuming", "unknown", 1),
        ("terminal", "cancelled", 1),
    ],
)
def test_bridge_r35_full_state_retains_original_action_round(
    phase: str, result_kind: str | None, pause_revision: int
) -> None:
    """Approved source model only; no claim of a committed native observation."""
    from kokoro_agent.protocol.events import ChatInteractionState

    groups: list[dict[str, JsonValue]] = []
    if phase in {"waiting", "resuming"}:
        groups = [
            {
                "group_id": "root-group",
                "items": [
                    {
                        "item_id": "item-1",
                        "request_id": "native-same-id",
                        "kind": "input",
                        "allowed_decisions": ["submit", "reject"],
                        "display": {
                            "name": "input",
                            "description": "Input",
                            "editable": False,
                            "input_schema": {},
                        },
                        "validation": {
                            "code": "json_schema_invalid",
                            "instance_path": ["otp"],
                        }
                        if result_kind == "validation_failed"
                        else None,
                    }
                ],
            }
        ]
    action: dict[str, JsonValue] | None = (
        None
        if result_kind is None
        else {
            "command_id": "original-command",
            "pause_revision": 1,
            "kind": result_kind,
        }
    )
    state = ChatInteractionState.model_validate(
        {
            "interaction_revision": 4,
            "pause_revision": pause_revision,
            "pause_ref": f"pause-{pause_revision}",
            "phase": phase,
            "groups": groups,
            "action_result": action,
        }
    )
    result = state.model_dump(mode="json")
    assert result["action_result"] == action
    assert result["pause_revision"] == pause_revision
    assert bool(result["groups"]) == (phase in {"waiting", "resuming"})
    if result_kind == "validation_failed":
        assert action is not None and action["pause_revision"] != pause_revision
    assert "checkpoint" not in state.model_dump_json()


def test_bridge_r35_full_state_requires_explicit_action_result_even_when_null() -> None:
    from kokoro_agent.protocol.events import ChatInteractionState

    with pytest.raises(ValidationError, match="action_result"):
        ChatInteractionState.model_validate(
            {
                "interaction_revision": 1,
                "pause_revision": 1,
                "pause_ref": "pause-1",
                "phase": "active",
                "groups": [],
            }
        )


async def test_r70_mixed_batch_consumes_native_once_reconciles_and_replays() -> None:
    from kokoro_agent.domain.run.interactions import (
        ConsumedPauseEvidence,
        IntentStatus,
        Phase,
        ReplayedResume,
        StartedResume,
    )
    from kokoro_agent.infrastructure.checkpoint_interactions import ObservedNativeResume
    from kokoro_agent.infrastructure.postgres_run_interactions import (
        canonical_interaction_bytes,
    )

    fx = await _fixture((_approval(), _human("review"), _human("input")))
    waiting = await fx.repository.read_interaction(fx.run)
    assert waiting is not None and waiting.state.phase is Phase.WAITING
    assert waiting.pause == fx.paused.pause
    pause = decode_pause_snapshot(fx.paused.pause)
    items = [item for group in pause.groups for item in group.items]
    assert len(pause.groups) == 3 and len(items) == 4
    assert {item.kind for item in items} == {"tool_approval", "result_review", "input"}
    assert any(group.checkpoint_ns for group in pause.locator.groups)
    assert any(not group.checkpoint_ns for group in pause.locator.groups)
    decisions: list[dict[str, JsonValue]] = [
        {"item_id": item.item_id, "type": "submit", "value": {"otp": "1"}}
        if item.kind == "input"
        else {"item_id": item.item_id, "type": "approve"}
        for item in reversed(items)
    ]
    prepared = await _prepare(fx, decisions)
    mapping = _JSON.validate_python(prepared.command.resume)
    assert isinstance(mapping, dict) and len(mapping) == 3
    assert fx.effects == []
    lease = await fx.repository.get_fence(fx.run.run_id)
    assert lease is not None
    started = await fx.repository.start_resume(fx.run, lease, "decision", prepared.plan)
    assert isinstance(started, StartedResume)
    second_start = await fx.repository.start_resume(
        fx.run, lease, "decision", prepared.plan
    )
    assert isinstance(second_start, ReplayedResume)
    context = await fx.repository.read_resume_context(fx.run, "decision")
    assert context is not None and context.dispatch_plan == prepared.plan
    assert context.intent.status is IntentStatus.DISPATCH_STARTED
    # Execute the actual SDK graph, including its child checkpoint namespace.
    await _r70_invoke_native_batch(fx, prepared, lease, started.attempt_id)
    assert len(fx.effects) == 3
    assert sorted(canonical_interaction_bytes(value) for value in fx.effects) == sorted(
        canonical_interaction_bytes(value) for value in mapping.values()
    )
    observed = await fx.bridge.read_interaction(
        request=fx.run,
        lease=lease,
        handle=fx.handle,
        target=ResumeReadTarget(command_id="decision"),
    )
    assert isinstance(observed, ObservedNativeResume)
    evidence = observed.evidence
    assert isinstance(evidence, ConsumedPauseEvidence)
    assert (evidence.command_id, evidence.attempt_id, evidence.attempt_generation) == (
        "decision",
        started.attempt_id,
        lease.generation,
    )
    assert (
        evidence.pause_revision,
        evidence.pause_ref,
        evidence.collection_digest,
    ) == (
        waiting.state.pause_revision,
        fx.paused.pause.pause_ref,
        fx.paused.pause.digest,
    )
    assert evidence.disposition == "active" and evidence.next_pause is None
    assert evidence.observation_digests
    for observation in observed.observations:
        await fx.repository.record_checkpoint_observation(fx.run, lease, observation)
    reconciled = await fx.repository.reconcile_resume(fx.run, lease, evidence)
    assert reconciled.snapshot.state.phase is Phase.ACTIVE
    assert reconciled.snapshot.state.groups == ()
    replay = await fx.repository.accept_resume(fx.run, "decision", "test-consumer")
    assert isinstance(replay, ReplayedResume)
    assert replay.original_intent is not None
    assert replay.original_intent.status is IntentStatus.RECONCILED
    read = await fx.bridge.read_interaction(
        request=fx.run,
        lease=lease,
        handle=fx.handle,
        target=ResumeReadTarget(command_id="decision"),
    )
    assert isinstance(read, ReplayedResume)
    assert read.snapshot == replay.snapshot == reconciled.snapshot
    assert isinstance(
        await fx.repository.start_resume(fx.run, lease, "decision", prepared.plan),
        ReplayedResume,
    )
    assert await fx.repository.read_interaction(fx.run) == reconciled.snapshot
    assert len(fx.effects) == 3


async def _r70_invoke_native_batch(
    fx: _NativeFixture,
    prepared: PreparedNativeResume,
    lease: LeaseFence,
    attempt_id: str,
) -> None:
    graph = fx.handle.runnable
    assert isinstance(graph, _CompiledGraph)
    config: RunnableConfig = {
        "configurable": {"thread_id": RunScope.of(fx.run).scoped_thread_id},
        "metadata": {
            "kokoro_run_id": fx.run.run_id,
            "kokoro_generation": lease.generation,
            "kokoro_command_id": "decision",
            "kokoro_attempt_id": attempt_id,
        },
    }
    await graph.ainvoke(prepared.command, config)
