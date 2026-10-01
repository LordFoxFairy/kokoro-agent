"""Official checkpoint delegation and independently read HITL evidence.

SDK values are confined to this adapter. Received writes are never treated as
committed facts; only exact reads through the independent saver form evidence.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence, Mapping
from collections import deque
from dataclasses import dataclass
import hashlib
from typing import Any, Literal, Protocol
from uuid import uuid4

from langchain_core.runnables.config import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    ChannelVersions,
    DeltaChannelHistory,
)
from langgraph.types import Command
from pydantic import JsonValue, TypeAdapter

from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.domain.run.interactions import (
    CheckpointObservation,
    ConsumedPauseEvidence,
    DurablePauseSnapshot,
    InteractionConflict,
    InteractionCorrupt,
    IntentStatus,
    ObservationTarget,
    Quiescence,
    QuiescentProbe,
    ReplayedResume,
    ResumeDispatchPlan,
    ResumeReadContext,
    UnknownResumeEvidence,
)
from kokoro_agent.domain.run.repository import RunRepository, LeaseFence
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.hitl.request import HumanRequest
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.events import (
    InteractionGroup,
    InteractionItem,
    InteractionValidation,
)
from kokoro_agent.infrastructure.postgres_run_interactions import (
    canonical_interaction_bytes,
    decode_interaction_bytes,
    decode_pause_snapshot,
    StoredResumePlan,
    decode_checkpoint_observation,
)

_JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
_RESUME = "__resume__"
_INTERRUPT = "__interrupt__"
_ERROR = "__error__"


def _digest(value: object) -> str:
    return hashlib.sha256(
        canonical_interaction_bytes(_JSON.validate_python(value, strict=True))
    ).hexdigest()


def _locator(config: RunnableConfig) -> dict[str, str]:
    source = config.get("configurable", {})
    result = {
        key: source[key]
        for key in ("thread_id", "checkpoint_ns", "checkpoint_id")
        if isinstance(source.get(key), str)
    }
    if not result.get("thread_id") or not result.get("checkpoint_id"):
        raise InteractionCorrupt("incomplete native checkpoint locator")
    result.setdefault("checkpoint_ns", "")
    return result


def _config(locator: dict[str, str]) -> RunnableConfig:
    return {"configurable": dict(locator)}


def _parent(value: CheckpointTuple) -> str | None:
    return (
        _locator(value.parent_config)["checkpoint_id"] if value.parent_config else None
    )


def _vector(value: CheckpointTuple, task_id: str) -> list[JsonValue]:
    vectors = [
        data
        for task, channel, data in value.pending_writes or ()
        if task == task_id and channel == _RESUME
    ]
    if not vectors:
        return []
    if len(vectors) != 1 or not isinstance(vectors[0], list):
        raise InteractionCorrupt("ambiguous native resume vector")
    return TypeAdapter(list[JsonValue]).validate_python(vectors[0], strict=True)


@dataclass(frozen=True, slots=True, kw_only=True)
class PauseReadTarget:
    command_id: str | None
    attempt_id: str | None

    def __post_init__(self) -> None:
        if (self.command_id is None) != (self.attempt_id is None):
            raise ValueError("incomplete pause target")


@dataclass(frozen=True, slots=True, kw_only=True)
class ResumeReadTarget:
    command_id: str


@dataclass(frozen=True, slots=True, kw_only=True)
class NativePauseRead:
    pause: DurablePauseSnapshot
    observation: CheckpointObservation


@dataclass(frozen=True, slots=True, kw_only=True)
class PreparedNativeResume:
    plan: ResumeDispatchPlan
    command: Command[Any]


@dataclass(frozen=True, slots=True, kw_only=True)
class ObservedNativeResume:
    observations: tuple[CheckpointObservation, ...]
    evidence: ConsumedPauseEvidence | UnknownResumeEvidence


class InteractionReader(Protocol):
    async def __call__(
        self,
        *,
        request: RunRequest,
        lease: LeaseFence,
        handle: AgentHandle,
        target: PauseReadTarget | ResumeReadTarget,
    ) -> (
        NativePauseRead | PreparedNativeResume | ObservedNativeResume | ReplayedResume
    ): ...


class InteractionCheckpointer(BaseCheckpointSaver[str]):
    def __init__(
        self,
        *,
        saver: BaseCheckpointSaver[str],
        reader: BaseCheckpointSaver[str],
        repository: RunRepository,
    ) -> None:
        if saver is reader:
            raise ValueError("checkpoint evidence requires an independent reader")
        super().__init__(serde=saver.serde)
        self._saver = saver
        self._reader = reader
        self._repository = repository
        self._writes: dict[tuple[str, str, str], int] = {}
        self._write_stages: deque[tuple[str, tuple[str, str, str]]] = deque(maxlen=64)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await self._saver.aget_tuple(config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        async for item in self._saver.alist(
            config, filter=filter, before=before, limit=limit
        ):
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await self._saver.aput(config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        # Only scalar location/stage diagnostics. Never copy SDK objects or keep
        # a mutable received vector and later call it committed evidence.
        locator = _locator(config)
        key = (locator["thread_id"], locator["checkpoint_ns"], locator["checkpoint_id"])
        self._writes[key] = self._writes.get(key, 0) + 1
        self._write_stages.append(("received", key))
        try:
            await self._saver.aput_writes(config, writes, task_id, task_path)
        except BaseException:
            self._write_stages.append(("exception", key))
            raise
        else:
            self._write_stages.append(("delegated", key))
        finally:
            remaining = self._writes[key] - 1
            if remaining:
                self._writes[key] = remaining
            else:
                del self._writes[key]

    def get_next_version(self, current: str | None, channel: None) -> str:
        return self._saver.get_next_version(current, channel)

    async def adelete_thread(self, thread_id: str) -> None:
        await self._saver.adelete_thread(thread_id)

    async def aget_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        return await self._saver.aget_delta_channel_history(
            config=config, channels=channels
        )

    async def _exact(
        self, locator: dict[str, str], request: RunRequest
    ) -> CheckpointTuple:
        if locator["thread_id"] != RunScope.of(request).scoped_thread_id:
            raise InteractionCorrupt("native thread differs from Run")
        value = await self._reader.aget_tuple(_config(locator))
        if (
            value is None
            or _locator(value.config) != locator
            or value.metadata.get("kokoro_run_id") != request.run_id
        ):
            raise InteractionCorrupt("unattributed native checkpoint")
        return value

    async def _leaves(
        self, handle: AgentHandle, request: RunRequest
    ) -> list[tuple[dict[str, str], str, Any]]:
        # SDK StateSnapshot/PregelTask are validated here, never in domain.
        root: Any = await handle.runnable.aget_state(
            {"configurable": {"thread_id": RunScope.of(request).scoped_thread_id}},
            subgraphs=True,
        )
        result: list[tuple[dict[str, str], str, Any]] = []

        def visit(state: Any) -> None:
            if not state.tasks:
                return
            locator = _locator(state.config)
            for task in state.tasks:
                if task.state is not None and hasattr(task.state, "tasks"):
                    visit(task.state)
                else:
                    for interrupt in task.interrupts:
                        if (
                            not isinstance(task.id, str)
                            or not task.id
                            or not isinstance(interrupt.id, str)
                            or not interrupt.id
                        ):
                            raise InteractionCorrupt(
                                "invalid native interrupt identity"
                            )
                        result.append((locator, task.id, interrupt))

        visit(root)
        result.sort(key=lambda row: (row[0]["checkpoint_ns"], row[1], row[2].id))
        if len({i.id for _, _, i in result}) != len(result):
            raise InteractionCorrupt("duplicate native interrupt identity")
        return result

    def _items(
        self, interrupt: Any, group_id: str, handle: AgentHandle
    ) -> list[InteractionItem]:
        human = HumanRequest.from_interrupt_value(interrupt.value)
        items: list[InteractionItem] = []
        if human is not None:
            context = human.context
            name = context.get("name")
            if not isinstance(name, str) or not name:
                raise InteractionCorrupt("human pause lacks safe display name")
            allowed = {
                "input": ["submit", "reject"],
                "review": ["approve", "respond", "reject"],
                "question": ["respond", "reject"],
                "approval": ["approve", "edit", "reject"],
            }[human.kind]
            validation = (
                InteractionValidation.model_validate(context["validation_error"])
                if "validation_error" in context
                else None
            )
            # Raw arguments and results are never copied to public evidence.
            items.append(
                InteractionItem.model_validate(
                    {
                        "item_id": _digest([group_id, 0]),
                        "request_id": human.request_id,
                        "kind": {
                            "approval": "tool_approval",
                            "question": "ask_user_question",
                            "review": "result_review",
                            "input": "input",
                        }[human.kind],
                        "allowed_decisions": allowed,
                        "display": {
                            "name": name,
                            "description": handle.describe_tool(name) or "",
                            "editable": human.kind == "approval",
                            "input_schema": human.response_schema or {},
                        },
                        "validation": validation.model_dump(mode="json")
                        if validation
                        else None,
                    }
                )
            )
        else:
            raw = _JSON.validate_python(interrupt.value, strict=True)
            if (
                not isinstance(raw, dict)
                or set(raw) != {"action_requests", "review_configs"}
                or not isinstance(raw["action_requests"], list)
                or not isinstance(raw["review_configs"], list)
            ):
                raise InteractionCorrupt("unsupported native interrupt payload")
            configs = {
                c["action_name"]: c
                for c in raw["review_configs"]
                if isinstance(c, dict) and isinstance(c.get("action_name"), str)
            }
            if len(configs) != len(raw["review_configs"]):
                raise InteractionCorrupt("duplicate native review configuration")
            for index, action in enumerate(raw["action_requests"]):
                if (
                    not isinstance(action, dict)
                    or not isinstance(action.get("name"), str)
                    or action["name"] not in configs
                ):
                    raise InteractionCorrupt("missing native approval configuration")
                name = action["name"]
                if not isinstance(name, str):
                    raise InteractionCorrupt("invalid native action name")
                item_id = _digest([group_id, index])
                items.append(
                    InteractionItem.model_validate(
                        {
                            "item_id": item_id,
                            "request_id": item_id,
                            "kind": "tool_approval",
                            "allowed_decisions": configs[name]["allowed_decisions"],
                            "display": {
                                "name": name,
                                "description": handle.describe_tool(name) or "",
                                "editable": True,
                                "input_schema": {},
                            },
                        }
                    )
                )
        return items

    async def _materialize(
        self,
        request: RunRequest,
        lease: LeaseFence,
        handle: AgentHandle,
        target: PauseReadTarget,
    ) -> tuple[DurablePauseSnapshot, list[dict[str, Any]]]:
        leaves = await self._leaves(handle, request)
        if not leaves:
            raise InteractionConflict("native_not_paused")
        groups: list[dict[str, Any]] = []
        locators: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []
        for locator, task_id, interrupt in leaves:
            exact = await self._exact(locator, request)
            if (
                target.command_id is None
                and exact.metadata.get("kokoro_generation") != lease.generation
            ):
                raise InteractionConflict("native_pause_generation_unattributed")
            writes = [
                data
                for task, channel, data in exact.pending_writes or ()
                if task == task_id and channel == _INTERRUPT
            ]
            durable = [item for batch in writes for item in batch]
            if (
                len(durable) != 1
                or durable[0].id != interrupt.id
                or durable[0].value != interrupt.value
            ):
                raise InteractionCorrupt("native pause is not independently committed")
            vector = _vector(exact, task_id)
            group_id = _digest([locator, task_id, interrupt.id])
            items = self._items(interrupt, group_id, handle)
            group = InteractionGroup(group_id=group_id, items=items)
            groups.append(group.model_dump(mode="json", exclude_unset=True))
            task = {
                "task_id": task_id,
                "interrupt_id": interrupt.id,
                "item_ids": [item.item_id for item in items],
            }
            locators.append({"group_id": group_id, **locator, "tasks": [task]})
            facts.append(
                {
                    "group_id": group_id,
                    **locator,
                    "parent_checkpoint_id": _parent(exact),
                    "tasks": [
                        {
                            **task,
                            "resume_length": len(vector),
                            "resume_digest": _digest(vector),
                            "error": any(
                                t == task_id and c == _ERROR
                                for t, c, _ in exact.pending_writes or ()
                            ),
                            "interrupt_digest": _digest(
                                [
                                    item.model_dump(mode="json", exclude_unset=True)
                                    for item in items
                                ]
                            ),
                        }
                    ],
                    "successors": [],
                }
            )
        raw = canonical_interaction_bytes(
            {
                "format": "kokoro-agent:pause-collection:1",
                "groups": groups,
                "locator": {"groups": locators},
            }
        )
        digest = hashlib.sha256(raw).hexdigest()
        # Same checkpoint/interrupt can validate repeatedly: committed vector and
        # trusted attempt identify the new occurrence, not checkpoint ID alone.
        ref = _digest([digest, facts, target.command_id, target.attempt_id])
        pause = DurablePauseSnapshot(pause_ref=ref, canonical_bytes=raw, digest=digest)
        decode_pause_snapshot(pause)
        return pause, facts

    def _make_observation(
        self,
        request: RunRequest,
        lease: LeaseFence,
        target: ObservationTarget,
        kind: Literal["pause", "read", "resume"],
        facts: list[dict[str, Any]],
        **extra: object,
    ) -> CheckpointObservation:
        raw = canonical_interaction_bytes(
            {
                "format": "kokoro-agent:checkpoint-observation:1",
                "run_id": request.run_id,
                "generation": lease.generation,
                "command_id": target.command_id,
                "attempt_id": target.attempt_id,
                "kind": kind,
                "probe_read_id": None,
                "facts": facts,
                **extra,
            }
        )
        result = CheckpointObservation(
            target=target,
            kind=kind,
            canonical_bytes=raw,
            digest=hashlib.sha256(raw).hexdigest(),
        )
        decode_checkpoint_observation(result, request)
        return result

    async def _prepare(
        self, request: RunRequest, handle: AgentHandle, context: ResumeReadContext
    ) -> PreparedNativeResume:
        original = decode_pause_snapshot(context.original_pause)
        leaves = await self._leaves(handle, request)
        by_id = {i.id: (locator, task, i) for locator, task, i in leaves}
        expected = {t.interrupt_id for g in original.locator.groups for t in g.tasks}
        if set(by_id) != expected:
            raise InteractionConflict("native_pause_changed")
        mapping: dict[str, JsonValue] = {}
        plans: list[dict[str, Any]] = []
        for group, decided in zip(
            original.locator.groups, context.intent.groups, strict=True
        ):
            planned: list[dict[str, Any]] = []
            decisions = {d.item_id: d for d in decided.decisions}
            for task in group.tasks:
                locator, task_id, interrupt = by_id[task.interrupt_id]
                if (
                    locator
                    != {
                        "thread_id": group.thread_id,
                        "checkpoint_ns": group.checkpoint_ns,
                        "checkpoint_id": group.checkpoint_id,
                    }
                    or task_id != task.task_id
                ):
                    raise InteractionConflict("native_locator_changed")
                exact = await self._exact(locator, request)
                pre = _vector(exact, task_id)
                human = HumanRequest.from_interrupt_value(interrupt.value)
                native: list[JsonValue] = []
                public = {
                    item.item_id: item for g in original.groups for item in g.items
                }
                for item_id in task.item_ids:
                    decision = decisions[item_id]
                    payload = decode_interaction_bytes(decision.payload)
                    if human is not None:
                        native.append(
                            {
                                "request_id": public[item_id].request_id,
                                "type": decision.kind,
                                **payload,
                            }
                        )
                    elif decision.kind == "edit" or (
                        decision.kind == "approve" and "args" in payload
                    ):
                        native.append(
                            {
                                "type": "edit",
                                "edited_action": {
                                    "name": public[item_id].display.name,
                                    "args": payload["args"],
                                },
                            }
                        )
                    elif decision.kind == "reject":
                        native.append(
                            {"type": "reject", "message": payload.get("reason", "")}
                        )
                    else:
                        native.append({"type": decision.kind})
                value: JsonValue = (
                    native if human is not None else {"decisions": native}
                )
                mapping[task.interrupt_id] = value
                planned.append(
                    {
                        "task_id": task_id,
                        "interrupt_id": task.interrupt_id,
                        "pre_resume_length": len(pre),
                        "pre_resume_digest": _digest(pre),
                        "expected_append_digest": _digest([*pre, value]),
                    }
                )
            plans.append({"group_id": group.group_id, "tasks": planned})
        attempt = uuid4().hex
        raw = canonical_interaction_bytes(
            {
                "format": "kokoro-agent:resume-dispatch:1",
                "attempt_id": attempt,
                "groups": plans,
            }
        )
        StoredResumePlan.model_validate(decode_interaction_bytes(raw))
        return PreparedNativeResume(
            plan=ResumeDispatchPlan(attempt_id=attempt, canonical_bytes=raw),
            command=Command(resume=mapping),
        )

    async def _observe(
        self,
        request: RunRequest,
        lease: LeaseFence,
        handle: AgentHandle,
        context: ResumeReadContext,
    ) -> ObservedNativeResume:
        if (
            context.dispatch_plan is None
            or context.intent.attempt_id is None
            or context.attempt_generation != lease.generation
        ):
            raise InteractionConflict("missing original native attempt authority")
        original = decode_pause_snapshot(context.original_pause)
        plan = StoredResumePlan.model_validate(
            decode_interaction_bytes(context.dispatch_plan.canonical_bytes)
        )
        leaves = await self._leaves(handle, request)
        next_pause = None
        if leaves:
            next_pause, _ = await self._materialize(
                request,
                lease,
                handle,
                PauseReadTarget(
                    command_id=context.intent.command_id,
                    attempt_id=context.intent.attempt_id,
                ),
            )
        facts: list[dict[str, Any]] = []
        complete = True
        for group, planned in zip(original.locator.groups, plan.groups, strict=True):
            locator = {
                "thread_id": group.thread_id,
                "checkpoint_ns": group.checkpoint_ns,
                "checkpoint_id": group.checkpoint_id,
            }
            exact = await self._exact(locator, request)
            tasks: list[dict[str, Any]] = []
            for task, expected in zip(group.tasks, planned.tasks, strict=True):
                vector = _vector(exact, task.task_id)
                error = any(
                    t == task.task_id and channel == _ERROR
                    for t, channel, _ in exact.pending_writes or ()
                )
                matching = [
                    i
                    for loc, t, i in leaves
                    if loc["checkpoint_ns"] == group.checkpoint_ns and t == task.task_id
                ]
                tasks.append(
                    {
                        "task_id": task.task_id,
                        "interrupt_id": task.interrupt_id,
                        "item_ids": task.item_ids,
                        "resume_length": len(vector),
                        "resume_digest": _digest(vector),
                        "error": error,
                        "interrupt_digest": _digest(
                            [
                                self._items(i, group.group_id, handle)[0].model_dump(
                                    mode="json", exclude_unset=True
                                )
                                for i in matching
                            ]
                        ),
                    }
                )
                complete &= (
                    not error
                    and len(vector) == expected.pre_resume_length + 1
                    and _digest(vector[: expected.pre_resume_length])
                    == expected.pre_resume_digest
                    and _digest(vector) == expected.expected_append_digest
                )
            candidates: list[CheckpointTuple] = []
            async for candidate in self._reader.alist(
                {
                    "configurable": {
                        "thread_id": group.thread_id,
                        "checkpoint_ns": group.checkpoint_ns,
                    }
                },
                limit=257,
            ):
                candidates.append(candidate)
            if len(candidates) > 256:
                complete = False
            children: dict[str, list[CheckpointTuple]] = {}
            for candidate in candidates:
                parent = _parent(candidate)
                if parent is not None:
                    children.setdefault(parent, []).append(candidate)
            successors: list[dict[str, Any]] = []
            cursor = group.checkpoint_id
            seen = {cursor}
            while cursor in children:
                branches = children[cursor]
                if len(branches) != 1:
                    complete = False
                    break
                candidate = branches[0]
                loc = _locator(candidate.config)
                metadata = candidate.metadata
                if (
                    metadata.get("kokoro_run_id"),
                    metadata.get("kokoro_generation"),
                    metadata.get("kokoro_command_id"),
                    metadata.get("kokoro_attempt_id"),
                ) != (
                    request.run_id,
                    lease.generation,
                    context.intent.command_id,
                    context.intent.attempt_id,
                ):
                    complete = False
                    break
                state: Any = await handle.runnable.aget_state(
                    _config(loc), subgraphs=True
                )
                if any(
                    channel == _ERROR
                    for _, channel, _ in candidate.pending_writes or ()
                ) or any(task.error is not None for task in state.tasks):
                    complete = False
                successors.append(
                    {
                        "checkpoint_id": loc["checkpoint_id"],
                        "parent_checkpoint_id": cursor,
                        "current_task_ids": [t.id for t in state.tasks],
                        "current_interrupt_ids": [i.id for i in state.interrupts],
                    }
                )
                cursor = loc["checkpoint_id"]
                if cursor in seen:
                    raise InteractionCorrupt("cyclic native checkpoint ancestry")
                seen.add(cursor)
            if next_pause is None and (
                not successors
                or successors[-1]["current_task_ids"]
                or successors[-1]["current_interrupt_ids"]
            ):
                complete = False
            facts.append(
                {
                    "group_id": group.group_id,
                    **locator,
                    "parent_checkpoint_id": _parent(exact),
                    "tasks": tasks,
                    "successors": successors,
                }
            )
        target = ObservationTarget(
            generation=lease.generation,
            command_id=context.intent.command_id,
            attempt_id=context.intent.attempt_id,
        )
        if complete:
            observed = self._make_observation(
                request,
                lease,
                target,
                "resume",
                facts,
                pause_revision=context.intent.pause_revision,
                pause_ref=context.intent.pause_ref,
                collection_digest=context.original_pause.digest,
                next_pause=decode_interaction_bytes(next_pause.canonical_bytes)
                if next_pause
                else None,
                next_pause_ref=next_pause.pause_ref if next_pause else None,
            )
            evidence = ConsumedPauseEvidence(
                command_id=context.intent.command_id,
                attempt_id=context.intent.attempt_id,
                attempt_generation=lease.generation,
                pause_revision=context.intent.pause_revision,
                pause_ref=context.intent.pause_ref,
                collection_digest=context.original_pause.digest,
                observation_digests=(observed.digest,),
                disposition="waiting" if next_pause else "active",
                next_pause=next_pause,
            )
        else:
            observed = self._make_observation(request, lease, target, "read", facts)
            evidence = UnknownResumeEvidence(
                command_id=context.intent.command_id,
                attempt_id=context.intent.attempt_id,
                attempt_generation=lease.generation,
                pause_revision=context.intent.pause_revision,
                pause_ref=context.intent.pause_ref,
                collection_digest=context.original_pause.digest,
                observation_digests=(observed.digest,),
            )
        return ObservedNativeResume(observations=(observed,), evidence=evidence)

    async def read_interaction(
        self,
        *,
        request: RunRequest,
        lease: LeaseFence,
        handle: AgentHandle,
        target: PauseReadTarget | ResumeReadTarget,
    ) -> NativePauseRead | PreparedNativeResume | ObservedNativeResume | ReplayedResume:
        if any(
            thread == RunScope.of(request).scoped_thread_id
            for thread, _, _ in self._writes
        ):
            raise InteractionConflict("native_writes_inflight")
        if isinstance(target, PauseReadTarget):
            pause, facts = await self._materialize(request, lease, handle, target)
            observation = self._make_observation(
                request,
                lease,
                ObservationTarget(
                    generation=lease.generation,
                    command_id=target.command_id,
                    attempt_id=target.attempt_id,
                ),
                "pause" if target.command_id is None else "read",
                facts,
                **(
                    {
                        "pause": decode_interaction_bytes(pause.canonical_bytes),
                        "pause_ref": pause.pause_ref,
                    }
                    if target.command_id is None
                    else {}
                ),
            )
            return NativePauseRead(pause=pause, observation=observation)
        context = await self._repository.read_resume_context(request, target.command_id)
        if context is None:
            raise InteractionConflict("resume_not_accepted")
        if (
            context.intent.status in (IntentStatus.RECONCILED, IntentStatus.TERMINAL)
            or context.snapshot.state.phase.value == "terminal"
        ):
            return ReplayedResume(
                snapshot=context.snapshot,
                original_intent=context.intent,
                accepted_source_index=None,
            )
        if context.intent.status is IntentStatus.ACCEPTED:
            return await self._prepare(request, handle, context)
        return await self._observe(request, lease, handle, context)


def make_interaction_checkpointer(
    *,
    saver: BaseCheckpointSaver[str],
    reader: BaseCheckpointSaver[str],
    repository: RunRepository,
) -> InteractionCheckpointer:
    return InteractionCheckpointer(saver=saver, reader=reader, repository=repository)


def make_quiescent_probe(
    observed: ObservedNativeResume,
    *,
    request: RunRequest,
    worker_boot_id: str,
    invocation_id: str,
) -> QuiescentProbe:
    """Attach local-drained authority to a fresh independent read, not a new classification."""
    if (
        not isinstance(observed.evidence, UnknownResumeEvidence)
        or len(observed.observations) != 1
    ):
        raise InteractionConflict("probe_requires_unknown_read")
    observation = observed.observations[0]
    value = decode_checkpoint_observation(observation, request)
    if value.kind != "read":
        raise InteractionConflict("probe_requires_read")
    read_id = uuid4().hex
    progress = _digest([group.model_dump(mode="json") for group in value.facts])
    quiescence = Quiescence(worker_boot_id=worker_boot_id, invocation_id=invocation_id)
    raw = decode_interaction_bytes(observation.canonical_bytes)
    raw.update(
        kind="probe",
        probe_read_id=read_id,
        progress_digest=progress,
        quiescence={
            "kind": "local_drained",
            "worker_boot_id": worker_boot_id,
            "invocation_id": invocation_id,
        },
    )
    encoded = canonical_interaction_bytes(raw)
    return QuiescentProbe(
        read_id=read_id,
        observation=CheckpointObservation(
            target=observation.target,
            kind="probe",
            canonical_bytes=encoded,
            digest=hashlib.sha256(encoded).hexdigest(),
        ),
        progress_digest=progress,
        quiescence=quiescence,
    )
