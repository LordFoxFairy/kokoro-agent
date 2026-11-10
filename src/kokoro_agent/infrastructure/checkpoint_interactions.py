"""Official checkpoint delegation and independently read HITL evidence.

SDK values are confined to this adapter. Received writes are never treated as
committed facts; only exact reads through the independent saver form evidence.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence, Mapping
from collections import deque
from dataclasses import dataclass
import hashlib
import logging
from typing import Any, Literal, Protocol, TypeGuard
from uuid import uuid4

from langchain_core.messages import HumanMessage
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
from kokoro_agent.features.catalog import get_feature
from kokoro_agent.hitl.request import HumanRequest
from kokoro_agent.infrastructure.checkpoints import CheckpointSettings
from kokoro_agent.infrastructure.checkpoint_workspace_outputs import (
    CheckpointWorkspaceOutputs,
)
from kokoro_agent.clients.storage import WorkspaceOutputUploadClient
from kokoro_agent.domain.run.workspace_output import OUTPUT_METADATA, OUTPUT_ROOT
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all
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
    SwarmRootHandoff,
    decode_checkpoint_observation,
)

_JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
_RESUME = "__resume__"
_INTERRUPT = "__interrupt__"
_ERROR = "__error__"
_STEER_NODE = "SteeringMiddleware.before_model"
_STEER_TASK_PATH = f"~__pregel_pull, {_STEER_NODE}"
_LOGGER = logging.getLogger(__name__)


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


def _is_message_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _steer_messages(value: object) -> list[tuple[str, str]]:
    items = value if _is_message_list(value) else [value]
    exact: list[tuple[str, str]] = []
    for item in items:
        if isinstance(item, HumanMessage) and isinstance(item.id, str):
            # SDK message blocks have a dynamically typed nested shape.
            content: object = getattr(item, "content", None)
            if isinstance(content, str):
                exact.append((item.id, content))
    return exact


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
        settings: CheckpointSettings | None = None,
        workspace_storage: WorkspaceOutputUploadClient | None = None,
    ) -> None:
        if saver is reader:
            raise ValueError("checkpoint evidence requires an independent reader")
        super().__init__(serde=saver.serde)
        self._saver = saver
        self._reader = reader
        self._repository = repository
        self._settings = settings
        self._workspace_outputs = (
            CheckpointWorkspaceOutputs(settings, repository, reader, workspace_storage)
            if settings is not None
            else None
        )
        self._writes: dict[tuple[str, str, str], int] = {}
        self._write_stages: deque[tuple[str, tuple[str, str, str]]] = deque(maxlen=64)
        # Candidate locators only; the independent committed read is authority.
        self._steer_writes: deque[tuple[dict[str, str], str]] = deque(maxlen=64)

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
        saved = await self._saver.aput(config, checkpoint, metadata, new_versions)
        if (
            "messages" in (checkpoint.get("updated_channels") or ())
            and self._steer_writes
        ):
            try:
                if config.get("configurable", {}).get("checkpoint_id"):
                    parent = _locator(config)
                    tasks = {
                        task_id
                        for candidate, task_id in self._steer_writes
                        if candidate == parent
                    }
                    if tasks:
                        await self._confirm_steer_checkpoint(saved, parent, tasks)
            except Exception:
                # Evidence is supplemental. No successful receipt or Run
                # failure is inferred if the independent read/write fails.
                _LOGGER.exception("steer checkpoint confirmation deferred")
        if (
            self._workspace_outputs is not None
            and "files" in (checkpoint.get("updated_channels") or ())
            and config.get("metadata", {}).get(OUTPUT_METADATA) == OUTPUT_ROOT
        ):
            try:
                await self._workspace_outputs.capture(config, saved)
            except Exception as error:
                # The committed checkpoint remains authoritative; the same
                # Run's settling/recovery path re-reads it without redoing the tool.
                _LOGGER.warning(
                    "workspace output capture deferred",
                    extra={"error_type": type(error).__name__},
                )
        return saved

    async def recover_workspace_outputs(
        self, request: RunRequest, lease: LeaseFence
    ) -> None:
        if self._workspace_outputs is not None:
            await self._workspace_outputs.recover(request, lease)

    async def _confirm_steer_checkpoint(
        self,
        saved: RunnableConfig,
        parent_locator: dict[str, str],
        task_ids: set[str],
        *,
        recovery: tuple[RunRequest, LeaseFence] | None = None,
    ) -> None:
        locator = _locator(saved)
        exact = await self._reader.aget_tuple(_config(locator))
        if (
            exact is None
            or _locator(exact.config) != locator
            or exact.parent_config is None
            or _locator(exact.parent_config) != parent_locator
        ):
            return
        # The saver merges trusted runnable metadata at serialization time;
        # the graph-supplied write argument alone does not carry Run identity.
        run_id = exact.metadata.get("kokoro_run_id")
        generation = exact.metadata.get("kokoro_generation")
        if not isinstance(run_id, str) or type(generation) is not int:
            return
        parent = await self._reader.aget_tuple(_config(parent_locator))
        if (
            parent is None
            or _locator(parent.config) != parent_locator
            or parent.metadata.get("kokoro_run_id") != run_id
            or parent.metadata.get("kokoro_generation") != generation
            or exact.checkpoint["channel_versions"].get("messages")
            == parent.checkpoint["channel_versions"].get("messages")
        ):
            return
        # LangGraph saves DeltaChannel writes against the parent before
        # committing its successor. Only that exact successor proves the
        # steering node's HumanMessages entered the committed messages channel.
        messages = [
            message
            for task_id, channel, value in parent.pending_writes or ()
            if task_id in task_ids and channel == "messages"
            for message in _steer_messages(value)
        ]
        if not messages:
            return
        if recovery is None:
            request = await self._repository.get_request(run_id)
            lease = await self._repository.get_fence(run_id)
            if lease is None or lease.generation != generation:
                return
        else:
            request, lease = recovery
            # A recovered owner can attest a previously committed checkpoint of
            # this Run, but never a future generation or a different Run.
            if request.run_id != run_id or not 0 < generation <= lease.generation:
                return
        if (
            request is not None
            and RunScope.of(request).scoped_thread_id == locator["thread_id"]
            and await self._repository.confirm_steers(run_id, lease, messages)
        ):
            for candidate in tuple(self._steer_writes):
                if candidate[0] == parent_locator:
                    self._steer_writes.remove(candidate)

    async def recover_steers(self, request: RunRequest, lease: LeaseFence) -> None:
        """Recover a missed receipt from exact committed SDK rows, not process memory."""
        settings = self._settings
        if settings is None:
            return
        try:
            async with asyncio.timeout(2):
                if not await self._repository.peek_steers(request.run_id):
                    return
                thread = RunScope.of(request).scoped_thread_id
                checkpoints = qualified(settings.schema_name, "checkpoints")
                writes = qualified(settings.schema_name, "checkpoint_writes")
                async with (
                    connect_pg(settings.database_url) as conn,
                    conn.cursor() as cur,
                ):
                    # Start from each namespace's latest checkpoint for this
                    # Run and follow its committed ancestry only. The thread
                    # prefix is indexed; no global scan or per-model probe.
                    await execute_sql(
                        cur,
                        f"""WITH RECURSIVE head AS (
                            SELECT DISTINCT ON (checkpoint_ns)
                                   checkpoint_ns,checkpoint_id,parent_checkpoint_id
                            FROM {checkpoints}
                            WHERE thread_id=%s AND metadata->>'kokoro_run_id'=%s
                            ORDER BY checkpoint_ns,checkpoint_id DESC
                        ), lineage AS (
                            SELECT head.*, 0 AS depth FROM head
                            UNION ALL
                            SELECT p.checkpoint_ns,p.checkpoint_id,p.parent_checkpoint_id,l.depth+1
                            FROM lineage l JOIN {checkpoints} p
                              ON p.thread_id=%s AND p.checkpoint_ns=l.checkpoint_ns
                              AND p.checkpoint_id=l.parent_checkpoint_id
                            WHERE l.depth<255 AND p.metadata->>'kokoro_run_id'=%s
                        )
                        SELECT DISTINCT l.checkpoint_ns,l.checkpoint_id,l.parent_checkpoint_id,
                                        w.task_id,l.depth
                        FROM lineage l JOIN {writes} w
                          ON w.thread_id=%s AND w.checkpoint_ns=l.checkpoint_ns
                          AND w.checkpoint_id=l.parent_checkpoint_id
                        WHERE w.task_path=%s AND w.channel='messages'
                        ORDER BY l.depth,l.checkpoint_ns,l.checkpoint_id,w.task_id
                        LIMIT 64""",
                        (
                            thread,
                            request.run_id,
                            thread,
                            request.run_id,
                            thread,
                            _STEER_TASK_PATH,
                        ),
                    )
                    candidates = await fetch_all(cur)
                for row in candidates:
                    path = {
                        "thread_id": thread,
                        "checkpoint_ns": str(row["checkpoint_ns"]),
                    }
                    await self._confirm_steer_checkpoint(
                        _config({**path, "checkpoint_id": str(row["checkpoint_id"])}),
                        {**path, "checkpoint_id": str(row["parent_checkpoint_id"])},
                        {str(row["task_id"])},
                        recovery=(request, lease),
                    )
        except Exception:
            _LOGGER.exception(
                "steer checkpoint recovery deferred run_id=%s", request.run_id
            )

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
            if task_path == _STEER_TASK_PATH and any(
                channel == "messages" for channel, _ in writes
            ):
                self._steer_writes.append((locator, task_id))
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
                    elif decision.kind == "respond":
                        native.append(
                            {"type": "respond", "message": payload["response"]}
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

    async def _read_swarm_root_handoff(
        self,
        request: RunRequest,
        lease: LeaseFence,
        handle: AgentHandle,
        context: ResumeReadContext,
        group_id: str,
        leaf: CheckpointTuple,
        leaf_head_id: str,
    ) -> SwarmRootHandoff | None:
        """A retired direct peer is not the lifecycle authority of an official Swarm."""
        leaf_locator = _locator(leaf.config)
        namespace = leaf_locator["checkpoint_ns"]
        source, separator, source_task = namespace.partition(":")
        if not separator or not source_task or "|" in namespace or ":" in source_task:
            return None
        try:
            feature = get_feature(request.feature_key)
        except KeyError:
            return None
        if not any(peer == source for peer, _ in feature.handoffs):
            return None
        root_anchor_id = (leaf.metadata.get("parents") or {}).get("")
        if not isinstance(root_anchor_id, str) or not root_anchor_id:
            return None
        root = {"thread_id": leaf_locator["thread_id"], "checkpoint_ns": ""}
        anchor_locator = {**root, "checkpoint_id": root_anchor_id}
        anchor = await self._reader.aget_tuple(_config(anchor_locator))
        if (
            anchor is None
            or _locator(anchor.config) != anchor_locator
            or anchor.metadata.get("kokoro_run_id") != request.run_id
        ):
            return None
        anchor_state: Any = await handle.runnable.aget_state(_config(anchor_locator))
        if (
            anchor_state.values.get("active_agent") != source
            or not any(
                task.id == source_task and task.name == source
                for task in anchor_state.tasks
            )
            or any(task.error is not None for task in anchor_state.tasks)
            or any(channel == _ERROR for _, channel, _ in anchor.pending_writes or ())
        ):
            return None
        destinations = [
            value
            for task, channel, value in anchor.pending_writes or ()
            if task == source_task and channel == "active_agent"
        ]
        if len(destinations) != 1:
            return None
        destination = destinations[0]
        if (
            not isinstance(destination, str)
            or (source, destination) not in feature.handoffs
        ):
            return None
        head = await self._reader.aget_tuple(_config(root))
        if head is None:
            return None
        head_locator = _locator(head.config)
        if (
            head_locator["thread_id"] != root["thread_id"]
            or head_locator["checkpoint_ns"] != ""
        ):
            return None
        children: dict[str, list[CheckpointTuple]] = {}
        anchor_found = False
        async for candidate in self._reader.alist(_config(root), limit=65):
            locator = _locator(candidate.config)
            if locator == anchor_locator:
                anchor_found = True
            if (parent := _parent(candidate)) is not None:
                children.setdefault(parent, []).append(candidate)
        if not anchor_found:
            return None
        successors: list[str] = []
        quiet = False
        cursor, seen = root_anchor_id, {root_anchor_id}
        target = (
            request.run_id,
            lease.generation,
            context.intent.command_id,
            context.intent.attempt_id,
        )
        while cursor in children:
            branch = children[cursor]
            if len(branch) != 1 or len(successors) == 64:
                return None
            candidate = branch[0]
            locator = _locator(candidate.config)
            metadata = candidate.metadata
            if (
                locator["thread_id"] != root["thread_id"]
                or locator["checkpoint_ns"] != ""
                or locator["checkpoint_id"] in seen
                or (
                    metadata.get("kokoro_run_id"),
                    metadata.get("kokoro_generation"),
                    metadata.get("kokoro_command_id"),
                    metadata.get("kokoro_attempt_id"),
                )
                != target
                or any(
                    channel == _ERROR
                    for _, channel, _ in candidate.pending_writes or ()
                )
            ):
                return None
            state: Any = await handle.runnable.aget_state(_config(locator))
            if (
                state.values.get("active_agent") != destination
                or state.interrupts
                or any(task.error is not None for task in state.tasks)
            ):
                return None
            if not successors and (
                tuple(state.next) != (destination,)
                or len(state.tasks) != 1
                or state.tasks[0].name != destination
            ):
                return None
            quiet = not state.next and not state.tasks
            cursor = locator["checkpoint_id"]
            successors.append(cursor)
            seen.add(cursor)
        if cursor != head_locator["checkpoint_id"] or len(successors) < 2 or not quiet:
            return None
        return SwarmRootHandoff.model_validate(
            {
                "group_id": group_id,
                "leaf_checkpoint_id": leaf_head_id,
                "source_peer": source,
                "source_task_id": source_task,
                "destination_peer": destination,
                "root_anchor_id": root_anchor_id,
                "root_successor_ids": successors,
            }
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
        swarm_handoff: SwarmRootHandoff | None = None
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
                if (
                    complete
                    and len(original.locator.groups) == 1
                    and successors
                    and successors[-1]["current_task_ids"]
                    and not successors[-1]["current_interrupt_ids"]
                ):
                    swarm_handoff = await self._read_swarm_root_handoff(
                        request, lease, handle, context, group.group_id, exact, cursor
                    )
                complete = swarm_handoff is not None
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
                **(
                    {"swarm_root_handoff": swarm_handoff.model_dump(mode="json")}
                    if swarm_handoff
                    else {}
                ),
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
    settings: CheckpointSettings | None = None,
    workspace_storage: WorkspaceOutputUploadClient | None = None,
) -> InteractionCheckpointer:
    return InteractionCheckpointer(
        saver=saver,
        reader=reader,
        repository=repository,
        settings=settings,
        workspace_storage=workspace_storage,
    )


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
