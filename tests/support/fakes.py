"""测试共用的强类型 fake：总线、run 状态存储、v3 投影流与 agent。"""

from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from contextlib import contextmanager
from datetime import UTC, datetime
from support.chat import FakeChatRepository
from kokoro_agent.domain.run.interactions import (
    AcceptedResume,
    CheckpointObservation,
    ConsumedPauseEvidence,
    UnknownResumeEvidence,
    ObservationStored,
    ObservationTarget,
    QuiescentProbe,
    ReconcileProbeResult,
    ResumeReadContext,
    InteractionRecoveryTarget,
    InteractionState,
    InteractionConflict,
    InteractionAuthorityLost,
    InteractionRunMissing,
    PendingGroup,
    PendingItem,
    ValidationIssue,
    IntentStatus,
    Phase,
    DurablePauseSnapshot,
    InteractionCommitted,
    InteractionSnapshot,
    ReplayedResume,
    ResumeDispatchPlan,
    StartedResume,
)
from kokoro_agent.infrastructure.postgres_run_interactions import (
    canonical_interaction_bytes,
    decode_pause_snapshot,
    decode_resume_command,
    decode_checkpoint_observation,
    StoredResumePlan,
)
from kokoro_agent.domain.chat.projection import project_chat_fact
from kokoro_agent.domain.chat.models import chat_event_id
from kokoro_agent.protocol import RunStartedPayload
from kokoro_agent.domain.run.models import (
    StaticRecipeBinding,
    StaticRecipeIncompatible,
    StaticRecipeAuthorityLost,
    RunTerminalOutcome,
    TerminalAuthority,
    TerminalCommitResult,
    ExecutionTerminalAuthority,
    CancelTerminalAuthority,
    QuarantineTerminalAuthority,
)
from kokoro_agent.protocol import (
    RunCompletedPayload,
    RunFailedPayload,
    RunControlReceiptPayload,
    TokenUsage,
)
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
    Generator,
)
from dataclasses import dataclass, field, replace
from typing import Literal, TypeVar, TypeGuard, cast

from langchain_core.messages import AIMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Interrupt, Command
from pydantic import JsonValue

from kokoro_agent.protocol import (
    AgentEvent,
    ExecutionIdentity,
    IdentityRef,
    RunInput,
    RunRequest,
    RunResume,
    RunCancel,
    RunSteer,
    agent_event_adapter,
    run_events_stream,
)
from kokoro_agent.protocol.control import control_request_digest
from kokoro_agent.protocol import REQUESTS_STREAM, RUN_EVENTS_MAXLEN
from kokoro_agent.domain.run.models import RunUsageSegment
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.execution.events import RunEmitter, outbox_wire_event
from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.execution.protocols import AgentRunnable
from kokoro_agent.infrastructure.checkpoint_interactions import (
    PauseReadTarget,
    ResumeReadTarget,
    NativePauseRead,
    PreparedNativeResume,
    ObservedNativeResume,
)
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.domain.run.scope import runtime_namespace, RunScope
from kokoro_agent.domain.run.repository import (
    RunControlCommandRecord,
    ControlAdmission,
    ControlAdmissionStatus,
    ControlCommandConflict,
    ControlAdmissionReceipt,
    DispatchAdmission,
    DispatchConflict,
    LeaseFence,
    LeasedRun,
    OutboxFrame,
    ReceiptReconcile,
    SandboxBackendKind,
    SandboxCleanupIntent,
    StagedFrame,
    ToolJournalRecord,
    UsageIdentityConflict,
)
from kokoro_agent.streams.protocol import StreamItem

_T = TypeVar("_T")
_E = TypeVar("_E")


async def aiter_items(items: Sequence[_T]) -> AsyncIterator[_T]:
    for item in items:
        yield item


def _as_int(value: object) -> int:
    # 内存 fake 的 outbox/manifest 行值是 object：断言收窄为 int（脏形状 fail-loud）。
    assert isinstance(value, int)
    return value


def find_events(events: Sequence[AgentEvent], cls: type[_E]) -> list[_E]:
    return [event for event in events if isinstance(event, cls)]


def find_event(events: Sequence[AgentEvent], cls: type[_E]) -> _E:
    matched = find_events(events, cls)
    assert matched, f"no {cls.__name__} in {[e.kind for e in events]}"
    return matched[0]


class FakeBus:
    """内存 fake：publish 落地即可 read_all（供 RunEmitter.attach 续接），ack 记账。"""

    def __init__(
        self,
        inbound: Sequence[StreamItem] = (),
        control: Mapping[str, Sequence[StreamItem]] | None = None,
    ) -> None:
        self.published: list[tuple[str, dict[str, JsonValue], int]] = []
        self.acked: list[str] = []
        self.deleted: list[str] = []
        self.expired_streams: list[tuple[str, int]] = []
        self._inbound = tuple(inbound)
        # per-run control 流独立化：请求流投 _inbound，各 control 流投各自项。
        self._control = {
            stream: tuple(items) for stream, items in (control or {}).items()
        }

    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        self.published.append((stream, dict(event), maxlen))
        return StreamItem(cursor=str(len(self.published)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return [
            StreamItem(cursor=str(i), event=event)
            for i, (name, event, _maxlen) in enumerate(self.published)
            if name == stream
        ]

    async def subscribe(
        self, stream: str, *, group: str, consumer: str
    ) -> AsyncIterator[StreamItem]:
        items = (
            self._inbound
            if stream == REQUESTS_STREAM
            else self._control.get(stream, ())
        )
        for item in items:
            yield item

    async def ack(self, stream: str, group: str, cursor: str) -> None:
        self.acked.append(cursor)

    async def delete(self, stream: str) -> None:
        self.deleted.append(stream)

    async def expire(self, stream: str, ttl_s: int) -> None:
        self.expired_streams.append((stream, ttl_s))

    def run_events(self, run_id: str) -> list[AgentEvent]:
        return [
            agent_event_adapter.validate_python(event)
            for name, event, _maxlen in self.published
            if name == run_events_stream(run_id)
        ]

    def kinds(self, run_id: str) -> list[str]:
        return [event.kind for event in self.run_events(run_id)]


class FakeRunRepository:
    """协议等价的内存 store：租约以 leases dict 表达，None=暂停哨兵。"""

    async def read_interaction(self, request: RunRequest) -> InteractionSnapshot | None:
        if self.requests.get(request.run_id) != request:
            return None
        return self.interaction_snapshots.get(
            request.run_id,
            InteractionSnapshot(
                state=InteractionState(), pause=None, source_index=None
            ),
        )

    async def _interaction_authority(
        self, request: RunRequest, lease: LeaseFence
    ) -> None:
        if self.requests.get(request.run_id) != request:
            raise InteractionRunMissing()
        if not await self.is_lease_current(request.run_id, lease):
            raise InteractionAuthorityLost()

    def _save_interaction(
        self, run_id: str, state: InteractionState, pause: DurablePauseSnapshot | None
    ) -> InteractionSnapshot:
        snapshot = InteractionSnapshot(
            state=state, pause=pause, source_index=state.interaction_revision
        )
        self.interaction_snapshots[run_id] = snapshot
        return snapshot

    async def record_pause(
        self, request: RunRequest, lease: LeaseFence, pause: DurablePauseSnapshot
    ) -> InteractionCommitted | ReplayedResume:
        await self._interaction_authority(request, lease)
        parsed = decode_pause_snapshot(pause)
        current = await self.read_interaction(request)
        assert current is not None
        groups = tuple(
            PendingGroup(
                group_id=g.group_id,
                items=tuple(
                    PendingItem(
                        item_id=i.item_id,
                        request_id=i.request_id,
                        allowed_decisions=tuple(i.allowed_decisions),
                        validation=None
                        if i.validation is None
                        else ValidationIssue(
                            instance_path=tuple(i.validation.instance_path)
                        ),
                    )
                    for i in g.items
                ),
            )
            for g in parsed.groups
        )
        state = current.state.pause(pause_ref=pause.pause_ref, groups=groups)
        snapshot = self._save_interaction(request.run_id, state, pause)
        self.leases[request.run_id] = None
        self.paused_runs.append(request.run_id)
        return InteractionCommitted(snapshot=snapshot)

    async def accept_resume(
        self, request: RunRequest, command_id: str, owner: str
    ) -> AcceptedResume | ReplayedResume:
        current = await self.read_interaction(request)
        if current is None:
            raise InteractionRunMissing()
        entry = self.control_commands.get((request.run_id, command_id))
        if entry is None:
            raise InteractionConflict("command_missing")
        body, digest = entry["body"], entry["request_digest"]
        assert isinstance(body, str) and isinstance(digest, str)
        submission = decode_resume_command(
            body,
            digest,
            run_id=request.run_id,
            session_id=request.session_id,
            command_id=command_id,
        )
        context = self.resume_contexts.get((request.run_id, command_id))
        if context is not None:
            context.intent.verify_replay(submission)
            return ReplayedResume(
                snapshot=current,
                original_intent=context.intent,
                accepted_source_index=context.snapshot.source_index,
            )
        state = current.state.accept(submission)
        assert current.pause is not None and state.intent is not None
        if request.run_id in self.terminals:
            raise InteractionConflict("terminal")
        self.generations[request.run_id] += 1
        self.owners[request.run_id] = owner
        self.leases[request.run_id] = self._active_expiry()
        lease = self.current_lease(request.run_id)
        assert lease is not None
        snapshot = self._save_interaction(request.run_id, state, current.pause)
        self.resume_contexts[(request.run_id, command_id)] = ResumeReadContext(
            snapshot=snapshot,
            original_pause=current.pause,
            intent=state.intent,
            dispatch_plan=None,
            attempt_generation=None,
            observation_digest=None,
        )
        return AcceptedResume(snapshot=snapshot, lease=lease)

    async def start_resume(
        self,
        request: RunRequest,
        lease: LeaseFence,
        command_id: str,
        plan: ResumeDispatchPlan,
    ) -> StartedResume | ReplayedResume:
        await self._interaction_authority(request, lease)
        context = await self.read_resume_context(request, command_id)
        if context is None:
            raise InteractionConflict("command_missing")
        if not context.intent.can_dispatch:
            return ReplayedResume(
                snapshot=context.snapshot,
                original_intent=context.intent,
                accepted_source_index=context.snapshot.source_index,
            )
        stored_plan = StoredResumePlan.model_validate_json(plan.canonical_bytes)
        assert stored_plan.attempt_id == plan.attempt_id
        state = context.snapshot.state.start(
            command_id=command_id, attempt_id=plan.attempt_id
        )
        assert state.intent is not None
        snapshot = self._save_interaction(request.run_id, state, context.original_pause)
        self.resume_contexts[(request.run_id, command_id)] = replace(
            context,
            snapshot=snapshot,
            intent=state.intent,
            dispatch_plan=plan,
            attempt_generation=lease.generation,
        )
        return StartedResume(snapshot=snapshot, attempt_id=plan.attempt_id)

    async def mark_resume_unknown(
        self, request: RunRequest, lease: LeaseFence, command_id: str, attempt_id: str
    ) -> InteractionCommitted | ReplayedResume:
        await self._interaction_authority(request, lease)
        context = await self.read_resume_context(request, command_id)
        if context is None or context.intent.attempt_id != attempt_id:
            raise InteractionConflict("attempt_conflict")
        state = context.snapshot.state.unknown(command_id=command_id)
        assert state.intent is not None
        snapshot = self._save_interaction(request.run_id, state, context.original_pause)
        self.resume_contexts[(request.run_id, command_id)] = replace(
            context, snapshot=snapshot, intent=state.intent
        )
        return InteractionCommitted(snapshot=snapshot)

    async def read_resume_context(
        self, request: RunRequest, command_id: str
    ) -> ResumeReadContext | None:
        current = await self.read_interaction(request)
        context = self.resume_contexts.get((request.run_id, command_id))
        return (
            None
            if current is None or context is None
            else replace(context, snapshot=current)
        )

    async def record_checkpoint_observation(
        self, request: RunRequest, lease: LeaseFence, observation: CheckpointObservation
    ) -> ObservationStored:
        if self.requests.get(request.run_id) != request:
            raise InteractionRunMissing()
        decode_checkpoint_observation(observation, request)
        key = (request.run_id, observation.digest)
        existing = self.checkpoint_observations.get(key)
        if existing is not None and existing != observation:
            raise InteractionConflict("observation_conflict")
        current = (
            await self.is_lease_current(request.run_id, lease)
            and observation.target.generation == lease.generation
        )
        self.checkpoint_observations[key] = observation
        return ObservationStored(
            digest=observation.digest,
            disposition="current" if current else "audit",
            inserted=existing is None,
        )

    async def read_checkpoint_observations(
        self, request: RunRequest, target: ObservationTarget
    ) -> tuple[CheckpointObservation, ...]:
        if self.requests.get(request.run_id) != request:
            return ()
        return tuple(
            value
            for (run_id, _), value in self.checkpoint_observations.items()
            if run_id == request.run_id and value.target == target
        )

    async def reconcile_resume(
        self,
        request: RunRequest,
        lease: LeaseFence,
        evidence: ConsumedPauseEvidence | UnknownResumeEvidence,
    ) -> InteractionCommitted | ReplayedResume:
        await self._interaction_authority(request, lease)
        context = await self.read_resume_context(request, evidence.command_id)
        if context is None or (
            context.intent.attempt_id,
            context.attempt_generation,
            context.original_pause.digest,
        ) != (
            evidence.attempt_id,
            evidence.attempt_generation,
            evidence.collection_digest,
        ):
            raise InteractionConflict("attempt_conflict")
        if not evidence.observation_digests or any(
            (request.run_id, d) not in self.checkpoint_observations
            for d in evidence.observation_digests
        ):
            raise InteractionConflict("observation_missing")
        if isinstance(evidence, UnknownResumeEvidence):
            return await self.mark_resume_unknown(
                request, lease, evidence.command_id, evidence.attempt_id
            )
        state = context.snapshot.state
        intent = replace(context.intent, status=IntentStatus.RECONCILED)
        if evidence.disposition == "waiting":
            assert evidence.next_pause is not None
            # Reuse the same domain transition, with a current lease, not a second selector.
            result = await self.record_pause(request, lease, evidence.next_pause)
            snapshot = result.snapshot
        else:
            state = replace(
                state,
                phase=Phase.ACTIVE,
                groups=(),
                intent=intent,
                interaction_revision=state.interaction_revision + 1,
            )
            snapshot = self._save_interaction(
                request.run_id, state, context.original_pause
            )
        self.resume_contexts[(request.run_id, evidence.command_id)] = replace(
            context,
            snapshot=snapshot,
            intent=intent,
            observation_digest=evidence.observation_digests[-1],
        )
        return InteractionCommitted(snapshot=snapshot)

    async def record_reconcile_probe(
        self, request: RunRequest, lease: LeaseFence, probe: QuiescentProbe
    ) -> ReconcileProbeResult:
        await self._interaction_authority(request, lease)
        command_id = probe.observation.target.command_id
        attempt_id = probe.observation.target.attempt_id
        assert command_id is not None and attempt_id is not None
        context = await self.read_resume_context(request, command_id)
        if (
            context is None
            or context.intent.attempt_id != attempt_id
            or context.attempt_generation != lease.generation
        ):
            raise InteractionConflict("attempt_conflict")
        key = (request.run_id, command_id, attempt_id)
        prior = self.reconcile_probes.get(key)
        if prior is not None and prior[0].read_id == probe.read_id:
            return ReconcileProbeResult(
                count=prior[1], exhausted=prior[1] >= 3, snapshot=context.snapshot
            )
        count = (
            1
            if prior is None
            else min(prior[1] + 1, 3)
            if (prior[0].progress_digest, prior[0].quiescence)
            == (probe.progress_digest, probe.quiescence)
            else 0
        )
        await self.record_checkpoint_observation(request, lease, probe.observation)
        self.reconcile_probes[key] = (probe, count)
        return ReconcileProbeResult(
            count=count, exhausted=count >= 3, snapshot=context.snapshot
        )

    async def reset_reconcile_probe(
        self, request: RunRequest, lease: LeaseFence, command_id: str, attempt_id: str
    ) -> None:
        await self._interaction_authority(request, lease)
        self.reconcile_probes.pop((request.run_id, command_id, attempt_id), None)

    async def list_unsettled_interactions(
        self, limit: int = 100
    ) -> tuple[InteractionRecoveryTarget, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("invalid limit")
        return tuple(
            InteractionRecoveryTarget(run_id=run_id, command_id=command_id)
            for (run_id, command_id), context in self.resume_contexts.items()
            if run_id not in self.terminals
            and context.intent.status
            in {
                IntentStatus.ACCEPTED,
                IntentStatus.DISPATCH_STARTED,
                IntentStatus.NATIVE_OBSERVED,
                IntentStatus.UNKNOWN,
            }
        )[:limit]

    def __init__(self) -> None:
        self.interaction_snapshots: dict[str, InteractionSnapshot] = {}
        self.resume_contexts: dict[tuple[str, str], ResumeReadContext] = {}
        self.checkpoint_observations: dict[tuple[str, str], CheckpointObservation] = {}
        self.reconcile_probes: dict[
            tuple[str, str, str], tuple[QuiescentProbe, int]
        ] = {}
        self.chat_repository = FakeChatRepository()
        self.requests: dict[str, RunRequest] = {}
        self.request_json: dict[str, str] = {}
        self.static_recipes: dict[str, StaticRecipeBinding] = {}
        self.terminals: set[str] = set()
        self.leases: dict[str, int | None] = {}
        self.owners: dict[str, str] = {}
        self.generations: dict[str, int] = {}
        self.renewed: list[str] = []
        self.paused_runs: list[str] = []
        self.expired: list[RunRequest] = []
        self.tool_results: dict[tuple[str, str], tuple[str, bool]] = {}
        self.token_totals: dict[str, int] = {}
        self.usage_totals: dict[str, tuple[int, int]] = {}
        self.usage_segments: dict[tuple[str, int], tuple[int, int]] = {}
        self.steers: dict[str, list[tuple[str, str]]] = {}
        self.sandbox_ids: dict[str, str] = {}
        self.sandbox_generations: dict[str, int] = {}
        self.sandbox_bindings: dict[str, tuple[SandboxBackendKind, str]] = {}
        self.sandbox_cleanups: dict[str, dict[str, object]] = {}
        self.terminal_at: dict[str, int] = {}
        self.clock_ms = 0
        # dispatch CAS 记录（run_id → status）：默认无记录=放行；测试可预置 pending/claimed。
        self.dispatches: dict[str, str] = {}
        self.dispatch_fences: dict[str, str] = {}
        self.dispatch_namespaces: dict[str, str] = {}
        self.dispatch_requests: dict[str, RunRequest] = {}
        self.dlq: list[tuple[str, str, str]] = []
        # R4 critical outbox：per-run durable_seq 计数、local fence、outbox 行；回执/清单由测试 seed。
        self.durable_counter: dict[str, int] = {}
        self.event_index_counter: dict[str, int] = {}
        self.terminal_fence: dict[str, int] = {}
        self.outbox: dict[str, list[dict[str, object]]] = {}
        # session 写域（测试 seed）：run_event_receipts 行 + run_receipt_manifests 单行。
        self.receipts: dict[str, list[dict[str, object]]] = {}
        self.manifests: dict[str, dict[str, object]] = {}
        # 单一 control command ledger（R2）：(run_id, command_id) → command state。
        # HTTP admission 与 worker delivery 共用同一条记录，避免 fake 产生双真相。
        self.control_commands: dict[tuple[str, str], dict[str, object]] = {}
        # tool effect journal（R3）：(run_id, tool_call_id) → {name,status,result,is_error}。
        self.tool_journal: dict[tuple[str, str], dict[str, object]] = {}

    async def freeze_or_verify_static_recipe(
        self, request: RunRequest, lease: LeaseFence, binding: StaticRecipeBinding
    ) -> Literal["frozen", "matched"]:
        from kokoro_agent.infrastructure.postgres_run_profiles import (
            validate_static_recipe,
        )

        validate_static_recipe(binding, lease)
        run_id = request.run_id
        if not await self.is_lease_current(run_id, lease):
            raise StaticRecipeAuthorityLost("static recipe authority lost")
        if self.request_json.get(run_id, "").encode(
            "utf-8"
        ) != request.model_dump_json().encode("utf-8"):
            raise StaticRecipeIncompatible(lease)
        saved = self.static_recipes.get(run_id)
        if saved is not None:
            validate_static_recipe(saved, lease)
            if saved != binding:
                raise StaticRecipeIncompatible(lease)
            return "matched"
        if (
            self.durable_counter.get(run_id, 0) != 0
            or self.event_index_counter.get(run_id, 0) != 0
            or self.token_totals.get(run_id, 0) != 0
            or self.usage_totals.get(run_id, (0, 0)) != (0, 0)
            or run_id in self.sandbox_ids
        ):
            raise StaticRecipeIncompatible(lease)
        self.static_recipes[run_id] = binding
        return "frozen"

    def _active_expiry(self) -> int:
        return self.clock_ms + 90_000

    async def enqueue_dispatch(
        self, request: RunRequest, namespace: str, fence: str
    ) -> DispatchAdmission:
        existing = self.dispatch_fences.get(request.run_id)
        if existing is not None and existing != fence:
            raise DispatchConflict(
                f"run id {request.run_id!r} was reused with a different fence"
            )
        if existing is None:
            self.dispatch_fences[request.run_id] = fence
            self.dispatch_namespaces[request.run_id] = namespace
            self.dispatch_requests[request.run_id] = request
            self.dispatches[request.run_id] = "pending"
            return DispatchAdmission(replayed=False, publish_required=True)
        return DispatchAdmission(
            replayed=True,
            publish_required=self.dispatches.get(request.run_id) == "pending",
        )

    async def try_claim(
        self, request: RunRequest, owner: str = "test-consumer"
    ) -> LeaseFence | None:
        if request.run_id in self.requests:
            return None
        self.requests[request.run_id] = request
        self.request_json[request.run_id] = request.model_dump_json()
        self.leases[request.run_id] = self._active_expiry()
        self.owners[request.run_id] = owner
        self.generations[request.run_id] = 1
        return LeaseFence(owner=owner, generation=1)

    async def claim_dispatch(
        self, request: RunRequest, consumer: str = "test-consumer"
    ) -> LeaseFence | None:
        # Supervisor 单测默认把未显式布置的请求视为已注入 pending intent；需要验证
        # 缺失/重复时，测试应明确预置对应状态。
        run_id = request.run_id
        status = self.dispatches.get(run_id)
        if status is None:
            return None
        canonical = self.dispatch_requests.get(run_id)
        if canonical is not None and canonical != request:
            return None
        if status == "pending":
            self.dispatches[run_id] = "claimed"
            if run_id in self.requests:
                return None
            self.requests[run_id] = request
            self.request_json[run_id] = request.model_dump_json()
            self.leases[run_id] = self._active_expiry()
            self.owners[run_id] = consumer
            self.generations[run_id] = 1
            return LeaseFence(owner=consumer, generation=1)
        return None

    async def get_pending_dispatch(self, run_id: str) -> RunRequest | None:
        if self.dispatches.get(run_id) != "pending":
            return None
        return self.dispatch_requests.get(run_id)

    async def list_pending_dispatches(self, limit: int = 100) -> list[RunRequest]:
        pending = [
            request
            for run_id, request in sorted(self.dispatch_requests.items())
            if self.dispatches.get(run_id) == "pending"
        ]
        return pending[:limit]

    async def quarantine_dispatch(
        self, raw_hash: str, source: str, reason: str
    ) -> None:
        self.dlq.append((raw_hash, source, reason))

    async def stage_critical_frame(
        self,
        run_id: str,
        lease: LeaseFence,
        kind: str,
        timestamp: int,
        payload_json: str,
        *,
        terminal: bool,
        event_id: str | None = None,
    ) -> StagedFrame | None:
        lease_current = (
            await self.is_lease_current(run_id, lease)
            if kind == "run.started"
            else await self.is_fence_current(run_id, lease)
        )
        if not lease_current:
            return None
        if event_id is not None:
            for existing in self.outbox.get(run_id, []):
                if existing["event_id"] == event_id:
                    if (
                        existing["kind"] != kind
                        or existing["payload_json"] != payload_json
                    ):
                        raise RuntimeError("delivery event identity drift")
                    return StagedFrame(
                        durable_seq=cast(int, existing["durable_seq"]),
                        event_id=event_id,
                        index=cast(int, existing["index"]),
                        timestamp=cast(int, existing["timestamp"]),
                        published=existing["status"] == "published",
                        newly_staged=False,
                    )
        if kind == "run.started":
            existing_rows = [
                row
                for row in self.outbox.get(run_id, [])
                if row["kind"] == kind and row["status"] in {"queued", "published"}
            ]
            if len(existing_rows) > 1:
                raise RuntimeError("duplicate durable run.started facts")
            if existing_rows:
                existing = existing_rows[0]
                if (
                    existing.get("index") is None
                    or existing["payload_json"] != payload_json
                ):
                    raise RuntimeError("run.started identity drift")
                return StagedFrame(
                    durable_seq=cast(int, existing["durable_seq"]),
                    event_id=cast(str, existing["event_id"]),
                    index=cast(int, existing["index"]),
                    timestamp=cast(int, existing["timestamp"]),
                    published=existing["status"] == "published",
                    newly_staged=False,
                )
        seq = self.durable_counter.get(run_id, 0) + 1
        self.durable_counter[run_id] = seq
        if terminal and run_id not in self.terminal_fence:
            self.terminal_fence[run_id] = seq
        fence = self.terminal_fence.get(run_id)
        event_id = event_id or f"evt_fake_{run_id}_{seq}"
        rows = self.outbox.setdefault(run_id, [])
        if fence is not None and seq > fence:
            rows.append(
                {
                    "durable_seq": seq,
                    "event_id": event_id,
                    "kind": kind,
                    "status": "superseded",
                }
            )
            return None
        index = self.event_index_counter.get(run_id, 0)
        self.event_index_counter[run_id] = index + 1
        rows.append(
            {
                "durable_seq": seq,
                "event_id": event_id,
                "kind": kind,
                "index": index,
                "timestamp": timestamp,
                "payload_json": payload_json,
                "status": "queued",
            }
        )
        return StagedFrame(
            durable_seq=seq, event_id=event_id, index=index, timestamp=timestamp
        )

    async def next_event_index(self, run_id: str) -> int:
        return self.event_index_counter.get(run_id, 0)

    async def reserve_event_index(self, run_id: str, lease: LeaseFence) -> int | None:
        if not await self.is_lease_current(run_id, lease):
            return None
        index = self.event_index_counter.get(run_id, 0)
        self.event_index_counter[run_id] = index + 1
        return index

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None:
        for row in self.outbox.get(run_id, []):
            if row["durable_seq"] == durable_seq and row["status"] == "queued":
                row["status"] = "published"
                row["published_at"] = self.clock_ms
                return

    async def list_unpublished_outbox(self) -> list[OutboxFrame]:
        frames: list[OutboxFrame] = []
        for run_id, rows in self.outbox.items():
            for row in rows:
                if row["status"] != "queued":
                    continue
                frames.append(
                    OutboxFrame(
                        run_id=run_id,
                        durable_seq=_as_int(row["durable_seq"]),
                        event_id=str(row["event_id"]),
                        kind=str(row["kind"]),
                        index=_as_int(row["index"]),
                        timestamp=_as_int(row["timestamp"]),
                        payload_json=str(row["payload_json"]),
                    )
                )
        frames.sort(key=lambda f: (f.run_id, f.durable_seq))
        return frames

    async def list_open_outbox_runs(self) -> list[str]:
        return sorted(
            run_id
            for run_id, rows in self.outbox.items()
            if any(row["status"] in ("queued", "published") for row in rows)
        )

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30_000
    ) -> ReceiptReconcile:
        now = self.clock_ms
        rows = self.outbox.get(run_id, [])
        live = [r for r in rows if r["status"] in ("queued", "published")]
        if not live:
            return ReceiptReconcile()
        receipts = {_as_int(r["durable_seq"]): r for r in self.receipts.get(run_id, [])}
        rejected = sorted(s for s, r in receipts.items() if r["status"] == "rejected")
        if rejected:
            return ReceiptReconcile(rejected_seq=rejected[0])
        # published 无回执且超宽限期 → 重发候选（touch published_at 复位计时）。
        stale = [
            r
            for r in live
            if r["status"] == "published"
            and _as_int(r["durable_seq"]) not in receipts
            and r.get("published_at") is not None
            and now - _as_int(r["published_at"]) >= republish_grace_ms
        ]
        republish = [
            OutboxFrame(
                run_id=run_id,
                durable_seq=_as_int(r["durable_seq"]),
                event_id=str(r["event_id"]),
                kind=str(r["kind"]),
                index=_as_int(r["index"]),
                timestamp=_as_int(r["timestamp"]),
                payload_json=str(r["payload_json"]),
            )
            for r in stale
        ]
        for r in stale:
            r["published_at"] = now
        manifest = self.manifests.get(run_id)
        if manifest is None:
            return ReceiptReconcile(receipt_state_lost=True, republish=republish)
        consumed = _as_int(manifest.get("consumed_seq") or 0)
        by_seq = {_as_int(r["durable_seq"]): r for r in rows}
        advanced = consumed
        seq = consumed + 1
        while seq in receipts and receipts[seq]["status"] == "persisted":
            row = by_seq.get(seq)
            if row is not None and row["event_id"] != receipts[seq]["event_id"]:
                break
            advanced = seq
            seq += 1
        if advanced > consumed:
            manifest["consumed_seq"] = advanced
        self.outbox[run_id] = [
            r
            for r in rows
            if _as_int(r["durable_seq"]) > advanced
            or (r["kind"] == "delivery.created" and run_id not in self.terminals)
        ]
        fence = self.terminal_fence.get(run_id)
        remaining = [
            r
            for r in self.outbox.get(run_id, [])
            if r["status"] in ("queued", "published")
        ]
        close_requested = False
        if fence is not None and advanced >= fence and not remaining:
            if not manifest.get("producer_close_requested"):
                manifest["producer_close_requested"] = True
                close_requested = True
        return ReceiptReconcile(
            consumed_through=advanced if advanced > consumed else None,
            close_requested=close_requested,
            republish=republish,
        )

    async def record_control_delivery(
        self,
        run_id: str,
        command_id: str,
        request_digest: str | None,
        fingerprint: str | None,
        body: str,
    ) -> bool:
        del body
        if run_id not in self.requests or run_id in self.terminals:
            return False
        existing = self.control_commands.get((run_id, command_id))
        if existing is None:
            return False
        if request_digest is not None and existing["request_digest"] != request_digest:
            raise ControlCommandConflict("command digest mismatch")
        if existing["status"] != "admitted":
            return False
        existing["status"] = "persisted"
        existing["fingerprint"] = fingerprint
        return True

    async def admit_control(
        self, run_id: str, command_id: str, request_digest: str, body: str
    ) -> ControlAdmission:
        if run_id not in self.requests:
            raise InteractionRunMissing("Run was not found")
        key = (run_id, command_id)
        existing = self.control_commands.get(key)
        if existing is not None:
            if existing["request_digest"] != request_digest:
                raise ControlCommandConflict("command digest mismatch")
            status = str(existing["status"])
            public_status = cast(
                ControlAdmissionStatus,
                {
                    "admitted": "pending",
                    "persisted": "pending",
                    "applied": "succeeded",
                    "succeeded": "succeeded",
                    "failed": "failed",
                    "superseded": "failed",
                }[status],
            )
            return ControlAdmission(
                receipt=ControlAdmissionReceipt(
                    run_id=run_id,
                    command_id=command_id,
                    request_digest=request_digest,
                    status=public_status,
                    error_code=cast(str | None, existing["error_code"]),
                ),
                replayed=True,
                publish_required=status in {"admitted", "persisted"},
            )
        self.control_commands[key] = {
            "run_id": run_id,
            "command_id": command_id,
            "request_digest": request_digest,
            "fingerprint": None,
            "status": "admitted",
            "body": body,
            "error_code": None,
        }
        return ControlAdmission(
            receipt=ControlAdmissionReceipt(
                run_id=run_id,
                command_id=command_id,
                request_digest=request_digest,
                status="pending",
            ),
            replayed=False,
            publish_required=True,
        )

    async def mark_control_succeeded(self, run_id: str, command_id: str) -> None:
        command = self.control_commands.get((run_id, command_id))
        if command is not None and command["status"] in {
            "admitted",
            "persisted",
            "applied",
        }:
            command["status"] = "succeeded"

    async def mark_control_failed(
        self, run_id: str, command_id: str, error_code: str | None = None
    ) -> None:
        command = self.control_commands.get((run_id, command_id))
        if command is not None and command["status"] in {
            "admitted",
            "persisted",
            "applied",
        }:
            command["status"] = "failed"
            command["error_code"] = error_code

    async def mark_control_applied(self, run_id: str, command_id: str) -> None:
        command = self.control_commands.get((run_id, command_id))
        if command is not None and command["status"] == "persisted":
            command["status"] = "applied"

    async def mark_control_superseded(self, run_id: str, command_id: str) -> None:
        command = self.control_commands.get((run_id, command_id))
        if command is not None and command["status"] == "persisted":
            command["status"] = "superseded"

    async def list_pending_control_delivery(self) -> list[RunControlCommandRecord]:
        records: list[RunControlCommandRecord] = []
        for (run_id, _command_id), entry in sorted(self.control_commands.items()):
            if run_id in self.terminals or entry["status"] != "persisted":
                continue
            records.append(
                RunControlCommandRecord(
                    run_id=run_id,
                    command_id=str(entry["command_id"]),
                    request_digest=cast(str | None, entry["request_digest"]),
                    fingerprint=cast(str | None, entry["fingerprint"]),
                    body=str(entry["body"]),
                )
            )
        return records

    async def renew(self, run_id: str, lease: LeaseFence) -> bool:
        self.renewed.append(run_id)
        if not await self.is_lease_current(run_id, lease):
            return False
        self.leases[run_id] = self._active_expiry()
        return True

    async def adopt(
        self, run_id: str, owner: str = "test-consumer"
    ) -> LeaseFence | None:
        if run_id in self.terminals or self.leases.get(run_id) is not None:
            return None
        generation = self.generations.get(run_id, 0) + 1
        self.leases[run_id] = self._active_expiry()
        self.owners[run_id] = owner
        self.generations[run_id] = generation
        return LeaseFence(owner=owner, generation=generation)

    async def pause(self, run_id: str, lease: LeaseFence | None = None) -> bool:
        self.paused_runs.append(run_id)
        active = lease or self.current_lease(run_id)
        if (
            run_id in self.terminals
            or active is None
            or not await self.is_lease_current(run_id, active)
        ):
            return False
        self.leases[run_id] = None
        return True

    async def reclaim_expired(self, owner: str = "test-consumer") -> list[LeasedRun]:
        out = self.expired
        self.expired = []
        leased: list[LeasedRun] = []
        for req in out:
            generation = self.generations.get(req.run_id, 0) + 1
            self.owners[req.run_id] = owner
            self.generations[req.run_id] = generation
            self.leases[req.run_id] = self._active_expiry()
            leased.append(
                LeasedRun(
                    request=req,
                    lease=LeaseFence(owner=owner, generation=generation),
                )
            )
        return leased

    def current_lease(self, run_id: str) -> LeaseFence | None:
        owner = self.owners.get(run_id)
        generation = self.generations.get(run_id)
        if owner is None or generation is None:
            return None
        return LeaseFence(owner=owner, generation=generation)

    async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool:
        expires_at = self.leases.get(run_id)
        return (
            run_id not in self.terminals
            and self.owners.get(run_id) == lease.owner
            and self.generations.get(run_id) == lease.generation
            and expires_at is not None
            and expires_at > self.clock_ms
        )

    async def is_fence_current(self, run_id: str, lease: LeaseFence) -> bool:
        return (
            self.owners.get(run_id) == lease.owner
            and self.generations.get(run_id) == lease.generation
        )

    async def get_fence(self, run_id: str) -> LeaseFence | None:
        return self.current_lease(run_id)

    async def list_paused(self) -> list[str]:
        return sorted(
            run_id
            for run_id, lease in self.leases.items()
            if lease is None and run_id not in self.terminals
        )

    async def get_request(self, run_id: str) -> RunRequest | None:
        return self.requests.get(run_id)

    async def get_request_scoped(
        self, run_id: str, tenant_ref: str, namespace: str
    ) -> RunRequest | None:
        request = self.requests.get(run_id)
        if request is None:
            return None
        if request.execution_identity.tenant_ref != tenant_ref:
            return None
        if runtime_namespace(request.execution_identity) != namespace:
            return None
        return request

    async def add_tokens(
        self, run_id: str, lease: LeaseFence, count: int
    ) -> int | None:
        if not await self.is_lease_current(run_id, lease):
            return None
        self.token_totals[run_id] = self.token_totals.get(run_id, 0) + count
        return self.token_totals[run_id]

    async def add_usage(
        self,
        run_id: str,
        lease: LeaseFence,
        input_tokens: int,
        output_tokens: int,
    ) -> tuple[int, int] | None:
        current = (
            await self.is_fence_current(run_id, lease)
            if run_id in self.terminals
            else await self.is_lease_current(run_id, lease)
        )
        if not current:
            return None
        segment = (run_id, lease.generation)
        usage = (input_tokens, output_tokens)
        existing = self.usage_segments.get(segment)
        if existing is not None:
            if existing != usage:
                raise UsageIdentityConflict(
                    "usage identity conflict for "
                    f"run {run_id!r} generation {lease.generation}"
                )
            return self.usage_totals.get(run_id, (0, 0))
        if run_id in self.terminals:
            return None
        cur_in, cur_out = self.usage_totals.get(run_id, (0, 0))
        self.usage_segments[segment] = usage
        self.usage_totals[run_id] = (cur_in + input_tokens, cur_out + output_tokens)
        return self.usage_totals[run_id]

    async def verify_terminal_frame(self, frame: OutboxFrame) -> None:
        request = self.requests.get(frame.run_id)
        if request is None or frame.run_id not in self.terminals:
            raise RuntimeError("terminal frame has no committed Run")
        namespace = runtime_namespace(request.execution_identity)
        fact = next(
            (
                r
                for r in self.chat_repository.records
                if r.run_id == frame.run_id
                and r.tenant_id == request.execution_identity.tenant_ref
                and r.namespace == namespace
                and r.session_id == request.session_id
                and r.source_index == frame.index
                and r.chat_event_id
                == chat_event_id(namespace, frame.run_id, frame.index)
            ),
            None,
        )
        if fact is None:
            raise RuntimeError("terminal Chat identity is missing or corrupt")
        payload = (
            RunCompletedPayload.model_validate_json(frame.payload_json)
            if frame.kind == "run.completed"
            else RunFailedPayload.model_validate_json(frame.payload_json)
        )
        projection = project_chat_fact(
            tenant_id=fact.tenant_id,
            namespace=namespace,
            session_id=fact.session_id,
            run_id=frame.run_id,
            source_index=frame.index,
            created_at=datetime.fromtimestamp(frame.timestamp / 1000, tz=UTC),
            payload=payload,
        )
        assert projection is not None
        if (
            fact.event_type != projection.event.event_type
            or fact.payload_json != projection.event.payload_json
        ):
            raise RuntimeError("terminal Chat payload drift")

    async def finalize_terminal(
        self,
        run_id: str,
        authority: TerminalAuthority,
        outcome: RunTerminalOutcome,
        delivery_snapshot: tuple[tuple[str, str, str], ...],
    ) -> TerminalCommitResult:
        lease = self.current_lease(run_id)
        if lease is None:
            return TerminalCommitResult(status="lost", lease=None)
        quarantine = isinstance(authority, QuarantineTerminalAuthority)
        command = (
            self.control_commands.get((run_id, authority.command_id))
            if isinstance(authority, CancelTerminalAuthority)
            else None
        )
        if (
            isinstance(authority, ExecutionTerminalAuthority)
            and authority.lease != lease
        ):
            return TerminalCommitResult(status="lost", lease=None)
        if isinstance(authority, CancelTerminalAuthority) and (
            command is None or command["status"] not in {"persisted", "succeeded"}
        ):
            return TerminalCommitResult(status="lost", lease=None)
        if isinstance(authority, QuarantineTerminalAuthority):
            matches = [
                r
                for r in self.receipts.get(run_id, [])
                if r["status"] == "rejected"
                and r["durable_seq"] == authority.rejected_seq
            ]
            if not any(
                o["durable_seq"] == r["durable_seq"] and o["event_id"] == r["event_id"]
                for r in matches
                for o in self.outbox.get(run_id, [])
            ):
                return TerminalCommitResult(status="lost", lease=None)
            fence = self.terminal_fence.get(run_id)
            if fence is not None and fence != authority.rejected_seq:
                return TerminalCommitResult(status="lost", lease=None)
        if run_id in self.terminals:
            if isinstance(authority, CancelTerminalAuthority) and (
                command is None or command["status"] != "succeeded"
            ):
                return TerminalCommitResult(status="lost", lease=None)
            if quarantine:
                audit = next(
                    (
                        item
                        for item in self.outbox.get(run_id, [])
                        if item["event_id"]
                        == "evt_"
                        + hashlib.sha256(f"terminal\0{run_id}".encode()).hexdigest()
                    ),
                    None,
                )
                if (
                    audit is None
                    or audit["status"] != "superseded"
                    or audit["kind"] != "run.failed"
                    or audit.get("index") is not None
                    or audit.get("timestamp") != self.terminal_at.get(run_id)
                    or audit["durable_seq"] != self.durable_counter.get(run_id)
                    or audit.get("payload_json")
                    != outcome.payload.model_dump_json(exclude_none=True)
                ):
                    return TerminalCommitResult(status="lost", lease=None)
                return TerminalCommitResult(status="replayed", lease=lease)
            persisted = next(
                (
                    r
                    for r in self.chat_repository.records
                    if r.run_id == run_id
                    and r.event_type in {"run.completed", "run.failed"}
                ),
                None,
            )
            if persisted is None:
                raise RuntimeError("terminal Chat fact is missing")
            payload = outcome.payload
            totals = self.usage_totals.get(run_id, (0, 0))
            if (
                isinstance(payload, RunCompletedPayload)
                and payload.status == "completed"
            ):
                payload = RunCompletedPayload(
                    status="completed",
                    token_usage=(
                        TokenUsage(input_tokens=totals[0], output_tokens=totals[1])
                        if any(totals)
                        else None
                    ),
                )
            projection = project_chat_fact(
                tenant_id=persisted.tenant_id,
                namespace=persisted.namespace,
                session_id=persisted.session_id,
                run_id=run_id,
                source_index=persisted.source_index,
                created_at=persisted.created_at,
                payload=payload,
            )
            assert projection is not None
            if (
                projection.event.event_type != persisted.event_type
                or projection.event.payload_json != persisted.payload_json
            ):
                return TerminalCommitResult(status="lost", lease=None)
            if (
                outcome.usage is not None
                and await self.add_usage(
                    run_id,
                    lease,
                    outcome.usage.input_tokens,
                    outcome.usage.output_tokens,
                )
                is None
            ):
                return TerminalCommitResult(status="lost", lease=None)
            retained = tuple(
                f
                for f in await self.list_unpublished_outbox()
                if f.run_id == run_id
                and (
                    f.kind in {"run.completed", "run.failed"}
                    or (
                        isinstance(authority, CancelTerminalAuthority)
                        and f.event_id
                        == "evt_"
                        + hashlib.sha256(
                            f"control\0{run_id}\0{authority.command_id}".encode()
                        ).hexdigest()
                    )
                )
            )
            return TerminalCommitResult(
                status="replayed", lease=lease, retained_frames=retained
            )
        if isinstance(
            authority, ExecutionTerminalAuthority
        ) and not await self.is_lease_current(run_id, lease):
            return TerminalCommitResult(status="lost", lease=None)
        if not quarantine and any(
            r["status"] == "rejected"
            and r["durable_seq"] == o["durable_seq"]
            and r["event_id"] == o["event_id"]
            for r in self.receipts.get(run_id, [])
            for o in self.outbox.get(run_id, [])
        ):
            return TerminalCommitResult(status="lost", lease=None)
        if not quarantine and (
            tuple(await self.list_delivery_journal(run_id)) != delivery_snapshot
            or any(status == "started" for _, status, _ in delivery_snapshot)
        ):
            return TerminalCommitResult(status="deferred", lease=None)
        state = deepcopy(
            {k: v for k, v in self.__dict__.items() if k != "chat_repository"}
        )
        chat_state = deepcopy(self.chat_repository.__dict__)
        try:
            totals = self.usage_totals.get(run_id, (0, 0))
            if outcome.usage is not None:
                updated = await self.add_usage(
                    run_id,
                    lease,
                    outcome.usage.input_tokens,
                    outcome.usage.output_tokens,
                )
                if updated is None:
                    return TerminalCommitResult(status="lost", lease=None)
                totals = updated
            payload = outcome.payload
            if (
                isinstance(payload, RunCompletedPayload)
                and payload.status == "completed"
            ):
                payload = RunCompletedPayload(
                    status="completed",
                    token_usage=(
                        TokenUsage(input_tokens=totals[0], output_tokens=totals[1])
                        if any(totals)
                        else None
                    ),
                )
            seq, index = (
                self.durable_counter.get(run_id, 0),
                self.event_index_counter.get(run_id, 0),
            )
            frames: list[OutboxFrame] = []
            if isinstance(authority, CancelTerminalAuthority):
                seq += 1
                receipt = RunControlReceiptPayload(
                    command_id=authority.command_id, control_status="applied"
                )
                frames.append(
                    OutboxFrame(
                        run_id=run_id,
                        durable_seq=seq,
                        index=index,
                        timestamp=self.clock_ms,
                        event_id="evt_"
                        + hashlib.sha256(
                            f"control\0{run_id}\0{authority.command_id}".encode()
                        ).hexdigest(),
                        kind="run.control.receipt",
                        payload_json=receipt.model_dump_json(exclude_none=True),
                    )
                )
                index += 1
            seq += 1
            frames.append(
                OutboxFrame(
                    run_id=run_id,
                    durable_seq=seq,
                    index=index,
                    timestamp=self.clock_ms,
                    event_id="evt_"
                    + hashlib.sha256(f"terminal\0{run_id}".encode()).hexdigest(),
                    kind="run.failed"
                    if isinstance(payload, RunFailedPayload)
                    else "run.completed",
                    payload_json=payload.model_dump_json(exclude_none=True),
                )
            )
            if not quarantine:
                request = self.requests[run_id]
                for started in sorted(
                    self.outbox.get(run_id, []),
                    key=lambda item: _as_int(item["durable_seq"]),
                ):
                    if started["kind"] != "run.started" or started["status"] not in {
                        "queued",
                        "published",
                    }:
                        continue
                    started_projection = project_chat_fact(
                        tenant_id=request.execution_identity.tenant_ref,
                        namespace=runtime_namespace(request.execution_identity),
                        session_id=request.session_id,
                        run_id=run_id,
                        source_index=_as_int(started["index"]),
                        created_at=datetime.fromtimestamp(
                            _as_int(started["timestamp"]) / 1000, tz=UTC
                        ),
                        payload=RunStartedPayload.model_validate_json(
                            str(started["payload_json"])
                        ),
                    )
                    assert started_projection is not None
                    await self.chat_repository.append(started_projection)
                projection = project_chat_fact(
                    tenant_id=request.execution_identity.tenant_ref,
                    namespace=runtime_namespace(request.execution_identity),
                    session_id=request.session_id,
                    run_id=run_id,
                    source_index=index,
                    created_at=datetime.fromtimestamp(self.clock_ms / 1000, tz=UTC),
                    payload=payload,
                )
                assert projection is not None
                await self.chat_repository.append(projection)
            if isinstance(authority, QuarantineTerminalAuthority):
                for row in self.outbox.get(run_id, []):
                    if _as_int(row["durable_seq"]) >= authority.rejected_seq and row[
                        "status"
                    ] in {"queued", "published"}:
                        row["status"] = "superseded"
                        row["index"] = None
            for frame in frames:
                self.outbox.setdefault(run_id, []).append(
                    {
                        **frame.model_dump(),
                        "status": "superseded" if quarantine else "queued",
                        "index": None if quarantine else frame.index,
                    }
                )
            if not isinstance(authority, ExecutionTerminalAuthority):
                lease = LeaseFence(
                    owner=authority.owner, generation=lease.generation + 1
                )
                self.owners[run_id], self.generations[run_id] = (
                    lease.owner,
                    lease.generation,
                )
            current_interaction = self.interaction_snapshots.get(run_id)
            if current_interaction is not None:
                terminal_state = current_interaction.state.terminal()
                self._save_interaction(
                    run_id, terminal_state, current_interaction.pause
                )
                for key, context in tuple(self.resume_contexts.items()):
                    if key[0] == run_id:
                        self.resume_contexts[key] = replace(
                            context,
                            intent=replace(
                                context.intent, status=IntentStatus.TERMINAL
                            ),
                        )
            self.terminals.add(run_id)
            self.leases[run_id] = None
            self.terminal_at[run_id] = self.clock_ms
            self.durable_counter[run_id] = seq
            self.event_index_counter[run_id] = index if quarantine else index + 1
            self.terminal_fence[run_id] = (
                authority.rejected_seq
                if isinstance(authority, QuarantineTerminalAuthority)
                else seq
            )
            if command is not None:
                command["status"] = "succeeded"
            await self._queue_bound_sandbox_cleanup(run_id)
            return TerminalCommitResult(
                status="committed",
                lease=lease,
                retained_frames=() if quarantine else tuple(frames),
            )
        except BaseException:
            self.__dict__.update(state)
            self.chat_repository.__dict__.clear()
            self.chat_repository.__dict__.update(chat_state)
            raise

    async def purge_terminal(self, max_age_ms: int) -> int:
        cutoff = self.clock_ms - max_age_ms
        blocked = {
            str(row["run_id"])
            for row in self.sandbox_cleanups.values()
            if row["status"] != "completed"
        }
        stale = [
            r
            for r in self.terminals
            if self.terminal_at.get(r, 0) <= cutoff and r not in blocked
        ]
        for run_id in stale:
            self.terminals.discard(run_id)
            self.terminal_at.pop(run_id, None)
            self.interaction_snapshots.pop(run_id, None)
            self.resume_contexts = {
                key: value
                for key, value in self.resume_contexts.items()
                if key[0] != run_id
            }
            self.checkpoint_observations = {
                key: value
                for key, value in self.checkpoint_observations.items()
                if key[0] != run_id
            }
            self.reconcile_probes = {
                key: value
                for key, value in self.reconcile_probes.items()
                if key[0] != run_id
            }
            self.requests.pop(run_id, None)
            self.leases.pop(run_id, None)
            self.owners.pop(run_id, None)
            self.generations.pop(run_id, None)
            self.token_totals.pop(run_id, None)
            self.usage_totals.pop(run_id, None)
            self.usage_segments = {
                key: value
                for key, value in self.usage_segments.items()
                if key[0] != run_id
            }
            self.steers.pop(run_id, None)
            self.sandbox_ids.pop(run_id, None)
            self.sandbox_generations.pop(run_id, None)
            self.sandbox_bindings.pop(run_id, None)
            self.sandbox_cleanups = {
                key: value
                for key, value in self.sandbox_cleanups.items()
                if value["run_id"] != run_id
            }
            self.tool_results = {
                k: v for k, v in self.tool_results.items() if k[0] != run_id
            }
            self.tool_journal = {
                k: v for k, v in self.tool_journal.items() if k[0] != run_id
            }
            self.control_commands = {
                k: v for k, v in self.control_commands.items() if k[0] != run_id
            }
        return len(stale)

    async def is_terminal(self, run_id: str) -> bool:
        return run_id in self.terminals

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        if run_id not in self.requests:
            return
        box = self.steers.setdefault(run_id, [])
        if all(mid != message_id for mid, _ in box):
            box.append((message_id, content))

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        return list(self.steers.get(run_id, []))

    async def ack_steers(
        self, run_id: str, lease: LeaseFence, message_ids: list[str]
    ) -> bool:
        if not await self.is_lease_current(run_id, lease):
            return False
        box = self.steers.get(run_id)
        if box is None:
            return True
        self.steers[run_id] = [
            (mid, c) for mid, c in box if mid not in set(message_ids)
        ]
        return True

    async def put_tool_result(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_id: str,
        result: str,
        is_error: bool,
    ) -> tuple[str, bool] | None:
        if not await self.is_lease_current(run_id, lease):
            return None
        self.tool_results.setdefault((run_id, tool_id), (result, is_error))
        return self.tool_results[(run_id, tool_id)]

    async def get_tool_result(
        self, run_id: str, tool_id: str
    ) -> tuple[str, bool] | None:
        return self.tool_results.get((run_id, tool_id))

    async def journal_tool_started(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, name: str
    ) -> bool:
        if not await self.is_lease_current(run_id, lease):
            return False
        key = (run_id, tool_call_id)
        if key in self.tool_journal:
            return False
        self.tool_journal[key] = {
            "name": name,
            "status": "started",
            "result": None,
            "is_error": None,
        }
        return True

    async def journal_delivery_intent(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, intent: str
    ) -> bool:
        if not await self.is_lease_current(run_id, lease):
            return False
        entry = self.tool_journal.get((run_id, tool_call_id))
        if entry is None or entry["name"] != "deliver" or entry["status"] != "started":
            return False
        entry["result"] = intent
        return True

    async def journal_tool_finished(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_call_id: str,
        result: str,
        is_error: bool,
    ) -> bool:
        if not await self.is_lease_current(run_id, lease):
            return False
        entry = self.tool_journal.get((run_id, tool_call_id))
        if entry is None or entry["status"] != "started":
            return False
        entry["status"] = "failed" if is_error else "succeeded"
        entry["result"] = result
        entry["is_error"] = is_error
        return True

    async def clear_tool_journal(
        self, run_id: str, lease: LeaseFence, tool_call_id: str
    ) -> bool:
        if not await self.is_lease_current(run_id, lease):
            return False
        self.tool_journal.pop((run_id, tool_call_id), None)
        return True

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None:
        entry = self.tool_journal.get((run_id, tool_call_id))
        if entry is None:
            return None
        return ToolJournalRecord(
            name=str(entry["name"]),
            status=str(entry["status"]),
            result=str(entry["result"] or ""),
            is_error=bool(entry["is_error"]),
        )

    async def list_delivery_journal(self, run_id: str) -> list[tuple[str, str, str]]:
        return [
            (tool_id, str(row["status"]), str(row["result"] or ""))
            for (row_run, tool_id), row in sorted(self.tool_journal.items())
            if row_run == run_id and row["name"] == "deliver"
        ]

    async def bind_sandbox_id(
        self,
        run_id: str,
        lease: LeaseFence,
        *,
        expected_sandbox_id: str | None,
        sandbox_id: str,
        backend_kind: SandboxBackendKind,
        teardown_ref: str,
    ) -> str | None:
        if not await self.is_lease_current(run_id, lease):
            return None
        current = self.sandbox_ids.get(run_id)
        if (
            current is not None
            and self.sandbox_generations.get(run_id) == lease.generation
        ):
            return current
        if current != expected_sandbox_id:
            return current
        self.sandbox_ids[run_id] = sandbox_id
        self.sandbox_generations[run_id] = lease.generation
        if current != sandbox_id or run_id not in self.sandbox_bindings:
            self.sandbox_bindings[run_id] = (backend_kind, teardown_ref)
        return sandbox_id

    async def get_sandbox_id(self, run_id: str) -> str | None:
        return self.sandbox_ids.get(run_id)

    async def register_sandbox_cleanup(
        self,
        *,
        run_id: str,
        lease_generation: int,
        backend_kind: SandboxBackendKind,
        sandbox_id: str,
        teardown_ref: str,
    ) -> SandboxCleanupIntent:
        cleanup_id = f"cleanup:{run_id}:{lease_generation}:{backend_kind}:{sandbox_id}"
        row = self.sandbox_cleanups.setdefault(
            cleanup_id,
            {
                "cleanup_id": cleanup_id,
                "run_id": run_id,
                "lease_generation": lease_generation,
                "backend_kind": backend_kind,
                "sandbox_id": sandbox_id,
                "teardown_ref": teardown_ref,
                "status": "pending",
                "attempt_count": 0,
                "next_attempt_at": self.clock_ms,
            },
        )
        if row["teardown_ref"] != teardown_ref:
            raise RuntimeError(f"sandbox cleanup identity conflict for {sandbox_id!r}")
        return self._sandbox_cleanup_record(row)

    async def claim_sandbox_cleanups(
        self,
        owner: str,
        *,
        run_id: str | None = None,
        limit: int = 100,
        lease_ms: int = 30_000,
    ) -> list[SandboxCleanupIntent]:
        due = [
            row
            for row in self.sandbox_cleanups.values()
            if row["status"] in {"pending", "processing"}
            and _as_int(row["next_attempt_at"]) <= self.clock_ms
            and (run_id is None or row["run_id"] == run_id)
        ]
        due.sort(
            key=lambda row: (
                _as_int(row["next_attempt_at"]),
                str(row["cleanup_id"]),
            )
        )
        claimed: list[SandboxCleanupIntent] = []
        for row in due[:limit]:
            row["status"] = "processing"
            row["cleanup_owner"] = owner
            row["attempt_count"] = _as_int(row["attempt_count"]) + 1
            row["next_attempt_at"] = self.clock_ms + lease_ms
            claimed.append(self._sandbox_cleanup_record(row))
        return claimed

    async def complete_sandbox_cleanup(self, cleanup_id: str) -> bool:
        row = self.sandbox_cleanups.get(cleanup_id)
        if row is None or row["status"] == "completed":
            return False
        row["status"] = "completed"
        row["cleanup_owner"] = None
        row["last_error"] = None
        return True

    async def reschedule_sandbox_cleanup(
        self, cleanup_id: str, error: str, *, retry_delay_ms: int
    ) -> bool:
        row = self.sandbox_cleanups.get(cleanup_id)
        if row is None or row["status"] == "completed":
            return False
        row["status"] = "pending"
        row["cleanup_owner"] = None
        row["last_error"] = error[:1_000]
        row["next_attempt_at"] = self.clock_ms + retry_delay_ms
        return True

    async def _queue_bound_sandbox_cleanup(self, run_id: str) -> None:
        sandbox_id = self.sandbox_ids.get(run_id)
        generation = self.sandbox_generations.get(run_id)
        binding = self.sandbox_bindings.get(run_id)
        if sandbox_id is None:
            return
        if generation is None or binding is None:
            raise RuntimeError(
                "terminal sandbox binding has incomplete cleanup identity"
            )
        await self.register_sandbox_cleanup(
            run_id=run_id,
            lease_generation=generation,
            backend_kind=binding[0],
            sandbox_id=sandbox_id,
            teardown_ref=binding[1],
        )

    @staticmethod
    def _sandbox_cleanup_record(row: Mapping[str, object]) -> SandboxCleanupIntent:
        return SandboxCleanupIntent(
            cleanup_id=str(row["cleanup_id"]),
            run_id=str(row["run_id"]),
            lease_generation=_as_int(row["lease_generation"]),
            backend_kind=cast(SandboxBackendKind, row["backend_kind"]),
            sandbox_id=str(row["sandbox_id"]),
            teardown_ref=str(row["teardown_ref"]),
            attempt_count=_as_int(row["attempt_count"]),
            next_attempt_at=_as_int(row["next_attempt_at"]),
        )


@dataclass
class FakeToolCall:
    tool_call_id: str
    tool_name: str
    input: dict[str, object] | None = None
    output: object = None
    error: str | None = None
    completed: bool = True
    deltas: Sequence[object] = ()

    @property
    def output_deltas(self) -> AsyncIterator[object]:
        return aiter_items(tuple(self.deltas))


@dataclass
class FakeModel:
    text_deltas: Sequence[str] = ()
    reasoning_deltas: Sequence[str] = ()
    output_message: AIMessage | None = None
    message_id: str | None = "seg"
    namespace: list[str] = field(default_factory=list[str])
    node: str | None = "model"

    @property
    def text(self) -> AsyncIterator[str]:
        return aiter_items(self.text_deltas)

    @property
    def reasoning(self) -> AsyncIterator[str]:
        return aiter_items(self.reasoning_deltas)


@dataclass
class FakeSubagentRun:
    name: str | None = "researcher"
    trigger_call_id: str | None = "sub-call-1"
    task_input: str | None = "investigate"
    status: str = "success"
    models: Sequence[FakeModel] = ()
    tool_views: Sequence[FakeToolCall] = ()

    @property
    def messages(self) -> AsyncIterator[FakeModel]:
        return aiter_items(self.models)

    @property
    def tool_calls(self) -> AsyncIterator[FakeToolCall]:
        return aiter_items(self.tool_views)

    @property
    def subagents(self) -> AsyncIterator["FakeSubagentRun"]:
        return aiter_items(())

    @property
    def custom(self) -> AsyncIterator[object]:
        return aiter_items(())


@dataclass
class FakeRunStream:
    models: Sequence[FakeModel] = ()
    tool_views: Sequence[FakeToolCall] = ()
    subagent_runs: Sequence[FakeSubagentRun] = ()
    custom_items: Sequence[object] = ()
    is_interrupted: bool = False
    raise_on_messages: bool = False
    context_exited: bool = False

    @property
    def messages(self) -> AsyncIterator[FakeModel]:
        if self.raise_on_messages:
            raise RuntimeError("boom")
        return aiter_items(self.models)

    @property
    def tool_calls(self) -> AsyncIterator[FakeToolCall]:
        return aiter_items(self.tool_views)

    @property
    def subagents(self) -> AsyncIterator[FakeSubagentRun]:
        return aiter_items(self.subagent_runs)

    @property
    def custom(self) -> AsyncIterator[object]:
        return aiter_items(self.custom_items)

    async def interrupted(self) -> bool:
        return self.is_interrupted

    async def __aenter__(self) -> "FakeRunStream":
        self.context_exited = False
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.context_exited = True
        return None


@dataclass
class FakeState:
    interrupts: tuple[Interrupt, ...] = ()
    values: Mapping[str, object] = field(default_factory=dict[str, object])


@dataclass
class FakeAgent:
    run: FakeRunStream = field(default_factory=FakeRunStream)
    state: FakeState = field(default_factory=FakeState)
    raise_on_stream: Exception | None = None
    seen_payloads: list[object] = field(default_factory=list[object])
    seen_config: dict[str, object] = field(default_factory=dict[str, object])
    # 每次 astream_events 依序消费一个 gate（不足则不阻塞）：模拟长时运行与任务竞态。
    gates: list[asyncio.Event] = field(default_factory=list[asyncio.Event])
    seen_contexts: list[object] = field(default_factory=list[object])
    _calls: int = 0

    async def astream_events(
        self,
        payload: object,
        *,
        version: str,
        config: RunnableConfig,
        transformers: Sequence[object],
        context: object | None = None,
    ) -> FakeRunStream:
        self.seen_payloads.append(payload)
        self.seen_contexts.append(context)
        self.seen_config.update(config)
        call = self._calls
        self._calls += 1
        if call < len(self.gates):
            await self.gates[call].wait()
        if self.raise_on_stream is not None:
            raise self.raise_on_stream
        return self.run

    async def aget_state(
        self, config: RunnableConfig, *, subgraphs: bool = False
    ) -> FakeState:
        return self.state


def text_model(text: str, *, msg_id: str = "seg") -> FakeModel:
    return FakeModel(
        text_deltas=(text,),
        output_message=AIMessage(content=text, id=msg_id),
        message_id=msg_id,
    )


def text_run(text: str = "done") -> FakeRunStream:
    return FakeRunStream(models=(text_model(text),))


def request(
    run_id: str,
    *,
    session_id: str = "s1",
    thread_id: str = "c1",
    namespace: str = "local:s1",
    content: str = "hello",
    approval_tools: tuple[str, ...] = (),
    review_tools: tuple[str, ...] = (),
) -> RunRequest:
    del thread_id, namespace, approval_tools, review_tools
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        session_id=session_id,
        feature_key="chat",
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="test-tenant",
            actor=IdentityRef(kind="user", opaque_ref="test-actor"),
            subject=IdentityRef(kind="user", opaque_ref="test-subject"),
            identity_assertion_ref="test-assertion",
        ),
        input=RunInput(message_id=f"{run_id}-m", content=content),
    )


def usage_recorder() -> tuple[
    Callable[[int, int], Awaitable[tuple[int, int]]], dict[str, int]
]:
    """invoke_once 用量入账的测试替身：返回 (recorder, 累计观测)。"""
    seen = {"input": 0, "output": 0}

    async def record(input_tokens: int, output_tokens: int) -> tuple[int, int]:
        seen["input"] += input_tokens
        seen["output"] += output_tokens
        return (seen["input"], seen["output"])

    return record, seen


async def finish_run(
    repository: "RunRepository",
    run_id: str,
    lease: LeaseFence,
) -> bool:
    """Test setup using the real terminal port; never a production compatibility API."""
    result = await repository.finalize_terminal(
        run_id,
        ExecutionTerminalAuthority(lease=lease),
        RunTerminalOutcome(payload=RunCompletedPayload(status="completed"), usage=None),
        tuple(await repository.list_delivery_journal(run_id)),
    )
    return result.status == "committed"


def terminal_emitter(
    emitter: "RunEmitter",
    claim: Callable[[], Awaitable[bool]],
    recorder: Callable[[int, int], Awaitable[tuple[int, int]]],
) -> Callable[
    [RunCompletedPayload | RunFailedPayload, tuple[int, int]], Awaitable[bool]
]:
    """Pure runnable test callback, not evidence of persistent atomicity."""

    async def finish(
        payload: RunCompletedPayload | RunFailedPayload, usage: tuple[int, int]
    ) -> bool:
        totals = await recorder(*usage)
        if not await claim():
            return False
        if isinstance(payload, RunCompletedPayload) and payload.status == "completed":
            payload = RunCompletedPayload(
                status="completed",
                token_usage=(
                    TokenUsage(input_tokens=totals[0], output_tokens=totals[1])
                    if any(totals)
                    else None
                ),
            )
        await emitter.emit(payload)
        return True

    return finish


def repository_terminal_callback(
    repository: "RunRepository",
    bus: "StreamProtocol",
    run_id: str,
    lease: LeaseFence,
) -> Callable[
    [RunCompletedPayload | RunFailedPayload, tuple[int, int]], Awaitable[bool]
]:
    async def finish(
        payload: RunCompletedPayload | RunFailedPayload, usage: tuple[int, int]
    ) -> bool:
        result = await repository.finalize_terminal(
            run_id,
            ExecutionTerminalAuthority(lease=lease),
            RunTerminalOutcome(
                payload=payload,
                usage=RunUsageSegment(input_tokens=usage[0], output_tokens=usage[1]),
            ),
            tuple(await repository.list_delivery_journal(run_id)),
        )
        for frame in result.retained_frames:
            try:
                await bus.publish(
                    run_events_stream(run_id),
                    outbox_wire_event(frame),
                    maxlen=RUN_EVENTS_MAXLEN,
                )
                await repository.mark_critical_published(run_id, frame.durable_seq)
            except Exception:
                break
        return result.status in {"committed", "replayed"}

    return finish


async def read_unpaused_interaction(
    *,
    request: RunRequest,
    lease: LeaseFence,
    handle: AgentHandle,
    target: PauseReadTarget | ResumeReadTarget,
) -> NativePauseRead | PreparedNativeResume | ObservedNativeResume | ReplayedResume:
    """Explicit non-HITL fixture: inspect native state and reject any pending work.

    This reader supplies no checkpoint evidence. Tests with pauses/resume must
    provide a real reader or explicitly scripted evidence instead of using it.
    """
    del lease
    assert isinstance(target, PauseReadTarget), "resume needs explicit evidence fixture"
    state = await handle.runnable.aget_state(
        {"configurable": {"thread_id": RunScope.of(request).scoped_thread_id}},
        subgraphs=True,
    )
    assert not state.interrupts, "pending native state needs explicit evidence fixture"
    raise InteractionConflict("native_not_paused")


def settled_state_callback(
    agent: AgentRunnable, thread_id: str
) -> Callable[
    [Callable[[], Awaitable[tuple[int, int]]]],
    Awaitable[Literal["active", "waiting", "unknown"]],
]:
    """Invoke-only fixture records actual state; it does not claim durable binding."""

    async def settle(
        seal_usage: Callable[[], Awaitable[tuple[int, int]]],
    ) -> Literal["active", "waiting", "unknown"]:
        if isinstance(agent, FakeAgent):
            assert agent.run.context_exited, "settlement precedes native drain"
        state = await agent.aget_state(
            {"configurable": {"thread_id": thread_id}}, subgraphs=True
        )
        if state.interrupts:
            await seal_usage()
            return "waiting"
        return "active"

    return settle


def interaction_pause_fixture(
    run: RunRequest, *, pause_ref: str = "pause-1"
) -> DurablePauseSnapshot:
    """Explicit trusted storage fixture, not evidence of native writes."""
    raw = canonical_interaction_bytes(
        {
            "format": "kokoro-agent:pause-collection:1",
            "groups": [
                {
                    "group_id": "group-1",
                    "items": [
                        {
                            "item_id": "item-A",
                            "request_id": "call-A",
                            "kind": "tool_approval",
                            "allowed_decisions": ["approve", "edit", "reject"],
                            "display": {
                                "name": "danger",
                                "description": "Danger",
                                "editable": True,
                                "input_schema": dict[str, JsonValue](),
                            },
                            "validation": None,
                        }
                    ],
                }
            ],
            "locator": {
                "groups": [
                    {
                        "group_id": "group-1",
                        "thread_id": RunScope.of(run).scoped_thread_id,
                        "checkpoint_ns": "",
                        "checkpoint_id": "unit-checkpoint",
                        "tasks": [
                            {
                                "task_id": "unit-task",
                                "interrupt_id": "unit-interrupt",
                                "item_ids": ["item-A"],
                            }
                        ],
                    }
                ]
            },
        }
    )
    return DurablePauseSnapshot(
        pause_ref=pause_ref, canonical_bytes=raw, digest=hashlib.sha256(raw).hexdigest()
    )


async def admit_resume_fixture(
    repository: FakeRunRepository,
    run: RunRequest,
    *,
    command_id: str,
    decisions: list[dict[str, JsonValue]],
    revision: int = 1,
    pause_ref: str = "pause-1",
) -> RunResume:
    """The same strictly typed bytes/digest enter fake ingress and port decoding."""
    command = RunResume.model_validate(
        {
            "kind": "run.resume",
            "run_id": run.run_id,
            "session_id": run.session_id,
            "command_id": command_id,
            "expected_pause_revision": revision,
            "pause_ref": pause_ref,
            "decisions": decisions,
        }
    )
    digest = control_request_digest(command)
    command = command.model_copy(update={"request_digest": digest})
    await repository.admit_control(
        run.run_id, command_id, digest, command.model_dump_json()
    )
    return command


class InitialPauseThenUnknownReader:
    """Explicit supervisor-only scenario: initial pause, permitted call, unknown.

    Values are declared fixtures, never inferred from a native interrupt. Actual
    adapter mapping/consumption is tested separately with native and real PG.
    A started attempt always remains unknown here: no fake successful evidence.
    """

    def __init__(
        self, repository: FakeRunRepository, *, native_value: JsonValue
    ) -> None:
        self.repository = repository
        self.native_value = native_value
        self.initial_reads: set[str] = set()
        self.calls: list[PauseReadTarget | ResumeReadTarget] = []

    async def __call__(
        self,
        *,
        request: RunRequest,
        lease: LeaseFence,
        handle: AgentHandle,
        target: PauseReadTarget | ResumeReadTarget,
    ) -> NativePauseRead | PreparedNativeResume | ObservedNativeResume | ReplayedResume:
        del handle
        self.calls.append(target)
        if isinstance(target, PauseReadTarget):
            assert target.command_id is None
            if request.run_id not in self.initial_reads:
                self.initial_reads.add(request.run_id)
                raise InteractionConflict("native_not_paused")
            pause = interaction_pause_fixture(request)
            parsed = decode_pause_snapshot(pause)
            locator = parsed.locator.groups[0]
            empty_digest = hashlib.sha256(canonical_interaction_bytes([])).hexdigest()
            raw = canonical_interaction_bytes(
                {
                    "format": "kokoro-agent:checkpoint-observation:1",
                    "run_id": request.run_id,
                    "generation": lease.generation,
                    "command_id": None,
                    "attempt_id": None,
                    "kind": "pause",
                    "probe_read_id": None,
                    "facts": [
                        {
                            "group_id": locator.group_id,
                            "thread_id": locator.thread_id,
                            "checkpoint_ns": locator.checkpoint_ns,
                            "checkpoint_id": locator.checkpoint_id,
                            "parent_checkpoint_id": None,
                            "successors": [],
                            "tasks": [
                                {
                                    "task_id": "unit-task",
                                    "interrupt_id": "unit-interrupt",
                                    "item_ids": ["item-A"],
                                    "resume_length": 0,
                                    "resume_digest": empty_digest,
                                    "error": False,
                                    "interrupt_digest": hashlib.sha256(
                                        pause.canonical_bytes
                                    ).hexdigest(),
                                }
                            ],
                        }
                    ],
                    "pause": parsed.model_dump(mode="json", exclude_unset=True),
                    "pause_ref": pause.pause_ref,
                }
            )
            observation = CheckpointObservation(
                target=ObservationTarget(
                    generation=lease.generation, command_id=None, attempt_id=None
                ),
                kind="pause",
                canonical_bytes=raw,
                digest=hashlib.sha256(raw).hexdigest(),
            )
            return NativePauseRead(pause=pause, observation=observation)
        context = await self.repository.read_resume_context(request, target.command_id)
        assert context is not None
        if context.intent.status in {IntentStatus.RECONCILED, IntentStatus.TERMINAL}:
            return ReplayedResume(
                snapshot=context.snapshot,
                original_intent=context.intent,
                accepted_source_index=context.snapshot.source_index,
            )
        if context.intent.status is IntentStatus.ACCEPTED:
            attempt = "unit-attempt-" + target.command_id
            raw = canonical_interaction_bytes(
                {
                    "format": "kokoro-agent:resume-dispatch:1",
                    "attempt_id": attempt,
                    "groups": [
                        {
                            "group_id": "group-1",
                            "tasks": [
                                {
                                    "task_id": "unit-task",
                                    "interrupt_id": "unit-interrupt",
                                    "pre_resume_length": 0,
                                    "pre_resume_digest": hashlib.sha256(
                                        canonical_interaction_bytes([])
                                    ).hexdigest(),
                                    "expected_append_digest": hashlib.sha256(
                                        canonical_interaction_bytes([self.native_value])
                                    ).hexdigest(),
                                }
                            ],
                        }
                    ],
                }
            )
            return PreparedNativeResume(
                plan=ResumeDispatchPlan(attempt_id=attempt, canonical_bytes=raw),
                command=Command(resume={"unit-interrupt": self.native_value}),
            )
        assert (
            context.intent.attempt_id is not None
            and context.attempt_generation is not None
        )
        raw = canonical_interaction_bytes(
            {
                "format": "kokoro-agent:checkpoint-observation:1",
                "run_id": request.run_id,
                "generation": lease.generation,
                "command_id": target.command_id,
                "attempt_id": context.intent.attempt_id,
                "kind": "read",
                "probe_read_id": None,
                "facts": [],
            }
        )
        observation = CheckpointObservation(
            target=ObservationTarget(
                generation=lease.generation,
                command_id=target.command_id,
                attempt_id=context.intent.attempt_id,
            ),
            kind="read",
            canonical_bytes=raw,
            digest=hashlib.sha256(raw).hexdigest(),
        )
        return ObservedNativeResume(
            observations=(observation,),
            evidence=UnknownResumeEvidence(
                command_id=target.command_id,
                attempt_id=context.intent.attempt_id,
                attempt_generation=context.attempt_generation,
                pause_revision=context.intent.pause_revision,
                pause_ref=context.intent.pause_ref,
                collection_digest=context.original_pause.digest,
                observation_digests=(observation.digest,),
            ),
        )


async def admit_control_fixture(
    repository: FakeRunRepository,
    message: RunResume | RunCancel | RunSteer,
) -> RunResume | RunCancel | RunSteer:
    if isinstance(message, RunResume):
        digest = control_request_digest(message)
    else:
        payload = message.model_dump(
            mode="json", exclude={"command_id", "request_digest"}, exclude_none=True
        )
        digest = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
        )
    normalized = message.model_copy(update={"request_digest": digest})
    await repository.admit_control(
        normalized.run_id, normalized.command_id, digest, normalized.model_dump_json()
    )
    return normalized


def _mutable_registry(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


@contextmanager
def isolated_native_registry() -> Generator[None, None, None]:
    """Raw SDK unit probes restore exactly the registry state they inherited."""
    from deepagents.profiles import _builtin_profiles as bootstrap
    from deepagents.profiles.harness import harness_profiles
    from deepagents.profiles.provider import provider_profiles

    harness: object = getattr(harness_profiles, "_HARNESS_PROFILES")
    provider: object = getattr(provider_profiles, "_PROVIDER_PROFILES")
    assert _mutable_registry(harness) and _mutable_registry(provider)
    harness_before, provider_before = dict(harness), dict(provider)
    fields = ("_loaded", "_loading_thread_id", "_BOOTSTRAP_HARNESS_KEYS")
    before: dict[str, object] = {name: getattr(bootstrap, name) for name in fields}
    try:
        yield
    finally:
        harness.clear()
        harness.update(harness_before)
        provider.clear()
        provider.update(provider_before)
        for name, value in before.items():
            setattr(bootstrap, name, value)
