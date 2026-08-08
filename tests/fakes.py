"""测试共用的强类型 fake：总线、run 状态存储、v3 投影流与 agent。"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeVar

from langchain_core.messages import AIMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Interrupt
from pydantic import BaseModel, JsonValue

from kokoro_agent.contract import (
    AgentEvent,
    ControlReceiptStatus,
    ExecutionContextIntentRoot,
    ModelConfig,
    Permissions,
    RunInput,
    RunRequest,
    RunControlReceiptPayload,
    RuntimeConfig,
    RuntimeContext,
    PlanProposedPayload,
    ToolAwaitingApprovalPayload,
    agent_event_adapter,
    run_events_stream,
)
from kokoro_agent.contract import REQUESTS_STREAM
from kokoro_agent.evidence.models import (
    DurableOutputDraft,
    DurableOutputRecord,
    make_durable_output_record,
)
from kokoro_agent.presentation.model import (
    DeliveryRecord,
    PresentationState,
)
from kokoro_agent.presentation.planner import plan_presentation_batch
from kokoro_agent.storage.ledger import (
    ControlInboxRecord,
    DurableRetentionStats,
    OutboxFrame,
    ReceiptReconcile,
    StagedFrame,
    ToolJournalRecord,
)
from kokoro_agent.storage.execution_context import (
    ClaimedCompletionFrames,
    CompletionEventDraft,
    CompletedExecutionContext,
    DurableCompletionFrame,
    ExecutionCheckpoint,
    ExecutionContextBinding,
    ExecutionContextConflict,
)
from kokoro_agent.storage.owner_event import OwnerEventCommitResult
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
        self._control = {stream: tuple(items) for stream, items in (control or {}).items()}

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
        items = self._inbound if stream == REQUESTS_STREAM else self._control.get(stream, ())
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


class FakeLedger:
    """协议等价的内存 store：租约以 leases dict 表达，None=暂停哨兵。"""

    def __init__(self) -> None:
        self.requests: dict[str, RunRequest] = {}
        self.terminals: set[str] = set()
        self.leases: dict[str, int | None] = {}
        self.owners: dict[str, str] = {}
        self.renewed: list[str] = []
        self.paused_runs: list[str] = []
        self.expired: list[RunRequest] = []
        self.tool_results: dict[tuple[str, str], tuple[str, bool]] = {}
        self.token_totals: dict[str, int] = {}
        self.usage_totals: dict[str, tuple[int, int]] = {}
        self.steers: dict[str, list[tuple[str, str]]] = {}
        self.sandbox_ids: dict[str, str] = {}
        self.terminal_at: dict[str, int] = {}
        self.clock_ms = 0
        # dispatch CAS 记录（run_id → status）：默认无记录=放行；测试可预置 pending/claimed/expired。
        self.dispatches: dict[str, str] = {}
        self.dispatch_deadlines: dict[str, int] = {}
        self.dlq: list[tuple[str, str, str]] = []
        # R4 critical outbox：per-run durable_seq 计数、local fence、outbox 行；回执/清单由测试 seed。
        self.durable_counter: dict[str, int] = {}
        self.terminal_fence: dict[str, int] = {}
        self.outbox: dict[str, list[dict[str, object]]] = {}
        self.semantic_critical: dict[tuple[str, str], tuple[str, str, int, str]] = {}
        self.output_records: dict[str, list[DurableOutputRecord]] = {}
        self.output_sources: dict[tuple[str, str], tuple[str, DurableOutputRecord]] = {}
        self.output_batches: dict[
            tuple[str, str],
            tuple[str, tuple[tuple[str, DurableOutputRecord], ...]],
        ] = {}
        self.output_text_seq: dict[tuple[str, str], int] = {}
        self.presentation_records: dict[str, list[DeliveryRecord]] = {}
        self.presentation_planner_states: dict[str, PresentationState] = {}
        self.presentation_source_commits: dict[
            tuple[str, str], tuple[str, tuple[DeliveryRecord, ...]]
        ] = {}
        self.run_stream_producers: dict[str, tuple[str, int]] = {}
        self.assembly_digests: dict[str, str] = {}
        self.owner_heads: dict[str, int] = {}
        # session 写域（测试 seed）：run_event_receipts 行 + run_receipt_manifests 单行。
        self.receipts: dict[str, list[dict[str, object]]] = {}
        self.manifests: dict[str, dict[str, object]] = {}
        # control inbox（R2）：run_id → [{decision_id,fingerprint,status,body}]，keep-first。
        self.control_inbox: dict[str, list[dict[str, str | None]]] = {}
        # tool effect journal（R3）：(run_id, tool_call_id) → {name,status,result,is_error}。
        self.tool_journal: dict[tuple[str, str], dict[str, object]] = {}
        self.execution_bindings: dict[str, ExecutionContextBinding] = {}
        self.execution_completions: dict[str, CompletedExecutionContext] = {}
        self.execution_continuations: dict[str, str] = {}
        self.retention_archived: set[str] = set()

    async def try_claim(self, request: RunRequest, owner: str = "test-consumer") -> bool:
        if request.run_id in self.requests:
            return False
        self.requests[request.run_id] = request
        self.leases[request.run_id] = 1
        self.owners[request.run_id] = owner
        self.run_stream_producers[request.run_id] = (
            "run_stream_"
            + hashlib.sha256(f"fake\0{request.run_id}".encode()).hexdigest()[:32],
            1,
        )
        return True

    async def bind_assembly_digest(
        self, run_id: str, assembly_digest: str, lease_owner_ref: str
    ) -> None:
        from kokoro_agent.storage.assembly import AssemblyDigestConflict

        if self.owners.get(run_id) != lease_owner_ref:
            raise AssemblyDigestConflict()
        current = self.assembly_digests.setdefault(run_id, assembly_digest)
        if current != assembly_digest:
            raise AssemblyDigestConflict()

    async def require_assembly_digest(self, run_id: str, assembly_digest: str) -> None:
        from kokoro_agent.storage.assembly import AssemblyDigestConflict

        if self.assembly_digests.get(run_id) != assembly_digest:
            raise AssemblyDigestConflict()

    async def owner_event_head(self, run_id: str) -> int:
        return self.owner_heads.get(run_id, 0)

    async def commit_owner_event(
        self,
        *,
        run_id: str,
        expected_index: int,
        kind: str,
        payload: BaseModel,
        lease_owner_ref: str,
        agent_thread_ref: str | None,
        semantic_owner_ref: str | None = None,
    ) -> OwnerEventCommitResult:
        from kokoro_agent.execution.events import (
            CRITICAL_KINDS,
            TERMINAL_KINDS,
        )
        from kokoro_agent.evidence.models import (
            durable_output_drafts_for_event,
            is_durable_output_capable_event,
        )

        if self.owners.get(run_id, lease_owner_ref) != lease_owner_ref:
            return OwnerEventCommitResult(status="fence_lost")
        if run_id in self.terminals:
            return OwnerEventCommitResult(status="fence_lost")
        if self.owner_heads.get(run_id, 0) != expected_index:
            raise ValueError("OWNER_EVENT_HEAD_CONFLICT")
        timestamp = self.clock_ms
        owner_ref = "evt_" + hashlib.sha256(
            f"owner-v1\0{run_id}\0{expected_index}".encode()
        ).hexdigest()[:32]
        event = agent_event_adapter.validate_python({
            "kind": kind,
            "run_id": run_id,
            "index": expected_index,
            "timestamp": timestamp,
            "event_id": owner_ref,
            "payload": payload,
        })
        semantic_key = (
            f"action_owner:{semantic_owner_ref or payload.tool_id}:{payload.tool_id}"
            if isinstance(payload, ToolAwaitingApprovalPayload)
            else f"plan.proposed:{payload.owner_ref}"
            if isinstance(payload, PlanProposedPayload)
            else None
        )
        semantic_payload_json = json.dumps(
            payload.model_dump(mode="json", exclude_none=True)
        )
        if semantic_key is not None:
            existing = self.semantic_critical.get((run_id, semantic_key))
            if existing is not None:
                existing_kind, existing_digest, _seq, _event_id = existing
                digest = hashlib.sha256(semantic_payload_json.encode()).hexdigest()
                if existing_kind != kind or existing_digest != digest:
                    raise ValueError(
                        f"semantic critical frame conflict for {semantic_key!r}"
                    )
                return OwnerEventCommitResult(status="idempotent")
        before = copy.deepcopy(self.__dict__)
        try:
            payload_json = json.dumps(
                payload.model_dump(mode="json", exclude_none=True),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            payload_digest = hashlib.sha256(payload_json.encode()).hexdigest()
            if is_durable_output_capable_event(payload):
                drafts = durable_output_drafts_for_event(payload)
                if await self.append_durable_outputs(
                    run_id,
                    owner_ref,
                    drafts,
                    recorded_at_ms=timestamp,
                    source_payload_sha256=payload_digest,
                ) is None:
                    raise ValueError("OWNER_EVENT_OUTPUT_REJECTED")
            if agent_thread_ref is not None:
                if await self.append_presentation_event(
                    event, agent_thread_ref=agent_thread_ref
                ) is None:
                    raise ValueError("OWNER_EVENT_PRESENTATION_REJECTED")
            if kind in CRITICAL_KINDS:
                staged = await self.stage_critical_frame(
                    run_id,
                    kind,
                    expected_index,
                    timestamp,
                    semantic_payload_json,
                    terminal=kind in TERMINAL_KINDS,
                    semantic_key=semantic_key,
                )
                if staged is None or not staged.created:
                    return OwnerEventCommitResult(status="idempotent")
                event = event.model_copy(
                    update={"durable_seq": staged.durable_seq, "event_id": staged.event_id}
                )
            self.owner_heads[run_id] = expected_index + 1
            if kind in TERMINAL_KINDS:
                self.terminals.add(run_id)
            return OwnerEventCommitResult(status="committed", event=event)
        except BaseException:
            self.__dict__.clear()
            self.__dict__.update(before)
            raise

    async def commit_control_receipt(
        self,
        *,
        run_id: str,
        decision_id: str,
        status: ControlReceiptStatus,
    ) -> OwnerEventCommitResult:
        box = self.control_inbox.get(run_id, [])
        allowed = {"persisted", "applied"} if status == "persisted" else {"applied"}
        if run_id in self.terminals or not any(
            entry["decision_id"] == decision_id and entry["status"] in allowed
            for entry in box
        ):
            return OwnerEventCommitResult(status="fence_lost")
        payload = RunControlReceiptPayload(
            decision_id=decision_id,
            control_status=status,
        )
        payload_json = json.dumps(
            payload.model_dump(mode="json", exclude_none=True),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        semantic_key = f"control_receipt:{decision_id}:{status}"
        existing = self.semantic_critical.get((run_id, semantic_key))
        digest = hashlib.sha256(payload_json.encode()).hexdigest()
        if existing is not None:
            existing_kind, existing_digest, _seq, _event_id = existing
            if existing_kind != "run.control.receipt" or existing_digest != digest:
                raise ValueError(f"semantic critical frame conflict for {semantic_key!r}")
            return OwnerEventCommitResult(status="idempotent")
        index = self.owner_heads.get(run_id, 0)
        staged = await self.stage_critical_frame(
            run_id,
            "run.control.receipt",
            index,
            self.clock_ms,
            payload_json,
            terminal=False,
            semantic_key=semantic_key,
        )
        if staged is None or not staged.created:
            return OwnerEventCommitResult(status="idempotent")
        event = agent_event_adapter.validate_python(
            {
                "kind": "run.control.receipt",
                "run_id": run_id,
                "index": index,
                "timestamp": self.clock_ms,
                "durable_seq": staged.durable_seq,
                "event_id": staged.event_id,
                "payload": payload,
            }
        )
        self.owner_heads[run_id] = index + 1
        return OwnerEventCommitResult(status="committed", event=event)

    async def claim_dispatch(self, run_id: str, consumer: str = "test-consumer") -> bool:
        # 无 intent 记录=放行（与 MongoLedger 同语义）；pending 且未过期=赢转 claimed；
        # 已 claimed/expired 或已过 deadline=丢弃。
        status = self.dispatches.get(run_id)
        if status is None:
            return True
        deadline = self.dispatch_deadlines.get(run_id)
        if status == "pending" and (deadline is None or deadline > self.clock_ms):
            self.dispatches[run_id] = "claimed"
            return True
        return False

    async def quarantine_dispatch(
        self, raw_hash: str, source: str, reason: str
    ) -> None:
        self.dlq.append((raw_hash, source, reason))

    async def stage_critical_frame(
        self,
        run_id: str,
        kind: str,
        index: int,
        timestamp: int,
        payload_json: str,
        *,
        terminal: bool,
        semantic_key: str | None = None,
    ) -> StagedFrame | None:
        if semantic_key is not None:
            identity = (run_id, semantic_key)
            digest = hashlib.sha256(payload_json.encode()).hexdigest()
            existing = self.semantic_critical.get(identity)
            if existing is not None:
                existing_kind, existing_digest, seq, event_id = existing
                if existing_kind != kind or existing_digest != digest:
                    raise ValueError(
                        f"semantic critical frame conflict for {semantic_key!r}"
                    )
                return StagedFrame(durable_seq=seq, event_id=event_id, created=False)
        seq = self.durable_counter.get(run_id, 0) + 1
        self.durable_counter[run_id] = seq
        if terminal and run_id not in self.terminal_fence:
            self.terminal_fence[run_id] = seq
        fence = self.terminal_fence.get(run_id)
        event_id = f"evt_fake_{run_id}_{seq}"
        if semantic_key is not None:
            self.semantic_critical[(run_id, semantic_key)] = (
                kind,
                hashlib.sha256(payload_json.encode()).hexdigest(),
                seq,
                event_id,
            )
        rows = self.outbox.setdefault(run_id, [])
        if fence is not None and seq > fence:
            rows.append({"durable_seq": seq, "event_id": event_id, "kind": kind, "status": "superseded"})
            return None
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
        return StagedFrame(durable_seq=seq, event_id=event_id, created=True)

    async def append_durable_outputs(
        self,
        run_id: str,
        source_event_ref: str,
        drafts: tuple[DurableOutputDraft, ...],
        *,
        recorded_at_ms: int,
        source_payload_sha256: str,
    ) -> tuple[DurableOutputRecord, ...] | None:
        if len(source_payload_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in source_payload_sha256
        ):
            raise ValueError("OUTPUT_SOURCE_PAYLOAD_DIGEST_INVALID")
        batch_identity = (run_id, source_event_ref)
        source_prefix = f"{source_event_ref}:"
        persisted_source_rows = [
            value
            for (source_run_id, source_ref), value in self.output_sources.items()
            if source_run_id == run_id
            and source_ref.startswith(source_prefix)
            and source_ref.removeprefix(source_prefix).isdigit()
        ]
        existing_batch = self.output_batches.get(batch_identity)
        if existing_batch is not None:
            existing_source_payload_sha256, existing_records = existing_batch
            if len(existing_records) != len(drafts):
                raise ValueError("OUTPUT_SOURCE_BATCH_CONFLICT")
            if existing_source_payload_sha256 != source_payload_sha256:
                raise ValueError("OUTPUT_SOURCE_CONFLICT")
            if len(persisted_source_rows) != len(existing_records):
                raise ValueError("OUTPUT_SOURCE_PARTIAL")
            replayed: list[DurableOutputRecord] = []
            for (draft_payload_sha256, record), draft in zip(
                existing_records, drafts, strict=True
            ):
                if draft_payload_sha256 != draft.source_payload_sha256:
                    raise ValueError("OUTPUT_SOURCE_CONFLICT")
                replayed.append(record)
            return tuple(replayed)
        if persisted_source_rows:
            raise ValueError("OUTPUT_SOURCE_PARTIAL")
        identities = [
            (run_id, f"{source_event_ref}:{ordinal}")
            for ordinal in range(len(drafts))
        ]
        existing = [self.output_sources.get(identity) for identity in identities]
        if any(item is not None for item in existing):
            raise ValueError("OUTPUT_SOURCE_PARTIAL")
        if run_id in self.terminals:
            return None
        producer = self.run_stream_producers.get(run_id)
        if producer is None:
            return None
        records = self.output_records.setdefault(run_id, [])
        next_text_seq = dict(self.output_text_seq)
        appended: list[DurableOutputRecord] = []
        for offset, draft in enumerate(drafts, start=1):
            replaces_through = (
                next_text_seq.get((run_id, draft.text_part_ref), 0)
                if draft.is_text_snapshot and draft.text_part_ref is not None
                else 0
            )
            record = make_durable_output_record(
                run_id=run_id,
                output_seq=len(records) + offset,
                draft=draft,
                replaces_through_output_seq=replaces_through,
                recorded_at_ms=recorded_at_ms,
                producer_instance_ref=producer[0],
                producer_generation=producer[1],
            )
            appended.append(record)
            if draft.text_part_ref is not None:
                next_text_seq[(run_id, draft.text_part_ref)] = record.output_seq
        records.extend(appended)
        for identity, draft, record in zip(identities, drafts, appended, strict=True):
            self.output_sources[identity] = (draft.source_payload_sha256, record)
        self.output_batches[batch_identity] = (
            source_payload_sha256,
            tuple(
                (draft.source_payload_sha256, record)
                for draft, record in zip(drafts, appended, strict=True)
            ),
        )
        self.output_text_seq = next_text_seq
        return tuple(appended)

    async def pull_durable_output_records(
        self, run_id: str, after_output_seq: int, limit: int
    ) -> list[DurableOutputRecord]:
        return [
            record
            for record in self.output_records.get(run_id, [])
            if record.output_seq > after_output_seq
        ][:limit]

    async def append_presentation_event(
        self, event: AgentEvent, *, agent_thread_ref: str
    ) -> tuple[DeliveryRecord, ...] | None:
        producer = self.run_stream_producers.get(event.run_id)
        if producer is None:
            return None
        state = self.presentation_planner_states.get(
            event.run_id, PresentationState()
        )
        batch = plan_presentation_batch(event, state, agent_thread_ref)
        identity = (event.run_id, batch.source_event_ref)
        existing = self.presentation_source_commits.get(identity)
        if existing is not None:
            digest, records = existing
            if digest != batch.source_payload_sha256:
                raise ValueError("PRESENTATION_SOURCE_CONFLICT")
            return records
        records = tuple(
            DeliveryRecord.from_submission(
                run_id=event.run_id,
                delivery_seq=int(submission.source.event_ordinal) + 1,
                submission=submission,
                producer_instance_ref=producer[0],
                producer_generation=producer[1],
            )
            for submission in batch.submissions
        )
        self.presentation_records.setdefault(event.run_id, []).extend(records)
        self.presentation_planner_states[event.run_id] = batch.next_state
        self.presentation_source_commits[identity] = (
            batch.source_payload_sha256,
            records,
        )
        return records

    async def presentation_head(self, run_id: str) -> int:
        return len(self.presentation_records.get(run_id, ()))

    async def get_durable_retention_stats(self) -> DurableRetentionStats:
        return DurableRetentionStats(
            output_records=sum(len(records) for records in self.output_records.values()),
            evidence_records=0,
        )

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None:
        for row in self.outbox.get(run_id, []):
            if row["durable_seq"] == durable_seq and row["status"] == "queued":
                row["status"] = "published"
                row["published_at"] = self.clock_ms
                return

    async def list_unpublished_outbox(self) -> list[OutboxFrame]:
        frames: list[OutboxFrame] = []
        for run_id, rows in self.outbox.items():
            live = sorted(
                (row for row in rows if row["status"] in ("queued", "published")),
                key=lambda row: _as_int(row["durable_seq"]),
            )
            for row in live:
                if row["status"] != "queued":
                    break
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
            seq = rejected[0]
            fence = self.terminal_fence.get(run_id)
            if fence is None or fence > seq:
                self.terminal_fence[run_id] = seq
            return ReceiptReconcile(rejected_seq=seq)
        manifest = self.manifests.get(run_id)
        consumed = _as_int(manifest.get("consumed_seq") or 0) if manifest is not None else 0
        by_seq = {_as_int(r["durable_seq"]): r for r in rows}
        # Only the contiguous causal prefix from consumed+1 may be republished. A queued row,
        # a not-yet-stale published row, or an identity mismatch blocks every successor.
        stale: list[dict[str, object]] = []
        seq = consumed + 1
        while (row := by_seq.get(seq)) is not None:
            receipt = receipts.get(seq)
            if receipt is not None and receipt["status"] == "persisted":
                if row["event_id"] != receipt["event_id"]:
                    break
                seq += 1
                continue
            if row["status"] != "published":
                break
            published_at = row.get("published_at")
            if not isinstance(published_at, int) or now - published_at < republish_grace_ms:
                break
            stale.append(row)
            seq += 1
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
        if manifest is None:
            return ReceiptReconcile(receipt_state_lost=True, republish=republish)
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
            self.outbox[run_id] = [r for r in rows if _as_int(r["durable_seq"]) > advanced]
        fence = self.terminal_fence.get(run_id)
        remaining = [r for r in self.outbox.get(run_id, []) if r["status"] in ("queued", "published")]
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

    async def record_control_inbox(
        self, run_id: str, decision_id: str, fingerprint: str | None, body: str
    ) -> bool:
        # 与 MongoLedger 同语义：run 文档须存在（try_claim 后），keep-first 去重。
        if run_id not in self.requests:
            return False
        box = self.control_inbox.setdefault(run_id, [])
        if any(entry["decision_id"] == decision_id for entry in box):
            return False
        box.append(
            {"decision_id": decision_id, "fingerprint": fingerprint, "status": "persisted", "body": body}
        )
        return True

    async def mark_control_applied(self, run_id: str, decision_id: str) -> None:
        for entry in self.control_inbox.get(run_id, []):
            if entry["decision_id"] == decision_id and entry["status"] == "persisted":
                entry["status"] = "applied"

    async def mark_control_superseded(self, run_id: str, decision_id: str) -> None:
        for entry in self.control_inbox.get(run_id, []):
            if entry["decision_id"] == decision_id and entry["status"] == "persisted":
                entry["status"] = "superseded"

    async def list_pending_control_inbox(self) -> list[ControlInboxRecord]:
        records: list[ControlInboxRecord] = []
        for run_id, box in sorted(self.control_inbox.items()):
            if run_id in self.terminals:
                continue
            for entry in box:
                if entry["status"] != "persisted":
                    continue
                records.append(
                    ControlInboxRecord(
                        run_id=run_id,
                        decision_id=str(entry["decision_id"]),
                        fingerprint=entry["fingerprint"],
                        body=str(entry["body"]),
                    )
                )
        return records

    async def renew(self, run_id: str, owner: str = "test-consumer") -> bool:
        self.renewed.append(run_id)
        if run_id in self.terminals:
            return False
        if self.owners.get(run_id, owner) != owner:
            return False
        self.leases[run_id] = 1
        self.owners.setdefault(run_id, owner)
        return True

    async def adopt(self, run_id: str, owner: str = "test-consumer") -> None:
        if run_id not in self.terminals:
            self.leases[run_id] = 1
            self.owners[run_id] = owner

    async def pause(self, run_id: str) -> None:
        self.paused_runs.append(run_id)
        if run_id not in self.terminals:
            self.leases[run_id] = None

    async def reclaim_expired(self, owner: str = "test-consumer") -> list[RunRequest]:
        out = self.expired
        self.expired = []
        for req in out:
            self.requests[req.run_id] = req
            self.owners[req.run_id] = owner
            self.leases[req.run_id] = 1
            self.run_stream_producers.setdefault(
                req.run_id,
                (
                    "run_stream_"
                    + hashlib.sha256(f"fake\0{req.run_id}".encode()).hexdigest()[:32],
                    1,
                ),
            )
            self.assembly_digests.setdefault(req.run_id, "a" * 64)
        return out

    async def list_paused(self) -> list[str]:
        return sorted(
            run_id
            for run_id, lease in self.leases.items()
            if lease is None and run_id not in self.terminals
        )

    async def get_request(self, run_id: str) -> RunRequest | None:
        return self.requests.get(run_id)

    async def get_execution_context_binding(
        self, run_id: str
    ) -> ExecutionContextBinding | None:
        return self.execution_bindings.get(run_id)

    async def bind_execution_context(
        self, run_id: str, binding: ExecutionContextBinding
    ) -> ExecutionContextBinding:
        current = self.execution_bindings.setdefault(run_id, binding)
        if current != binding:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_CONFLICT")
        return current

    async def update_execution_checkpoint(
        self, run_id: str, checkpoint: ExecutionCheckpoint
    ) -> None:
        binding = self.execution_bindings.get(run_id)
        if binding is None:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_MISSING")
        self.execution_bindings[run_id] = binding.model_copy(
            update={"active_checkpoint": checkpoint}
        )

    async def resolve_execution_parent(
        self,
        *,
        namespace: str,
        anchor: str,
        digest: str,
        continuation_run_id: str | None,
    ) -> ExecutionCheckpoint | None:
        completion = self.execution_completions.get(anchor)
        if completion is None or completion.namespace != namespace or completion.digest != digest:
            return None
        if continuation_run_id is not None:
            current = self.execution_continuations.setdefault(anchor, continuation_run_id)
            if current != continuation_run_id:
                return None
        return completion.checkpoint

    async def try_complete_execution_context(
        self,
        completion: CompletedExecutionContext,
        owner_event: CompletionEventDraft,
        terminal_event: CompletionEventDraft,
        *,
        lease_owner_ref: str,
        agent_thread_ref: str | None,
    ) -> ClaimedCompletionFrames | None:
        if completion.owner_revision != 1 or completion.continuation_run_id is not None:
            raise ValueError("new completion owner must start at revision one")
        if owner_event.kind != "run.owner.completed":
            raise ValueError("completion owner event kind mismatch")
        if terminal_event.kind != "run.completed" or terminal_event.index != owner_event.index + 1:
            raise ValueError("completion terminal event must immediately follow owner")
        if completion.run_id in self.terminals:
            return None
        if self.owners.get(completion.run_id, lease_owner_ref) != lease_owner_ref:
            return None
        if self.owner_heads.get(completion.run_id, 0) != owner_event.index:
            return None
        if completion.anchor in self.execution_completions:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_ANCHOR_COLLISION")
        owner_seq = self.durable_counter.get(completion.run_id, 0) + 1
        terminal_seq = owner_seq + 1
        owner_id = f"evt_fake_{completion.run_id}_{owner_seq}"
        terminal_id = f"evt_fake_{completion.run_id}_{terminal_seq}"
        rows = self.outbox.setdefault(completion.run_id, [])
        rows.extend(
            [
                {
                    **owner_event.model_dump(),
                    "durable_seq": owner_seq,
                    "event_id": owner_id,
                    "status": "queued",
                },
                {
                    **terminal_event.model_dump(),
                    "durable_seq": terminal_seq,
                    "event_id": terminal_id,
                    "status": "queued",
                },
            ]
        )
        self.durable_counter[completion.run_id] = terminal_seq
        self.terminal_fence[completion.run_id] = terminal_seq
        self.terminals.add(completion.run_id)
        self.terminal_at[completion.run_id] = self.clock_ms
        self.owner_heads[completion.run_id] = terminal_event.index + 1
        self.execution_completions[completion.anchor] = completion
        self.semantic_critical[(completion.run_id, "run.owner.completed")] = (
            owner_event.kind,
            hashlib.sha256(owner_event.payload_json.encode()).hexdigest(),
            owner_seq,
            owner_id,
        )
        if agent_thread_ref is not None:
            terminal_agent_event = agent_event_adapter.validate_python(
                {
                    "kind": terminal_event.kind,
                    "run_id": completion.run_id,
                    "index": terminal_event.index,
                    "timestamp": terminal_event.timestamp,
                    "durable_seq": terminal_seq,
                    "event_id": terminal_id,
                    "payload": json.loads(terminal_event.payload_json),
                }
            )
            await self.append_presentation_event(
                terminal_agent_event, agent_thread_ref=agent_thread_ref
            )
        return ClaimedCompletionFrames(
            owner=DurableCompletionFrame(
                **owner_event.model_dump(), durable_seq=owner_seq, event_id=owner_id
            ),
            terminal=DurableCompletionFrame(
                **terminal_event.model_dump(), durable_seq=terminal_seq, event_id=terminal_id
            ),
        )

    async def add_tokens(self, run_id: str, count: int) -> int:
        self.token_totals[run_id] = self.token_totals.get(run_id, 0) + count
        return self.token_totals[run_id]

    async def add_usage(self, run_id: str, input_tokens: int, output_tokens: int) -> tuple[int, int]:
        cur_in, cur_out = self.usage_totals.get(run_id, (0, 0))
        self.usage_totals[run_id] = (cur_in + input_tokens, cur_out + output_tokens)
        return self.usage_totals[run_id]

    async def try_mark_terminal(self, run_id: str) -> bool:
        if run_id in self.terminals:
            return False
        self.terminals.add(run_id)
        self.terminal_at[run_id] = self.clock_ms
        return True

    async def purge_terminal(self, max_age_ms: int) -> int:
        cutoff = self.clock_ms - max_age_ms
        stale = [
            run_id
            for run_id in self.terminals
            if self.terminal_at.get(run_id, 0) <= cutoff
            and not any(
                row["status"] in ("queued", "published")
                for row in self.outbox.get(run_id, [])
            )
        ]
        changed = 0
        completed_run_ids = {
            completion.run_id for completion in self.execution_completions.values()
        }
        for run_id in stale:
            self.output_records.pop(run_id, None)
            self.output_sources = {
                key: value for key, value in self.output_sources.items() if key[0] != run_id
            }
            self.output_batches = {
                key: value for key, value in self.output_batches.items() if key[0] != run_id
            }
            self.output_text_seq = {
                key: value for key, value in self.output_text_seq.items() if key[0] != run_id
            }
            if run_id in completed_run_ids:
                if run_id in self.retention_archived:
                    continue
                self.retention_archived.add(run_id)
                self.requests.pop(run_id, None)
                self.leases.pop(run_id, None)
                self.token_totals.pop(run_id, None)
                self.usage_totals.pop(run_id, None)
                self.steers.pop(run_id, None)
                self.tool_results = {k: v for k, v in self.tool_results.items() if k[0] != run_id}
                self.tool_journal = {k: v for k, v in self.tool_journal.items() if k[0] != run_id}
                changed += 1
                continue
            self.terminals.discard(run_id)
            self.terminal_at.pop(run_id, None)
            self.requests.pop(run_id, None)
            self.leases.pop(run_id, None)
            self.token_totals.pop(run_id, None)
            self.usage_totals.pop(run_id, None)
            self.steers.pop(run_id, None)
            self.tool_results = {k: v for k, v in self.tool_results.items() if k[0] != run_id}
            self.tool_journal = {k: v for k, v in self.tool_journal.items() if k[0] != run_id}
            changed += 1
        return changed

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

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None:
        box = self.steers.get(run_id)
        if box is None:
            return
        self.steers[run_id] = [(mid, c) for mid, c in box if mid not in set(message_ids)]

    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None:
        self.tool_results.setdefault((run_id, tool_id), (result, is_error))

    async def get_tool_result(self, run_id: str, tool_id: str) -> tuple[str, bool] | None:
        return self.tool_results.get((run_id, tool_id))

    async def journal_tool_started(self, run_id: str, tool_call_id: str, name: str) -> bool:
        key = (run_id, tool_call_id)
        if key in self.tool_journal:
            return False
        self.tool_journal[key] = {"name": name, "status": "started", "result": None, "is_error": None}
        return True

    async def journal_tool_finished(
        self, run_id: str, tool_call_id: str, result: str, is_error: bool
    ) -> None:
        entry = self.tool_journal.get((run_id, tool_call_id))
        if entry is None or entry["status"] != "started":
            return
        entry["status"] = "failed" if is_error else "succeeded"
        entry["result"] = result
        entry["is_error"] = is_error

    async def clear_tool_journal(self, run_id: str, tool_call_id: str) -> None:
        self.tool_journal.pop((run_id, tool_call_id), None)

    async def get_tool_journal(self, run_id: str, tool_call_id: str) -> ToolJournalRecord | None:
        entry = self.tool_journal.get((run_id, tool_call_id))
        if entry is None:
            return None
        return ToolJournalRecord(
            name=str(entry["name"]),
            status=str(entry["status"]),
            result=str(entry["result"] or ""),
            is_error=bool(entry["is_error"]),
        )

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None:
        self.sandbox_ids.setdefault(run_id, sandbox_id)

    async def get_sandbox_id(self, run_id: str) -> str | None:
        return self.sandbox_ids.get(run_id)


class FakeExecutionContextAuthority:
    """Supervisor fake that preserves opaque binding semantics without a real checkpointer."""

    def __init__(self, store: FakeLedger) -> None:
        self._store = store

    async def open(self, request: RunRequest) -> RunnableConfig:
        existing = await self._store.get_execution_context_binding(request.run_id)
        if existing is None:
            intent = request.execution_context
            parent: ExecutionCheckpoint | None = None
            if intent.mode == "root":
                thread_id = f"thread_{request.run_id}"
            else:
                parent = await self._store.resolve_execution_parent(
                    namespace=request.context.namespace,
                    anchor=intent.parent_anchor,
                    digest=intent.parent_digest,
                    continuation_run_id=request.run_id if intent.mode == "continue" else None,
                )
                if parent is None:
                    raise ExecutionContextConflict("EXECUTION_CONTEXT_PARENT_UNAVAILABLE")
                thread_id = parent.thread_id
            intent_digest = hashlib.sha256(
                request.execution_context.model_dump_json().encode()
            ).hexdigest()
            existing = await self._store.bind_execution_context(
                request.run_id,
                ExecutionContextBinding(
                    namespace=request.context.namespace,
                    intent_digest=intent_digest,
                    physical_thread_id=thread_id,
                    base_checkpoint=parent,
                ),
            )
        return self._config(request.run_id, existing)

    async def config_for_run(self, run_id: str) -> RunnableConfig:
        binding = await self._store.get_execution_context_binding(run_id)
        if binding is None:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_MISSING")
        return self._config(run_id, binding)

    async def capture(self, run_id: str) -> ExecutionCheckpoint:
        binding = await self._store.get_execution_context_binding(run_id)
        if binding is None:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_MISSING")
        checkpoint = ExecutionCheckpoint(
            thread_id=binding.physical_thread_id,
            checkpoint_ns="",
            checkpoint_id=f"checkpoint_{run_id}",
        )
        await self._store.update_execution_checkpoint(run_id, checkpoint)
        return checkpoint

    async def prepare_completion(self, run_id: str) -> CompletedExecutionContext:
        checkpoint = await self.capture(run_id)
        binding = await self._store.get_execution_context_binding(run_id)
        assert binding is not None
        anchor = f"ctx_test_{run_id}"
        digest = hashlib.sha256(
            f"{binding.namespace}\0{anchor}\0{checkpoint.checkpoint_id}".encode()
        ).hexdigest()
        return CompletedExecutionContext(
            run_id=run_id,
            namespace=binding.namespace,
            anchor=anchor,
            digest=digest,
            owner_revision=1,
            checkpoint=checkpoint,
        )

    @staticmethod
    def _config(run_id: str, binding: ExecutionContextBinding) -> RunnableConfig:
        checkpoint = binding.active_checkpoint or binding.base_checkpoint
        configurable: dict[str, object] = {"thread_id": binding.physical_thread_id}
        if checkpoint is not None:
            configurable.update(
                checkpoint_ns=checkpoint.checkpoint_ns,
                checkpoint_id=checkpoint.checkpoint_id,
            )
        return {
            "configurable": configurable,
            "metadata": {"kokoro_run_id": run_id},
        }


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
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
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
    seen_state_configs: list[RunnableConfig] = field(default_factory=list[RunnableConfig])
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

    async def aget_state(self, config: RunnableConfig) -> FakeState:
        self.seen_state_configs.append(config)
        return self.state


def text_model(text: str, *, msg_id: str = "seg") -> FakeModel:
    return FakeModel(
        text_deltas=(text,), output_message=AIMessage(content=text, id=msg_id), message_id=msg_id
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
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        thread_id=thread_id,
        input=RunInput(message_id=f"{run_id}-m", content=content),
        runtime=RuntimeConfig(
            agent_catalog_ref=f"agent-catalog:sha256:{'a' * 64}",
            agent_type="general",
            model=ModelConfig(provider="anthropic", name="claude", authorization_handle="model-authz:test"),
            tools=[],
            skills=[],
            mcp_servers=[],
            subagents=[],
            backend="state",
            permissions=Permissions(
                approval_tools=list(approval_tools),
                review_tools=list(review_tools),
                subagent_create="deny",
                filesystem="read_only",
            ),
        ),
        context=RuntimeContext(namespace=namespace, session_id=session_id),
        execution_context=ExecutionContextIntentRoot(mode="root"),
    )


def usage_recorder() -> tuple[Callable[[int, int], Awaitable[tuple[int, int]]], dict[str, int]]:
    """invoke_once 用量入账的测试替身：返回 (recorder, 累计观测)。"""
    seen = {"input": 0, "output": 0}

    async def record(input_tokens: int, output_tokens: int) -> tuple[int, int]:
        seen["input"] += input_tokens
        seen["output"] += output_tokens
        return (seen["input"], seen["output"])

    return record, seen


async def completed_execution_context(run_id: str = "r1") -> CompletedExecutionContext:
    return CompletedExecutionContext(
        run_id=run_id,
        namespace="local:s1",
        anchor=f"ctx_test_{run_id}",
        digest="a" * 64,
        owner_revision=1,
        checkpoint=ExecutionCheckpoint(
            thread_id=f"thread_{run_id}", checkpoint_ns="", checkpoint_id=f"checkpoint_{run_id}"
        ),
    )
