"""测试共用的强类型 fake：总线、run 状态存储、v3 投影流与 agent。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeVar, cast

from langchain_core.messages import AIMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Interrupt
from pydantic import JsonValue

from kokoro_agent.protocol import (
    AgentEvent,
    ExecutionIdentity,
    IdentityRef,
    RunInput,
    RunRequest,
    agent_event_adapter,
    run_events_stream,
)
from kokoro_agent.protocol import REQUESTS_STREAM
from kokoro_agent.execution.scope import runtime_namespace
from kokoro_agent.repositories.run_repository import (
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

    def __init__(self) -> None:
        self.requests: dict[str, RunRequest] = {}
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
    ) -> StagedFrame | None:
        lease_current = (
            await self.is_lease_current(run_id, lease)
            if kind == "run.started"
            else await self.is_fence_current(run_id, lease)
        )
        if not lease_current:
            return None
        seq = self.durable_counter.get(run_id, 0) + 1
        self.durable_counter[run_id] = seq
        if terminal and run_id not in self.terminal_fence:
            self.terminal_fence[run_id] = seq
        fence = self.terminal_fence.get(run_id)
        event_id = f"evt_fake_{run_id}_{seq}"
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
        return StagedFrame(durable_seq=seq, event_id=event_id, index=index)

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
            seq = rejected[0]
            fence = self.terminal_fence.get(run_id)
            if fence is None or fence > seq:
                self.terminal_fence[run_id] = seq
            return ReceiptReconcile(rejected_seq=seq)
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
                r for r in rows if _as_int(r["durable_seq"]) > advanced
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
        # worker unit fixture 可直接投递 control；缺少 HTTP admission 时在同一 ledger 建立 persisted 行。
        if run_id not in self.requests:
            return False
        key = (run_id, command_id)
        existing = self.control_commands.get(key)
        if existing is not None:
            if (
                request_digest is not None
                and existing["request_digest"] != request_digest
            ):
                raise ControlCommandConflict("command digest mismatch")
            return False
        self.control_commands[key] = {
            "run_id": run_id,
            "command_id": command_id,
            "request_digest": request_digest,
            "fingerprint": fingerprint,
            "status": "persisted",
            "body": body,
            "error_code": None,
        }
        return True

    async def admit_control(
        self, run_id: str, command_id: str, request_digest: str, body: str
    ) -> ControlAdmission:
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
        self, run_id: str, namespace: str
    ) -> RunRequest | None:
        request = self.requests.get(run_id)
        if request is None:
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
        cur_in, cur_out = self.usage_totals.get(run_id, (0, 0))
        self.usage_segments[segment] = usage
        self.usage_totals[run_id] = (cur_in + input_tokens, cur_out + output_tokens)
        return self.usage_totals[run_id]

    async def try_mark_terminal(
        self, run_id: str, lease: LeaseFence | None = None
    ) -> bool:
        active = lease or self.current_lease(run_id)
        if (
            run_id in self.terminals
            or active is None
            or not await self.is_lease_current(run_id, active)
        ):
            return False
        self.terminals.add(run_id)
        self.terminal_at[run_id] = self.clock_ms
        await self._queue_bound_sandbox_cleanup(run_id)
        return True

    async def fence_and_mark_terminal(
        self, run_id: str, owner: str = "test-consumer"
    ) -> LeaseFence | None:
        if run_id not in self.requests or run_id in self.terminals:
            return None
        generation = self.generations.get(run_id, 0) + 1
        self.owners[run_id] = owner
        self.generations[run_id] = generation
        self.leases[run_id] = None
        self.terminals.add(run_id)
        self.terminal_at[run_id] = self.clock_ms
        await self._queue_bound_sandbox_cleanup(run_id)
        return LeaseFence(owner=owner, generation=generation)

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

    async def execute_active_effect(
        self,
        run_id: str,
        lease: LeaseFence,
        effect: Callable[[], Awaitable[None]],
    ) -> bool:
        if not await self.is_lease_current(run_id, lease):
            return False
        await effect()
        return True

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
