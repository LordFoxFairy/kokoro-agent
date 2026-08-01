"""Mongo 后端：跨 pod 共享的 run 状态存储，$setOnInsert/条件更新给原子认领。"""

from __future__ import annotations

import hashlib
import re
import secrets
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from kokoro_agent.contract import RunCompletedPayload, RunOwnerCompletedPayload, RunRequest
from kokoro_agent.storage.execution_context import (
    ClaimedCompletionFrames,
    CompletionEventDraft,
    CompletedExecutionContext,
    DurableCompletionFrame,
    ExecutionCheckpoint,
    ExecutionContextBinding,
    ExecutionContextConflict,
)
from kokoro_agent.contract.storage import (
    RUN_DISPATCHES_COLLECTION,
    RUN_EVENT_RECEIPTS_COLLECTION,
    RUN_RECEIPT_MANIFESTS_COLLECTION,
    RunEventReceiptDoc,
    run_event_receipts_doc_adapter,
)
from kokoro_agent.evidence.models import (
    DurableExecutionEvidence,
    DurableOutputDraft,
    DurableOutputRecord,
    DurableRetentionStats,
    append_output_digest,
    evidence_kind_for_event,
    initial_output_digest,
    make_durable_output_record,
    make_durable_execution_evidence,
)

# 不可解析帧死信集合（R1 quarantine 简版；identity 感知的畸形帧留 R5）。
DISPATCH_DLQ_COLLECTION = "dispatch_dlq"
AGENT_EXECUTION_EVIDENCE_COLLECTION = "agent_execution_evidence"
AGENT_DURABLE_OUTPUT_COLLECTION = "agent_durable_output"
AGENT_DURABLE_OUTPUT_SOURCE_BATCH_COLLECTION = "agent_durable_output_source_batch"
AGENT_ACTION_OWNER_REVISIONS_COLLECTION = "agent_action_owner_revisions"

_T = TypeVar("_T")
_OUTPUT_APPEND_MAX_ATTEMPTS = 64


class _OutputAppendContention(RuntimeError):
    """A live run advanced its output counter before this append CAS."""


def _output_source_batch_digest(payload_sha256s: tuple[str, ...]) -> str:
    digest = hashlib.sha256(b"kokoro-output-source-batch-v1\0")
    digest.update(len(payload_sha256s).to_bytes(4, "big"))
    for payload_sha256 in payload_sha256s:
        encoded = payload_sha256.encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _output_source_batch_id(run_id: str, source_event_ref: str) -> str:
    identity = hashlib.sha256(f"v1\0{run_id}\0{source_event_ref}".encode()).hexdigest()
    return f"output_batch_{identity}"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _event_id() -> str:
    # critical 帧稳定身份（evt_ 前缀 + 128bit 随机）：崩溃补发复用同 event_id，session 去重不漂移。
    return f"evt_{secrets.token_hex(16)}"


class _ToolResultEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    result: str
    is_error: bool


_TOOL_RESULTS_ADAPTER: TypeAdapter[dict[str, _ToolResultEntry]] = TypeAdapter(
    dict[str, _ToolResultEntry]
)


class _SteerEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    message_id: str
    content: str


_STEERS_ADAPTER: TypeAdapter[list[_SteerEntry]] = TypeAdapter(list[_SteerEntry])


class _ControlInboxEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    decision_id: str
    fingerprint: str | None
    status: str
    body: str


_CONTROL_INBOX_ADAPTER: TypeAdapter[list[_ControlInboxEntry]] = TypeAdapter(
    list[_ControlInboxEntry]
)


class ControlInboxRecord(BaseModel):
    """R2 control inbox 待续办条目：persisted 未 applied 的 resume/cancel（重启 scanner 消费）。

    定义在低层 mongo 模块以避免 ledger↔mongo 循环；ledger 面再导出为契约类型。
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    decision_id: str
    # 记录时的 interrupt 指纹（resume 有值；cancel 无 interrupt 依赖=None）：
    # 重启续办按此校验当前 interrupt 是否仍匹配，不匹配即 stale→superseded。
    fingerprint: str | None
    # 待续办的 control 帧原文（JSON）：重启逐字重放 apply。
    body: str


class _ToolJournalEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    # started | succeeded | failed（started=unknown-outcome：落 started 后崩在执行中）。
    status: str
    # 终态结果（succeeded/failed 才有；started 时缺席）：重放短路直接回灌，不重执行副作用。
    result: str | None = None
    is_error: bool | None = None


_TOOL_JOURNAL_ADAPTER: TypeAdapter[dict[str, _ToolJournalEntry]] = TypeAdapter(
    dict[str, _ToolJournalEntry]
)


class ToolJournalRecord(BaseModel):
    """R3 tool effect journal 行（副作用工具锚=tool_call_id）：重放守门读此判是否短路/重执行。

    定义在低层 mongo 模块以避免 ledger↔mongo 循环；ledger 面再导出为契约类型。
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    name: str
    status: str
    # started 时为空串（无结果）；succeeded/failed 为记录结果。
    result: str
    is_error: bool


class _OutboxEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    durable_seq: int
    event_id: str
    kind: str
    status: str  # queued | published | superseded
    # queued/published 携完整重建元数据；superseded 只留 kind+event_id 摘要（payload 删）。
    index: int | None = None
    timestamp: int | None = None
    payload_json: str | None = None
    # publish 落定时刻（回执一直不来时按此判宽限期超时重发；queued/superseded 缺席）。
    published_at: int | None = None


_OUTBOX_ADAPTER: TypeAdapter[list[_OutboxEntry]] = TypeAdapter(list[_OutboxEntry])


class _SemanticCriticalFrame(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    semantic_key: str
    kind: str
    payload_sha256: str
    durable_seq: int
    event_id: str


_SEMANTIC_CRITICAL_ADAPTER: TypeAdapter[list[_SemanticCriticalFrame]] = TypeAdapter(
    list[_SemanticCriticalFrame]
)
_DURABLE_EVIDENCE_ADAPTER: TypeAdapter[DurableExecutionEvidence] = TypeAdapter(
    DurableExecutionEvidence
)
_DURABLE_OUTPUT_ADAPTER: TypeAdapter[DurableOutputRecord] = TypeAdapter(
    DurableOutputRecord
)

_EXECUTION_BINDING_ADAPTER: TypeAdapter[ExecutionContextBinding] = TypeAdapter(
    ExecutionContextBinding
)
_COMPLETED_EXECUTION_CONTEXT_ADAPTER: TypeAdapter[CompletedExecutionContext] = TypeAdapter(
    CompletedExecutionContext
)


class StagedFrame(BaseModel):
    """critical 帧 durable 身份分配结果：seq 独立于 live index，崩溃补发复用同一对。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    durable_seq: int
    event_id: str
    # False=semantic duplicate：既有 durable identity 已落定，caller 不再 publish/index++。
    created: bool = True


class ActionOwnerRevision(BaseModel):
    """Immutable revision reservation for one stable tool/request interaction owner."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str = Field(min_length=1, max_length=128)
    owner_ref: str = Field(min_length=1, max_length=256)
    owner_version: int = Field(gt=0)
    checkpoint_ref: str = Field(min_length=1, max_length=256)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    predecessor_owner_version: int | None = Field(default=None, gt=0)
    predecessor_checkpoint_ref: str | None = Field(
        default=None, min_length=1, max_length=256
    )
    predecessor_payload_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )

    @model_validator(mode="after")
    def validate_predecessor(self) -> ActionOwnerRevision:
        predecessor = (
            self.predecessor_owner_version,
            self.predecessor_checkpoint_ref,
            self.predecessor_payload_sha256,
        )
        if self.owner_version == 1:
            if any(value is not None for value in predecessor):
                raise ValueError("ACTION_OWNER_INITIAL_PREDECESSOR_INVALID")
        elif (
            self.predecessor_owner_version != self.owner_version - 1
            or self.predecessor_checkpoint_ref is None
            or self.predecessor_payload_sha256 is None
        ):
            raise ValueError("ACTION_OWNER_SUCCESSOR_PREDECESSOR_INVALID")
        return self


def action_owner_pause_revision_ref(
    checkpoint_ref: str,
    applied_decision_id: str | None,
) -> str:
    """Seal one private pause cause without exposing control identity on the wire.

    LangGraph can replace the interrupt writes of an existing checkpoint when invalid input
    re-prompts the same task, so checkpoint identity alone is not a revision fence. The durable
    applied control decision is the causal successor; persisted/unapplied controls are ignored.
    """

    if not checkpoint_ref:
        raise ValueError("ACTION_OWNER_CHECKPOINT_REQUIRED")
    cause = (
        b"initial"
        if applied_decision_id is None
        else b"applied\0" + applied_decision_id.encode()
    )
    checkpoint = checkpoint_ref.encode()
    canonical = (
        len(checkpoint).to_bytes(4, "big")
        + checkpoint
        + len(cause).to_bytes(4, "big")
        + cause
    )
    digest = hashlib.sha256(
        b"kokoro-action-owner-pause-revision.v1\0" + canonical
    ).hexdigest()
    return f"apause_{digest}"


class OutboxFrame(BaseModel):
    """queued outbox 行的重建视图（scanner 补发用）：完整还原 wire 帧。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    durable_seq: int
    event_id: str
    kind: str
    index: int
    timestamp: int
    payload_json: str


class ReceiptReconcile(BaseModel):
    """一次 per-run 回执对账结果：供 supervisor 决定终局收口/告警。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    # session NACK（quarantine）：非空即须停止执行与分配、按 contract_incompatible 终局。
    rejected_seq: int | None = None
    # manifest 行缺失且未 close：不删 outbox，ERROR 告警。
    receipt_state_lost: bool = False
    # 本次推进到的 consumed_seq（None=未推进）。
    consumed_through: int | None = None
    # 本次经握手置下 producer_close_requested。
    close_requested: bool = False
    # published 但回执一直不来、超宽限期的行：交 supervisor 复用固定身份重发（session 去重幂等）。
    republish: list[OutboxFrame] = Field(default_factory=list["OutboxFrame"])


class MongoLedger:
    """单 collection、以 run_id 为 _id：upsert 与条件 update 提供跨 pod 原子认领。"""

    def __init__(
        self,
        collection: AsyncCollection[dict[str, object]],
        *,
        ttl_ms: int,
        clock: Callable[[], int] = _now_ms,
        producer_instance_ref: str = "agent-runtime",
        producer_generation: int = 1,
        allow_nontransactional_evidence_for_tests: bool = False,
    ) -> None:
        self._coll = collection
        self._ttl_ms = ttl_ms
        self._clock = clock
        self._producer_instance_ref = producer_instance_ref
        self._producer_generation = producer_generation
        self._allow_nontransactional_evidence_for_tests = (
            allow_nontransactional_evidence_for_tests
        )
        # dispatch CAS 与死信同库兄弟集合（session 在 message-store 库写 run_dispatches；
        # gate/closure env 令两库同名同实例，单文档 CAS 跨进程可见）。
        self._dispatches = collection.database[RUN_DISPATCHES_COLLECTION]
        self._dlq = collection.database[DISPATCH_DLQ_COLLECTION]
        # R4 critical outbox 回执/清单同库兄弟集合（同库部署，写者分域见 contract/spec/storage.yaml）：
        # run_event_receipts 由 session 写、agent 读；run_receipt_manifests 双方 CAS 各自字段。
        self._receipts = collection.database[RUN_EVENT_RECEIPTS_COLLECTION]
        self._manifests = collection.database[RUN_RECEIPT_MANIFESTS_COLLECTION]
        # Evidence is one document per durable fact. It must never grow the run document;
        # the write is committed with the outbox allocation in one Mongo transaction.
        self._evidence = collection.database[AGENT_EXECUTION_EVIDENCE_COLLECTION]
        # Output is an independent per-run append-only authority. Records never share the
        # lifecycle durable counter and never grow the run document.
        self._outputs = collection.database[AGENT_DURABLE_OUTPUT_COLLECTION]
        # One private marker per source event records cardinality (including zero) and the
        # ordered payload identity without polluting public output cursors or hash chains.
        self._output_source_batches = collection.database[
            AGENT_DURABLE_OUTPUT_SOURCE_BATCH_COLLECTION
        ]
        # One append-only row per stable action owner revision. The private pause ref binds the
        # exact checkpoint to the latest durable applied resume cause: replay reuses the row;
        # a true resumed pause appends the exact predecessor-bound successor.
        self._action_owner_revisions = collection.database[
            AGENT_ACTION_OWNER_REVISIONS_COLLECTION
        ]

    async def _run_evidence_transaction(
        self,
        callback: Callable[[AsyncClientSession | None], Awaitable[_T]],
    ) -> _T:
        if self._allow_nontransactional_evidence_for_tests:
            return await callback(None)
        async with self._coll.database.client.start_session() as session:
            return await session.with_transaction(callback)

    async def try_claim(self, request: RunRequest, owner: str) -> bool:
        # $setOnInsert + upsert：仅 _id 不存在时写入；并发 upsert 撞 _id 抛 DuplicateKeyError
        # （mongo 文档明载的 upsert 竞态）→ 视为已被他人认领。
        try:
            result = await self._coll.update_one(
                {"_id": request.run_id},
                {
                    "$setOnInsert": {
                        "request_json": request.model_dump_json(),
                        "terminal": False,
                        "lease_expires_ms": self._clock() + self._ttl_ms,
                        "owner": owner,
                        # R4：run.started 收编进 critical outbox（seq 1 惯例），claim 不再写
                        # run_started_published 布尔位——身份/补发一律走 outbox（stage→publish→scanner）。
                    }
                },
                upsert=True,
            )
        except DuplicateKeyError:
            return False
        return result.upserted_id is not None

    async def claim_dispatch(self, run_id: str, consumer: str) -> bool:
        # dispatch CAS（D5）：pending→claimed 单文档条件转移。赢=授权执行；已 claimed/expired
        # =重复投递/迟到帧丢弃；无记录=兼容放行（迁移/无 intent 期不误杀 run，执行去重仍由
        # try_claim 兜底）。deadline_at>now 是与 session 超时 reconciler 的赛跑闸：deadline 已过
        # 只能被 reconciler 转 expired，agent 不再认领。
        now = self._clock()
        won = await self._dispatches.find_one_and_update(
            {"run_id": run_id, "status": "pending", "deadline_at": {"$gt": now}},
            {"$set": {"status": "claimed", "claimed_by": consumer, "updated_at": now}},
        )
        if won is not None:
            return True
        exists = await self._dispatches.find_one({"run_id": run_id}, {"_id": 1})
        return exists is None

    async def quarantine_dispatch(self, raw_hash: str, source: str, reason: str) -> None:
        # 不可解析帧死信：记录后由调用方 ACK（坏帧无 identity 不重投）。schema 局部定义（R5 重整）。
        await self._dlq.insert_one(
            {"raw_hash": raw_hash, "source": source, "reason": reason, "at": self._clock()}
        )

    async def append_durable_outputs(
        self,
        run_id: str,
        source_event_ref: str,
        drafts: tuple[DurableOutputDraft, ...],
        *,
        recorded_at_ms: int,
        source_payload_sha256: str,
    ) -> tuple[DurableOutputRecord, ...] | None:
        """Allocate every output for one source event in a single transaction.

        source_event_ref is the stable run-local event identity. Ordinals derive
        keep-first row identities but are never returned by the public contract.
        """
        if not source_event_ref or len(source_event_ref) > 240:
            raise ValueError("OUTPUT_SOURCE_REF_INVALID")
        if re.fullmatch(r"[0-9a-f]{64}", source_payload_sha256) is None:
            raise ValueError("OUTPUT_SOURCE_PAYLOAD_DIGEST_INVALID")
        source_refs = tuple(
            f"{source_event_ref}:{ordinal}" for ordinal in range(len(drafts))
        )
        source_payload_sha256s = tuple(
            draft.source_payload_sha256 for draft in drafts
        )
        source_batch_digest = _output_source_batch_digest(source_payload_sha256s)
        source_batch_doc: dict[str, object] = {
            "_id": _output_source_batch_id(run_id, source_event_ref),
            "run_id": run_id,
            "source_event_ref": source_event_ref,
            "batch_size": len(drafts),
            "source_payload_sha256": source_payload_sha256,
            "ordered_payload_sha256": source_batch_digest,
            "recorded_at_ms": recorded_at_ms,
        }

        async def append(
            session: AsyncClientSession | None,
        ) -> tuple[DurableOutputRecord, ...] | None:
            marker = await self._output_source_batches.find_one(
                {"run_id": run_id, "source_event_ref": source_event_ref},
                session=session,
            )
            if marker is not None:
                if marker.get("batch_size") != len(drafts):
                    raise ValueError("OUTPUT_SOURCE_BATCH_CONFLICT")
                if marker.get("source_payload_sha256") != source_payload_sha256:
                    raise ValueError("OUTPUT_SOURCE_CONFLICT")
                if marker.get("ordered_payload_sha256") != source_batch_digest:
                    raise ValueError("OUTPUT_SOURCE_CONFLICT")
                existing_rows = [
                    row
                    async for row in self._outputs.find(
                        {
                            "run_id": run_id,
                            "source_event_ref": {
                                "$regex": f"^{re.escape(source_event_ref)}:[0-9]+$"
                            },
                        },
                        session=session,
                    )
                ]
                if not drafts:
                    if existing_rows:
                        raise ValueError("OUTPUT_SOURCE_PARTIAL")
                    return ()
                if any(
                    row.get("source_batch_size") != len(drafts)
                    for row in existing_rows
                ):
                    raise ValueError("OUTPUT_SOURCE_BATCH_CONFLICT")
                existing_by_source = {
                    row.get("source_event_ref"): row for row in existing_rows
                }
                if len(existing_by_source) != len(source_refs):
                    raise ValueError("OUTPUT_SOURCE_PARTIAL")
                replayed: list[DurableOutputRecord] = []
                for ordinal, (source_ref, payload_sha256) in enumerate(
                    zip(source_refs, source_payload_sha256s, strict=True)
                ):
                    existing = existing_by_source.get(source_ref)
                    if existing is None:
                        raise ValueError("OUTPUT_SOURCE_PARTIAL")
                    if existing.get("source_ordinal") != ordinal:
                        raise ValueError("OUTPUT_SOURCE_BATCH_CONFLICT")
                    if existing.get("source_payload_sha256") != payload_sha256:
                        raise ValueError("OUTPUT_SOURCE_CONFLICT")
                    public = {
                        key: value
                        for key, value in existing.items()
                        if key
                        not in {
                            "_id",
                            "source_event_ref",
                            "source_payload_sha256",
                            "source_batch_size",
                            "source_ordinal",
                            "text_part_ref_sha256",
                        }
                    }
                    replayed.append(_DURABLE_OUTPUT_ADAPTER.validate_python(public))
                return tuple(replayed)

            legacy_row = await self._outputs.find_one(
                {
                    "run_id": run_id,
                    "source_event_ref": {
                        "$regex": f"^{re.escape(source_event_ref)}:[0-9]+$"
                    },
                },
                {"_id": 1},
                session=session,
            )
            if legacy_row is not None:
                # This version requires marker and rows to be born in one transaction.
                # Rows without their marker are incomplete authority, never backfilled.
                raise ValueError("OUTPUT_SOURCE_PARTIAL")

            run = await self._coll.find_one(
                {"_id": run_id},
                {
                    "terminal": 1,
                    "output_counter": 1,
                    "output_digest_sha256": 1,
                },
                session=session,
            )
            if run is None or run.get("terminal") is True:
                return None
            if not drafts:
                fenced = await self._coll.update_one(
                    {"_id": run_id, "terminal": {"$ne": True}},
                    {"$inc": {"output_source_batch_revision": 1}},
                    session=session,
                )
                if fenced.modified_count != 1:
                    return None
                await self._output_source_batches.insert_one(
                    source_batch_doc, session=session
                )
                return ()
            current_seq = run.get("output_counter", 0)
            if not isinstance(current_seq, int) or current_seq < 0:
                raise TypeError("OUTPUT_COUNTER_INVALID")
            previous_digest = run.get("output_digest_sha256")
            if previous_digest is None:
                previous_digest = initial_output_digest(run_id)
            if not isinstance(previous_digest, str):
                raise TypeError("OUTPUT_DIGEST_INVALID")

            text_part_hashes = {
                draft.text_part_ref: hashlib.sha256(draft.text_part_ref.encode()).hexdigest()
                for draft in drafts
                if draft.text_part_ref is not None
            }
            latest_text_seq: dict[str, int] = {}
            for part_ref, part_hash in text_part_hashes.items():
                previous_text = await self._outputs.find_one(
                    {"run_id": run_id, "text_part_ref_sha256": part_hash},
                    {"output_seq": 1},
                    sort=[("output_seq", -1)],
                    session=session,
                )
                if previous_text is None:
                    continue
                previous_seq = previous_text.get("output_seq")
                if not isinstance(previous_seq, int) or previous_seq < 1:
                    raise TypeError("OUTPUT_TEXT_SEQUENCE_INVALID")
                latest_text_seq[part_ref] = previous_seq

            records: list[DurableOutputRecord] = []
            next_digest = previous_digest
            for ordinal, draft in enumerate(drafts):
                output_seq = current_seq + ordinal + 1
                replaces_through_output_seq = (
                    latest_text_seq.get(draft.text_part_ref, 0)
                    if draft.is_text_snapshot and draft.text_part_ref is not None
                    else 0
                )
                record = make_durable_output_record(
                    run_id=run_id,
                    output_seq=output_seq,
                    draft=draft,
                    replaces_through_output_seq=replaces_through_output_seq,
                    recorded_at_ms=recorded_at_ms,
                    producer_instance_ref=self._producer_instance_ref,
                    producer_generation=self._producer_generation,
                )
                records.append(record)
                next_digest = append_output_digest(
                    next_digest, output_seq, record.payload_sha256
                )
                if draft.text_part_ref is not None:
                    latest_text_seq[draft.text_part_ref] = output_seq

            final_seq = current_seq + len(records)
            run_filter: dict[str, object] = {
                "_id": run_id,
                "terminal": {"$ne": True},
            }
            if current_seq == 0:
                run_filter["$or"] = [
                    {"output_counter": {"$exists": False}},
                    {"output_counter": 0},
                ]
            else:
                run_filter["output_counter"] = current_seq
            advanced = await self._coll.update_one(
                run_filter,
                {
                    "$set": {
                        "output_counter": final_seq,
                        "output_digest_sha256": next_digest,
                    }
                },
                session=session,
            )
            if advanced.modified_count != 1:
                fence = await self._coll.find_one(
                    {"_id": run_id}, {"terminal": 1}, session=session
                )
                if fence is None or fence.get("terminal") is True:
                    return None
                raise _OutputAppendContention
            rows: list[dict[str, object]] = []
            for ordinal, (
                source_ref,
                draft_payload_sha256,
                draft,
                record,
            ) in enumerate(
                zip(source_refs, source_payload_sha256s, drafts, records, strict=True)
            ):
                row: dict[str, object] = {
                    "_id": record.output_ref,
                    **record.model_dump(mode="python"),
                    "source_event_ref": source_ref,
                    "source_payload_sha256": draft_payload_sha256,
                    "source_batch_size": len(records),
                    "source_ordinal": ordinal,
                }
                if draft.text_part_ref is not None:
                    row["text_part_ref_sha256"] = text_part_hashes[draft.text_part_ref]
                rows.append(row)
            await self._outputs.insert_many(rows, ordered=True, session=session)
            await self._output_source_batches.insert_one(
                source_batch_doc, session=session
            )
            return tuple(records)

        for attempt in range(_OUTPUT_APPEND_MAX_ATTEMPTS):
            try:
                return await self._run_evidence_transaction(append)
            except (_OutputAppendContention, DuplicateKeyError):
                if attempt + 1 == _OUTPUT_APPEND_MAX_ATTEMPTS:
                    raise RuntimeError("OUTPUT_APPEND_CONTENTION") from None
        raise AssertionError("unreachable output append retry")

    async def reserve_action_owner_revision(
        self,
        run_id: str,
        owner_ref: str,
        checkpoint_ref: str,
        payload_sha256: str,
    ) -> ActionOwnerRevision | None:
        """Reserve/replay an immutable revision bound to checkpoint + applied resume cause."""

        async def reserve(
            session: AsyncClientSession | None,
        ) -> ActionOwnerRevision | None:
            run = await self._coll.find_one(
                {"_id": run_id}, {"terminal": 1, "control_inbox": 1}, session=session
            )
            if run is None or run.get("terminal") is True:
                return None
            controls = _CONTROL_INBOX_ADAPTER.validate_python(
                run.get("control_inbox") or []
            )
            latest_applied_decision_id = next(
                (
                    entry.decision_id
                    for entry in reversed(controls)
                    if entry.status == "applied"
                ),
                None,
            )
            pause_revision_ref = action_owner_pause_revision_ref(
                checkpoint_ref,
                latest_applied_decision_id,
            )
            requested = ActionOwnerRevision(
                run_id=run_id,
                owner_ref=owner_ref,
                owner_version=1,
                checkpoint_ref=pause_revision_ref,
                payload_sha256=payload_sha256,
            )

            exact = await self._action_owner_revisions.find_one(
                {
                    "run_id": run_id,
                    "owner_ref": owner_ref,
                    "checkpoint_ref": pause_revision_ref,
                },
                {"_id": 0},
                session=session,
            )
            if exact is not None:
                existing = ActionOwnerRevision.model_validate(exact)
                if existing.payload_sha256 != requested.payload_sha256:
                    raise ValueError("ACTION_OWNER_REVISION_CONFLICT")
                return existing

            # Touch the run authority in the same transaction. Terminal fencing and concurrent
            # successor allocation therefore serialize instead of producing a revision after
            # terminal or two rows with the same monotonic version.
            touched = await self._coll.update_one(
                {"_id": run_id, "terminal": {"$ne": True}},
                {"$inc": {"action_owner_revision_cas_count": 1}},
                session=session,
            )
            if touched.modified_count != 1:
                return None

            head_raw = await self._action_owner_revisions.find_one(
                {"run_id": run_id, "owner_ref": owner_ref},
                {"_id": 0},
                sort=[("owner_version", -1)],
                session=session,
            )
            head = (
                None
                if head_raw is None
                else ActionOwnerRevision.model_validate(head_raw)
            )
            revision = ActionOwnerRevision(
                run_id=run_id,
                owner_ref=owner_ref,
                owner_version=1 if head is None else head.owner_version + 1,
                checkpoint_ref=pause_revision_ref,
                payload_sha256=payload_sha256,
                predecessor_owner_version=(
                    None if head is None else head.owner_version
                ),
                predecessor_checkpoint_ref=(
                    None if head is None else head.checkpoint_ref
                ),
                predecessor_payload_sha256=(
                    None if head is None else head.payload_sha256
                ),
            )
            identity = hashlib.sha256(
                (
                    f"action-owner-revision:v1\0{run_id}\0{owner_ref}\0"
                    f"{revision.owner_version}"
                ).encode()
            ).hexdigest()
            await self._action_owner_revisions.insert_one(
                {"_id": f"aorev_{identity}", **revision.model_dump(mode="python")},
                session=session,
            )
            return revision

        # Unique indexes convert concurrent same-checkpoint or same-version writers into a
        # retry that reads the immutable winner. No payload is overwritten.
        for attempt in range(_OUTPUT_APPEND_MAX_ATTEMPTS):
            try:
                return await self._run_evidence_transaction(reserve)
            except DuplicateKeyError:
                if attempt + 1 == _OUTPUT_APPEND_MAX_ATTEMPTS:
                    raise RuntimeError("ACTION_OWNER_REVISION_CONTENTION") from None
        raise AssertionError("unreachable action owner revision retry")

    async def pull_durable_output_records(
        self, run_id: str, after_output_seq: int, limit: int
    ) -> list[DurableOutputRecord]:
        if limit < 1 or limit > 65 or after_output_seq < 0:
            raise ValueError("OUTPUT_CURSOR_INVALID")
        cursor = (
            self._outputs.find(
                {"run_id": run_id, "output_seq": {"$gt": after_output_seq}},
                {
                    "_id": 0,
                    "source_event_ref": 0,
                    "source_payload_sha256": 0,
                    "source_batch_size": 0,
                    "source_ordinal": 0,
                    "text_part_ref_sha256": 0,
                },
            )
            .sort("output_seq", 1)
            .limit(limit)
        )
        return [_DURABLE_OUTPUT_ADAPTER.validate_python(row) async for row in cursor]

    async def get_durable_retention_stats(self) -> DurableRetentionStats:
        return DurableRetentionStats(
            output_records=await self._outputs.estimated_document_count(),
            evidence_records=await self._evidence.estimated_document_count(),
        )

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
        # R4：原子分配 per-run durable_seq（从 1 连续）+ event_id，落 outbox queued 行。
        # 终态帧 CAS 设 local terminal_fence_seq（仅当未设，first-terminal 赢）。返回 None=
        # 该 seq>fence（post-fence）→行以 superseded 摘要落库、永不发布，caller 不上 wire。
        event_id = _event_id()
        payload_sha256 = hashlib.sha256(payload_json.encode()).hexdigest()
        evidence_kind = evidence_kind_for_event(kind)
        recorded_at_ms = self._clock()
        queued_row: dict[str, object] = {
            "durable_seq": "$durable_counter",
            "event_id": {"$literal": event_id},
            "kind": {"$literal": kind},
            "index": index,
            "timestamp": timestamp,
            "payload_json": {"$literal": payload_json},
            "status": {"$literal": "queued"},
        }
        superseded_row: dict[str, object] = {
            "durable_seq": "$durable_counter",
            "event_id": {"$literal": event_id},
            "kind": {"$literal": kind},
            "status": {"$literal": "superseded"},
        }
        # $ifNull 归一：未设的 terminal_fence_seq（缺席字段）在聚合里须显式当作 null，
        # 否则 missing≠null 会误判 post-fence（把无 fence 的首帧错标 superseded）。
        post_fence = {
            "$and": [
                {"$ne": [{"$ifNull": ["$terminal_fence_seq", None]}, None]},
                {"$gt": ["$durable_counter", {"$ifNull": ["$terminal_fence_seq", -1]}]},
            ]
        }
        pipeline: list[dict[str, object]] = [
                {"$set": {"durable_counter": {"$add": [{"$ifNull": ["$durable_counter", 0]}, 1]}}},
                {
                    "$set": {
                        "terminal_fence_seq": {
                            "$cond": [
                                {
                                    "$and": [
                                        terminal,
                                        {"$eq": [{"$ifNull": ["$terminal_fence_seq", None]}, None]},
                                    ]
                                },
                                "$durable_counter",
                                "$terminal_fence_seq",
                            ]
                        }
                    }
                },
                {
                    "$set": {
                        "outbox": {
                            "$concatArrays": [
                                {"$ifNull": ["$outbox", []]},
                                [{"$cond": [post_fence, superseded_row, queued_row]}],
                            ]
                        }
                    }
                },
            ]
        if semantic_key is not None:
            pipeline.append(
                {
                    "$set": {
                        "semantic_critical_frames": {
                            "$concatArrays": [
                                {"$ifNull": ["$semantic_critical_frames", []]},
                                [
                                    {
                                        "semantic_key": {"$literal": semantic_key},
                                        "kind": {"$literal": kind},
                                        "payload_sha256": {"$literal": payload_sha256},
                                        "durable_seq": "$durable_counter",
                                        "event_id": {"$literal": event_id},
                                    }
                                ],
                            ]
                        }
                    }
                }
            )
        query: dict[str, object] = {"_id": run_id}
        if semantic_key is not None:
            query["semantic_critical_frames.semantic_key"] = {"$ne": semantic_key}
        async def stage(
            session: AsyncClientSession | None,
        ) -> StagedFrame | None:
            doc = await self._coll.find_one_and_update(
                query,
                pipeline,
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if doc is None:
                if semantic_key is not None:
                    existing_doc = await self._coll.find_one(
                        {"_id": run_id},
                        {"semantic_critical_frames": 1},
                        session=session,
                    )
                    if existing_doc is None:
                        return None
                    entries = _SEMANTIC_CRITICAL_ADAPTER.validate_python(
                        existing_doc.get("semantic_critical_frames") or []
                    )
                    existing = next(
                        (
                            entry
                            for entry in entries
                            if entry.semantic_key == semantic_key
                        ),
                        None,
                    )
                    if existing is None:
                        return None
                    if (
                        existing.kind != kind
                        or existing.payload_sha256 != payload_sha256
                    ):
                        raise ValueError(
                            f"semantic critical frame conflict for {semantic_key!r}"
                        )
                    return StagedFrame(
                        durable_seq=existing.durable_seq,
                        event_id=existing.event_id,
                        created=False,
                    )
                return None
            seq = doc.get("durable_counter")
            fence = doc.get("terminal_fence_seq")
            if not isinstance(seq, int):
                raise TypeError(
                    f"durable_counter for {run_id!r} is not an int: {seq!r}"
                )
            if isinstance(fence, int) and seq > fence:
                return None
            if evidence_kind is not None:
                output_high_watermark = doc.get("output_sealed_high_watermark", 0)
                output_digest_sha256 = doc.get(
                    "output_sealed_digest_sha256", initial_output_digest(run_id)
                )
                if not isinstance(output_high_watermark, int) or not isinstance(
                    output_digest_sha256, str
                ):
                    raise TypeError("OUTPUT_SEAL_INVALID")
                record = make_durable_execution_evidence(
                    run_id=run_id,
                    durable_seq=seq,
                    event_id=event_id,
                    event_kind=kind,
                    payload_json=payload_json,
                    recorded_at_ms=recorded_at_ms,
                    producer_instance_ref=self._producer_instance_ref,
                    producer_generation=self._producer_generation,
                    output_high_watermark=output_high_watermark,
                    output_digest_sha256=output_digest_sha256,
                )
                await self._evidence.insert_one(
                    {"_id": record.evidence_ref, **record.model_dump(mode="python")},
                    session=session,
                )
            return StagedFrame(durable_seq=seq, event_id=event_id, created=True)

        if evidence_kind is None:
            return await stage(None)
        return await self._run_evidence_transaction(stage)

    async def pull_durable_execution_evidence(
        self, run_id: str, after_durable_seq: int, limit: int
    ) -> list[DurableExecutionEvidence]:
        if limit < 1 or limit > 257 or after_durable_seq < 0:
            raise ValueError("EVIDENCE_CURSOR_INVALID")
        cursor = (
            self._evidence.find(
                {"run_id": run_id, "durable_seq": {"$gt": after_durable_seq}},
                {"_id": 0},
            )
            .sort("durable_seq", 1)
            .limit(limit)
        )
        return [
            _DURABLE_EVIDENCE_ADAPTER.validate_python(row) async for row in cursor
        ]

    async def get_durable_execution_evidence(
        self, run_id: str, evidence_ref: str
    ) -> DurableExecutionEvidence | None:
        row = await self._evidence.find_one(
            {"run_id": run_id, "evidence_ref": evidence_ref}, {"_id": 0}
        )
        return (
            None
            if row is None
            else _DURABLE_EVIDENCE_ADAPTER.validate_python(row)
        )

    async def get_run_durable_checkpoint(
        self, run_id: str
    ) -> DurableExecutionEvidence | None:
        row = await self._evidence.find_one(
            {"run_id": run_id, "kind": "run.owner.completed"},
            {"_id": 0},
            sort=[("durable_seq", -1)],
        )
        return (
            None
            if row is None
            else _DURABLE_EVIDENCE_ADAPTER.validate_python(row)
        )

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None:
        # publish 确认：queued→published + 记 published_at（回执超时重发的计时锚）。补发前崩溃留 queued。
        await self._coll.update_one(
            {"_id": run_id, "outbox": {"$elemMatch": {"durable_seq": durable_seq, "status": "queued"}}},
            {"$set": {"outbox.$.status": "published", "outbox.$.published_at": self._clock()}},
        )

    async def list_unpublished_outbox(self) -> list[OutboxFrame]:
        # 补发扫描：queued（落库但发布未确认）的 critical 行——崩溃/瞬时故障后按 seq 序补发（幂等）。
        cursor = self._coll.find({"outbox.status": "queued"}, {"_id": 1, "outbox": 1}).sort("_id", 1)
        frames: list[OutboxFrame] = []
        async for doc in cursor:
            run_id = str(doc["_id"])
            entries = _OUTBOX_ADAPTER.validate_python(doc.get("outbox") or [])
            live = sorted(
                (entry for entry in entries if entry.status in ("queued", "published")),
                key=lambda entry: entry.durable_seq,
            )
            for entry in live:
                # Never jump an unresolved published predecessor or a queued gap. Once the first
                # live non-queued row is reached, later seqs are not eligible for startup replay.
                if entry.status != "queued":
                    break
                if entry.index is None or entry.timestamp is None or entry.payload_json is None:
                    break
                frames.append(
                    OutboxFrame(
                        run_id=run_id,
                        durable_seq=entry.durable_seq,
                        event_id=entry.event_id,
                        kind=entry.kind,
                        index=entry.index,
                        timestamp=entry.timestamp,
                        payload_json=entry.payload_json,
                    )
                )
        frames.sort(key=lambda f: (f.run_id, f.durable_seq))
        return frames

    async def list_open_outbox_runs(self) -> list[str]:
        # 回执对账扫描：仍有 queued/published（未 consumed 收敛）outbox 行的 run。
        cursor = self._coll.find(
            {"outbox.status": {"$in": ["queued", "published"]}}, {"_id": 1}
        ).sort("_id", 1)
        return [str(doc["_id"]) async for doc in cursor]

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30_000
    ) -> ReceiptReconcile:
        # R4 consume/close 握手：读 session 回执→推进 consumed→硬删已确认 outbox 行→
        # rejected NACK / receipt_state_lost / producer_close_requested 各自收口（写者分域 CAS）。
        # 另：published 但回执一直不来、超宽限期的行（events 流被修剪/丢失致 session 从未见帧）→
        # 交 supervisor 复用固定身份重发，避免连续性水位永久卡死。
        now = self._clock()
        doc = await self._coll.find_one(
            {"_id": run_id}, {"outbox": 1, "terminal_fence_seq": 1}
        )
        if doc is None:
            return ReceiptReconcile()
        entries = _OUTBOX_ADAPTER.validate_python(doc.get("outbox") or [])
        live = [e for e in entries if e.status in ("queued", "published")]
        if not live:
            return ReceiptReconcile()
        receipts: dict[int, RunEventReceiptDoc] = {}
        async for raw in self._receipts.find({"run_id": run_id}, {"_id": 0}):
            rec = run_event_receipts_doc_adapter.validate_python(raw)
            receipts[int(rec.durable_seq)] = rec
        # rejected NACK 最高优先：同步 local fence=min(existing, rejected_seq)，交 supervisor 终局。
        rejected = sorted(s for s, r in receipts.items() if r.status == "rejected")
        if rejected:
            await self._sync_local_fence(run_id, rejected[0])
            return ReceiptReconcile(rejected_seq=rejected[0])
        manifest = await self._manifests.find_one({"run_id": run_id})
        raw_consumed = manifest.get("consumed_seq") if manifest is not None else 0
        consumed = raw_consumed if isinstance(raw_consumed, int) else 0
        by_seq = {e.durable_seq: e for e in entries}
        # Republish only a contiguous causal prefix starting at the Session watermark. A queued
        # predecessor, a not-yet-stale published predecessor, or an identity mismatch blocks all
        # newer seqs even if they are independently stale.
        stale: list[_OutboxEntry] = []
        candidate_seq = consumed + 1
        while (entry := by_seq.get(candidate_seq)) is not None:
            receipt = receipts.get(candidate_seq)
            if receipt is not None and receipt.status == "persisted":
                if entry.event_id != receipt.event_id:
                    break
                candidate_seq += 1
                continue
            if (
                entry.status != "published"
                or entry.published_at is None
                or now - entry.published_at < republish_grace_ms
                or entry.index is None
                or entry.timestamp is None
                or entry.payload_json is None
            ):
                break
            stale.append(entry)
            candidate_seq += 1
        republish = [
            OutboxFrame(
                run_id=run_id,
                durable_seq=e.durable_seq,
                event_id=e.event_id,
                kind=e.kind,
                index=e.index or 0,
                timestamp=e.timestamp or 0,
                payload_json=e.payload_json or "",
            )
            for e in stale
        ]
        if stale:
            await self._touch_published(run_id, [e.durable_seq for e in stale], now)
        if manifest is None:
            # 有 published 行待确认却无 manifest 且未 close：receipt_state_lost，绝不删 outbox。
            return ReceiptReconcile(receipt_state_lost=True, republish=republish)
        advanced = consumed
        seq = consumed + 1
        while seq in receipts and receipts[seq].status == "persisted":
            row = by_seq.get(seq)
            if row is not None and row.event_id != receipts[seq].event_id:
                # event_id 不一致（身份漂移）：停在此处，不 GC（fail-safe，交人排查）。
                break
            advanced = seq
            seq += 1
        if advanced > consumed:
            await self._advance_manifest_consumed(run_id, advanced)
            await self._gc_outbox_through(run_id, advanced)
        fence = doc.get("terminal_fence_seq")
        remaining = [e for e in live if e.durable_seq > advanced]
        close_requested = False
        if isinstance(fence, int) and advanced >= fence and not remaining:
            close_requested = await self._request_producer_close(run_id)
        return ReceiptReconcile(
            consumed_through=advanced if advanced > consumed else None,
            close_requested=close_requested,
            republish=republish,
        )

    async def _touch_published(self, run_id: str, seqs: list[int], now: int) -> None:
        # 重发候选 published_at 复位为 now：下一宽限窗才再评估，避免每拍重复重发。
        await self._coll.update_one(
            {"_id": run_id},
            {"$set": {"outbox.$[e].published_at": now}},
            array_filters=[{"e.durable_seq": {"$in": seqs}}],
        )

    async def _sync_local_fence(self, run_id: str, seq: int) -> None:
        await self._coll.update_one(
            {
                "_id": run_id,
                "$or": [{"terminal_fence_seq": None}, {"terminal_fence_seq": {"$gt": seq}}],
            },
            {"$set": {"terminal_fence_seq": seq}},
        )

    async def _advance_manifest_consumed(self, run_id: str, seq: int) -> None:
        # agent 写域：consumed_seq 单调前向 CAS（仅推进，绝不回退）。
        await self._manifests.update_one(
            {"run_id": run_id, "consumed_seq": {"$lt": seq}},
            {"$set": {"consumed_seq": seq, "updated_at": self._clock()}},
        )

    async def _gc_outbox_through(self, run_id: str, seq: int) -> None:
        # 已 consumed 确认的行硬删（payload 随之回收）。
        await self._coll.update_one(
            {"_id": run_id}, {"$pull": {"outbox": {"durable_seq": {"$lte": seq}}}}
        )

    async def _request_producer_close(self, run_id: str) -> bool:
        # agent 写域：终态 consumed 且无可发布行→请求 close（producer_closed 归 session 终设）。
        result = await self._manifests.update_one(
            {"run_id": run_id, "producer_close_requested": {"$ne": True}},
            {"$set": {"producer_close_requested": True, "updated_at": self._clock()}},
        )
        return result.modified_count == 1

    async def record_control_inbox(
        self, run_id: str, decision_id: str, fingerprint: str | None, body: str
    ) -> bool:
        # keep-first：$ne 数组守卫（同 add_steer 模式）——仅当无同 decision_id 条目时 push。
        # modified_count==1=首次落库（persisted）；0=重复 decision_id 或 run 文档缺失→丢弃不重放。
        result = await self._coll.update_one(
            {"_id": run_id, "control_inbox.decision_id": {"$ne": decision_id}},
            {
                "$push": {
                    "control_inbox": {
                        "decision_id": decision_id,
                        "fingerprint": fingerprint,
                        "status": "persisted",
                        "body": body,
                    }
                }
            },
        )
        return result.modified_count == 1

    async def mark_control_applied(self, run_id: str, decision_id: str) -> None:
        # 仅 persisted→applied 前向推进（positional $ 命中 elemMatch 元素）。
        await self._coll.update_one(
            {
                "_id": run_id,
                "control_inbox": {"$elemMatch": {"decision_id": decision_id, "status": "persisted"}},
            },
            {"$set": {"control_inbox.$.status": "applied"}},
        )

    async def mark_control_superseded(self, run_id: str, decision_id: str) -> None:
        await self._coll.update_one(
            {
                "_id": run_id,
                "control_inbox": {"$elemMatch": {"decision_id": decision_id, "status": "persisted"}},
            },
            {"$set": {"control_inbox.$.status": "superseded"}},
        )

    async def list_pending_control_inbox(self) -> list[ControlInboxRecord]:
        # 重启补办扫描：非终态 run 里 persisted 未 applied 的 control 条目。
        cursor = self._coll.find(
            {"terminal": {"$ne": True}, "control_inbox.status": "persisted"},
            {"_id": 1, "control_inbox": 1},
        ).sort("_id", 1)
        records: list[ControlInboxRecord] = []
        async for doc in cursor:
            entries = _CONTROL_INBOX_ADAPTER.validate_python(doc.get("control_inbox") or [])
            for entry in entries:
                if entry.status != "persisted":
                    continue
                records.append(
                    ControlInboxRecord(
                        run_id=str(doc["_id"]),
                        decision_id=entry.decision_id,
                        fingerprint=entry.fingerprint,
                        body=entry.body,
                    )
                )
        return records

    async def renew(self, run_id: str, owner: str) -> bool:
        # 严格属主续租（fencing）：owner 不符即失败——假死副本苏醒后据此让渡。
        result = await self._coll.update_one(
            {"_id": run_id, "terminal": {"$ne": True}, "owner": owner},
            {"$set": {"lease_expires_ms": self._clock() + self._ttl_ms}},
        )
        return result.matched_count == 1

    async def adopt(self, run_id: str, owner: str) -> None:
        # 所有权交接（resume 收养）：置 owner 并把暂停哨兵拉回活跃租约。
        await self._coll.update_one(
            {"_id": run_id, "terminal": {"$ne": True}},
            {"$set": {"lease_expires_ms": self._clock() + self._ttl_ms, "owner": owner}},
        )

    async def pause(self, run_id: str) -> None:
        # null 哨兵：HITL 等人可以是小时级，暂停 run 绝不被过期重拾重跑。
        await self._coll.update_one(
            {"_id": run_id, "terminal": {"$ne": True}},
            {"$set": {"lease_expires_ms": None}},
        )

    async def reclaim_expired(self, owner: str) -> list[RunRequest]:
        now = self._clock()
        reclaimed: list[RunRequest] = []
        while True:
            # find_one_and_update 原子认领：多 pod 并发 reclaim 时每个 run 恰被一个赢家拾走；
            # $lte 数值比较按 BSON 类型分桶，天然不命中 null 暂停哨兵。
            doc = await self._coll.find_one_and_update(
                {
                    "terminal": {"$ne": True},
                    "request_json": {"$type": "string"},
                    "lease_expires_ms": {"$lte": now},
                },
                {"$set": {"lease_expires_ms": now + self._ttl_ms, "owner": owner}},
            )
            if doc is None:
                return reclaimed
            raw = doc.get("request_json")
            if isinstance(raw, str):
                reclaimed.append(RunRequest.model_validate_json(raw))

    async def list_paused(self) -> list[str]:
        cursor = self._coll.find(
            {
                "terminal": {"$ne": True},
                "lease_expires_ms": None,
                "request_json": {"$ne": None},
            },
            {"_id": 1},
        ).sort("_id", 1)
        return [str(doc["_id"]) async for doc in cursor]

    async def add_tokens(self, run_id: str, count: int) -> int:
        doc = await self._coll.find_one_and_update(
            {"_id": run_id},
            {"$inc": {"token_total": count}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        total = doc.get("token_total") if doc else None
        if not isinstance(total, int):
            raise TypeError(f"token_total for {run_id!r} is not an int: {total!r}")
        return total

    async def add_usage(self, run_id: str, input_tokens: int, output_tokens: int) -> tuple[int, int]:
        doc = await self._coll.find_one_and_update(
            {"_id": run_id},
            {"$inc": {"usage_input": input_tokens, "usage_output": output_tokens}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        input_total = doc.get("usage_input") if doc else None
        output_total = doc.get("usage_output") if doc else None
        if not isinstance(input_total, int) or not isinstance(output_total, int):
            raise TypeError(f"usage totals for {run_id!r} are not ints")
        return (input_total, output_total)

    async def get_request(self, run_id: str) -> RunRequest | None:
        doc = await self._coll.find_one({"_id": run_id})
        if doc is None:
            return None
        raw = doc.get("request_json")
        if not isinstance(raw, str):
            return None
        return RunRequest.model_validate_json(raw)

    async def get_execution_context_binding(
        self, run_id: str
    ) -> ExecutionContextBinding | None:
        doc = await self._coll.find_one(
            {"_id": run_id}, {"execution_context_binding": 1}
        )
        if doc is None or doc.get("execution_context_binding") is None:
            return None
        return _EXECUTION_BINDING_ADAPTER.validate_python(doc["execution_context_binding"])

    async def bind_execution_context(
        self, run_id: str, binding: ExecutionContextBinding
    ) -> ExecutionContextBinding:
        doc = await self._coll.find_one_and_update(
            {
                "_id": run_id,
                "terminal": {"$ne": True},
                "execution_context_binding": {"$exists": False},
            },
            {"$set": {"execution_context_binding": binding.model_dump(mode="json")}},
            return_document=ReturnDocument.AFTER,
        )
        if doc is not None:
            return _EXECUTION_BINDING_ADAPTER.validate_python(
                doc["execution_context_binding"]
            )
        existing = await self.get_execution_context_binding(run_id)
        if existing is None or existing != binding:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_CONFLICT")
        return existing

    async def update_execution_checkpoint(
        self, run_id: str, checkpoint: ExecutionCheckpoint
    ) -> None:
        result = await self._coll.update_one(
            {
                "_id": run_id,
                "terminal": {"$ne": True},
                "execution_context_binding": {"$exists": True},
            },
            {
                "$set": {
                    "execution_context_binding.active_checkpoint": checkpoint.model_dump(
                        mode="json"
                    )
                }
            },
        )
        if result.matched_count != 1:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_CONFLICT")

    async def resolve_execution_parent(
        self,
        *,
        namespace: str,
        anchor: str,
        digest: str,
        continuation_run_id: str | None,
    ) -> ExecutionCheckpoint | None:
        query: dict[str, object] = {
            "terminal": True,
            "execution_context_completion.anchor": anchor,
            "execution_context_completion.digest": digest,
            "execution_context_completion.namespace": namespace,
            "execution_context_completion.owner_revision": 1,
        }
        if continuation_run_id is None:
            doc = await self._coll.find_one(query, {"execution_context_completion": 1})
        else:
            query["$or"] = [
                {"execution_context_completion.continuation_run_id": None},
                {
                    "execution_context_completion.continuation_run_id": continuation_run_id
                },
            ]
            doc = await self._coll.find_one_and_update(
                query,
                {
                    "$set": {
                        "execution_context_completion.continuation_run_id": continuation_run_id
                    }
                },
                projection={"execution_context_completion": 1},
                return_document=ReturnDocument.AFTER,
            )
        if doc is None or doc.get("execution_context_completion") is None:
            return None
        completion = _COMPLETED_EXECUTION_CONTEXT_ADAPTER.validate_python(
            doc["execution_context_completion"]
        )
        return completion.checkpoint

    async def try_complete_execution_context(
        self,
        completion: CompletedExecutionContext,
        owner_event: CompletionEventDraft,
        terminal_event: CompletionEventDraft,
    ) -> ClaimedCompletionFrames | None:
        if completion.owner_revision != 1 or completion.continuation_run_id is not None:
            raise ValueError("new completion owner must start at revision one")
        if owner_event.kind != "run.owner.completed":
            raise ValueError("completion owner event kind mismatch")
        if terminal_event.kind != "run.completed" or terminal_event.index != owner_event.index + 1:
            raise ValueError("completion terminal event must immediately follow owner")
        owner_payload = RunOwnerCompletedPayload.model_validate_json(owner_event.payload_json)
        if (
            owner_payload.execution_context_anchor != completion.anchor
            or owner_payload.execution_context_digest != completion.digest
            or owner_payload.owner_revision != completion.owner_revision
        ):
            raise ValueError("completion owner payload mismatch")
        terminal_payload = RunCompletedPayload.model_validate_json(terminal_event.payload_json)
        if terminal_payload.status != "completed":
            raise ValueError("execution context completion requires completed terminal")

        checkpoint = completion.checkpoint
        owner_event_id = _event_id()
        terminal_event_id = _event_id()
        owner_payload_sha256 = hashlib.sha256(owner_event.payload_json.encode()).hexdigest()
        now = self._clock()
        owner_evidence = make_durable_execution_evidence(
            run_id=completion.run_id,
            durable_seq=1,
            event_id=owner_event_id,
            event_kind=owner_event.kind,
            payload_json=owner_event.payload_json,
            recorded_at_ms=now,
            producer_instance_ref=self._producer_instance_ref,
            producer_generation=self._producer_generation,
        )
        owner_row: dict[str, object] = {
            "durable_seq": {"$subtract": ["$durable_counter", 1]},
            "event_id": {"$literal": owner_event_id},
            "kind": {"$literal": owner_event.kind},
            "index": owner_event.index,
            "timestamp": owner_event.timestamp,
            "payload_json": {"$literal": owner_event.payload_json},
            "status": {"$literal": "queued"},
        }
        terminal_row: dict[str, object] = {
            "durable_seq": "$durable_counter",
            "event_id": {"$literal": terminal_event_id},
            "kind": {"$literal": terminal_event.kind},
            "index": terminal_event.index,
            "timestamp": terminal_event.timestamp,
            "payload_json": {"$literal": terminal_event.payload_json},
            "status": {"$literal": "queued"},
        }
        pipeline: list[dict[str, object]] = [
            {
                "$set": {
                    "durable_counter": {
                        "$add": [{"$ifNull": ["$durable_counter", 0]}, 2]
                    }
                }
            },
            {
                "$set": {
                    "terminal": True,
                    "terminal_at_ms": now,
                    "terminal_fence_seq": "$durable_counter",
                    "output_sealed_high_watermark": {"$ifNull": ["$output_counter", 0]},
                    "output_sealed_digest_sha256": {
                        "$ifNull": [
                            "$output_digest_sha256",
                            {"$literal": initial_output_digest(completion.run_id)},
                        ]
                    },
                    "execution_context_completion": {
                        "$literal": completion.model_dump(mode="json")
                    },
                    "outbox": {
                        "$concatArrays": [
                            {"$ifNull": ["$outbox", []]},
                            [owner_row, terminal_row],
                        ]
                    },
                    "semantic_critical_frames": {
                        "$concatArrays": [
                            {"$ifNull": ["$semantic_critical_frames", []]},
                            [
                                {
                                    "semantic_key": {"$literal": "run.owner.completed"},
                                    "kind": {"$literal": owner_event.kind},
                                    "payload_sha256": {"$literal": owner_payload_sha256},
                                    "durable_seq": {"$subtract": ["$durable_counter", 1]},
                                    "event_id": {"$literal": owner_event_id},
                                }
                            ],
                        ]
                    },
                }
            },
        ]
        async def complete(
            session: AsyncClientSession | None,
        ) -> dict[str, object] | None:
            try:
                doc = await self._coll.find_one_and_update(
                    {
                        "_id": completion.run_id,
                        "terminal": {"$ne": True},
                        "$or": [
                            {"terminal_fence_seq": {"$exists": False}},
                            {"terminal_fence_seq": None},
                        ],
                        "semantic_critical_frames.semantic_key": {
                            "$ne": "run.owner.completed"
                        },
                        "execution_context_binding.namespace": completion.namespace,
                        "execution_context_binding.active_checkpoint.thread_id": checkpoint.thread_id,
                        "execution_context_binding.active_checkpoint.checkpoint_ns": checkpoint.checkpoint_ns,
                        "execution_context_binding.active_checkpoint.checkpoint_id": checkpoint.checkpoint_id,
                    },
                    pipeline,
                    return_document=ReturnDocument.AFTER,
                    session=session,
                )
            except DuplicateKeyError as error:
                raise ExecutionContextConflict(
                    "EXECUTION_CONTEXT_ANCHOR_COLLISION"
                ) from error
            if doc is None:
                return None
            final_seq = doc.get("durable_counter")
            if not isinstance(final_seq, int) or final_seq < 2:
                raise TypeError(
                    f"durable_counter for {completion.run_id!r} is not an int: "
                    f"{final_seq!r}"
                )
            output_high_watermark = doc.get("output_sealed_high_watermark")
            output_digest_sha256 = doc.get("output_sealed_digest_sha256")
            if not isinstance(output_high_watermark, int) or not isinstance(
                output_digest_sha256, str
            ):
                raise TypeError("OUTPUT_SEAL_INVALID")
            terminal_evidence = make_durable_execution_evidence(
                run_id=completion.run_id,
                durable_seq=final_seq,
                event_id=terminal_event_id,
                event_kind=terminal_event.kind,
                payload_json=terminal_event.payload_json,
                recorded_at_ms=now,
                producer_instance_ref=self._producer_instance_ref,
                producer_generation=self._producer_generation,
                output_high_watermark=output_high_watermark,
                output_digest_sha256=output_digest_sha256,
            )
            records = [
                owner_evidence.model_copy(update={"durable_seq": final_seq - 1}),
                terminal_evidence,
            ]
            await self._evidence.insert_many(
                [
                    {"_id": record.evidence_ref, **record.model_dump(mode="python")}
                    for record in records
                ],
                session=session,
            )
            return doc

        doc = await self._run_evidence_transaction(complete)
        if doc is None:
            return None
        final_seq = doc.get("durable_counter")
        if not isinstance(final_seq, int) or final_seq < 2:
            raise TypeError(
                f"durable_counter for {completion.run_id!r} is not an int: {final_seq!r}"
            )
        return ClaimedCompletionFrames(
            owner=DurableCompletionFrame(
                **owner_event.model_dump(),
                durable_seq=final_seq - 1,
                event_id=owner_event_id,
            ),
            terminal=DurableCompletionFrame(
                **terminal_event.model_dump(),
                durable_seq=final_seq,
                event_id=terminal_event_id,
            ),
        )

    async def purge_terminal(self, max_age_ms: int) -> int:
        # Completed lineage is product state, not ephemeral worker state. Keep its binding and
        # exact checkpoint owner for the same retention horizon as the checkpoint collection;
        # only scrub operational payloads after all durable outbox rows have been consumed.
        cutoff = self._clock() - max_age_ms
        stale_without_live_outbox: dict[str, object] = {
            "terminal": True,
            "terminal_at_ms": {"$lte": cutoff},
            "outbox.status": {"$nin": ["queued", "published"]},
        }
        candidates = self._coll.find(
            {
                **stale_without_live_outbox,
                "$or": [
                    {"execution_context_completion": {"$exists": False}},
                    {"retention_archived": {"$ne": True}},
                ],
            },
            {"_id": 1, "execution_context_completion": 1},
        )
        changed = 0
        async for candidate in candidates:
            run_id = candidate.get("_id")
            if not isinstance(run_id, str):
                raise TypeError("RETENTION_RUN_ID_INVALID")
            retains_completion = "execution_context_completion" in candidate

            async def purge_one(session: AsyncClientSession | None) -> bool:
                query: dict[str, object] = {
                    "_id": run_id,
                    **stale_without_live_outbox,
                    "execution_context_completion": {
                        "$exists": retains_completion
                    },
                }
                if retains_completion:
                    query["retention_archived"] = {"$ne": True}
                current = await self._coll.find_one(
                    query, {"_id": 1}, session=session
                )
                if current is None:
                    return False

                # Delete children before the parent/archive marker in nontransactional tests;
                # production wraps all private child collections in one transaction. A crash
                # can only leave an eligible parent for an idempotent retry, never orphans.
                await self._outputs.delete_many({"run_id": run_id}, session=session)
                await self._output_source_batches.delete_many(
                    {"run_id": run_id}, session=session
                )
                await self._evidence.delete_many({"run_id": run_id}, session=session)
                await self._action_owner_revisions.delete_many(
                    {"run_id": run_id}, session=session
                )
                if retains_completion:
                    archived = await self._coll.update_one(
                        query,
                        {
                            "$set": {"retention_archived": True},
                            "$unset": {
                                "request_json": "",
                                "lease_expires_ms": "",
                                "owner": "",
                                "usage_input": "",
                                "usage_output": "",
                                "token_total": "",
                                "steers": "",
                                "sandbox_id": "",
                                "tool_results": "",
                                "tool_journal": "",
                                "control_inbox": "",
                                "outbox": "",
                            },
                        },
                        session=session,
                    )
                    return archived.modified_count == 1
                deleted = await self._coll.delete_one(query, session=session)
                return deleted.deleted_count == 1

            if await self._run_evidence_transaction(purge_one):
                changed += 1
        return changed

    async def try_mark_terminal(self, run_id: str) -> bool:
        # 条件 update + upsert：已终态则过滤不中、upsert 撞 _id 抛 Duplicate → 已被认领。
        try:
            result = await self._coll.update_one(
                {"_id": run_id, "terminal": {"$ne": True}},
                [
                    {
                        "$set": {
                            "terminal": True,
                            "terminal_at_ms": self._clock(),
                            "output_sealed_high_watermark": {
                                "$ifNull": ["$output_counter", 0]
                            },
                            "output_sealed_digest_sha256": {
                                "$ifNull": [
                                    "$output_digest_sha256",
                                    {"$literal": initial_output_digest(run_id)},
                                ]
                            },
                        }
                    }
                ],
                upsert=True,
            )
        except DuplicateKeyError:
            return False
        return result.modified_count == 1 or result.upserted_id is not None

    async def is_terminal(self, run_id: str) -> bool:
        return await self._coll.find_one({"_id": run_id, "terminal": True}) is not None


    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None:
        # keep-first：字段已存在（重入/并发）不覆盖首跑结果。
        await self._coll.update_one(
            {"_id": run_id, f"tool_results.{tool_id}": {"$exists": False}},
            {"$set": {f"tool_results.{tool_id}": {"result": result, "is_error": is_error}}},
        )

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        # 仅更新已认领 run 的文档（filter 含 _id 存在语义）；$ne 数组守卫给 keep-first。
        await self._coll.update_one(
            {"_id": run_id, "steers.message_id": {"$ne": message_id}},
            {"$push": {"steers": {"message_id": message_id, "content": content}}},
        )

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        doc = await self._coll.find_one({"_id": run_id}, {"steers": 1})
        if doc is None:
            return []
        entries = _STEERS_ADAPTER.validate_python(doc.get("steers") or [])
        return [(entry.message_id, entry.content) for entry in entries]

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None:
        if not message_ids:
            return
        await self._coll.update_one(
            {"_id": run_id},
            {"$pull": {"steers": {"message_id": {"$in": message_ids}}}},
        )

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None:
        # keep-first：resume 竞态下首个绑定生效（与 put_tool_result 同模式）。
        await self._coll.update_one(
            {"_id": run_id, "sandbox_id": {"$exists": False}},
            {"$set": {"sandbox_id": sandbox_id}},
        )

    async def get_sandbox_id(self, run_id: str) -> str | None:
        doc = await self._coll.find_one({"_id": run_id}, {"sandbox_id": 1})
        if doc is None:
            return None
        value = doc.get("sandbox_id")
        return value if isinstance(value, str) else None

    async def get_tool_result(self, run_id: str, tool_id: str) -> tuple[str, bool] | None:
        doc = await self._coll.find_one({"_id": run_id}, {f"tool_results.{tool_id}": 1})
        if doc is None:
            return None
        # mongo 文档是 Any 边界：整块交 Pydantic 洗净（缓存是本仓自写，脏形状即 fail-loud）。
        raw: Any = doc.get("tool_results")
        if raw is None:
            return None
        entries = _TOOL_RESULTS_ADAPTER.validate_python(raw)
        entry = entries.get(tool_id)
        if entry is None:
            return None
        return (entry.result, entry.is_error)

    async def journal_tool_started(self, run_id: str, tool_call_id: str, name: str) -> bool:
        # R3：副作用工具执行前落 started 行（keep-first，锚=tool_call_id）。已存在（重入/并发）
        # 不覆盖，返回 False；首次落库返回 True。行随 run 终态由 purge_terminal 整文档回收。
        result = await self._coll.update_one(
            {"_id": run_id, f"tool_journal.{tool_call_id}": {"$exists": False}},
            {"$set": {f"tool_journal.{tool_call_id}": {"name": name, "status": "started"}}},
        )
        return result.modified_count == 1

    async def journal_tool_finished(
        self, run_id: str, tool_call_id: str, result: str, is_error: bool
    ) -> None:
        # started→succeeded|failed 前向推进（仅命中 started 行）：附记录结果供重放短路。
        status = "failed" if is_error else "succeeded"
        await self._coll.update_one(
            {"_id": run_id, f"tool_journal.{tool_call_id}.status": "started"},
            {
                "$set": {
                    f"tool_journal.{tool_call_id}.status": status,
                    f"tool_journal.{tool_call_id}.result": result,
                    f"tool_journal.{tool_call_id}.is_error": is_error,
                }
            },
        )

    async def clear_tool_journal(self, run_id: str, tool_call_id: str) -> None:
        # 工具内 interrupt（HITL 暂停，非崩溃）：撤销本次 started 行（视同无行），resume 按设计
        # 重进不被守门误判 unknown-outcome。真进程死不走此路，守门语义不变。
        await self._coll.update_one(
            {"_id": run_id}, {"$unset": {f"tool_journal.{tool_call_id}": ""}}
        )

    async def get_tool_journal(self, run_id: str, tool_call_id: str) -> ToolJournalRecord | None:
        doc = await self._coll.find_one({"_id": run_id}, {f"tool_journal.{tool_call_id}": 1})
        if doc is None:
            return None
        raw: Any = doc.get("tool_journal")
        if raw is None:
            return None
        entries = _TOOL_JOURNAL_ADAPTER.validate_python(raw)
        entry = entries.get(tool_call_id)
        if entry is None:
            return None
        return ToolJournalRecord(
            name=entry.name,
            status=entry.status,
            result=entry.result or "",
            is_error=bool(entry.is_error),
        )


def make_mongo_collection(
    url: str, db: str
) -> tuple[AsyncMongoClient[dict[str, object]], AsyncCollection[dict[str, object]]]:
    # 建客户端并取 ledger collection；调用方负责 client 生命周期。
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(url)
    return client, client[db]["ledger"]
