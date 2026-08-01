"""RunLedger 契约与后端工厂：多 pod 去重、TTL 租约、HITL 暂停哨兵、终态原子认领。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Protocol

from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import RunRequest
from kokoro_agent.evidence.models import (
    DurableOutputDraft,
    DurableOutputRecord,
    DurableRetentionStats,
)
from kokoro_agent.presentation.runtime import (
    PresentationAcknowledgeCommand,
    PresentationAcknowledgeState,
    PresentationCandidateRecord,
    PresentationQuarantineCommand,
)
from kokoro_agent.contract import AgentEvent
from kokoro_agent.evidence.service import ExecutionEvidenceReader
from kokoro_agent.storage.mongo import (
    AGENT_DURABLE_OUTPUT_COLLECTION,
    AGENT_DURABLE_OUTPUT_SOURCE_BATCH_COLLECTION,
    AGENT_EXECUTION_EVIDENCE_COLLECTION,
    AGENT_PRESENTATION_ADMISSION_COMMAND_COLLECTION,
    AGENT_PRESENTATION_CANDIDATE_COLLECTION,
    AGENT_PRESENTATION_DELIVERY_COLLECTION,
    AGENT_PRESENTATION_SOURCE_BATCH_COLLECTION,
    AGENT_PRESENTATION_STATE_COLLECTION,
    ControlInboxRecord,
    MongoLedger,
    OutboxFrame,
    ReceiptReconcile,
    StagedFrame,
    ToolJournalRecord,
    make_mongo_collection,
)
from kokoro_agent.storage.execution_context import ExecutionContextStore
from kokoro_agent.storage.owner_event import OwnerEventCommitResult

DEFAULT_LEASE_TTL_S = 90
DURABLE_OUTPUT_RETENTION_REQUIRES_CONSUMER_ACK = (
    "DURABLE_OUTPUT_RETENTION_REQUIRES_CONSUMER_ACK"
)

# 这些记录类型定义在低层 mongo 模块（避免 ledger↔mongo 循环）；此处再导出为 ledger 面契约。
__all__ = [
    "ControlInboxRecord",
    "DEFAULT_LEASE_TTL_S",
    "DURABLE_OUTPUT_RETENTION_REQUIRES_CONSUMER_ACK",
    "DurableRetentionStats",
    "LedgerSettings",
    "EvidenceLedger",
    "OutboxFrame",
    "ReceiptReconcile",
    "RunLedger",
    "StagedFrame",
    "ToolJournalRecord",
    "make_ledger",
]


class RunLedger(ExecutionContextStore, Protocol):
    async def owner_event_head(self, run_id: str) -> int: ...

    async def commit_owner_event(
        self,
        *,
        run_id: str,
        expected_index: int,
        kind: str,
        payload: BaseModel,
        lease_owner_ref: str,
        agent_thread_ref: str | None,
    ) -> OwnerEventCommitResult:
        """Fenced owner UoW for outputs, presentation and critical outbox."""
        ...

    async def try_claim(self, request: RunRequest, owner: str) -> bool:
        # 原子认领新 run：首个认领者持有 TTL 租约并返 True，重复广播去重返 False。
        ...

    async def claim_dispatch(self, run_id: str, consumer: str) -> bool:
        # dispatch CAS（D5）：run_dispatches pending→claimed。True=授权执行；False=迟到(expired)
        # /重复(claimed)帧丢弃不执行。无 intent 记录=兼容放行（执行去重仍由 try_claim 兜底）。
        ...

    async def quarantine_dispatch(self, raw_hash: str, source: str, reason: str) -> None:
        # 不可解析帧死信：记 {raw_hash,source,reason,at} 后由调用方 ACK（坏帧无 identity 不重投）。
        ...

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
        # R4：分配 per-run durable_seq（从 1）+ event_id，落 outbox queued 行；终态帧 CAS 设
        # local fence。semantic_key 非空时同 key+同 payload 原子复用既有身份，不同 payload
        # fail-loud；marker 保留到 run retention，不随 outbox receipt GC 删除。
        # 返回 None=post-fence（seq>fence）→superseded 摘要落库、caller 不发布。
        ...

    async def append_durable_outputs(
        self,
        run_id: str,
        source_event_ref: str,
        drafts: tuple[DurableOutputDraft, ...],
        *,
        recorded_at_ms: int,
        source_payload_sha256: str,
    ) -> tuple[DurableOutputRecord, ...] | None:
        # 一个 live event 的全部 outputs 在同一事务中分配连续 output_seq；stable source
        # identity + canonical source payload + batch cardinality/ordinal keep-first；零条
        # 也持久 marker，source 漂移、增删、重排均 fail-closed。
        # 终态 fence 后返回 None。
        ...

    async def pull_durable_output_records(
        self, run_id: str, after_output_seq: int, limit: int
    ) -> list[DurableOutputRecord]: ...

    async def append_presentation_event(
        self, event: AgentEvent, *, agent_thread_ref: str
    ) -> tuple[PresentationCandidateRecord, ...] | None: ...

    async def presentation_head(self, run_id: str) -> int: ...

    async def pull_presentation_candidates(
        self,
        run_id: str,
        after_presentation_seq: int,
        through_presentation_seq: int,
        limit: int,
    ) -> tuple[PresentationCandidateRecord, ...]: ...

    async def acknowledge_presentation_admissions(
        self, command: PresentationAcknowledgeCommand
    ) -> PresentationAcknowledgeState: ...

    async def quarantine_presentation_admission(
        self, command: PresentationQuarantineCommand
    ) -> PresentationAcknowledgeState: ...

    async def get_presentation_delivery_state(
        self, run_id: str
    ) -> PresentationAcknowledgeState: ...

    async def get_durable_retention_stats(self) -> DurableRetentionStats: ...

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None:
        # publish 确认：outbox 行 queued→published。补发前崩溃留 queued，scanner 幂等补发。
        ...

    async def list_unpublished_outbox(self) -> list[OutboxFrame]:
        # 补发扫描：queued 的 critical 行（落库但发布未确认）——重建 wire 帧按 seq 序补发。
        ...

    async def list_open_outbox_runs(self) -> list[str]:
        # 回执对账扫描：仍有 queued/published（未 consumed 收敛）outbox 行的 run。
        ...

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30_000
    ) -> ReceiptReconcile:
        # consume/close 握手：读 session 回执→推进 consumed→硬删已确认行；rejected NACK /
        # receipt_state_lost / producer_close_requested 各自收口（写者分域 CAS）。published 无回执
        # 且超 republish_grace_ms 的行→republish 列表交调用方复用固定身份重发（session 去重幂等）。
        ...

    async def record_control_inbox(
        self, run_id: str, decision_id: str, fingerprint: str | None, body: str
    ) -> bool:
        # R2 control inbox：keep-first 落 {decision_id,fingerprint,status:persisted,body}。
        # 首次落库返 True（persisted，续 apply）；重复 decision_id（重发/重投）返 False（丢弃不重放）。
        ...

    async def mark_control_applied(self, run_id: str, decision_id: str) -> None:
        # apply（Command resume/cancel）+ checkpoint 后置 applied：仅 persisted→applied 前向推进。
        ...

    async def mark_control_superseded(self, run_id: str, decision_id: str) -> None:
        # 重启续办发现 stale（fingerprint 不匹配/已终态）：标 superseded 不 apply。
        ...

    async def list_pending_control_inbox(self) -> list[ControlInboxRecord]:
        # 重启补办扫描：persisted 未 applied 且非终态的 control 条目。
        ...

    async def renew(self, run_id: str, owner: str) -> bool:
        # 严格属主续租（fencing）：仅当前 owner 可续；False=所有权已被他处夺走，
        # 调用方必须让渡本地执行（裂脑双跑收窄到一个心跳窗）。
        ...

    async def adopt(self, run_id: str, owner: str) -> None:
        # 所有权交接（resume 收养/过期重拾的赢家）：置 owner 并恢复活跃租约。
        ...

    async def pause(self, run_id: str) -> None:
        # HITL 暂停：租约置哨兵，等人期间不参与过期重拾。
        ...

    async def reclaim_expired(self, owner: str) -> list[RunRequest]:
        # 过期且无终态的 run 原子重认领并连同原始 request 返回，供从 checkpoint 续跑。
        ...

    async def get_request(self, run_id: str) -> RunRequest | None:
        # 取原 request 供 resume 重建 agent。
        ...

    async def list_paused(self) -> list[str]:
        # 哨兵暂停且非终态的 run：control 监听收养的数据源（认领 worker 崩溃后接续 HITL）。
        ...

    async def add_tokens(self, run_id: str, count: int) -> int:
        # token 预算的跨段累计（resume 重建不清零）：原子加并返回累计值。
        ...

    async def add_usage(self, run_id: str, input_tokens: int, output_tokens: int) -> tuple[int, int]:
        # run.completed 用量真源：跨段累计 input/output（与预算计数分开，语义不同）。
        ...

    async def purge_terminal(self, max_age_ms: int) -> int:
        # retention 清扫：终态且超龄的 run 连同附属（tool_results/计数/信箱）整体清除，返回条数。
        ...

    async def try_mark_terminal(self, run_id: str) -> bool:
        # 原子认领终态：首个认领者返 True，杜绝重复终态事件。
        ...

    async def is_terminal(self, run_id: str) -> bool:
        # 只读查：resume stale 闸。
        ...

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        # steering 信箱：keep-first 幂等（message_id 重放不覆盖）；未认领 run 安全丢弃。
        ...

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        # 非破坏读信箱（按到达序返回 (message_id, content)）：模型轮前注入。
        ...

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None:
        # 确认消费：仅删除已随 checkpoint 落定的插话（下一轮见证原子性——排空绝不先于落盘）。
        ...

    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None:
        # 结果审核暂停的双执行防护：首跑结果 keep-first 落盘，resume 重入命中即跳过工具执行。
        ...

    async def get_tool_result(self, run_id: str, tool_id: str) -> tuple[str, bool] | None: ...

    async def journal_tool_started(self, run_id: str, tool_call_id: str, name: str) -> bool:
        # R3 tool effect journal：副作用工具执行前落 started 行（keep-first，锚=tool_call_id）。
        # True=首次落库（续执行）；False=行已存在（重入/并发，不覆盖）。
        ...

    async def journal_tool_finished(
        self, run_id: str, tool_call_id: str, result: str, is_error: bool
    ) -> None:
        # 工具返回后 started→succeeded|failed（附记录结果供重放短路）；仅推进 started 行。
        ...

    async def clear_tool_journal(self, run_id: str, tool_call_id: str) -> None:
        # 工具内 interrupt（HITL 暂停≠崩溃）：撤销本次 started 行（视同无行），resume 重进不被误拦。
        ...

    async def get_tool_journal(self, run_id: str, tool_call_id: str) -> ToolJournalRecord | None:
        # 重放守门读侧：无行=正常执行；succeeded/failed=短路记录结果；started=unknown-outcome。
        ...

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None:
        # e2b run 级箱绑定（keep-first）：HITL resume 重连既往 sandbox，暂停期文件不丢。
        ...

    async def get_sandbox_id(self, run_id: str) -> str | None: ...


class EvidenceLedger(RunLedger, ExecutionEvidenceReader, Protocol):
    """Concrete ledger capability returned by the factory without widening RunLedger fakes."""


class LedgerSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    mongo_url: str
    mongo_db: str
    lease_ttl_ms: Annotated[int, Field(gt=0)]
    producer_instance_ref: Annotated[str, Field(min_length=1, max_length=256)] = "agent-runtime"
    producer_generation: Annotated[int, Field(gt=0)] = 1


@asynccontextmanager
async def make_ledger(
    settings: LedgerSettings,
) -> AsyncGenerator[EvidenceLedger, None]:
    client, collection = make_mongo_collection(settings.mongo_url, settings.mongo_db)
    try:
        await collection.create_index(
            "execution_context_completion.anchor",
            name="execution_context_completion_anchor_unique",
            unique=True,
            sparse=True,
        )
        evidence = collection.database[AGENT_EXECUTION_EVIDENCE_COLLECTION]
        await evidence.create_index(
            [("run_id", 1), ("durable_seq", 1)],
            name="run_durable_seq_unique",
            unique=True,
        )
        await evidence.create_index(
            [("run_id", 1), ("evidence_ref", 1)],
            name="run_evidence_ref_unique",
            unique=True,
        )
        await evidence.create_index(
            [("run_id", 1), ("kind", 1), ("durable_seq", -1)],
            name="run_kind_latest",
        )
        outputs = collection.database[AGENT_DURABLE_OUTPUT_COLLECTION]
        await outputs.create_index(
            [("run_id", 1), ("output_seq", 1)],
            name="run_output_seq_unique",
            unique=True,
        )
        await outputs.create_index(
            [("run_id", 1), ("source_event_ref", 1)],
            name="run_output_source_unique",
            unique=True,
        )
        await outputs.create_index(
            [("run_id", 1), ("text_part_ref_sha256", 1), ("output_seq", -1)],
            name="run_output_text_latest",
        )
        output_source_batches = collection.database[
            AGENT_DURABLE_OUTPUT_SOURCE_BATCH_COLLECTION
        ]
        await output_source_batches.create_index(
            [("run_id", 1), ("source_event_ref", 1)],
            name="run_output_source_batch_unique",
            unique=True,
        )
        presentation_candidates = collection.database[
            AGENT_PRESENTATION_CANDIDATE_COLLECTION
        ]
        await presentation_candidates.create_index(
            [("run_id", 1), ("presentation_seq", 1)],
            name="run_presentation_seq_unique",
            unique=True,
        )
        await presentation_candidates.create_index(
            [("run_id", 1), ("presentation_ref", 1)],
            name="run_presentation_ref_unique",
            unique=True,
        )
        presentation_source_batches = collection.database[
            AGENT_PRESENTATION_SOURCE_BATCH_COLLECTION
        ]
        await presentation_source_batches.create_index(
            [("run_id", 1), ("source_event_ref", 1)],
            name="run_presentation_source_unique",
            unique=True,
        )
        await collection.database[AGENT_PRESENTATION_STATE_COLLECTION].create_index(
            [("_id", 1), ("revision", 1)],
            name="presentation_state_revision",
        )
        await collection.database[AGENT_PRESENTATION_DELIVERY_COLLECTION].create_index(
            [("_id", 1), ("revision", 1)],
            name="presentation_delivery_revision",
        )
        await collection.database[
            AGENT_PRESENTATION_ADMISSION_COMMAND_COLLECTION
        ].create_index(
            [("run_id", 1), ("_id", 1)],
            name="presentation_admission_command_unique",
            unique=True,
        )
        yield MongoLedger(
            collection,
            ttl_ms=settings.lease_ttl_ms,
            producer_instance_ref=settings.producer_instance_ref,
            producer_generation=settings.producer_generation,
        )
    finally:
        await client.close()
