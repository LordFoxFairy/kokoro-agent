"""RunLedger 规格：TTL 租约全生命周期 + 终态原子认领（mongo 唯一真后端）。"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from kokoro.agent.execution.v1 import agent_execution_evidence_pb2 as evidence_pb2

from fakes import request
from kokoro_agent.contract import (
    MessageDeltaPayload,
    PlanProposal,
    PlanProposedPayload,
    PlanStep,
    RunCompletedPayload,
    RunFailedPayload,
    RunOwnerCompletedPayload,
)
from kokoro_agent.evidence.models import (
    append_output_digest,
    durable_output_draft_for_event,
    initial_output_digest,
)
from kokoro_agent.contract.storage import (
    RUN_DISPATCHES_COLLECTION,
    RUN_EVENT_RECEIPTS_COLLECTION,
    RUN_RECEIPT_MANIFESTS_COLLECTION,
)
from kokoro_agent.storage.ledger import (
    LedgerSettings,
    RunLedger,
    make_ledger,
)
from kokoro_agent.storage.mongo import (
    AGENT_DURABLE_OUTPUT_COLLECTION,
    AGENT_DURABLE_OUTPUT_SOURCE_BATCH_COLLECTION,
    AGENT_EXECUTION_EVIDENCE_COLLECTION,
    DISPATCH_DLQ_COLLECTION,
    MongoLedger,
)
from kokoro_agent.storage.execution_context import (
    CompletionEventDraft,
    CompletedExecutionContext,
    ExecutionCheckpoint,
    ExecutionContextBinding,
)

_MONGO_URL = os.environ.get("KOKORO_MONGO_URL", "mongodb://127.0.0.1:27017")
_TTL_MS = 1000
OWNER = "worker-a"
OTHER = "worker-b"


def _plan_payload(owner_ref: str) -> str:
    return PlanProposedPayload(
        segment_id="seg-plan",
        owner_ref=owner_ref,
        owner_version=1,
        proposal=PlanProposal(
            summary="Plan",
            steps=[PlanStep(step_ref="step-1", label="Do", status="pending")],
            allowed_actions=["accept", "reject"],
        ),
    ).model_dump_json()


class FakeClock:
    def __init__(self, now: int = 1_000_000) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    def advance_ms(self, delta: int) -> None:
        self.now += delta


async def _mongo_collection() -> tuple[
    AsyncMongoClient[dict[str, object]], AsyncCollection[dict[str, object]]
]:
    # 真 mongo 前置：不可达即 fail-loud（不 skip、不灌绿数）。
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(
        _MONGO_URL, serverSelectionTimeoutMS=1000
    )
    try:
        await client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001 — 服务缺失显式炸
        await client.close()
        raise RuntimeError(f"mongo required but unreachable at {_MONGO_URL}: {exc}") from exc
    return client, client["kokoro_test"][f"ledger_{uuid.uuid4().hex}"]


@asynccontextmanager
async def _mongo_store(clock: FakeClock) -> AsyncGenerator[RunLedger]:
    client, coll = await _mongo_collection()
    try:
        yield MongoLedger(
            coll,
            ttl_ms=_TTL_MS,
            clock=clock,
            allow_nontransactional_evidence_for_tests=True,
        )
    finally:
        await coll.drop()
        await coll.database[AGENT_EXECUTION_EVIDENCE_COLLECTION].drop()
        await coll.database[AGENT_DURABLE_OUTPUT_COLLECTION].drop()
        await coll.database[AGENT_DURABLE_OUTPUT_SOURCE_BATCH_COLLECTION].drop()
        await client.close()


# --- 共用行为矩阵：任意 RunLedger 实例逐条对标 ---


async def _assert_claim_and_terminal(store: RunLedger) -> None:
    req = request("run-abc")
    assert await store.try_claim(req, OWNER) is True
    assert await store.try_claim(req, OWNER) is False  # 重复认领去重
    assert await store.get_request(req.run_id) == req
    assert await store.is_terminal(req.run_id) is False
    assert await store.try_mark_terminal(req.run_id) is True
    assert await store.try_mark_terminal(req.run_id) is False  # 终态恰一次
    assert await store.is_terminal(req.run_id) is True


async def _assert_unclaimed_mark_terminal(store: RunLedger) -> None:
    # 未认领的 run 也能直接认领终态（构建失败快速收口场景）。
    assert await store.try_mark_terminal("run-never-claimed") is True
    assert await store.get_request("run-never-claimed") is None


async def _assert_lease_lifecycle(store: RunLedger, clock: FakeClock) -> None:
    req = request("run-lease")
    await store.try_claim(req, OWNER)
    # 租约未到期：不可重拾。
    clock.advance_ms(_TTL_MS - 1)
    assert await store.reclaim_expired(OWNER) == []
    # 到期边界（expires<=now）：恰好重拾一次并连同原始 request 返回。
    clock.advance_ms(1)
    reclaimed = await store.reclaim_expired(OWNER)
    assert [r.run_id for r in reclaimed] == ["run-lease"]
    # 重拾即续租：立刻再拾为空。
    assert await store.reclaim_expired(OWNER) == []


async def _assert_renew_extends(store: RunLedger, clock: FakeClock) -> None:
    req = request("run-renew")
    await store.try_claim(req, OWNER)
    clock.advance_ms(_TTL_MS - 1)
    await store.renew(req.run_id, OWNER)
    clock.advance_ms(_TTL_MS - 1)
    assert await store.reclaim_expired(OWNER) == []  # 心跳续租后未过期
    clock.advance_ms(1)
    assert [r.run_id for r in await store.reclaim_expired(OWNER)] == ["run-renew"]


async def _assert_pause_excludes_reclaim(store: RunLedger, clock: FakeClock) -> None:
    req = request("run-paused")
    await store.try_claim(req, OWNER)
    await store.pause(req.run_id)
    # HITL 等人可以无限期：暂停哨兵永不参与过期重拾。
    clock.advance_ms(_TTL_MS * 1000)
    assert await store.reclaim_expired(OWNER) == []
    # resume 续租后重新纳入租约生命周期。
    await store.renew(req.run_id, OWNER)
    clock.advance_ms(_TTL_MS)
    assert [r.run_id for r in await store.reclaim_expired(OWNER)] == ["run-paused"]


async def _assert_usage_accumulation_two_columns(store: RunLedger, clock: FakeClock) -> None:
    # run.completed 用量真源：跨段累计 input/output 双列（与预算计数分开存，语义不同）。
    req = request("run-usage")
    await store.try_claim(req, OWNER)
    assert await store.add_usage("run-usage", 10, 1) == (10, 1)
    assert await store.add_usage("run-usage", 20, 2) == (30, 3)
    assert await store.add_usage("run-usage", 0, 0) == (30, 3)  # 零增量幂等读
    assert await store.add_usage("run-usage-other", 5, 5) == (5, 5)  # run 间隔离


async def _assert_token_accumulation_per_run(store: RunLedger, clock: FakeClock) -> None:
    # token 预算跨 HITL 段累计：resume 重建 middleware 后计数不清零；run 间隔离。
    req_a, req_b = request("run-tok-a"), request("run-tok-b")
    await store.try_claim(req_a, OWNER)
    await store.try_claim(req_b, OWNER)
    assert await store.add_tokens("run-tok-a", 100) == 100
    assert await store.add_tokens("run-tok-a", 50) == 150
    assert await store.add_tokens("run-tok-b", 7) == 7  # 隔离
    assert await store.add_tokens("run-tok-a", 0) == 150  # 零增量幂等读


async def _assert_list_paused_only_pause_sentinel(store: RunLedger, clock: FakeClock) -> None:
    # control 监听收养的数据源：仅"哨兵暂停且非终态"的 run；活跃/终态/重续租均不入列。
    active, paused, done = request("run-active"), request("run-hitl"), request("run-final")
    for req in (active, paused, done):
        await store.try_claim(req, OWNER)
    await store.pause(paused.run_id)
    await store.try_mark_terminal(done.run_id)
    assert await store.list_paused() == [paused.run_id]
    await store.renew(paused.run_id, OWNER)  # resume 续租 → 离开暂停态
    assert await store.list_paused() == []
    await store.pause(paused.run_id)
    await store.try_mark_terminal(paused.run_id)  # 终态后即使哨兵仍在也不入列
    assert await store.list_paused() == []


async def _assert_terminal_excluded_from_reclaim(store: RunLedger, clock: FakeClock) -> None:
    req = request("run-done")
    await store.try_claim(req, OWNER)
    await store.try_mark_terminal(req.run_id)
    clock.advance_ms(_TTL_MS * 2)
    assert await store.reclaim_expired(OWNER) == []


async def _assert_concurrent_single_winner(store: RunLedger) -> None:
    req = request("run-race")
    claims = await asyncio.gather(*(store.try_claim(req, OWNER) for _ in range(8)))
    assert sum(claims) == 1
    terminals = await asyncio.gather(*(store.try_mark_terminal(req.run_id) for _ in range(8)))
    assert sum(terminals) == 1


async def _assert_tool_result_keep_first(store: RunLedger) -> None:
    # 结果审核缓存：首跑 keep-first，重入/并发写不覆盖；未知键返 None。
    req = request("run-tr")
    assert await store.try_claim(req, OWNER) is True
    assert await store.get_tool_result("run-tr", "t1") is None
    await store.put_tool_result("run-tr", "t1", "first", False)
    await store.put_tool_result("run-tr", "t1", "second", True)
    assert await store.get_tool_result("run-tr", "t1") == ("first", False)
    assert await store.get_tool_result("run-tr", "other") is None


async def _assert_sandbox_binding_keep_first(store: RunLedger) -> None:
    # e2b run 级箱绑定：首绑生效（resume 竞态不覆盖）、未绑定返 None、run 间隔离。
    req = request("run-sbx")
    assert await store.try_claim(req, OWNER) is True
    assert await store.get_sandbox_id("run-sbx") is None
    await store.put_sandbox_id("run-sbx", "sbx_first")
    await store.put_sandbox_id("run-sbx", "sbx_second")
    assert await store.get_sandbox_id("run-sbx") == "sbx_first"
    assert await store.get_sandbox_id("run-other") is None


async def _assert_steer_mailbox(store: RunLedger) -> None:
    # steering 信箱：keep-first 幂等、按到达序、peek 非破坏、ack 按 id 精确删（原子性=下一轮见证）。
    req = request("run-steer")
    assert await store.try_claim(req, OWNER) is True
    assert await store.peek_steers("run-steer") == []
    await store.add_steer("run-steer", "m1", "改成国内市场")
    await store.add_steer("run-steer", "m2", "再加一条")
    await store.add_steer("run-steer", "m1", "重放不覆盖")
    expected = [("m1", "改成国内市场"), ("m2", "再加一条")]
    assert await store.peek_steers("run-steer") == expected
    assert await store.peek_steers("run-steer") == expected  # peek 非破坏（崩溃窗口不丢）
    await store.ack_steers("run-steer", ["m1"])
    assert await store.peek_steers("run-steer") == [("m2", "再加一条")]  # 精确删，未落定的保留
    await store.ack_steers("run-steer", ["m2"])
    await store.ack_steers("run-steer", ["m2"])  # 重复 ack 幂等
    assert await store.peek_steers("run-steer") == []
    # 未认领 run 的插话安全丢弃（绝不预创建 run 文档毒化 try_claim 去重）。
    await store.add_steer("run-ghost", "mg", "orphan")
    assert await store.peek_steers("run-ghost") == []
    assert await store.try_claim(request("run-ghost"), OWNER) is True



async def _assert_owner_fencing(store: RunLedger, clock: FakeClock) -> None:
    # 裂脑 fencing：过期被 B 重拾后，原属主 A 续租必须失败（据此让渡本地执行）；
    # B 续租成功；resume 收养（adopt）同样完成所有权交接。
    req = request("run-fence")
    assert await store.try_claim(req, OWNER) is True
    assert await store.renew(req.run_id, OWNER) is True
    clock.advance_ms(_TTL_MS)
    reclaimed = await store.reclaim_expired(OTHER)
    assert [r.run_id for r in reclaimed] == ["run-fence"]
    assert await store.renew(req.run_id, OWNER) is False  # A 已失权
    assert await store.renew(req.run_id, OTHER) is True
    await store.adopt(req.run_id, OWNER)  # 收养交接回 A
    assert await store.renew(req.run_id, OWNER) is True
    assert await store.renew(req.run_id, OTHER) is False
    await store.try_mark_terminal(req.run_id)
    assert await store.renew(req.run_id, OWNER) is False  # 终态后无人可续


async def _assert_purge_terminal(store: RunLedger, clock: FakeClock) -> None:
    # retention 清扫：仅"终态且超龄"的 run 连同附属数据整体清除；活跃/暂停/新终态永不清。
    done_old, done_new, active = request("run-old"), request("run-new"), request("run-live")
    for req in (done_old, done_new, active):
        await store.try_claim(req, OWNER)
    await store.add_tokens("run-old", 5)
    await store.add_usage("run-old", 1, 2)
    await store.put_tool_result("run-old", "t1", "r", False)
    output_draft = durable_output_draft_for_event(
        MessageDeltaPayload(segment_id="segment-retention", delta="old output")
    )
    assert output_draft is not None
    assert await store.append_durable_outputs(
        "run-old",
        "retention-event",
        (output_draft,),
        recorded_at_ms=clock.now,
        source_payload_sha256="0" * 64,
    )
    await store.try_mark_terminal("run-old")
    clock.advance_ms(10_000)
    await store.try_mark_terminal("run-new")  # 新终态：未超龄
    assert await store.purge_terminal(max_age_ms=5_000) == 1
    # 超龄终态被清：run 行与附属全消失，可再次认领（id 重用语义）。
    assert await store.is_terminal("run-old") is False
    assert await store.get_tool_result("run-old", "t1") is None
    assert await store.pull_durable_output_records("run-old", 0, 64) == []
    assert await store.try_claim(request("run-old"), OWNER) is True
    assert await store.append_durable_outputs(
        "run-old",
        "retention-event",
        (),
        recorded_at_ms=clock.now,
        source_payload_sha256="f" * 64,
    ) == ()
    assert await store.add_tokens("run-old", 1) == 1  # 计数从零（旧累计已清）
    # 未超龄/活跃不受影响。
    assert await store.is_terminal("run-new") is True
    assert await store.get_request("run-live") is not None
    assert await store.purge_terminal(max_age_ms=5_000) == 0  # 幂等


_MATRIX: list[Callable[[RunLedger, FakeClock], Awaitable[None]]] = [
    lambda store, _clock: _assert_claim_and_terminal(store),
    lambda store, _clock: _assert_unclaimed_mark_terminal(store),
    _assert_lease_lifecycle,
    _assert_renew_extends,
    _assert_pause_excludes_reclaim,
    _assert_terminal_excluded_from_reclaim,
    lambda store, _clock: _assert_concurrent_single_winner(store),
    lambda store, _clock: _assert_tool_result_keep_first(store),
    lambda store, _clock: _assert_sandbox_binding_keep_first(store),
    _assert_list_paused_only_pause_sentinel,
    _assert_token_accumulation_per_run,
    _assert_usage_accumulation_two_columns,
    lambda store, _clock: _assert_steer_mailbox(store),
    _assert_purge_terminal,
    _assert_owner_fencing,
]
_MATRIX_IDS = [
    "claim_and_terminal",
    "unclaimed_mark_terminal",
    "lease_lifecycle",
    "renew_extends",
    "pause_excludes_reclaim",
    "terminal_excluded",
    "concurrent_single_winner",
    "tool_result_keep_first",
    "sandbox_binding_keep_first",
    "list_paused_only_pause_sentinel",
    "token_accumulation_per_run",
    "usage_accumulation_two_columns",
    "steer_mailbox",
    "purge_terminal",
    "owner_fencing",
]


@pytest.mark.parametrize("check", _MATRIX, ids=_MATRIX_IDS)
async def test_mongo_behaviour_matrix(
    check: Callable[[RunLedger, FakeClock], Awaitable[None]],
) -> None:
    clock = FakeClock()
    async with _mongo_store(clock) as store:
        await check(store, clock)


# --- 工厂：跨周期持久性 + 类型 ---


def _settings(mongo_db: str) -> LedgerSettings:
    return LedgerSettings.model_validate(
        {
            "mongo_url": _MONGO_URL,
            "mongo_db": mongo_db,
            "lease_ttl_ms": _TTL_MS,
        }
    )


async def test_factory_mongo_persists_across_cycles() -> None:
    settings = _settings(f"kokoro_test_{uuid.uuid4().hex}")
    req = request("run-persist")
    async with make_ledger(settings) as store:
        assert isinstance(store, MongoLedger)
        assert await store.try_claim(req, OWNER) is True
        await store.try_mark_terminal(req.run_id)
    # 全新工厂周期（模拟重启/另一 pod）从同一 mongo 续读。
    async with make_ledger(settings) as store:
        assert await store.get_request(req.run_id) == req
        assert await store.is_terminal(req.run_id) is True
    # 清扫本测专用 db。
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(_MONGO_URL)
    try:
        await client.drop_database(settings.mongo_db)
    finally:
        await client.close()


async def test_output_authority_is_independent_idempotent_and_terminal_sealed() -> None:
    clock = FakeClock()
    async with _mongo_store(clock) as raw_store:
        assert isinstance(raw_store, MongoLedger)
        store = raw_store
        req = request("run-output")
        assert await store.try_claim(req, OWNER) is True
        first_draft = durable_output_draft_for_event(
            MessageDeltaPayload(segment_id="segment-1", delta="hello")
        )
        second_draft = durable_output_draft_for_event(
            MessageDeltaPayload(segment_id="segment-1", delta=" world")
        )
        assert first_draft is not None and second_draft is not None

        first_batch = await store.append_durable_outputs(
            req.run_id,
            "event:0",
            (first_draft,),
            recorded_at_ms=clock.now,
            source_payload_sha256="0" * 64,
        )
        duplicate_batch = await store.append_durable_outputs(
            req.run_id,
            "event:0",
            (first_draft,),
            recorded_at_ms=clock.now,
            source_payload_sha256="0" * 64,
        )
        with pytest.raises(ValueError, match="OUTPUT_SOURCE_CONFLICT"):
            await store.append_durable_outputs(
                req.run_id,
                "event:0",
                (second_draft,),
                recorded_at_ms=clock.now,
                source_payload_sha256="0" * 64,
            )
        second_batch = await store.append_durable_outputs(
            req.run_id,
            "event:1",
            (second_draft,),
            recorded_at_ms=clock.now + 1,
            source_payload_sha256="1" * 64,
        )
        assert first_batch is not None and second_batch is not None
        first, second = first_batch[0], second_batch[0]
        assert duplicate_batch is not None
        duplicate = duplicate_batch[0]
        assert duplicate == first
        assert [first.output_seq, second.output_seq] == [1, 2]

        records = await store.pull_durable_output_records(req.run_id, 0, 64)
        assert [record.output_seq for record in records] == [1, 2]
        expected_digest = append_output_digest(
            append_output_digest(
                initial_output_digest(req.run_id), 1, first.payload_sha256
            ),
            2,
            second.payload_sha256,
        )

        assert await store.try_mark_terminal(req.run_id) is True
        terminal = await store.stage_critical_frame(
            req.run_id,
            "run.failed",
            2,
            clock.now + 2,
            RunFailedPayload(
                code="internal_error", error_kind="RuntimeError", message="failed"
            ).model_dump_json(),
            terminal=True,
        )
        assert terminal is not None and terminal.durable_seq == 1
        evidence = await store.pull_durable_execution_evidence(req.run_id, 0, 10)
        failed = evidence_pb2.DurableExecutionCanonicalPayloadV1.FromString(
            evidence[-1].canonical_payload
        ).run_failed
        assert failed.output_high_watermark == 2
        assert failed.output_digest_sha256 == expected_digest
        assert (
            await store.append_durable_outputs(
                req.run_id,
                "event:3",
                (second_draft,),
                recorded_at_ms=clock.now + 3,
                source_payload_sha256="3" * 64,
            )
            is None
        )


async def test_output_authority_retries_contention_without_losing_records() -> None:
    clock = FakeClock()
    async with _mongo_store(clock) as raw_store:
        assert isinstance(raw_store, MongoLedger)
        store = raw_store
        run_id = "run-output-race"
        assert await store.try_claim(request(run_id), OWNER) is True
        drafts = [
            durable_output_draft_for_event(
                MessageDeltaPayload(segment_id="segment-race", delta=f"delta-{index}")
            )
            for index in range(16)
        ]
        assert all(draft is not None for draft in drafts)
        records = await asyncio.gather(
            *(
                store.append_durable_outputs(
                    run_id,
                    f"event:{index}",
                    (draft,),
                    recorded_at_ms=clock.now + index,
                    source_payload_sha256=f"{index:064x}",
                )
                for index, draft in enumerate(drafts)
                if draft is not None
            )
        )
        assert all(record is not None for record in records)
        assert sorted(
            record[0].output_seq for record in records if record is not None
        ) == list(range(1, 17))
        page = await store.pull_durable_output_records(run_id, 0, 64)
        assert [record.output_seq for record in page] == list(range(1, 17))


async def test_output_authority_rejects_matching_shorter_batch_replay() -> None:
    clock = FakeClock()
    async with _mongo_store(clock) as raw_store:
        assert isinstance(raw_store, MongoLedger)
        store = raw_store
        run_id = "run-output-batch-cardinality"
        assert await store.try_claim(request(run_id), OWNER) is True
        first = durable_output_draft_for_event(
            MessageDeltaPayload(segment_id="segment-batch", delta="first")
        )
        second = durable_output_draft_for_event(
            MessageDeltaPayload(segment_id="segment-batch", delta="second")
        )
        assert first is not None and second is not None
        inserted = await store.append_durable_outputs(
            run_id,
            "event:stable",
            (first, second),
            recorded_at_ms=clock.now,
            source_payload_sha256="0" * 64,
        )
        assert inserted is not None

        with pytest.raises(ValueError, match="OUTPUT_SOURCE_BATCH_CONFLICT"):
            await store.append_durable_outputs(
                run_id,
                "event:stable",
                (first,),
                recorded_at_ms=clock.now + 1,
                source_payload_sha256="0" * 64,
            )

        page = await store.pull_durable_output_records(run_id, 0, 64)
        assert [record.payload_sha256 for record in page] == [
            record.payload_sha256 for record in inserted
        ]


async def test_output_authority_persists_zero_cardinality_batch_identity() -> None:
    clock = FakeClock()
    async with _mongo_store(clock) as raw_store:
        assert isinstance(raw_store, MongoLedger)
        store = raw_store
        run_id = "run-output-zero-batch"
        assert await store.try_claim(request(run_id), OWNER) is True
        first = durable_output_draft_for_event(
            MessageDeltaPayload(segment_id="segment-zero", delta="first")
        )
        second = durable_output_draft_for_event(
            MessageDeltaPayload(segment_id="segment-zero", delta="second")
        )
        assert first is not None and second is not None

        assert await store.append_durable_outputs(
            run_id,
            "event-zero",
            (),
            recorded_at_ms=clock.now,
            source_payload_sha256="a" * 64,
        ) == ()
        assert await store.append_durable_outputs(
            run_id,
            "event-zero",
            (),
            recorded_at_ms=clock.now + 1,
            source_payload_sha256="a" * 64,
        ) == ()
        with pytest.raises(ValueError, match="OUTPUT_SOURCE_CONFLICT"):
            await store.append_durable_outputs(
                run_id,
                "event-zero",
                (),
                recorded_at_ms=clock.now + 2,
                source_payload_sha256="b" * 64,
            )
        with pytest.raises(ValueError, match="OUTPUT_SOURCE_BATCH_CONFLICT"):
            await store.append_durable_outputs(
                run_id,
                "event-zero",
                (first,),
                recorded_at_ms=clock.now + 3,
                source_payload_sha256="a" * 64,
            )

        inserted = await store.append_durable_outputs(
            run_id,
            "event-nonzero",
            (first, second),
            recorded_at_ms=clock.now + 4,
            source_payload_sha256="c" * 64,
        )
        assert inserted is not None
        assert [record.output_seq for record in inserted] == [1, 2]
        with pytest.raises(ValueError, match="OUTPUT_SOURCE_BATCH_CONFLICT"):
            await store.append_durable_outputs(
                run_id,
                "event-nonzero",
                (),
                recorded_at_ms=clock.now + 5,
                source_payload_sha256="c" * 64,
            )
        with pytest.raises(ValueError, match="OUTPUT_SOURCE_CONFLICT"):
            await store.append_durable_outputs(
                run_id,
                "event-nonzero",
                (second, first),
                recorded_at_ms=clock.now + 6,
                source_payload_sha256="d" * 64,
            )

        page = await store.pull_durable_output_records(run_id, 0, 64)
        assert page == list(inserted)
        assert await store.try_mark_terminal(run_id) is True
        assert (
            await store.append_durable_outputs(
                run_id,
                "event-post-terminal-zero",
                (),
                recorded_at_ms=clock.now + 7,
                source_payload_sha256="e" * 64,
            )
            is None
        )


async def test_output_factory_creates_unique_sequence_and_source_indexes() -> None:
    settings = _settings(f"kokoro_output_index_{uuid.uuid4().hex}")
    async with make_ledger(settings):
        pass
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(_MONGO_URL)
    try:
        indexes = await client[settings.mongo_db][
            AGENT_DURABLE_OUTPUT_COLLECTION
        ].index_information()
        source_batch_indexes = await client[settings.mongo_db][
            AGENT_DURABLE_OUTPUT_SOURCE_BATCH_COLLECTION
        ].index_information()
        assert indexes["run_output_seq_unique"]["unique"] is True
        assert indexes["run_output_source_unique"]["unique"] is True
        assert indexes["run_output_seq_unique"]["key"] == [
            ("run_id", 1),
            ("output_seq", 1),
        ]
        assert source_batch_indexes["run_output_source_batch_unique"]["unique"] is True
        assert source_batch_indexes["run_output_source_batch_unique"]["key"] == [
            ("run_id", 1),
            ("source_event_ref", 1),
        ]
    finally:
        await client.drop_database(settings.mongo_db)
        await client.close()

# --- Wave2 R1：dispatch CAS（run_dispatches pending→claimed）+ run.started outbox + DLQ ---


@asynccontextmanager
async def _mongo_ledger_with_dispatches(
    clock: FakeClock,
) -> AsyncGenerator[tuple[MongoLedger, AsyncCollection[dict[str, object]]]]:
    # 同库兄弟集合：run_dispatches 由 session 写、agent CAS 读（此处直接 seed 模拟 session）。
    client, coll = await _mongo_collection()
    dispatches = coll.database[RUN_DISPATCHES_COLLECTION]
    try:
        yield (
            MongoLedger(
                coll,
                ttl_ms=_TTL_MS,
                clock=clock,
                allow_nontransactional_evidence_for_tests=True,
            ),
            dispatches,
        )
    finally:
        await coll.drop()
        await dispatches.drop()
        await coll.database[DISPATCH_DLQ_COLLECTION].drop()
        await coll.database[AGENT_EXECUTION_EVIDENCE_COLLECTION].drop()
        await coll.database[AGENT_DURABLE_OUTPUT_COLLECTION].drop()
        await coll.database[AGENT_DURABLE_OUTPUT_SOURCE_BATCH_COLLECTION].drop()
        # R4 回执/清单是同库固定名兄弟集合（跨测试共享）：逐测试清空，串行安全。
        await coll.database[RUN_EVENT_RECEIPTS_COLLECTION].drop()
        await coll.database[RUN_RECEIPT_MANIFESTS_COLLECTION].drop()
        await client.close()


async def _seed_dispatch(
    dispatches: AsyncCollection[dict[str, object]],
    run_id: str,
    status: str,
    deadline_at: int,
) -> None:
    await dispatches.insert_one(
        {
            "run_id": run_id,
            "session_id": "s1",
            "namespace": "local:s1",
            "fence": f"fence-{run_id}",
            "status": status,
            "deadline_at": deadline_at,
            "created_at": 0,
            "updated_at": 0,
        }
    )


async def test_claim_dispatch_pending_wins_and_flips_claimed() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        await _seed_dispatch(dispatches, "run-win", "pending", deadline_at=clock.now + 1000)
        assert await store.claim_dispatch("run-win", OWNER) is True
        doc = await dispatches.find_one({"run_id": "run-win"})
        assert doc is not None and doc["status"] == "claimed"
        assert doc["claimed_by"] == OWNER
        # 已 claimed：重复投递帧 CAS 输，丢弃不执行（§8.3 claim 后 ACK 前崩溃重投）。
        assert await store.claim_dispatch("run-win", OTHER) is False


async def test_claim_dispatch_expired_and_deadline_passed_lose() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        # session reconciler 已转 expired：迟到帧永不执行。
        await _seed_dispatch(dispatches, "run-exp", "expired", deadline_at=clock.now + 1000)
        assert await store.claim_dispatch("run-exp", OWNER) is False
        # pending 但 deadline 已过（reconciler 尚未跑）：agent 不认领，交 reconciler 转 expired。
        await _seed_dispatch(dispatches, "run-late", "pending", deadline_at=clock.now - 1)
        assert await store.claim_dispatch("run-late", OWNER) is False
        doc = await dispatches.find_one({"run_id": "run-late"})
        assert doc is not None and doc["status"] == "pending"  # 未被 agent 篡改


async def test_claim_dispatch_missing_intent_is_permissive() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, _dispatches):
        # 无 intent 记录（迁移/无 dispatch 期）：放行，执行去重由 try_claim 兜底。
        assert await store.claim_dispatch("run-norecord", OWNER) is True


async def test_claim_dispatch_concurrent_race_single_winner() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        await _seed_dispatch(dispatches, "run-race", "pending", deadline_at=clock.now + 1000)
        # 单文档 CAS：多 pod 并发抢同一 pending intent，恰一个赢。
        results = await asyncio.gather(
            *(store.claim_dispatch("run-race", f"w{i}") for i in range(8))
        )
        assert results.count(True) == 1
        assert results.count(False) == 7


async def test_critical_outbox_stage_publish_scan() -> None:
    # R4：run.started 收编进 critical outbox（seq 1 惯例）；stage→queued→publish 确认→不再补发。
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, _dispatches):
        await store.try_claim(request("run-outbox"), OWNER)
        staged = await store.stage_critical_frame(
            "run-outbox", "run.started", 0, 111, "{}", terminal=False
        )
        assert staged is not None and staged.durable_seq == 1
        # 未 publish → scanner 见 queued 行（完整重建元数据）。
        frames = await store.list_unpublished_outbox()
        assert [(f.run_id, f.durable_seq, f.kind) for f in frames] == [
            ("run-outbox", 1, "run.started")
        ]
        assert frames[0].event_id == staged.event_id and frames[0].payload_json == "{}"
        evidence = await store.pull_durable_execution_evidence("run-outbox", 0, 10)
        assert [(row.durable_seq, row.event_id, row.kind) for row in evidence] == [
            (1, staged.event_id, "run.started")
        ]
        # publish 确认（queued→published）→ 不再补发。
        await store.mark_critical_published("run-outbox", 1)
        assert await store.list_unpublished_outbox() == []


async def test_completed_context_claim_is_atomic_causal_and_retained() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        req = request("run-context", namespace="opaque-ns")
        assert await store.try_claim(req, OWNER)
        output_draft = durable_output_draft_for_event(
            MessageDeltaPayload(segment_id="segment-context", delta="durable answer")
        )
        assert output_draft is not None
        output_batch = await store.append_durable_outputs(
            req.run_id,
            "completion:output:0",
            (output_draft,),
            recorded_at_ms=clock.now,
            source_payload_sha256="0" * 64,
        )
        assert output_batch is not None
        output = output_batch[0]
        expected_output_digest = append_output_digest(
            initial_output_digest(req.run_id), 1, output.payload_sha256
        )
        checkpoint = ExecutionCheckpoint(
            thread_id="physical-thread", checkpoint_ns="", checkpoint_id="checkpoint-final"
        )
        await store.bind_execution_context(
            req.run_id,
            ExecutionContextBinding(
                namespace=req.context.namespace,
                intent_digest="b" * 64,
                physical_thread_id=checkpoint.thread_id,
                active_checkpoint=checkpoint,
            ),
        )
        completion = CompletedExecutionContext(
            run_id=req.run_id,
            namespace=req.context.namespace,
            anchor="ctx_retained",
            digest="c" * 64,
            owner_revision=1,
            checkpoint=checkpoint,
        )
        owner = CompletionEventDraft(
            kind="run.owner.completed",
            index=0,
            timestamp=1,
            payload_json=RunOwnerCompletedPayload(
                execution_context_anchor=completion.anchor,
                execution_context_digest=completion.digest,
                owner_revision=completion.owner_revision,
            ).model_dump_json(),
        )
        terminal = CompletionEventDraft(
            kind="run.completed",
            index=1,
            timestamp=1,
            payload_json=RunCompletedPayload(status="completed").model_dump_json(
                exclude_none=True
            ),
        )

        attempts = await asyncio.gather(
            *(store.try_complete_execution_context(completion, owner, terminal) for _ in range(8))
        )
        winners = [item for item in attempts if item is not None]
        assert len(winners) == 1
        claimed = winners[0]
        assert claimed.owner.durable_seq + 1 == claimed.terminal.durable_seq
        assert [frame.kind for frame in await store.list_unpublished_outbox()] == [
            "run.owner.completed",
            "run.completed",
        ]
        evidence = await store.pull_durable_execution_evidence(req.run_id, 0, 10)
        assert [row.kind for row in evidence] == [
            "run.owner.completed",
            "run.completed",
        ]
        completed_evidence = evidence_pb2.DurableExecutionCanonicalPayloadV1.FromString(
            evidence[-1].canonical_payload
        ).run_completed
        assert completed_evidence.output_high_watermark == 1
        assert completed_evidence.output_digest_sha256 == expected_output_digest
        assert await store.get_run_durable_checkpoint(req.run_id) == evidence[0]

        # A published-but-unreceipted owner blocks the queued terminal.
        await store.mark_critical_published(req.run_id, claimed.owner.durable_seq)
        assert await store.list_unpublished_outbox() == []
        db = dispatches.database
        await db[RUN_EVENT_RECEIPTS_COLLECTION].insert_one(
            {
                "run_id": req.run_id,
                "durable_seq": claimed.owner.durable_seq,
                "event_id": claimed.owner.event_id,
                "status": "persisted",
                "created_at": 0,
            }
        )
        await db[RUN_RECEIPT_MANIFESTS_COLLECTION].insert_one(
            {
                "run_id": req.run_id,
                "persisted_seq": claimed.owner.durable_seq,
                "projected_seq": claimed.owner.durable_seq,
                "consumed_seq": 0,
                "producer_close_requested": False,
                "producer_closed": False,
                "updated_at": 0,
            }
        )
        await store.reconcile_receipts(req.run_id)
        eligible = await store.list_unpublished_outbox()
        assert [frame.kind for frame in eligible] == ["run.completed"]

        await store.mark_critical_published(req.run_id, claimed.terminal.durable_seq)
        await db[RUN_EVENT_RECEIPTS_COLLECTION].insert_one(
            {
                "run_id": req.run_id,
                "durable_seq": claimed.terminal.durable_seq,
                "event_id": claimed.terminal.event_id,
                "status": "persisted",
                "created_at": 0,
            }
        )
        await store.reconcile_receipts(req.run_id)
        clock.advance_ms(10_000)
        assert await store.purge_terminal(max_age_ms=5_000) == 1
        assert await store.pull_durable_output_records(req.run_id, 0, 64) == []
        assert await store.pull_durable_execution_evidence(req.run_id, 0, 10) == []
        assert await store.is_terminal(req.run_id) is True
        assert await store.get_request(req.run_id) is None
        assert await store.try_claim(req, OWNER) is False
        assert (
            await store.resolve_execution_parent(
                namespace=req.context.namespace,
                anchor=completion.anchor,
                digest=completion.digest,
                continuation_run_id=None,
            )
            == checkpoint
        )


async def test_semantic_critical_stage_is_atomic_and_survives_outbox_gc() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        await store.try_claim(request("run-plan"), OWNER)
        key = "plan.proposed:call-plan-A"
        attempts = await asyncio.gather(
            *(
                store.stage_critical_frame(
                    "run-plan",
                    "plan.proposed",
                    1,
                    111,
                    _plan_payload("call-plan-A"),
                    terminal=False,
                    semantic_key=key,
                )
                for _ in range(8)
            )
        )
        staged = [item for item in attempts if item is not None]
        assert len(staged) == 8
        assert sum(item.created for item in staged) == 1
        assert {(item.durable_seq, item.event_id) for item in staged} == {
            (staged[0].durable_seq, staged[0].event_id)
        }

        first = staged[0]
        await store.mark_critical_published("run-plan", first.durable_seq)
        db = dispatches.database
        await db[RUN_EVENT_RECEIPTS_COLLECTION].insert_one(
            {
                "run_id": "run-plan",
                "durable_seq": first.durable_seq,
                "event_id": first.event_id,
                "status": "persisted",
                "created_at": 0,
            }
        )
        await db[RUN_RECEIPT_MANIFESTS_COLLECTION].insert_one(
            {
                "run_id": "run-plan",
                "persisted_seq": first.durable_seq,
                "projected_seq": first.durable_seq,
                "consumed_seq": 0,
                "producer_close_requested": False,
                "producer_closed": False,
                "updated_at": 0,
            }
        )
        outcome = await store.reconcile_receipts("run-plan")
        assert outcome.consumed_through == first.durable_seq
        assert await store.list_open_outbox_runs() == []

        replay = await store.stage_critical_frame(
            "run-plan",
            "plan.proposed",
            99,
            999,
            _plan_payload("call-plan-A"),
            terminal=False,
            semantic_key=key,
        )
        assert replay is not None and replay.created is False
        assert (replay.durable_seq, replay.event_id) == (first.durable_seq, first.event_id)
        assert await store.list_open_outbox_runs() == []

        with pytest.raises(ValueError, match="semantic critical frame conflict"):
            await store.stage_critical_frame(
                "run-plan",
                "plan.proposed",
                100,
                1000,
                _plan_payload("different"),
                terminal=False,
                semantic_key=key,
            )


async def test_execution_evidence_is_run_document_independent_and_page_bounded() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        run_id = "run-many-evidence"
        await store.try_claim(request(run_id), OWNER)
        for index in range(600):
            owner_ref = f"call-plan-{index}"
            staged = await store.stage_critical_frame(
                run_id,
                "plan.proposed",
                index,
                index + 1,
                _plan_payload(owner_ref),
                terminal=False,
                semantic_key=f"plan.proposed:{owner_ref}",
            )
            assert staged is not None

        run_collection: AsyncCollection[dict[str, object]] = object.__getattribute__(
            store, "_coll"
        )
        run_doc = await run_collection.find_one(
            {"_id": run_id}, {"durable_evidence": 1}
        )
        assert run_doc is not None and "durable_evidence" not in run_doc
        evidence_collection = dispatches.database[AGENT_EXECUTION_EVIDENCE_COLLECTION]
        assert await evidence_collection.count_documents({"run_id": run_id}) == 600

        page = await store.pull_durable_execution_evidence(run_id, 500, 11)
        assert [row.durable_seq for row in page] == list(range(501, 512))


async def test_critical_outbox_first_terminal_fence_supersedes_suffix() -> None:
    # 终态帧 CAS 设 local fence；其后更大 seq 一律 superseded（None 返回），永不发布。
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, _dispatches):
        await store.try_claim(request("run-fence"), OWNER)
        s1 = await store.stage_critical_frame("run-fence", "run.started", 0, 1, "{}", terminal=False)
        assert s1 is not None and s1.durable_seq == 1
        # 首个终态帧=fence（seq 2）。
        s2 = await store.stage_critical_frame(
            "run-fence", "run.completed", 1, 2, '{"status":"completed"}', terminal=True
        )
        assert s2 is not None and s2.durable_seq == 2
        # fence 后的 critical 帧（seq 3）：post-fence → superseded，返回 None、不入补发。
        s3 = await store.stage_critical_frame(
            "run-fence", "run.control.receipt", 2, 3, "{}", terminal=False
        )
        assert s3 is None
        await store.mark_critical_published("run-fence", 1)
        await store.mark_critical_published("run-fence", 2)
        # 补发扫描只见非 superseded 的 queued（此处均已 published）→ 空。
        assert await store.list_unpublished_outbox() == []


async def test_reconcile_receipts_consumes_gcs_and_requests_close() -> None:
    # session 落 persisted 回执 + manifest → agent 推进 consumed、硬删已确认行、终态请求 close。
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        db = dispatches.database
        receipts = db[RUN_EVENT_RECEIPTS_COLLECTION]
        manifests = db[RUN_RECEIPT_MANIFESTS_COLLECTION]
        await store.try_claim(request("run-rc"), OWNER)
        s1 = await store.stage_critical_frame("run-rc", "run.started", 0, 1, "{}", terminal=False)
        s2 = await store.stage_critical_frame(
            "run-rc", "run.completed", 1, 2, '{"status":"completed"}', terminal=True
        )
        assert s1 is not None and s2 is not None
        await store.mark_critical_published("run-rc", 1)
        await store.mark_critical_published("run-rc", 2)
        await receipts.insert_many(
            [
                {"run_id": "run-rc", "durable_seq": 1, "event_id": s1.event_id, "status": "persisted", "created_at": 0},
                {"run_id": "run-rc", "durable_seq": 2, "event_id": s2.event_id, "status": "persisted", "created_at": 0},
            ]
        )
        await manifests.insert_one(
            {
                "run_id": "run-rc", "persisted_seq": 2, "projected_seq": 0, "consumed_seq": 0,
                "producer_close_requested": False, "producer_closed": False, "updated_at": 0,
            }
        )
        outcome = await store.reconcile_receipts("run-rc")
        assert outcome.consumed_through == 2 and outcome.close_requested is True
        # 已确认行硬删 → 无 open outbox。
        assert await store.list_open_outbox_runs() == []
        manifest = await manifests.find_one({"run_id": "run-rc"})
        assert manifest is not None
        assert manifest["consumed_seq"] == 2 and manifest["producer_close_requested"] is True


async def test_reconcile_receipts_rejected_nack_syncs_fence() -> None:
    # session NACK（rejected 回执）：同步 local fence=rejected_seq，交 supervisor 终局。
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        receipts = dispatches.database[RUN_EVENT_RECEIPTS_COLLECTION]
        await store.try_claim(request("run-nack"), OWNER)
        s1 = await store.stage_critical_frame("run-nack", "run.started", 0, 1, "{}", terminal=False)
        assert s1 is not None
        await store.mark_critical_published("run-nack", 1)
        await receipts.insert_one(
            {"run_id": "run-nack", "durable_seq": 1, "event_id": s1.event_id, "status": "rejected", "reason": "schema", "created_at": 0}
        )
        outcome = await store.reconcile_receipts("run-nack")
        assert outcome.rejected_seq == 1
        # fence 同步到 rejected_seq：其后 critical 帧一律 superseded。
        s2 = await store.stage_critical_frame(
            "run-nack", "run.completed", 1, 2, '{"status":"completed"}', terminal=True
        )
        assert s2 is None


async def test_reconcile_receipts_manifest_missing_is_receipt_state_lost() -> None:
    # published 行待确认却无 manifest 且未 close：receipt_state_lost，绝不删 outbox。
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, _dispatches):
        await store.try_claim(request("run-lost"), OWNER)
        await store.stage_critical_frame("run-lost", "run.started", 0, 1, "{}", terminal=False)
        await store.mark_critical_published("run-lost", 1)
        outcome = await store.reconcile_receipts("run-lost")
        assert outcome.receipt_state_lost is True
        # outbox 行未被删（仍 open）。
        assert await store.list_open_outbox_runs() == ["run-lost"]


async def test_reconcile_republishes_stale_published_without_receipt() -> None:
    # published 后回执一直不来、超宽限期 → 重发候选（复用固定 durable_seq/event_id）；touch 后不重复。
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, _dispatches):
        await store.try_claim(request("run-stale"), OWNER)
        s1 = await store.stage_critical_frame(
            "run-stale", "run.started", 0, 1, "{}", terminal=False
        )
        assert s1 is not None
        await store.mark_critical_published("run-stale", 1)  # published_at = clock.now
        # 宽限期内：不重发。
        assert (await store.reconcile_receipts("run-stale", republish_grace_ms=10_000)).republish == []
        # 超宽限期且无回执：重发候选。
        clock.advance_ms(20_000)
        out = await store.reconcile_receipts("run-stale", republish_grace_ms=10_000)
        assert [(f.durable_seq, f.event_id, f.kind) for f in out.republish] == [
            (1, s1.event_id, "run.started")
        ]
        # touch 复位 published_at（clock 未再前进）→ 下一拍不重复重发。
        assert (await store.reconcile_receipts("run-stale", republish_grace_ms=10_000)).republish == []


async def test_reconcile_no_stale_republish_when_receipt_present() -> None:
    # 有回执的 published 行绝不重发（即便远超宽限期）。
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        db = dispatches.database
        receipts = db[RUN_EVENT_RECEIPTS_COLLECTION]
        manifests = db[RUN_RECEIPT_MANIFESTS_COLLECTION]
        await store.try_claim(request("run-ok"), OWNER)
        s1 = await store.stage_critical_frame("run-ok", "run.started", 0, 1, "{}", terminal=False)
        assert s1 is not None
        await store.mark_critical_published("run-ok", 1)
        await receipts.insert_one(
            {"run_id": "run-ok", "durable_seq": 1, "event_id": s1.event_id, "status": "persisted", "created_at": 0}
        )
        await manifests.insert_one(
            {
                "run_id": "run-ok", "persisted_seq": 1, "projected_seq": 0, "consumed_seq": 0,
                "producer_close_requested": False, "producer_closed": False, "updated_at": 0,
            }
        )
        clock.advance_ms(60_000)  # 远超宽限期
        out = await store.reconcile_receipts("run-ok", republish_grace_ms=10_000)
        assert out.republish == [] and out.consumed_through == 1


async def test_control_inbox_keep_first_advance_and_scan() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, _dispatches):
        req = request("run-ci")
        await store.try_claim(req, OWNER)  # inbox 落法要求 run 文档已存在

        # keep-first：首次落库 True（persisted），重复 decision_id False 且不覆盖。
        assert await store.record_control_inbox("run-ci", "dec_1", "fp_a", '{"k":1}') is True
        assert await store.record_control_inbox("run-ci", "dec_1", "fp_b", '{"k":2}') is False

        pending = await store.list_pending_control_inbox()
        assert [(r.run_id, r.decision_id, r.fingerprint, r.body) for r in pending] == [
            ("run-ci", "dec_1", "fp_a", '{"k":1}')
        ]

        # persisted→applied 前向推进后不再入 pending。
        await store.mark_control_applied("run-ci", "dec_1")
        assert await store.list_pending_control_inbox() == []
        # applied 后再 mark 无副作用（仅 persisted→applied）。
        await store.mark_control_applied("run-ci", "dec_1")

        # 第二条 cancel（fingerprint=None）：superseded 后移出 pending。
        assert await store.record_control_inbox("run-ci", "dec_2", None, "{}") is True
        await store.mark_control_superseded("run-ci", "dec_2")
        assert await store.list_pending_control_inbox() == []

        # 终态 run 的 persisted 条目不入续办扫描。
        req2 = request("run-ci-term")
        await store.try_claim(req2, OWNER)
        await store.record_control_inbox("run-ci-term", "dec_x", "fp", "{}")
        await store.try_mark_terminal("run-ci-term")
        assert [r.run_id for r in await store.list_pending_control_inbox()] == []


async def test_quarantine_dispatch_records_dlq_row() -> None:
    clock = FakeClock()
    async with _mongo_ledger_with_dispatches(clock) as (store, dispatches):
        await store.quarantine_dispatch("deadbeef", source="requests", reason="unparseable")
        dlq = dispatches.database[DISPATCH_DLQ_COLLECTION]
        doc = await dlq.find_one({"raw_hash": "deadbeef"})
        assert doc is not None
        assert doc["source"] == "requests" and doc["reason"] == "unparseable"
        assert isinstance(doc["at"], int)


# --- Wave2 R3：tool effect journal（started keep-first → succeeded|failed，重放守门读侧）---


async def test_tool_journal_started_keep_first_then_finished() -> None:
    clock = FakeClock()
    async with _mongo_store(clock) as store:
        await store.try_claim(request("run-j"), OWNER)  # journal 落法要求 run 文档已存在
        # 首次落 started（True）；重复落同 tool_call_id 不覆盖（False）。
        assert await store.journal_tool_started("run-j", "call-1", "write_file") is True
        assert await store.journal_tool_started("run-j", "call-1", "write_file") is False
        rec = await store.get_tool_journal("run-j", "call-1")
        assert rec is not None and rec.status == "started" and rec.name == "write_file"
        assert rec.result == "" and rec.is_error is False

        # started→succeeded 附结果。
        await store.journal_tool_finished("run-j", "call-1", "wrote 3 bytes", is_error=False)
        rec = await store.get_tool_journal("run-j", "call-1")
        assert rec is not None and rec.status == "succeeded"
        assert rec.result == "wrote 3 bytes" and rec.is_error is False

        # 已 succeeded 不再被 finished 改写（仅推进 started 行）。
        await store.journal_tool_finished("run-j", "call-1", "clobber", is_error=True)
        rec = await store.get_tool_journal("run-j", "call-1")
        assert rec is not None and rec.status == "succeeded" and rec.result == "wrote 3 bytes"


async def test_tool_journal_failed_status_and_missing_row() -> None:
    clock = FakeClock()
    async with _mongo_store(clock) as store:
        await store.try_claim(request("run-j"), OWNER)
        assert await store.get_tool_journal("run-j", "absent") is None
        await store.journal_tool_started("run-j", "call-err", "execute")
        await store.journal_tool_finished("run-j", "call-err", "boom", is_error=True)
        rec = await store.get_tool_journal("run-j", "call-err")
        assert rec is not None and rec.status == "failed" and rec.is_error is True
        assert rec.result == "boom"


async def test_tool_journal_clear_removes_started_row() -> None:
    # 工具内 interrupt 撤销 started 行：清后视同无行，重进正常执行（不被守门误拦）。
    clock = FakeClock()
    async with _mongo_store(clock) as store:
        await store.try_claim(request("run-j"), OWNER)
        await store.journal_tool_started("run-j", "call-i", "mcp_call")
        started = await store.get_tool_journal("run-j", "call-i")
        assert started is not None and started.status == "started"
        await store.clear_tool_journal("run-j", "call-i")
        assert await store.get_tool_journal("run-j", "call-i") is None
