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

from fakes import request
from kokoro_agent.storage.ledger import (
    LedgerSettings,
    RunLedger,
    make_ledger,
)
from kokoro_agent.storage.mongo import MongoLedger

_MONGO_URL = os.environ.get("KOKORO_MONGO_URL", "mongodb://127.0.0.1:27017")
_TTL_MS = 1000
OWNER = "worker-a"
OTHER = "worker-b"


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
        yield MongoLedger(coll, ttl_ms=_TTL_MS, clock=clock)
    finally:
        await coll.drop()
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
    await store.try_mark_terminal("run-old")
    clock.advance_ms(10_000)
    await store.try_mark_terminal("run-new")  # 新终态：未超龄
    assert await store.purge_terminal(max_age_ms=5_000) == 1
    # 超龄终态被清：run 行与附属全消失，可再次认领（id 重用语义）。
    assert await store.is_terminal("run-old") is False
    assert await store.get_tool_result("run-old", "t1") is None
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

