"""RunLedger 规格：TTL 租约全生命周期 + 终态原子认领，sqlite/mongo 同语义矩阵。"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from fakes import request
from kokoro_agent.storage.mongo import MongoLedger
from kokoro_agent.storage.ledger import (
    LedgerSettings,
    RunLedger,
    make_ledger,
)
from kokoro_agent.storage.sqlite import SqliteLedger

_MONGO_URL = os.environ.get("KOKORO_MONGO_URL", "mongodb://127.0.0.1:27017")
_TTL_MS = 1000


class FakeClock:
    def __init__(self, now: int = 1_000_000) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    def advance_ms(self, delta: int) -> None:
        self.now += delta


@asynccontextmanager
async def _sqlite_store(tmp_path: Path, clock: FakeClock) -> AsyncGenerator[RunLedger]:
    async with aiosqlite.connect(str(tmp_path / "rs.db")) as db:
        store = SqliteLedger(db, ttl_ms=_TTL_MS, clock=clock)
        await store.setup()
        yield store


async def _mongo_collection_or_skip() -> tuple[
    AsyncMongoClient[dict[str, object]], AsyncCollection[dict[str, object]]
]:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(
        _MONGO_URL, serverSelectionTimeoutMS=500
    )
    try:
        await client.admin.command("ping")
    except Exception:  # noqa: BLE001 — 网络/服务不可达统一视为 skip 条件
        await client.close()
        pytest.skip(f"no mongo reachable at {_MONGO_URL}")
    return client, client["kokoro_test"][f"ledger_{uuid.uuid4().hex}"]


@asynccontextmanager
async def _mongo_store(clock: FakeClock) -> AsyncGenerator[RunLedger]:
    client, coll = await _mongo_collection_or_skip()
    try:
        yield MongoLedger(coll, ttl_ms=_TTL_MS, clock=clock)
    finally:
        await coll.drop()
        await client.close()


# --- 共用行为矩阵：任意 RunLedger 实例逐条对标 ---


async def _assert_claim_and_terminal(store: RunLedger) -> None:
    req = request("run-abc")
    assert await store.try_claim(req) is True
    assert await store.try_claim(req) is False  # 重复认领去重
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
    await store.try_claim(req)
    # 租约未到期：不可重拾。
    clock.advance_ms(_TTL_MS - 1)
    assert await store.reclaim_expired() == []
    # 到期边界（expires<=now）：恰好重拾一次并连同原始 request 返回。
    clock.advance_ms(1)
    reclaimed = await store.reclaim_expired()
    assert [r.run_id for r in reclaimed] == ["run-lease"]
    # 重拾即续租：立刻再拾为空。
    assert await store.reclaim_expired() == []


async def _assert_renew_extends(store: RunLedger, clock: FakeClock) -> None:
    req = request("run-renew")
    await store.try_claim(req)
    clock.advance_ms(_TTL_MS - 1)
    await store.renew(req.run_id)
    clock.advance_ms(_TTL_MS - 1)
    assert await store.reclaim_expired() == []  # 心跳续租后未过期
    clock.advance_ms(1)
    assert [r.run_id for r in await store.reclaim_expired()] == ["run-renew"]


async def _assert_pause_excludes_reclaim(store: RunLedger, clock: FakeClock) -> None:
    req = request("run-paused")
    await store.try_claim(req)
    await store.pause(req.run_id)
    # HITL 等人可以无限期：暂停哨兵永不参与过期重拾。
    clock.advance_ms(_TTL_MS * 1000)
    assert await store.reclaim_expired() == []
    # resume 续租后重新纳入租约生命周期。
    await store.renew(req.run_id)
    clock.advance_ms(_TTL_MS)
    assert [r.run_id for r in await store.reclaim_expired()] == ["run-paused"]


async def _assert_usage_accumulation_two_columns(store: RunLedger, clock: FakeClock) -> None:
    # run.completed 用量真源：跨段累计 input/output 双列（与预算计数分开存，语义不同）。
    req = request("run-usage")
    await store.try_claim(req)
    assert await store.add_usage("run-usage", 10, 1) == (10, 1)
    assert await store.add_usage("run-usage", 20, 2) == (30, 3)
    assert await store.add_usage("run-usage", 0, 0) == (30, 3)  # 零增量幂等读
    assert await store.add_usage("run-usage-other", 5, 5) == (5, 5)  # run 间隔离


async def _assert_token_accumulation_per_run(store: RunLedger, clock: FakeClock) -> None:
    # token 预算跨 HITL 段累计：resume 重建 middleware 后计数不清零；run 间隔离。
    req_a, req_b = request("run-tok-a"), request("run-tok-b")
    await store.try_claim(req_a)
    await store.try_claim(req_b)
    assert await store.add_tokens("run-tok-a", 100) == 100
    assert await store.add_tokens("run-tok-a", 50) == 150
    assert await store.add_tokens("run-tok-b", 7) == 7  # 隔离
    assert await store.add_tokens("run-tok-a", 0) == 150  # 零增量幂等读


async def _assert_list_paused_only_pause_sentinel(store: RunLedger, clock: FakeClock) -> None:
    # control 监听收养的数据源：仅"哨兵暂停且非终态"的 run；活跃/终态/重续租均不入列。
    active, paused, done = request("run-active"), request("run-hitl"), request("run-final")
    for req in (active, paused, done):
        await store.try_claim(req)
    await store.pause(paused.run_id)
    await store.try_mark_terminal(done.run_id)
    assert await store.list_paused() == [paused.run_id]
    await store.renew(paused.run_id)  # resume 续租 → 离开暂停态
    assert await store.list_paused() == []
    await store.pause(paused.run_id)
    await store.try_mark_terminal(paused.run_id)  # 终态后即使哨兵仍在也不入列
    assert await store.list_paused() == []


async def _assert_terminal_excluded_from_reclaim(store: RunLedger, clock: FakeClock) -> None:
    req = request("run-done")
    await store.try_claim(req)
    await store.try_mark_terminal(req.run_id)
    clock.advance_ms(_TTL_MS * 2)
    assert await store.reclaim_expired() == []


async def _assert_concurrent_single_winner(store: RunLedger) -> None:
    req = request("run-race")
    claims = await asyncio.gather(*(store.try_claim(req) for _ in range(8)))
    assert sum(claims) == 1
    terminals = await asyncio.gather(*(store.try_mark_terminal(req.run_id) for _ in range(8)))
    assert sum(terminals) == 1


async def _assert_tool_result_keep_first(store: RunLedger) -> None:
    # 结果审核缓存：首跑 keep-first，重入/并发写不覆盖；未知键返 None。
    req = request("run-tr")
    assert await store.try_claim(req) is True
    assert await store.get_tool_result("run-tr", "t1") is None
    await store.put_tool_result("run-tr", "t1", "first", False)
    await store.put_tool_result("run-tr", "t1", "second", True)
    assert await store.get_tool_result("run-tr", "t1") == ("first", False)
    assert await store.get_tool_result("run-tr", "other") is None


async def _assert_steer_mailbox(store: RunLedger) -> None:
    # steering 信箱：keep-first 幂等（message_id 重放不覆盖）、按到达序、drain 即清空、run 间隔离。
    req = request("run-steer")
    assert await store.try_claim(req) is True
    assert await store.drain_steers("run-steer") == []
    await store.add_steer("run-steer", "m1", "改成国内市场")
    await store.add_steer("run-steer", "m2", "再加一条")
    await store.add_steer("run-steer", "m1", "重放不覆盖")
    assert await store.drain_steers("run-steer") == [
        ("m1", "改成国内市场"),
        ("m2", "再加一条"),
    ]
    assert await store.drain_steers("run-steer") == []  # drain 即清空
    # 未认领 run 的插话安全丢弃（绝不预创建 run 文档毒化 try_claim 去重）。
    await store.add_steer("run-ghost", "mg", "orphan")
    assert await store.drain_steers("run-ghost") == []
    assert await store.try_claim(request("run-ghost")) is True



async def _assert_purge_terminal(store: RunLedger, clock: FakeClock) -> None:
    # retention 清扫：仅"终态且超龄"的 run 连同附属数据整体清除；活跃/暂停/新终态永不清。
    done_old, done_new, active = request("run-old"), request("run-new"), request("run-live")
    for req in (done_old, done_new, active):
        await store.try_claim(req)
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


async def _assert_thread_activity(store: RunLedger, clock: FakeClock) -> None:
    # thread 活跃账本：touch 即 upsert 最后活跃；清扫原子取出超龄 thread 并删行（幂等）。
    await store.touch_thread("ns:t1")
    clock.advance_ms(10_000)
    await store.touch_thread("ns:t2")
    await store.touch_thread("ns:t1")  # 再活跃：刷新
    clock.advance_ms(4_000)
    assert await store.purge_stale_threads(max_age_ms=5_000) == []  # t1/t2 都在 5s 内
    clock.advance_ms(2_000)
    assert await store.purge_stale_threads(max_age_ms=5_000) == ["ns:t1", "ns:t2"]
    assert await store.purge_stale_threads(max_age_ms=5_000) == []  # 已取出：幂等


_MATRIX: list[Callable[[RunLedger, FakeClock], Awaitable[None]]] = [
    lambda store, _clock: _assert_claim_and_terminal(store),
    lambda store, _clock: _assert_unclaimed_mark_terminal(store),
    _assert_lease_lifecycle,
    _assert_renew_extends,
    _assert_pause_excludes_reclaim,
    _assert_terminal_excluded_from_reclaim,
    lambda store, _clock: _assert_concurrent_single_winner(store),
    lambda store, _clock: _assert_tool_result_keep_first(store),
    _assert_list_paused_only_pause_sentinel,
    _assert_token_accumulation_per_run,
    _assert_usage_accumulation_two_columns,
    lambda store, _clock: _assert_steer_mailbox(store),
    _assert_purge_terminal,
    _assert_thread_activity,
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
    "list_paused_only_pause_sentinel",
    "token_accumulation_per_run",
    "usage_accumulation_two_columns",
    "steer_mailbox",
    "purge_terminal",
    "thread_activity",
]


@pytest.mark.parametrize("check", _MATRIX, ids=_MATRIX_IDS)
async def test_sqlite_behaviour_matrix(
    tmp_path: Path, check: Callable[[RunLedger, FakeClock], Awaitable[None]]
) -> None:
    clock = FakeClock()
    async with _sqlite_store(tmp_path, clock) as store:
        await check(store, clock)


@pytest.mark.parametrize("check", _MATRIX, ids=_MATRIX_IDS)
async def test_mongo_behaviour_matrix(
    check: Callable[[RunLedger, FakeClock], Awaitable[None]],
) -> None:
    clock = FakeClock()
    async with _mongo_store(clock) as store:
        await check(store, clock)


# --- sqlite 专项：跨连接争用与跨工厂周期持久性 ---


async def test_sqlite_concurrent_terminal_across_connections(tmp_path: Path) -> None:
    # 两个连接（模拟两进程）争用同一文件并发认领终态：busy_timeout 下互等、恰一个 True。
    db_path = str(tmp_path / "race.db")
    async with (
        aiosqlite.connect(db_path) as conn_a,
        aiosqlite.connect(db_path) as conn_b,
    ):
        store_a = SqliteLedger(conn_a, ttl_ms=_TTL_MS)
        store_b = SqliteLedger(conn_b, ttl_ms=_TTL_MS)
        await store_a.setup()
        await store_b.setup()
        await store_a.try_claim(request("race-x"))
        results = await asyncio.gather(
            store_a.try_mark_terminal("race-x"),
            store_b.try_mark_terminal("race-x"),
        )
    assert sum(results) == 1


def _settings(backend: str, tmp_path: Path) -> LedgerSettings:
    return LedgerSettings.model_validate(
        {
            "backend": backend,
            "sqlite_path": str(tmp_path / "factory.db"),
            "mongo_url": _MONGO_URL,
            "mongo_db": "kokoro_test",
            "lease_ttl_ms": _TTL_MS,
        }
    )


async def test_factory_sqlite_persists_across_cycles(tmp_path: Path) -> None:
    settings = _settings("sqlite", tmp_path)
    req = request("run-persist")
    async with make_ledger(settings) as store:
        assert isinstance(store, SqliteLedger)
        assert await store.try_claim(req) is True
        await store.try_mark_terminal(req.run_id)
    # 全新工厂周期（模拟重启/另一 pod）从同一文件续读。
    async with make_ledger(settings) as store:
        assert await store.get_request(req.run_id) == req
        assert await store.is_terminal(req.run_id) is True


async def test_factory_unknown_backend_fails_loud(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        _settings("bogus", tmp_path)


async def test_factory_mongo_yields_mongo_store(tmp_path: Path) -> None:
    client, _coll = await _mongo_collection_or_skip()
    await client.close()
    async with make_ledger(_settings("mongo", tmp_path)) as store:
        assert isinstance(store, MongoLedger)

