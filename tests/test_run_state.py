"""make_run_state_store 工厂：backend 选择、行为矩阵、sqlite 跨工厂周期持久性、未知后端显式失败。"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import aiosqlite
import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.run_state import (
    MemoryRunStateStore,
    MongoRunStateStore,
    RunStateStore,
    SqliteRunStateStore,
    make_run_state_store,
)
from kokoro_agent.infrastructure.run_state.sqlite_store import SqliteRunStateStore as _SqliteStore

_MONGO_URL = os.environ.get("KOKORO_MONGO_URL", "mongodb://127.0.0.1:27017")

_REQ = RunRequest(
    kind="run.request",
    run_id="run-abc",
    session_id="sess-1",
    conversation_id="conv-1",
    input="hello",
)

_REQ2 = RunRequest(
    kind="run.request",
    run_id="run-xyz",
    session_id="sess-2",
    conversation_id="conv-2",
    input="world",
)


# ---------------------------------------------------------------------------
# 工厂 backend 选择
# ---------------------------------------------------------------------------


async def test_memory_backend_yields_memory_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOKORO_RUN_STATE_BACKEND", "memory")
    async with make_run_state_store() as store:
        assert isinstance(store, MemoryRunStateStore)


async def test_sqlite_backend_yields_sqlite_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("KOKORO_RUN_STATE_BACKEND", "sqlite")
    monkeypatch.setenv("KOKORO_RUN_STATE_DB", str(tmp_path / "rs.db"))
    async with make_run_state_store() as store:
        assert isinstance(store, SqliteRunStateStore)


async def test_unknown_backend_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOKORO_RUN_STATE_BACKEND", "bogus")
    with pytest.raises(ValueError, match="bogus"):
        async with make_run_state_store():
            pass


# ---------------------------------------------------------------------------
# 共用行为矩阵 helper：接受任意 RunStateStore 实例直接跑完整断言
# ---------------------------------------------------------------------------


async def _assert_full_behaviour(store: RunStateStore, req: RunRequest) -> None:
    # try_register 首次 True
    assert await store.try_register(req) is True
    # try_register 重复 False
    assert await store.try_register(req) is False
    # get_request round-trip
    got = await store.get_request(req.run_id)
    assert got == req
    # is_terminal 认领前 False
    assert await store.is_terminal(req.run_id) is False
    # try_mark_terminal 首次 True
    assert await store.try_mark_terminal(req.run_id) is True
    # try_mark_terminal 再次 False
    assert await store.try_mark_terminal(req.run_id) is False
    # is_terminal 认领后 True
    assert await store.is_terminal(req.run_id) is True


async def _assert_unregistered_mark_terminal(store: RunStateStore) -> None:
    # 未注册的 run 也能认领终态（crash 前快速关闭场景）。
    assert await store.try_mark_terminal("run-never-registered") is True
    assert await store.get_request("run-never-registered") is None


# ---------------------------------------------------------------------------
# memory backend 行为矩阵
# ---------------------------------------------------------------------------


async def test_memory_full_behaviour() -> None:
    store = MemoryRunStateStore()
    await _assert_full_behaviour(store, _REQ)


async def test_memory_unregistered_mark_terminal() -> None:
    store = MemoryRunStateStore()
    await _assert_unregistered_mark_terminal(store)


# ---------------------------------------------------------------------------
# sqlite backend 行为矩阵
# ---------------------------------------------------------------------------


async def test_sqlite_full_behaviour(tmp_path: Path) -> None:
    async with aiosqlite.connect(str(tmp_path / "test.db")) as db:
        store = _SqliteStore(db)
        await store.setup()
        await _assert_full_behaviour(store, _REQ)


async def test_sqlite_unregistered_mark_terminal(tmp_path: Path) -> None:
    async with aiosqlite.connect(str(tmp_path / "test.db")) as db:
        store = _SqliteStore(db)
        await store.setup()
        await _assert_unregistered_mark_terminal(store)


# ---------------------------------------------------------------------------
# sqlite 跨工厂周期持久性
# ---------------------------------------------------------------------------


async def test_sqlite_persists_across_factory_reentry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db = str(tmp_path / "rs_persist.db")
    monkeypatch.setenv("KOKORO_RUN_STATE_BACKEND", "sqlite")
    monkeypatch.setenv("KOKORO_RUN_STATE_DB", db)

    # 写：首个工厂周期（模拟 pod A / 重启前）。
    async with make_run_state_store() as store:
        registered = await store.try_register(_REQ2)
        assert registered is True
        await store.try_mark_terminal(_REQ2.run_id)

    # 读：全新工厂周期（模拟重启 / 另一 pod）从同一文件续读。
    async with make_run_state_store() as store:
        got = await store.get_request(_REQ2.run_id)
        terminal = await store.is_terminal(_REQ2.run_id)

    assert got == _REQ2
    assert terminal is True


# ---------------------------------------------------------------------------
# mongo backend（需可达的 mongo；不可达即 skip，CI 无 mongo 优雅跳过）
# ---------------------------------------------------------------------------


async def _mongo_collection_or_skip() -> tuple[AsyncMongoClient[dict[str, object]], AsyncCollection[dict[str, object]]]:
    # 每用例独立 collection（uuid），避免 mongo 跨运行残留串扰。
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(
        _MONGO_URL, serverSelectionTimeoutMS=500
    )
    try:
        await client.admin.command("ping")
    except Exception:  # noqa: BLE001 — 网络/服务不可达统一视为 skip 条件
        await client.close()
        pytest.skip(f"no mongo reachable at {_MONGO_URL}")
    return client, client["kokoro_test"][f"run_state_{uuid.uuid4().hex}"]


async def test_mongo_full_behaviour() -> None:
    client, coll = await _mongo_collection_or_skip()
    try:
        await _assert_full_behaviour(MongoRunStateStore(coll), _REQ)
    finally:
        await coll.drop()
        await client.close()


async def test_mongo_unregistered_mark_terminal() -> None:
    client, coll = await _mongo_collection_or_skip()
    try:
        await _assert_unregistered_mark_terminal(MongoRunStateStore(coll))
    finally:
        await coll.drop()
        await client.close()


async def test_mongo_backend_yields_mongo_store(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = await _mongo_collection_or_skip()
    await client.close()
    monkeypatch.setenv("KOKORO_RUN_STATE_BACKEND", "mongo")
    monkeypatch.setenv("KOKORO_MONGO_URL", _MONGO_URL)
    async with make_run_state_store() as store:
        assert isinstance(store, MongoRunStateStore)
