"""SQLite 后端：跨进程/重启的 run 状态存储，WAL+busy_timeout 保真实争用下的原子性。"""

from __future__ import annotations

import time
from collections.abc import Callable

import aiosqlite

from kokoro_agent.contract import RunRequest

_DDL = """\
CREATE TABLE IF NOT EXISTS run_state(
    run_id           TEXT PRIMARY KEY,
    request_json     TEXT,
    terminal         INTEGER NOT NULL DEFAULT 0,
    lease_expires_ms INTEGER
)"""

# 结果审核暂停的双执行防护：resume 后节点从头重跑，首跑结果 keep-first 落盘，重入命中即跳过工具。
_TOOL_RESULTS_DDL = """\
CREATE TABLE IF NOT EXISTS tool_results(
    run_id   TEXT NOT NULL,
    tool_id  TEXT NOT NULL,
    result   TEXT NOT NULL,
    is_error INTEGER NOT NULL,
    PRIMARY KEY(run_id, tool_id)
)"""


def _now_ms() -> int:
    return int(time.time() * 1000)


class SqliteRunStateStore:
    def __init__(
        self, db: aiosqlite.Connection, *, ttl_ms: int, clock: Callable[[], int] = _now_ms
    ) -> None:
        self._db = db
        self._ttl_ms = ttl_ms
        self._clock = clock

    async def setup(self) -> None:
        # WAL + busy_timeout：跨进程共用同一文件时并发写互相等待而非立刻 SQLITE_BUSY 报错。
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA busy_timeout=5000")
        await self._db.execute(_DDL)
        await self._db.execute(_TOOL_RESULTS_DDL)
        await self._db.commit()

    async def try_claim(self, request: RunRequest) -> bool:
        # INSERT OR IGNORE：run_id 已存在（已被认领/终态）即去重丢弃，rowcount==0。
        cur = await self._db.execute(
            "INSERT OR IGNORE INTO run_state(run_id, request_json, lease_expires_ms)"
            " VALUES(?, ?, ?)",
            (request.run_id, request.model_dump_json(), self._clock() + self._ttl_ms),
        )
        await self._db.commit()
        return cur.rowcount == 1

    async def renew(self, run_id: str) -> None:
        # 心跳续租；也把 HITL 暂停哨兵（NULL）拉回活跃租约。
        await self._db.execute(
            "UPDATE run_state SET lease_expires_ms=? WHERE run_id=? AND terminal=0",
            (self._clock() + self._ttl_ms, run_id),
        )
        await self._db.commit()

    async def pause(self, run_id: str) -> None:
        # NULL 哨兵：HITL 等人可以是小时级，暂停 run 绝不被过期重拾重跑。
        await self._db.execute(
            "UPDATE run_state SET lease_expires_ms=NULL WHERE run_id=? AND terminal=0",
            (run_id,),
        )
        await self._db.commit()

    async def reclaim_expired(self) -> list[RunRequest]:
        now = self._clock()
        async with self._db.execute(
            "SELECT run_id, request_json FROM run_state"
            " WHERE terminal=0 AND request_json IS NOT NULL"
            " AND lease_expires_ms IS NOT NULL AND lease_expires_ms<=?",
            (now,),
        ) as cursor:
            rows = await cursor.fetchall()
        reclaimed: list[RunRequest] = []
        for run_id, request_json in rows:
            # 逐行条件更新原子认领：多 pod 并发 reclaim 时每个 run 恰被一个赢家拾走。
            cur = await self._db.execute(
                "UPDATE run_state SET lease_expires_ms=?"
                " WHERE run_id=? AND terminal=0"
                " AND lease_expires_ms IS NOT NULL AND lease_expires_ms<=?",
                (now + self._ttl_ms, run_id, now),
            )
            await self._db.commit()
            if cur.rowcount == 1:
                reclaimed.append(RunRequest.model_validate_json(request_json))
        return reclaimed

    async def list_paused(self) -> list[str]:
        async with self._db.execute(
            "SELECT run_id FROM run_state"
            " WHERE terminal=0 AND lease_expires_ms IS NULL AND request_json IS NOT NULL"
            " ORDER BY run_id"
        ) as cursor:
            rows = await cursor.fetchall()
        return [str(row[0]) for row in rows]

    async def get_request(self, run_id: str) -> RunRequest | None:
        async with self._db.execute(
            "SELECT request_json FROM run_state WHERE run_id=?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return RunRequest.model_validate_json(row[0])

    async def try_mark_terminal(self, run_id: str) -> bool:
        # UPSERT：未有记录时插入 terminal=1；已 terminal==1 时 rowcount==0 → 认领失败。
        cur = await self._db.execute(
            "INSERT INTO run_state(run_id, terminal) VALUES(?, 1)"
            " ON CONFLICT(run_id) DO UPDATE SET terminal=1 WHERE terminal=0",
            (run_id,),
        )
        await self._db.commit()
        return cur.rowcount == 1

    async def is_terminal(self, run_id: str) -> bool:
        async with self._db.execute(
            "SELECT terminal FROM run_state WHERE run_id=?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return row is not None and row[0] == 1

    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None:
        await self._db.execute(
            "INSERT OR IGNORE INTO tool_results(run_id, tool_id, result, is_error)"
            " VALUES(?, ?, ?, ?)",
            (run_id, tool_id, result, 1 if is_error else 0),
        )
        await self._db.commit()

    async def get_tool_result(self, run_id: str, tool_id: str) -> tuple[str, bool] | None:
        cur = await self._db.execute(
            "SELECT result, is_error FROM tool_results WHERE run_id=? AND tool_id=?",
            (run_id, tool_id),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return (str(row[0]), bool(row[1]))
