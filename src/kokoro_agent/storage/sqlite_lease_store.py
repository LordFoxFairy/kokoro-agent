"""SQLite 持久化实现：跨进程、跨重启的 run 状态存储。"""

from __future__ import annotations

import aiosqlite

from kokoro_agent.run.request import RunRequest

_DDL = """\
CREATE TABLE IF NOT EXISTS run_state(
    run_id       TEXT PRIMARY KEY,
    request_json TEXT,
    terminal     INTEGER NOT NULL DEFAULT 0
)"""


class SqliteRunStateStore:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def setup(self) -> None:
        # WAL + busy_timeout：跨进程共用同一文件时并发写互相等待而非立刻 SQLITE_BUSY 报错
        # （原子认领须在真实争用下也成立）；DDL 幂等，重启续用同一文件无需手动建表。
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA busy_timeout=5000")
        await self._db.execute(_DDL)
        await self._db.commit()

    async def try_register(self, request: RunRequest) -> bool:
        # INSERT OR IGNORE：run_id 已存在时静默跳过，rowcount==0 表示重复。
        cur = await self._db.execute(
            "INSERT OR IGNORE INTO run_state(run_id, request_json) VALUES(?, ?)",
            (request.run_id, request.model_dump_json()),
        )
        await self._db.commit()
        return cur.rowcount == 1

    async def get_request(self, run_id: str) -> RunRequest | None:
        # 取原 request 供 resume 重建 agent。
        async with self._db.execute(
            "SELECT request_json FROM run_state WHERE run_id=?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return RunRequest.model_validate_json(row[0])

    async def try_mark_terminal(self, run_id: str) -> bool:
        # UPSERT：未有记录时插入 terminal=1；已有记录且 terminal==0 时更新；已 terminal==1 时 rowcount==0。
        cur = await self._db.execute(
            "INSERT INTO run_state(run_id, terminal) VALUES(?, 1)"
            " ON CONFLICT(run_id) DO UPDATE SET terminal=1 WHERE terminal=0",
            (run_id,),
        )
        await self._db.commit()
        return cur.rowcount == 1

    async def is_terminal(self, run_id: str) -> bool:
        # 只读查：resume stale 闸。
        async with self._db.execute(
            "SELECT terminal FROM run_state WHERE run_id=?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return row is not None and row[0] == 1
