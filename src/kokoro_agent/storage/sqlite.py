"""SQLite 后端：跨进程/重启的 run 状态存储，WAL+busy_timeout 保真实争用下的原子性。"""

from __future__ import annotations

import time
from collections.abc import Callable

import aiosqlite

from kokoro_agent.contract import RunRequest

_DDL = """\
CREATE TABLE IF NOT EXISTS ledger(
    run_id           TEXT PRIMARY KEY,
    request_json     TEXT,
    terminal         INTEGER NOT NULL DEFAULT 0,
    terminal_at_ms   INTEGER,
    lease_expires_ms INTEGER
)"""

# 结果审核暂停的双执行防护：resume 后节点从头重跑，首跑结果 keep-first 落盘，重入命中即跳过工具。
_TOKEN_TOTALS_DDL = """\
CREATE TABLE IF NOT EXISTS token_totals(
    run_id TEXT PRIMARY KEY,
    total  INTEGER NOT NULL
)"""

_RUN_USAGE_DDL = """\
CREATE TABLE IF NOT EXISTS run_usage(
    run_id       TEXT PRIMARY KEY,
    input_total  INTEGER NOT NULL,
    output_total INTEGER NOT NULL
)"""

_TOOL_RESULTS_DDL = """\
CREATE TABLE IF NOT EXISTS tool_results(
    run_id   TEXT NOT NULL,
    tool_id  TEXT NOT NULL,
    result   TEXT NOT NULL,
    is_error INTEGER NOT NULL,
    PRIMARY KEY(run_id, tool_id)
)"""

# steering 信箱：seq 自增保到达序，UNIQUE(run_id, message_id) 保 keep-first 幂等。
_STEERS_DDL = """\
CREATE TABLE IF NOT EXISTS steers(
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT NOT NULL,
    message_id TEXT NOT NULL,
    content    TEXT NOT NULL,
    UNIQUE(run_id, message_id)
)"""

# e2b run 级 sandbox 绑定（ADR-009 1b）：HITL resume 重连既往箱，keep-first 防重建覆盖。
_SANDBOXES_DDL = """\
CREATE TABLE IF NOT EXISTS sandboxes(
    run_id     TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL
)"""


def _now_ms() -> int:
    return int(time.time() * 1000)


class SqliteLedger:
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
        await self._db.execute(_TOKEN_TOTALS_DDL)
        await self._db.execute(_RUN_USAGE_DDL)
        await self._db.execute(_STEERS_DDL)
        await self._db.execute(_SANDBOXES_DDL)
        await self._db.commit()

    async def try_claim(self, request: RunRequest) -> bool:
        # INSERT OR IGNORE：run_id 已存在（已被认领/终态）即去重丢弃，rowcount==0。
        cur = await self._db.execute(
            "INSERT OR IGNORE INTO ledger(run_id, request_json, lease_expires_ms)"
            " VALUES(?, ?, ?)",
            (request.run_id, request.model_dump_json(), self._clock() + self._ttl_ms),
        )
        await self._db.commit()
        return cur.rowcount == 1

    async def renew(self, run_id: str) -> None:
        # 心跳续租；也把 HITL 暂停哨兵（NULL）拉回活跃租约。
        await self._db.execute(
            "UPDATE ledger SET lease_expires_ms=? WHERE run_id=? AND terminal=0",
            (self._clock() + self._ttl_ms, run_id),
        )
        await self._db.commit()

    async def pause(self, run_id: str) -> None:
        # NULL 哨兵：HITL 等人可以是小时级，暂停 run 绝不被过期重拾重跑。
        await self._db.execute(
            "UPDATE ledger SET lease_expires_ms=NULL WHERE run_id=? AND terminal=0",
            (run_id,),
        )
        await self._db.commit()

    async def reclaim_expired(self) -> list[RunRequest]:
        now = self._clock()
        async with self._db.execute(
            "SELECT run_id, request_json FROM ledger"
            " WHERE terminal=0 AND request_json IS NOT NULL"
            " AND lease_expires_ms IS NOT NULL AND lease_expires_ms<=?",
            (now,),
        ) as cursor:
            rows = await cursor.fetchall()
        reclaimed: list[RunRequest] = []
        for run_id, request_json in rows:
            # 逐行条件更新原子认领：多 pod 并发 reclaim 时每个 run 恰被一个赢家拾走。
            cur = await self._db.execute(
                "UPDATE ledger SET lease_expires_ms=?"
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
            "SELECT run_id FROM ledger"
            " WHERE terminal=0 AND lease_expires_ms IS NULL AND request_json IS NOT NULL"
            " ORDER BY run_id"
        ) as cursor:
            rows = await cursor.fetchall()
        return [str(row[0]) for row in rows]

    async def add_tokens(self, run_id: str, count: int) -> int:
        # UPSERT 原子累加：跨段/跨进程计数单调，RETURNING 取累计值。
        cur = await self._db.execute(
            "INSERT INTO token_totals(run_id, total) VALUES(?, ?)"
            " ON CONFLICT(run_id) DO UPDATE SET total=total+excluded.total"
            " RETURNING total",
            (run_id, count),
        )
        row = await cur.fetchone()
        await self._db.commit()
        return int(row[0]) if row is not None else count

    async def add_usage(self, run_id: str, input_tokens: int, output_tokens: int) -> tuple[int, int]:
        cur = await self._db.execute(
            "INSERT INTO run_usage(run_id, input_total, output_total) VALUES(?, ?, ?)"
            " ON CONFLICT(run_id) DO UPDATE SET"
            " input_total=input_total+excluded.input_total,"
            " output_total=output_total+excluded.output_total"
            " RETURNING input_total, output_total",
            (run_id, input_tokens, output_tokens),
        )
        row = await cur.fetchone()
        await self._db.commit()
        if row is None:
            return (input_tokens, output_tokens)
        return (int(row[0]), int(row[1]))

    async def get_request(self, run_id: str) -> RunRequest | None:
        async with self._db.execute(
            "SELECT request_json FROM ledger WHERE run_id=?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return RunRequest.model_validate_json(row[0])

    async def purge_terminal(self, max_age_ms: int) -> int:
        cutoff = self._clock() - max_age_ms
        cur = await self._db.execute(
            "SELECT run_id FROM ledger WHERE terminal=1 AND terminal_at_ms<=?", (cutoff,)
        )
        run_ids = [str(row[0]) for row in await cur.fetchall()]
        for run_id in run_ids:
            for table in ("ledger", "tool_results", "token_totals", "run_usage", "steers", "sandboxes"):
                await self._db.execute(f"DELETE FROM {table} WHERE run_id=?", (run_id,))
        await self._db.commit()
        return len(run_ids)

    async def try_mark_terminal(self, run_id: str) -> bool:
        # UPSERT：未有记录时插入 terminal=1；已 terminal==1 时 rowcount==0 → 认领失败。
        cur = await self._db.execute(
            "INSERT INTO ledger(run_id, terminal, terminal_at_ms) VALUES(?, 1, ?)"
            " ON CONFLICT(run_id) DO UPDATE SET terminal=1,"
            " terminal_at_ms=excluded.terminal_at_ms WHERE terminal=0",
            (run_id, self._clock()),
        )
        await self._db.commit()
        return cur.rowcount == 1

    async def is_terminal(self, run_id: str) -> bool:
        async with self._db.execute(
            "SELECT terminal FROM ledger WHERE run_id=?", (run_id,)
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

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None:
        # keep-first：resume 竞态下首个绑定生效，重连语义不被后建箱覆盖。
        await self._db.execute(
            "INSERT OR IGNORE INTO sandboxes(run_id, sandbox_id) VALUES(?, ?)",
            (run_id, sandbox_id),
        )
        await self._db.commit()

    async def get_sandbox_id(self, run_id: str) -> str | None:
        cur = await self._db.execute(
            "SELECT sandbox_id FROM sandboxes WHERE run_id=?", (run_id,)
        )
        row = await cur.fetchone()
        return None if row is None else str(row[0])

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        # WHERE EXISTS：未认领 run 安全丢弃（与 mongo 语义对齐，绝不预创建 run 行）。
        await self._db.execute(
            "INSERT OR IGNORE INTO steers(run_id, message_id, content)"
            " SELECT ?, ?, ? WHERE EXISTS(SELECT 1 FROM ledger WHERE run_id=?)",
            (run_id, message_id, content, run_id),
        )
        await self._db.commit()

    async def drain_steers(self, run_id: str) -> list[tuple[str, str]]:
        cur = await self._db.execute(
            "SELECT message_id, content FROM steers WHERE run_id=? ORDER BY seq", (run_id,)
        )
        rows = await cur.fetchall()
        await self._db.execute("DELETE FROM steers WHERE run_id=?", (run_id,))
        await self._db.commit()
        return [(str(row[0]), str(row[1])) for row in rows]
