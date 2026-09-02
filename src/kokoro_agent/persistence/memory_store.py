"""长期记忆 store 工厂：PostgreSQL（durable long-term memory）。"""

# The LangGraph store protocol and psycopg dict-row stubs are not aligned with
# the installed runtime signatures; keep this adapter's dynamic boundary
# explicit while unit/contract tests validate behavior.
# pyright: reportCallIssue=false, reportArgumentType=false, reportReturnType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportIncompatibleMethodOverride=false, reportAbstractUsage=false, reportAttributeAccessIssue=false, reportUnnecessaryIsInstance=false

from __future__ import annotations

from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
import asyncio
import json
from typing import Any, Literal

from langgraph.store.base import (
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    NamespacePath,
    Op,
    PutOp,
    Result,
    SearchItem,
    SearchOp,
)
from kokoro_agent.persistence.checkpoints import CheckpointSettings
from kokoro_agent.persistence.postgres import DEFAULT_PG_SCHEMA, connect_pg, ensure_schema, qualified

MEMORY_COLLECTION = "kokoro_agent_memory"


class PgMemoryStore(BaseStore):
    def __init__(self, database_url: str, schema: str = DEFAULT_PG_SCHEMA) -> None:
        self._database_url = database_url
        self._schema = schema

    async def setup(self) -> None:
        async with connect_pg(self._database_url) as conn:
            await ensure_schema(conn, self._schema)
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        namespace text[] NOT NULL,
                        key text NOT NULL,
                        value_json text NOT NULL,
                        created_at bigint NOT NULL,
                        updated_at bigint NOT NULL,
                        PRIMARY KEY (namespace, key)
                    )
                    """.format(qualified(self._schema, MEMORY_COLLECTION))
                )

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        results: list[Result] = []
        for op in ops:
            if isinstance(op, GetOp):
                results.append(await self.aget(op.namespace, op.key, refresh_ttl=op.refresh_ttl))
                continue
            if isinstance(op, SearchOp):
                results.append(
                    await self.asearch(
                        op.namespace_prefix,
                        query=op.query,
                        filter=op.filter,
                        limit=op.limit,
                        offset=op.offset,
                        refresh_ttl=op.refresh_ttl,
                    )
                )
                continue
            if isinstance(op, PutOp):
                results.append(
                    await self.aput(
                        op.namespace,
                        op.key,
                        op.value,
                        index=op.index,
                        ttl=op.ttl,
                    )
                )
                continue
            if isinstance(op, ListNamespacesOp):
                results.append(
                    await self.alist_namespaces(
                        prefix=op.prefix,
                        suffix=op.suffix,
                        max_depth=op.max_depth,
                        limit=op.limit,
                        offset=op.offset,
                    )
                )
                continue
            raise TypeError(f"unsupported store op: {type(op)!r}")
        return results

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        """Synchronous BaseStore compatibility for non-async LangGraph callers.

        The worker uses ``abatch``.  Keeping the sync entry point here satisfies the
        framework contract without adding a second storage implementation; sync graph
        callers get the same PostgreSQL-backed operations.
        """
        return asyncio.run(self.abatch(ops))

    async def adelete(self, namespace: tuple[str, ...], key: str) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM {} WHERE namespace = %s AND key = %s".format(
                        qualified(self._schema, MEMORY_COLLECTION)
                    ),
                    (list(namespace), key),
                )

    async def aget(
        self, namespace: tuple[str, ...], key: str, *, refresh_ttl: bool | None = None
    ) -> Item | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT namespace, key, value_json, created_at, updated_at FROM {} WHERE namespace = %s AND key = %s".format(
                        qualified(self._schema, MEMORY_COLLECTION)
                    ),
                    (list(namespace), key),
                )
                row = await cur.fetchone()
        if row is None:
            return None
        return Item(
            namespace=tuple(row["namespace"]),
            key=row["key"],
            value=json.loads(row["value_json"]),
            created_at=_ms_to_dt(int(row["created_at"])),
            updated_at=_ms_to_dt(int(row["updated_at"])),
        )

    async def alist_namespaces(
        self,
        *,
        prefix: NamespacePath | None = None,
        suffix: NamespacePath | None = None,
        max_depth: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[tuple[str, ...]]:
        rows = await self._all_rows()
        namespaces = [tuple(row["namespace"]) for row in rows]
        matched = [
            namespace
            for namespace in namespaces
            if _matches_namespace(namespace, prefix, suffix, max_depth)
        ]
        deduped = list(dict.fromkeys(matched))
        return deduped[offset : offset + limit]

    async def aput(
        self,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        index: list[str] | Literal[False] | None = None,
        *,
        ttl: float | None = None,
    ) -> None:
        now = _now_ms()
        payload = json.dumps(value, sort_keys=True)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (namespace, key, value_json, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (namespace, key)
                    DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = EXCLUDED.updated_at
                    """.format(qualified(self._schema, MEMORY_COLLECTION)),
                    (list(namespace), key, payload, now, now),
                )

    async def asearch(
        self,
        namespace_prefix: tuple[str, ...],
        /,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
        refresh_ttl: bool | None = None,
    ) -> list[SearchItem]:
        rows = [
            row
            for row in await self._all_rows()
            if tuple(row["namespace"])[: len(namespace_prefix)] == namespace_prefix
        ]
        if query is not None:
            needle = query.strip().lower()
            rows = [
                row
                for row in rows
                if needle in row["key"].lower() or needle in row["value_json"].lower()
            ]
        matched = rows[offset : offset + limit]
        return [
            SearchItem(
                namespace=tuple(row["namespace"]),
                key=row["key"],
                value=json.loads(row["value_json"]),
                created_at=_ms_to_dt(int(row["created_at"])),
                updated_at=_ms_to_dt(int(row["updated_at"])),
                score=None,
            )
            for row in matched
        ]

    async def _all_rows(self) -> list[dict[str, Any]]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT namespace, key, value_json, created_at, updated_at FROM {}".format(
                        qualified(self._schema, MEMORY_COLLECTION)
                    )
                )
                return list(await cur.fetchall())


@asynccontextmanager
async def make_memory_store(
    settings: CheckpointSettings,
) -> AsyncGenerator[BaseStore, None]:
    store = PgMemoryStore(settings.database_url, settings.schema_name)
    await store.setup()
    try:
        yield store
    finally:
        pass


def _matches_namespace(
    namespace: tuple[str, ...],
    prefix: NamespacePath | None,
    suffix: NamespacePath | None,
    max_depth: int | None,
) -> bool:
    if prefix is not None and not tuple(namespace[: len(prefix)]) == tuple(prefix):
        return False
    if suffix is not None and len(suffix) > 0 and not tuple(namespace[-len(suffix) :]) == tuple(suffix):
        return False
    if max_depth is not None and len(namespace) > max_depth:
        return False
    return True


def _now_ms() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


def _ms_to_dt(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)
