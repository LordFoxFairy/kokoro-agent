"""长期记忆 store 工厂：PostgreSQL（durable long-term memory）。"""

# The LangGraph store protocol and psycopg dict-row stubs are not aligned with
# the installed runtime signatures; keep this adapter's dynamic boundary
# explicit while unit/contract tests validate behavior.

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
    MatchCondition,
    NamespacePath,
    NotProvided,
    Op,
    PutOp,
    Result,
    SearchItem,
    SearchOp,
)
from pydantic import JsonValue

from kokoro_agent.infrastructure.checkpoints import CheckpointSettings
from kokoro_agent.infrastructure.postgres import (
    DEFAULT_PG_SCHEMA,
    connect_pg,
    qualified,
)
from kokoro_agent.infrastructure.schema import MEMORY_TABLE, verify_agent_schema
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one


class PgMemoryStore(BaseStore):
    def __init__(self, database_url: str, schema: str = DEFAULT_PG_SCHEMA) -> None:
        self._database_url = database_url
        self._schema = schema

    async def setup(self) -> None:
        async with connect_pg(self._database_url) as conn:
            await verify_agent_schema(conn, self._schema)

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        results: list[Result] = []
        for op in ops:
            if isinstance(op, GetOp):
                results.append(
                    await self.aget(op.namespace, op.key, refresh_ttl=op.refresh_ttl)
                )
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
                if op.value is None:
                    await self.adelete(op.namespace, op.key)
                else:
                    await self.aput(
                        op.namespace,
                        op.key,
                        op.value,
                        index=op.index,
                        ttl=op.ttl,
                    )
                results.append(None)
                continue
            prefix, suffix = namespace_filters(op.match_conditions)
            results.append(
                await self.alist_namespaces(
                    prefix=prefix,
                    suffix=suffix,
                    max_depth=op.max_depth,
                    limit=op.limit,
                    offset=op.offset,
                )
            )
        return results

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        """Synchronous BaseStore compatibility for non-async LangGraph callers.

        The worker uses ``abatch``.  Keeping the sync entry point here satisfies the
        framework contract without adding a second durable-state implementation; sync graph
        callers get the same PostgreSQL-backed operations.
        """
        return asyncio.run(self.abatch(ops))

    async def adelete(self, namespace: tuple[str, ...], key: str) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    "DELETE FROM {} WHERE namespace = %s AND key = %s".format(
                        qualified(self._schema, MEMORY_TABLE)
                    ),
                    (list(namespace), key),
                )

    async def aget(
        self, namespace: tuple[str, ...], key: str, *, refresh_ttl: bool | None = None
    ) -> Item | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    "SELECT namespace, key, value_json, created_at, updated_at FROM {} WHERE namespace = %s AND key = %s".format(
                        qualified(self._schema, MEMORY_TABLE)
                    ),
                    (list(namespace), key),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return Item(
            namespace=tuple(row["namespace"]),
            key=row["key"],
            value=json.loads(row["value_json"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
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
            if matches_namespace(namespace, prefix, suffix)
        ]
        if max_depth is not None:
            matched = [namespace[:max_depth] for namespace in matched]
        deduped = sorted(set(matched))
        return deduped[offset : offset + limit]

    async def aput(
        self,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        index: list[str] | Literal[False] | None = None,
        *,
        ttl: float | None | NotProvided = None,
    ) -> None:
        if ttl is not None and not isinstance(ttl, NotProvided):
            raise NotImplementedError(
                f"TTL is not supported by {self.__class__.__name__}"
            )
        now = _now_ms()
        payload = json.dumps(value, sort_keys=True)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    INSERT INTO {} (namespace, key, value_json, created_at, updated_at)
                    VALUES (%s, %s, %s, to_timestamp(%s / 1000.0),
                            to_timestamp(%s / 1000.0))
                    ON CONFLICT (namespace, key)
                    DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = EXCLUDED.updated_at
                    """.format(qualified(self._schema, MEMORY_TABLE)),
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
        if filter:
            rows = [
                row
                for row in rows
                if matches_filter(json.loads(row["value_json"]), filter)
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
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                score=None,
            )
            for row in matched
        ]

    async def _all_rows(self) -> list[dict[str, Any]]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    "SELECT namespace, key, value_json, created_at, updated_at FROM {} "
                    "ORDER BY namespace ASC, key ASC".format(
                        qualified(self._schema, MEMORY_TABLE)
                    ),
                )
                return list(await fetch_all(cur))


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


def matches_namespace(
    namespace: tuple[str, ...],
    prefix: NamespacePath | None,
    suffix: NamespacePath | None,
) -> bool:
    if prefix is not None and not _matches_path(namespace, prefix, from_start=True):
        return False
    if suffix is not None and not _matches_path(namespace, suffix, from_start=False):
        return False
    return True


def namespace_filters(
    conditions: tuple[MatchCondition, ...] | None,
) -> tuple[NamespacePath | None, NamespacePath | None]:
    """Translate the framework's operation shape to the public store method."""

    prefix: NamespacePath | None = None
    suffix: NamespacePath | None = None
    for condition in conditions or ():
        if condition.match_type == "prefix":
            prefix = condition.path
        elif condition.match_type == "suffix":
            suffix = condition.path
        else:
            raise ValueError(
                f"unsupported namespace match type: {condition.match_type!r}"
            )
    return prefix, suffix


def _matches_path(
    namespace: tuple[str, ...], path: NamespacePath, *, from_start: bool
) -> bool:
    if len(namespace) < len(path):
        return False
    candidate = namespace[: len(path)] if from_start else namespace[-len(path) :]
    pairs = zip(candidate, path, strict=True)
    return all(pattern == "*" or value == pattern for value, pattern in pairs)


def matches_filter(value: JsonValue, expected: JsonValue) -> bool:
    """Match the small JSON filter language exposed by LangGraph's BaseStore."""

    if isinstance(expected, dict):
        if any(key.startswith("$") for key in expected):
            return all(
                _matches_operator(value, operator, operand)
                for operator, operand in expected.items()
            )
        if not isinstance(value, dict):
            return False
        return all(
            matches_filter(value.get(key), nested) for key, nested in expected.items()
        )
    if isinstance(expected, (list, tuple)):
        return (
            isinstance(value, (list, tuple))
            and len(value) == len(expected)
            and all(
                matches_filter(actual, wanted)
                for actual, wanted in zip(value, expected, strict=True)
            )
        )
    return value == expected


def _matches_operator(value: JsonValue, operator: str, operand: JsonValue) -> bool:
    if operator == "$eq":
        return value == operand
    if operator == "$ne":
        return value != operand
    if operator in {"$gt", "$gte", "$lt", "$lte"}:
        actual = _as_number(value)
        wanted = _as_number(operand)
        if actual is None or wanted is None:
            return False
        return {
            "$gt": actual > wanted,
            "$gte": actual >= wanted,
            "$lt": actual < wanted,
            "$lte": actual <= wanted,
        }[operator]
    raise ValueError(f"unsupported memory filter operator: {operator!r}")


def _as_number(value: JsonValue) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _now_ms() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)
