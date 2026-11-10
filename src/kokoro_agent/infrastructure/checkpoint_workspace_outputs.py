"""Capture immutable postimages proved by the pinned native root checkpoints."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from hashlib import sha256
import json
from typing import TypeGuard

from langchain_core.messages import ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple

from kokoro_agent.clients.storage import StorageClientError, WorkspaceOutputUploadClient
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.domain.run.workspace_output import (
    MAX_OUTPUT_BYTES,
    MAX_OUTPUT_VERSIONS,
    WorkspaceOutputGuard,
    OUTPUT_METADATA,
    OUTPUT_PROOF,
    OUTPUT_ROOT,
    OUTPUT_TOOLS,
    output_path_digest,
    matches_native_output_ack,
)
from kokoro_agent.infrastructure.checkpoints import CheckpointSettings
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
    TOOL_JOURNAL_TABLE,
    WORKSPACE_OUTPUT_VERSIONS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.protocol import RunRequest

_MAX_RECONCILE_STEPS = 512
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WorkspaceVersion:
    version_id: str
    checkpoint_id: str
    parent_checkpoint_id: str
    source_generation: int
    task_id: str
    tool_call_id: str
    path: str
    size_bytes: int
    content_sha256: str

    def sql_values(self) -> tuple[str | int, ...]:
        return (
            self.version_id,
            self.checkpoint_id,
            self.parent_checkpoint_id,
            self.source_generation,
            self.task_id,
            self.tool_call_id,
            self.path,
            self.size_bytes,
            self.content_sha256,
        )


def _locator(config: RunnableConfig) -> tuple[str, str] | None:
    values = config.get("configurable", {})
    thread = values.get("thread_id")
    checkpoint = values.get("checkpoint_id")
    if (
        isinstance(thread, str)
        and thread
        and isinstance(checkpoint, str)
        and checkpoint
        and values.get("checkpoint_ns") == ""
    ):
        return thread, checkpoint
    return None


def _config(thread: str, checkpoint: str) -> RunnableConfig:
    return {
        "configurable": {
            "thread_id": thread,
            "checkpoint_ns": "",
            "checkpoint_id": checkpoint,
        }
    }


def _items(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def _messages(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _postimages(
    request: RunRequest,
    parent: CheckpointTuple,
    child: CheckpointTuple,
) -> tuple[tuple[WorkspaceVersion, bytes], ...]:
    parent_id = _locator(parent.config)
    child_id = _locator(child.config)
    generation = child.metadata.get("kokoro_generation")
    parent_generation = parent.metadata.get("kokoro_generation")
    if (
        parent_id is None
        or child_id is None
        or parent_id == child_id
        or child.parent_config is None
        or _locator(child.parent_config) != parent_id
        or parent_id[0] != child_id[0]
        or child_id[0] != RunScope.of(request).scoped_thread_id
        or type(generation) is not int
        or generation <= 0
        or type(parent_generation) is not int
        or parent_generation != generation
        or any(
            item.metadata.get("kokoro_run_id") != request.run_id
            or item.metadata.get(OUTPUT_METADATA) != OUTPUT_ROOT
            or item.metadata.get("source") != "loop"
            for item in (parent, child)
        )
        or "files" not in child.checkpoint["channel_versions"]
        or child.checkpoint["channel_versions"]["files"]
        == parent.checkpoint["channel_versions"].get("files")
    ):
        return ()
    files: dict[str, list[tuple[str, object]]] = {}
    proofs: dict[str, list[tuple[str, str]]] = {}
    for task, channel, value in parent.pending_writes or ():
        if channel == "files" and _items(value):
            for path, postimage in value.items():
                if isinstance(path, str) and output_path_digest(path) is not None:
                    files.setdefault(path, []).append((task, postimage))
        if channel == "messages":
            for msg in value if _messages(value) else [value]:
                if (
                    isinstance(msg, ToolMessage)
                    and msg.name in OUTPUT_TOOLS
                    and msg.status == "success"
                ):
                    metadata: object = getattr(msg, "response_metadata")
                    digest = metadata.get(OUTPUT_PROOF) if _items(metadata) else None
                    if isinstance(digest, str) and msg.tool_call_id:
                        proofs.setdefault(task, []).append((msg.tool_call_id, digest))
    versions: list[tuple[WorkspaceVersion, bytes]] = []
    for path, writes in files.items():
        if len(writes) != 1:
            continue
        task, postimage = writes[0]
        matches = proofs.get(task, ())
        if len(matches) != 1 or matches[0][1] != output_path_digest(path):
            continue
        if not _items(postimage) or postimage.get("encoding") != "utf-8":
            continue
        content = postimage.get("content")
        if not isinstance(content, str) or len(content) > MAX_OUTPUT_BYTES:
            continue
        try:
            raw = content.encode("utf-8")
        except UnicodeEncodeError:
            continue
        if len(raw) > MAX_OUTPUT_BYTES:
            continue
        version = sha256(
            json.dumps(
                ["state-workspace-output:v1", request.run_id, child_id[1], task, path],
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()
        versions.append(
            (
                WorkspaceVersion(
                    "wsv_" + version,
                    child_id[1],
                    parent_id[1],
                    generation,
                    task,
                    matches[0][0],
                    path,
                    len(raw),
                    sha256(raw).hexdigest(),
                ),
                raw,
            )
        )
    return tuple(versions)


_COLUMNS = (
    "version_id",
    "checkpoint_id",
    "parent_checkpoint_id",
    "source_generation",
    "task_id",
    "tool_call_id",
    "path",
    "size_bytes",
    "content_sha256",
)


class CheckpointWorkspaceOutputs:
    """An independent native read plus a short fenced local write; no object I/O."""

    def __init__(
        self,
        settings: CheckpointSettings,
        runs: RunRepository,
        reader: BaseCheckpointSaver[str],
        storage: WorkspaceOutputUploadClient | None = None,
    ) -> None:
        self._context = PostgresRunRepositoryContext(
            settings.database_url, 0, settings.schema_name
        )
        self._runs = runs
        self._reader = reader
        self._storage = storage
        self._table = qualified(self._context.schema, WORKSPACE_OUTPUT_VERSIONS_TABLE)

    async def _read(
        self,
        request: RunRequest,
        parent: tuple[str, str],
        child: tuple[str, str],
    ) -> tuple[tuple[WorkspaceVersion, bytes], ...]:
        if parent[0] != RunScope.of(request).scoped_thread_id or child[0] != parent[0]:
            return ()
        exact_child = await self._reader.aget_tuple(_config(*child))
        if exact_child is None or _locator(exact_child.config) != child:
            return ()
        exact_parent = await self._reader.aget_tuple(_config(*parent))
        if exact_parent is None or _locator(exact_parent.config) != parent:
            return ()
        return _postimages(request, exact_parent, exact_child)

    async def _persist(
        self,
        request: RunRequest,
        lease: LeaseFence,
        versions: tuple[WorkspaceVersion, ...],
    ) -> None:
        if not versions:
            return
        columns = ",".join(_COLUMNS)
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction(), conn.cursor() as cur:
                if not await self._context.lock_active_lease(
                    cur, request.run_id, lease
                ):
                    return
                for version in versions:
                    values = version.sql_values()
                    await execute_sql(
                        cur,
                        f"""INSERT INTO {self._table} (run_id,{columns})
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (run_id,version_id) DO NOTHING""",
                        (request.run_id, *values),
                    )
                    await execute_sql(
                        cur,
                        f"SELECT {columns} FROM {self._table} WHERE run_id=%s AND version_id=%s",
                        (request.run_id, version.version_id),
                    )
                    row = await fetch_one(cur)
                    if (
                        row is None
                        or tuple(row[column] for column in _COLUMNS) != values
                    ):
                        raise RuntimeError("workspace output version identity conflict")

    async def capture(self, parent: RunnableConfig, child: RunnableConfig) -> None:
        source, target = _locator(parent), _locator(child)
        if source is None or target is None or source[0] != target[0]:
            return
        async with asyncio.timeout(2):
            saved = await self._reader.aget_tuple(_config(*target))
            if (
                saved is None
                or _locator(saved.config) != target
                or saved.metadata.get(OUTPUT_METADATA) != OUTPUT_ROOT
            ):
                return
            run_id = saved.metadata.get("kokoro_run_id")
            generation = saved.metadata.get("kokoro_generation")
            if not isinstance(run_id, str) or type(generation) is not int:
                return
            request = await self._runs.get_request(run_id)
            lease = await self._runs.get_fence(run_id)
            if (
                request is None
                or lease is None
                or lease.generation != generation
                or source[0] != RunScope.of(request).scoped_thread_id
            ):
                return
            prior = await self._reader.aget_tuple(_config(*source))
            if prior is not None and _locator(prior.config) == source:
                await self._persist(
                    request,
                    lease,
                    tuple(version for version, _ in _postimages(request, prior, saved)),
                )

    async def recover(
        self,
        request: RunRequest,
        lease: LeaseFence,
        *,
        native: bool = False,
        guard: WorkspaceOutputGuard | None = None,
    ) -> None:
        """Re-admit exact prior-generation versions along the actual Run lineage."""
        if (
            not await self._runs.is_lease_current(request.run_id, lease)
            or await self._runs.get_request(request.run_id) != request
        ):
            return
        if native:
            if self._storage is not None:
                await self._recover_native(self._storage, request, lease, guard)
            return
        thread = RunScope.of(request).scoped_thread_id
        table = qualified(self._context.schema, "checkpoints")
        async with asyncio.timeout(5):
            async with (
                connect_pg(self._context.database_url) as conn,
                conn.cursor() as cur,
            ):
                await execute_sql(
                    cur,
                    f"""WITH RECURSIVE latest AS (
                        SELECT checkpoint_id,parent_checkpoint_id,metadata,
                            checkpoint->'channel_versions'->'files' AS file_version
                        FROM {table} WHERE thread_id=%s AND checkpoint_ns=''
                        ORDER BY checkpoint_id DESC LIMIT 1
                    ), ancestry AS (
                        SELECT *, 0 AS depth FROM latest WHERE metadata->>'kokoro_run_id'=%s
                        UNION ALL
                        SELECT p.checkpoint_id,p.parent_checkpoint_id,p.metadata,
                            p.checkpoint->'channel_versions'->'files',a.depth+1
                        FROM ancestry a JOIN {table} p ON p.thread_id=%s AND p.checkpoint_ns=''
                          AND p.checkpoint_id=a.parent_checkpoint_id
                        WHERE a.depth < %s AND p.metadata->>'kokoro_run_id'=%s
                    )
                    SELECT a.checkpoint_id,a.parent_checkpoint_id,a.depth,
                        p.file_version IS DISTINCT FROM a.file_version AS changed
                    FROM ancestry a LEFT JOIN ancestry p ON p.depth=a.depth+1 AND p.checkpoint_id=a.parent_checkpoint_id
                    ORDER BY a.depth DESC""",
                    (
                        thread,
                        request.run_id,
                        thread,
                        _MAX_RECONCILE_STEPS,
                        request.run_id,
                    ),
                )
                rows = await fetch_all(cur)
            if len(rows) > _MAX_RECONCILE_STEPS:
                raise RuntimeError(
                    "workspace output recovery checkpoint budget exceeded"
                )
        for row in rows:
            parent_id, child_id = row["parent_checkpoint_id"], row["checkpoint_id"]
            if (
                not row["changed"]
                or not isinstance(parent_id, str)
                or not isinstance(child_id, str)
            ):
                continue
            async with asyncio.timeout(5):
                postimages = tuple(
                    (version, raw)
                    for version, raw in await self._read(
                        request, (thread, parent_id), (thread, child_id)
                    )
                    if version.source_generation <= lease.generation
                )
                await self._persist(
                    request, lease, tuple(version for version, _ in postimages)
                )
            if self._storage is not None:
                for version, raw in postimages:
                    await self._publish(
                        self._storage,
                        request,
                        lease,
                        version.version_id,
                        version.path,
                        version.content_sha256,
                        raw,
                    )

    async def _recover_native(
        self,
        storage: WorkspaceOutputUploadClient,
        request: RunRequest,
        lease: LeaseFence,
        guard: WorkspaceOutputGuard | None,
    ) -> None:
        # Never accumulate multiple raw postimages in Python or reopen the file.
        async with connect_pg(self._context.database_url) as conn, conn.cursor() as cur:
            await execute_sql(
                cur,
                f"""SELECT version_id FROM {self._table}
                WHERE run_id=%s AND pending_bytes IS NOT NULL AND source_generation<=%s
                ORDER BY created_at,version_id LIMIT %s""",
                (request.run_id, lease.generation, MAX_OUTPUT_VERSIONS + 1),
            )
            versions = await fetch_all(cur)
        if len(versions) > MAX_OUTPUT_VERSIONS:
            raise RuntimeError("native workspace output version budget exceeded")
        journal = qualified(self._context.schema, TOOL_JOURNAL_TABLE)
        for version in versions:
            if not await self._runs.is_lease_current(request.run_id, lease):
                return
            version_id = version["version_id"]
            async with (
                connect_pg(self._context.database_url) as conn,
                conn.cursor() as cur,
            ):
                await execute_sql(
                    cur,
                    f"""SELECT v.path,v.content_sha256,v.size_bytes,v.pending_bytes,j.name,j.result
                    FROM {self._table} v JOIN {journal} j USING (run_id,tool_call_id)
                    WHERE v.run_id=%s AND v.version_id=%s AND v.pending_bytes IS NOT NULL
                    AND v.source_generation<=%s AND j.name IN ('write_file','edit_file')
                    AND j.status='succeeded' AND j.is_error=FALSE""",
                    (request.run_id, version_id, lease.generation),
                )
                row = await fetch_one(cur)
            if row is None:
                continue
            path, digest, raw = row["path"], row["content_sha256"], row["pending_bytes"]
            if (
                not isinstance(version_id, str)
                or not isinstance(path, str)
                or output_path_digest(path) is None
                or not matches_native_output_ack(row["name"], path, row["result"])
                or not isinstance(digest, str)
                or not isinstance(raw, bytes)
                or len(raw) != row["size_bytes"]
                or len(raw) > MAX_OUTPUT_BYTES
                or sha256(raw).hexdigest() != digest
            ):
                raise RuntimeError("native workspace output evidence differs")
            if guard is not None and not await guard(path, raw):
                continue
            await self._publish(storage, request, lease, version_id, path, digest, raw)

    async def _publish(
        self,
        storage: WorkspaceOutputUploadClient,
        request: RunRequest,
        lease: LeaseFence,
        version_id: str,
        path: str,
        digest: str,
        raw: bytes,
    ) -> None:
        args = (request.run_id, version_id)
        async with connect_pg(self._context.database_url) as conn, conn.cursor() as cur:
            await execute_sql(
                cur,
                f"SELECT upload_id,source_version_id,asset_id FROM {self._table} WHERE run_id=%s AND version_id=%s",
                args,
            )
            row = await fetch_one(cur)
        if row is None or row["asset_id"] is not None:
            return
        upload_id, source_version_id = row["upload_id"], row["source_version_id"]
        selected = (
            (upload_id, source_version_id)
            if isinstance(upload_id, str) and isinstance(source_version_id, str)
            else None
        )

        async def freeze(upload_id: str, source_version_id: str) -> bool:
            async with connect_pg(self._context.database_url) as conn:
                async with conn.transaction(), conn.cursor() as cur:
                    if not await self._context.lock_active_lease(
                        cur, request.run_id, lease
                    ):
                        return False
                    await execute_sql(
                        cur,
                        f"""UPDATE {self._table} SET upload_id=%s,source_version_id=%s
                        WHERE run_id=%s AND version_id=%s AND asset_id IS NULL
                        AND (upload_id IS NULL OR (upload_id=%s AND source_version_id=%s))
                        RETURNING version_id""",
                        (
                            upload_id,
                            source_version_id,
                            *args,
                            upload_id,
                            source_version_id,
                        ),
                    )
                    return await fetch_one(cur) is not None

        try:
            asset_id = await storage.upload_workspace_output(
                request,
                lease,
                version_id,
                path,
                digest,
                raw,
                selected,
                freeze,
            )
        except StorageClientError as error:
            if error.retryable:
                raise
            # An owner rejection is never a public version. No path, URI, bytes,
            # provider detail or stale-generation output goes into the log.
            _LOGGER.warning("workspace output not admitted", extra={"code": error.code})
            return
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction(), conn.cursor() as cur:
                if not await self._context.lock_active_lease(
                    cur, request.run_id, lease
                ):
                    return
                await execute_sql(
                    cur,
                    f"""UPDATE {self._table} SET asset_id=%s,pending_bytes=NULL
                    WHERE run_id=%s AND version_id=%s AND upload_id IS NOT NULL
                    AND (asset_id IS NULL OR asset_id=%s) RETURNING version_id""",
                    (asset_id, *args, asset_id),
                )
                if await fetch_one(cur) is None:
                    raise RuntimeError("workspace output asset identity conflict")
