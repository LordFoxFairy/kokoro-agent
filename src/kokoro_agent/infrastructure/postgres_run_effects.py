"""Execution-effect, steer, tool-result, and journal capabilities."""

from __future__ import annotations

import hashlib
import json

from pydantic import JsonValue, TypeAdapter, ValidationError

from kokoro_agent.domain.run.repository import LeaseFence, ToolJournalRecord
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
    RUN_STEERS_TABLE,
    TOOL_JOURNAL_TABLE,
    TOOL_RESULTS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one

_INTENT = TypeAdapter(dict[str, JsonValue])
_DELIVERY_BASE_KEYS = frozenset(
    {
        "version",
        "run_id",
        "session_id",
        "tenant_ref",
        "subject_ref",
        "tool_call_id",
        "path",
        "title",
        "note",
        "mime_type",
        "size_bytes",
        "content_sha256",
    }
)
_DELIVERY_ADMITTED_KEYS = _DELIVERY_BASE_KEYS | {"upload_id"}
_DELIVERY_SELECTED_KEYS = _DELIVERY_ADMITTED_KEYS | {
    "source_version_id",
    "complete_command_id",
    "complete_request_digest",
}


def _json_exact(left: JsonValue, right: JsonValue) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _json_exact(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _json_exact(a, b) for a, b in zip(left, right, strict=True)
        )
    return left == right


def _delivery_stage(value: dict[str, JsonValue]) -> int | None:
    if type(value.get("version")) is not int or value["version"] != 2:
        return None
    keys = frozenset(value)
    if keys == _DELIVERY_BASE_KEYS:
        stage = 0
    elif keys == _DELIVERY_ADMITTED_KEYS:
        stage = 1
    elif keys == _DELIVERY_SELECTED_KEYS:
        stage = 2
    else:
        return None
    text_keys = _DELIVERY_BASE_KEYS - {"version", "size_bytes"}
    if any(not isinstance(value[key], str) for key in text_keys):
        return None
    size = value["size_bytes"]
    digest = value["content_sha256"]
    if (
        type(size) is not int
        or size < 0
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
    ):
        return None
    if stage >= 1 and (
        not isinstance(value["upload_id"], str) or not value["upload_id"]
    ):
        return None
    if stage == 2:
        source_version = value["source_version_id"]
        if (
            not isinstance(source_version, str)
            or not source_version
            or len(source_version) > 512
            or source_version == "null"
            or not source_version.strip()
            or any(ord(char) < 32 or ord(char) == 127 for char in source_version)
        ):
            return None
        seed = _delivery_digest([value["run_id"], value["tool_call_id"]])
        fields = [
            value["upload_id"],
            value["content_sha256"],
            value["size_bytes"],
            source_version,
        ]
        if value["complete_command_id"] != _delivery_digest(
            ["delivery-v2", seed, "complete_upload"]
        ) or value["complete_request_digest"] != _delivery_digest(
            ["delivery-v2", "complete_upload", fields]
        ):
            return None
    return stage


def _delivery_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class PostgresRunEffects:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        row = await self._context.get_claim_row(run_id)
        if row is None or bool(row["terminal"]):
            return
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    INSERT INTO {} (run_id, message_id, content, created_at)
                    VALUES (%s, %s, %s, to_timestamp(%s / 1000.0))
                    ON CONFLICT (run_id, message_id) DO NOTHING
                    """.format(qualified(self._context.schema, RUN_STEERS_TABLE)),
                    (run_id, message_id, content, self._context.clock()),
                )

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT message_id, content
                    FROM {}
                    WHERE run_id = %s
                    ORDER BY created_at ASC, message_id ASC
                    """.format(qualified(self._context.schema, RUN_STEERS_TABLE)),
                    (run_id,),
                )
                rows = await fetch_all(cur)
        return [(str(row["message_id"]), str(row["content"])) for row in rows]

    async def ack_steers(
        self, run_id: str, lease: LeaseFence, message_ids: list[str]
    ) -> bool:
        if not message_ids:
            return await self._context.is_lease_current(run_id, lease)
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        DELETE FROM {}
                        WHERE run_id = %s AND message_id = ANY(%s)
                        """.format(qualified(self._context.schema, RUN_STEERS_TABLE)),
                        (run_id, message_ids),
                    )
        return True

    async def put_tool_result(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_id: str,
        result: str,
        is_error: bool,
    ) -> tuple[str, bool] | None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return None
                    await execute_sql(
                        cur,
                        """
                        INSERT INTO {} (run_id, tool_id, result, is_error)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (run_id, tool_id) DO NOTHING
                        """.format(qualified(self._context.schema, TOOL_RESULTS_TABLE)),
                        (run_id, tool_id, result, is_error),
                    )
                    await execute_sql(
                        cur,
                        """
                        SELECT result, is_error
                        FROM {}
                        WHERE run_id = %s AND tool_id = %s
                        """.format(qualified(self._context.schema, TOOL_RESULTS_TABLE)),
                        (run_id, tool_id),
                    )
                    row = await fetch_one(cur)
        if row is None:
            raise RuntimeError(f"tool result missing after insert for {run_id!r}")
        return str(row["result"]), bool(row["is_error"])

    async def get_tool_result(
        self, run_id: str, tool_id: str
    ) -> tuple[str, bool] | None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT result, is_error
                    FROM {}
                    WHERE run_id = %s AND tool_id = %s
                    """.format(qualified(self._context.schema, TOOL_RESULTS_TABLE)),
                    (run_id, tool_id),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return str(row["result"]), bool(row["is_error"])

    async def journal_tool_started(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, name: str
    ) -> bool:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        INSERT INTO {} (run_id, tool_call_id, name, status, result, is_error)
                        VALUES (%s, %s, %s, 'started', '', FALSE)
                        ON CONFLICT (run_id, tool_call_id) DO NOTHING
                        RETURNING tool_call_id
                        """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                        (run_id, tool_call_id, name),
                    )
                    return await fetch_one(cur) is not None

    async def journal_delivery_intent(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, intent: str
    ) -> bool:
        """Freeze delivery, then keep-first the upload and selected object version.

        The existing journal row is the sole Agent-owned intent; no second table or
        cross-owner transaction is introduced. A row lock protects each local stage
        transition, but deliberately does not span signed ObjectStore PUT.
        """
        try:
            proposed = _INTENT.validate_json(intent)
        except ValidationError:
            return False
        proposed_stage = _delivery_stage(proposed)
        if proposed_stage is None:
            return False
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        SELECT name, status, result FROM {}
                        WHERE run_id = %s AND tool_call_id = %s FOR UPDATE
                        """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                        (run_id, tool_call_id),
                    )
                    row = await fetch_one(cur)
                    if (
                        row is None
                        or row["name"] != "deliver"
                        or row["status"] != "started"
                    ):
                        return False
                    previous = str(row["result"])
                    if previous:
                        try:
                            prior = _INTENT.validate_json(previous)
                        except ValidationError:
                            return False
                        prior_stage = _delivery_stage(prior)
                        if (
                            prior_stage is None
                            or proposed_stage < prior_stage
                            or proposed_stage > prior_stage + 1
                            or not prior.keys() <= proposed.keys()
                        ):
                            return False
                        for key, value in prior.items():
                            if key not in proposed or not _json_exact(
                                proposed[key], value
                            ):
                                return False
                    elif proposed_stage != 0:
                        return False
                    await execute_sql(
                        cur,
                        """
                        UPDATE {} SET result = %s, updated_at = NOW()
                        WHERE run_id = %s AND tool_call_id = %s AND status = 'started'
                        """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                        (intent, run_id, tool_call_id),
                    )
                    return True

    async def journal_tool_finished(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_call_id: str,
        result: str,
        is_error: bool,
    ) -> bool:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        UPDATE {}
                        SET status = %s, result = %s, is_error = %s
                        WHERE run_id = %s AND tool_call_id = %s AND status = 'started'
                        RETURNING tool_call_id
                        """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                        (
                            "failed" if is_error else "succeeded",
                            result,
                            is_error,
                            run_id,
                            tool_call_id,
                        ),
                    )
                    return await fetch_one(cur) is not None

    async def clear_tool_journal(
        self, run_id: str, lease: LeaseFence, tool_call_id: str
    ) -> bool:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        DELETE FROM {}
                        WHERE run_id = %s AND tool_call_id = %s
                        """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                        (run_id, tool_call_id),
                    )
        return True

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT name, status, result, is_error
                    FROM {}
                    WHERE run_id = %s AND tool_call_id = %s
                    """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                    (run_id, tool_call_id),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return ToolJournalRecord(**dict(row))

    async def list_delivery_journal(self, run_id: str) -> list[tuple[str, str, str]]:
        """Terminal barrier input: every deliver outcome for this Run."""
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT tool_call_id, status, result FROM {}
                    WHERE run_id = %s AND name = 'deliver'
                    ORDER BY created_at, tool_call_id
                    """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                    (run_id,),
                )
                rows = await fetch_all(cur)
        return [
            (str(row["tool_call_id"]), str(row["status"]), str(row["result"]))
            for row in rows
        ]
