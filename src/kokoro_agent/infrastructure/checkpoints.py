"""LangGraph checkpointer 工厂：PostgreSQL（stage1 durable state 真源）。"""

# LangGraph/psycopg expose the connection and row factory through runtime
# protocols whose current stubs do not describe the installed versions.
# pyright: reportCallIssue=false, reportArgumentType=false, reportIncompatibleMethodOverride=false

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from pydantic import BaseModel, ConfigDict
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from kokoro_agent.infrastructure.postgres import DEFAULT_PG_SCHEMA, ensure_schema


class CheckpointSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    database_url: str
    schema_name: str = DEFAULT_PG_SCHEMA


@asynccontextmanager
async def make_checkpointer(
    settings: CheckpointSettings,
) -> AsyncGenerator[BaseCheckpointSaver[str], None]:
    conn = await AsyncConnection.connect(
        settings.database_url,
        autocommit=True,
        row_factory=dict_row,
    )
    try:
        await ensure_schema(conn, settings.schema_name)
        # LangGraph's saver follows the connection search path; it does not
        # accept a schema_name constructor argument in the current release.
        await conn.execute(f'SET search_path TO "{settings.schema_name.replace(chr(34), chr(34) * 2)}"')
        saver = AsyncPostgresSaver(conn)
        await saver.setup()
        yield saver
    finally:
        await conn.close()
