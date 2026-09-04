"""LangGraph checkpointer 工厂：PostgreSQL（stage1 durable state 真源）。"""

# LangGraph/psycopg expose the connection and row factory through runtime
# protocols whose current stubs do not describe the installed versions.

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from pydantic import BaseModel, ConfigDict

from kokoro_agent.infrastructure.postgres import DEFAULT_PG_SCHEMA
from kokoro_agent.infrastructure.postgres import connect_pg
from kokoro_agent.infrastructure.schema import verify_agent_schema
from kokoro_agent.infrastructure.sql import execute_sql


class CheckpointSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    database_url: str
    schema_name: str = DEFAULT_PG_SCHEMA


@asynccontextmanager
async def make_checkpointer(
    settings: CheckpointSettings,
) -> AsyncGenerator[BaseCheckpointSaver[str], None]:
    async with connect_pg(settings.database_url) as conn:
        await verify_agent_schema(conn, settings.schema_name)
        # LangGraph's saver follows the connection search path; it does not
        # accept a schema_name constructor argument in the current release.
        escaped_schema = settings.schema_name.replace(chr(34), chr(34) * 2)
        await execute_sql(conn, f'SET search_path TO "{escaped_schema}"')
        saver_constructor: Any = AsyncPostgresSaver
        saver: BaseCheckpointSaver[str] = saver_constructor(conn)
        yield saver
