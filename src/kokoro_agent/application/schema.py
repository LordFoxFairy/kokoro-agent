"""Application boundary for the explicit canonical schema operator command."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import logging
import os

from dotenv import load_dotenv

from kokoro_agent.config import AppConfig
from kokoro_agent.infrastructure.postgres import connect_pg
from kokoro_agent.infrastructure.schema import (
    apply_agent_schema,
    canonical_schema_sql,
    expand_financial_close_schema,
)
from kokoro_agent.infrastructure.financial_close_schema import financial_close_ddl


async def apply_database_schema(environment: Mapping[str, str]) -> None:
    """Install the checked-in Agent schema into one empty configured namespace."""

    config = AppConfig.from_env(environment)
    canonical_schema_sql()  # Asset failures must precede the first DB connection.
    async with connect_pg(config.database_url) as connection:
        await apply_agent_schema(
            connection,
            config.database_schema,
            require_blank=True,
        )


def db_apply_schema_main() -> int:
    """Install the current schema as an explicit operator command."""

    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    asyncio.run(apply_database_schema(os.environ))
    print("installed canonical Agent schema")
    return 0


__all__ = ["apply_database_schema", "db_apply_schema_main"]


async def expand_model_financial_close(environment: Mapping[str, str] | None) -> None:
    config = AppConfig.from_env(os.environ if environment is None else environment)
    financial_close_ddl()
    async with connect_pg(config.database_url) as connection:
        await expand_financial_close_schema(connection, config.database_schema)
