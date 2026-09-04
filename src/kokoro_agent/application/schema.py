"""Application boundary for the explicit canonical schema operator command."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import logging
import os

from dotenv import load_dotenv

from kokoro_agent.config import AppConfig
from kokoro_agent.infrastructure.postgres import connect_pg
from kokoro_agent.infrastructure.schema import apply_agent_schema


async def apply_database_schema(environment: Mapping[str, str]) -> None:
    """Install the checked-in Agent schema into one empty configured namespace."""

    config = AppConfig.from_env(environment)
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
