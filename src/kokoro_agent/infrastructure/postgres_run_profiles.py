"""Immutable static Run recipe, committed before external assembly work."""

from __future__ import annotations

import hashlib
import json
from typing import Literal, TypeGuard

from kokoro_agent.domain.run.models import (
    LeaseFence,
    StaticRecipeAuthorityLost,
    StaticRecipeBinding,
    StaticRecipeIncompatible,
)
from kokoro_agent.execution.runtime_profile import canonical_json
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import RUN_CLAIMS_TABLE
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.protocol import RunRequest

MAX_STATIC_RECIPE_BYTES = 8_388_608


def _is_json_object(value: object) -> TypeGuard[dict[str, object]]:
    # json.loads creates only string object keys; validate its root container.
    return isinstance(value, dict)


def validate_static_recipe(binding: StaticRecipeBinding, lease: LeaseFence) -> None:
    """Validate persisted/input bytes without reinterpreting request identity."""
    try:
        if (
            type(binding.canonical_bytes) is not bytes
            or not 0 < len(binding.canonical_bytes) <= MAX_STATIC_RECIPE_BYTES
            or type(binding.fingerprint) is not str
            or hashlib.sha256(binding.canonical_bytes).hexdigest()
            != binding.fingerprint
        ):
            raise ValueError("invalid recipe digest or size")
        value: object = json.loads(binding.canonical_bytes.decode("utf-8"))
        if (
            not _is_json_object(value)
            or value.get("domain_tag") != "kokoro-agent:assembly-recipe:1"
            or canonical_json(value) != binding.canonical_bytes
        ):
            raise ValueError("invalid recipe encoding or version")
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise StaticRecipeIncompatible(lease) from error


class PostgresRunProfiles:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context

    async def freeze_or_verify_static_recipe(
        self, request: RunRequest, lease: LeaseFence, binding: StaticRecipeBinding
    ) -> Literal["frozen", "matched"]:
        validate_static_recipe(binding, lease)
        expected_request = request.model_dump_json().encode("utf-8")
        table = qualified(self._context.schema, RUN_CLAIMS_TABLE)
        async with connect_pg(self._context.database_url) as connection:
            async with connection.transaction():
                async with connection.cursor() as cursor:
                    await execute_sql(
                        cursor,
                        """SELECT tenant_id, request_json, owner, lease_generation,
                                  lease_expires_at, terminal, assembly_recipe_bytes,
                                  assembly_recipe_fingerprint, durable_counter,
                                  event_index_counter, token_total, usage_input_total,
                                  usage_output_total, sandbox_id, sandbox_generation,
                                  sandbox_backend_kind, sandbox_teardown_ref
                           FROM {} WHERE run_id = %s AND tenant_id = %s FOR UPDATE""".format(
                            table
                        ),
                        (request.run_id, request.execution_identity.tenant_ref),
                    )
                    row = await fetch_one(cursor)
                    # A separate statement after FOR UPDATE has returned: lock waiting
                    # must not reuse a pre-lock Python or statement-start timestamp.
                    await execute_sql(cursor, "SELECT clock_timestamp() AS db_now")
                    clock = await fetch_one(cursor)
                    if (
                        row is None
                        or clock is None
                        or row["owner"] != lease.owner
                        or row["lease_generation"] != lease.generation
                        or row["terminal"]
                        or row["lease_expires_at"] is None
                        or row["lease_expires_at"] <= clock["db_now"]
                    ):
                        raise StaticRecipeAuthorityLost("static recipe authority lost")
                    stored_request = row["request_json"]
                    if (
                        not isinstance(stored_request, str)
                        or stored_request.encode("utf-8") != expected_request
                    ):
                        raise StaticRecipeIncompatible(lease)
                    raw, digest = (
                        row["assembly_recipe_bytes"],
                        row["assembly_recipe_fingerprint"],
                    )
                    if raw is not None or digest is not None:
                        if not isinstance(raw, bytes) or not isinstance(digest, str):
                            raise StaticRecipeIncompatible(lease)
                        saved = StaticRecipeBinding(
                            canonical_bytes=raw, fingerprint=digest
                        )
                        validate_static_recipe(saved, lease)
                        if saved != binding:
                            raise StaticRecipeIncompatible(lease)
                        return "matched"
                    if any(
                        row[key] != 0
                        for key in (
                            "durable_counter",
                            "event_index_counter",
                            "token_total",
                            "usage_input_total",
                            "usage_output_total",
                        )
                    ) or any(
                        row[key] is not None
                        for key in (
                            "sandbox_id",
                            "sandbox_generation",
                            "sandbox_backend_kind",
                            "sandbox_teardown_ref",
                        )
                    ):
                        raise StaticRecipeIncompatible(lease)
                    await execute_sql(
                        cursor,
                        """UPDATE {} SET assembly_recipe_bytes = %s,
                                  assembly_recipe_fingerprint = %s,
                                  updated_at = clock_timestamp()
                           WHERE run_id = %s AND tenant_id = %s
                           RETURNING run_id""".format(table),
                        (
                            binding.canonical_bytes,
                            binding.fingerprint,
                            request.run_id,
                            request.execution_identity.tenant_ref,
                        ),
                    )
                    if await fetch_one(cursor) is None:
                        raise StaticRecipeAuthorityLost("static recipe write lost")
        return "frozen"
