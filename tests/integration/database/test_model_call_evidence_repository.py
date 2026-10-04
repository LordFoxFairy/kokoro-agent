"""Real-PostgreSQL schema gate for the approved A1 persistence boundary.

This first tests-only slice intentionally binds only to the formal fresh installer
and ``information_schema``.  The production repository seam does not exist yet,
so behavioral cases are not represented by a stub, fake, import failure, skip, or
xfail.  After the canonical tables and repository port are authorized, this same
module will bind prepare/ordinal, authorization/dispatch CAS, immutable evidence,
cancel, stale-fence, and terminal-finalizer cases to the real adapter, using two
connections where concurrency is material.

The schema assertions now verify the implemented canonical target. They remain
schema evidence, not runtime repository or provider evidence.
The shared fixture installs the exact canonical Agent schema in a unique namespace
and owns its cleanup; Root remains the only runner of the real PostgreSQL gate.
"""

from __future__ import annotations

from collections.abc import Collection
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from psycopg.errors import CheckViolation, UniqueViolation

from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql


_TARGET_TABLES = {
    "kokoro_agent_model_call_attempt",
    "kokoro_agent_model_usage_evidence",
}
_TARGET_RUN_COMPLETENESS_COLUMNS = {
    "model_attempt_count",
    "model_dispatched_attempt_count",
    "model_sealed_attempt_count",
    "model_unknown_attempt_count",
    "model_usage_completeness",
}
_CURRENT_RUN_CONTROL_COLUMNS = {
    "run_id",
    "tenant_id",
    "owner",
    "lease_generation",
    "lease_expires_at",
    "terminal",
    "token_total",
    "usage_input_total",
    "usage_output_total",
}
_DIGEST = "a" * 64
_OTHER_DIGEST = "b" * 64


async def _base_tables(database_url: str, schema: str) -> set[str]:
    async with connect_pg(database_url) as connection, connection.cursor() as cursor:
        await cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (schema,),
        )
        return {str(row["table_name"]) for row in await cursor.fetchall()}


async def _columns(database_url: str, schema: str, table: str) -> set[str]:
    async with connect_pg(database_url) as connection, connection.cursor() as cursor:
        await cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return {str(row["column_name"]) for row in await cursor.fetchall()}


def _missing(required: Collection[str], actual: Collection[str]) -> set[str]:
    return set(required).difference(actual)


async def test_fresh_install_contains_a1_attempt_and_evidence_tables(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    """The fresh installer owns both approved A1 tables."""
    tables = await _base_tables(run_chat_database_url, run_chat_schema)

    assert not _missing(_TARGET_TABLES, tables), (
        "A1 canonical tables are absent from the fresh installed owner schema: "
        f"{sorted(_missing(_TARGET_TABLES, tables))}"
    )


async def test_fresh_install_contains_a1_run_completeness_projection(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    """The fresh Run table exposes the closed A1 completeness projection."""
    columns = await _columns(run_chat_database_url, run_chat_schema, "kokoro_agent_run")

    assert not _missing(_TARGET_RUN_COMPLETENESS_COLUMNS, columns), (
        "A1 Run completeness columns are absent from the fresh canonical schema: "
        f"{sorted(_missing(_TARGET_RUN_COMPLETENESS_COLUMNS, columns))}"
    )


async def test_fresh_install_preserves_existing_run_fence_and_budget_columns(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    """Positive control: REDs ran against the formal current Run table."""
    columns = await _columns(run_chat_database_url, run_chat_schema, "kokoro_agent_run")

    assert not _missing(_CURRENT_RUN_CONTROL_COLUMNS, columns), (
        "fresh installer did not provide the existing Run authority/budget baseline: "
        f"{sorted(_missing(_CURRENT_RUN_CONTROL_COLUMNS, columns))}"
    )


@pytest.mark.parametrize(
    (
        "status",
        "authorization_state",
        "has_dispatch",
        "has_latest",
    ),
    [
        ("prepared", "not_requested", False, False),
        ("authorization_pending", "requested_unknown", False, False),
        ("authorization_denied", "denied", False, True),
        ("authorized", "granted", False, False),
        ("dispatch_started", "granted", True, False),
        ("observed", "granted", True, True),
        ("unknown", "granted", True, True),
        ("not_dispatched", "granted", False, True),
    ],
)
async def test_attempt_table_accepts_each_approved_state_shape(
    run_chat_database_url: str,
    run_chat_schema: str,
    status: str,
    authorization_state: str,
    has_dispatch: bool,
    has_latest: bool,
) -> None:
    prepared_at = datetime.now(UTC)
    async with connect_pg(run_chat_database_url) as connection:
        async with connection.cursor() as cursor:
            await execute_sql(
                cursor,
                """INSERT INTO {} (
                    attempt_id,tenant_id,run_id,call_id,attempt_ordinal,
                    lease_owner,lease_generation,attempt_binding_bytes,
                    attempt_binding_digest,planned_bytes,planned_digest,
                    authorization_bytes,authorization_digest,authorization_state,
                    status,prepared_at,dispatch_started_at,sealed_at,
                    latest_revision,latest_event_id,latest_evidence_digest
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s)""".format(
                    qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
                ),
                (
                    uuid4(),
                    "tenant-a1",
                    f"run-{uuid4()}",
                    uuid4(),
                    1,
                    "worker-a1",
                    1,
                    b"{}",
                    _DIGEST,
                    b"{}",
                    _DIGEST,
                    b"{}",
                    _DIGEST,
                    authorization_state,
                    status,
                    prepared_at,
                    prepared_at if has_dispatch else None,
                    prepared_at if has_latest else None,
                    1 if has_latest else 0,
                    uuid4() if has_latest else None,
                    _DIGEST if has_latest else None,
                ),
            )


@pytest.mark.parametrize(
    ("status", "authorization_state", "has_dispatch", "has_latest"),
    [
        ("prepared", "granted", False, False),
        ("prepared", "not_requested", True, False),
        ("authorization_pending", "requested_unknown", True, False),
        ("authorization_denied", "denied", False, False),
        ("authorized", "granted", False, True),
        ("dispatch_started", "granted", False, False),
        ("observed", "granted", True, False),
        ("unknown", "granted", False, True),
        ("not_dispatched", "denied", False, True),
        ("not-a-state", "granted", False, False),
    ],
)
async def test_attempt_table_rejects_invalid_state_cross_products(
    run_chat_database_url: str,
    run_chat_schema: str,
    status: str,
    authorization_state: str,
    has_dispatch: bool,
    has_latest: bool,
) -> None:
    prepared_at = datetime.now(UTC)
    async with connect_pg(run_chat_database_url) as connection:
        with pytest.raises(CheckViolation):
            async with connection.cursor() as cursor:
                await execute_sql(
                    cursor,
                    """INSERT INTO {} (
                        attempt_id,tenant_id,run_id,call_id,attempt_ordinal,
                        lease_owner,lease_generation,attempt_binding_bytes,
                        attempt_binding_digest,planned_bytes,planned_digest,
                        authorization_bytes,authorization_digest,authorization_state,
                        status,prepared_at,dispatch_started_at,sealed_at,
                        latest_revision,latest_event_id,latest_evidence_digest
                    ) VALUES (%s,%s,%s,%s,1,%s,1,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s)""".format(
                        qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
                    ),
                    (
                        uuid4(),
                        "tenant-a1",
                        f"run-{uuid4()}",
                        uuid4(),
                        "worker-a1",
                        b"{}",
                        _DIGEST,
                        b"{}",
                        _DIGEST,
                        b"{}",
                        _DIGEST,
                        authorization_state,
                        status,
                        prepared_at,
                        prepared_at if has_dispatch else None,
                        prepared_at if has_latest else None,
                        1 if has_latest else 0,
                        uuid4() if has_latest else None,
                        _DIGEST if has_latest else None,
                    ),
                )


async def test_attempt_identity_and_evidence_revision_uniqueness_are_durable(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    attempt_id = uuid4()
    call_id = uuid4()
    event_id = uuid4()
    prepared_at = datetime.now(UTC)
    async with connect_pg(run_chat_database_url) as connection:
        async with connection.cursor() as cursor:
            await execute_sql(
                cursor,
                """INSERT INTO {} (
                    attempt_id,tenant_id,run_id,call_id,attempt_ordinal,
                    lease_owner,lease_generation,attempt_binding_bytes,
                    attempt_binding_digest,planned_bytes,planned_digest,
                    authorization_bytes,authorization_digest,authorization_state,
                    status,prepared_at,sealed_at,latest_revision,
                    latest_event_id,latest_evidence_digest
                ) VALUES (%s,'tenant-a1','run-a1',%s,1,'worker-a1',1,
                    %s,%s,%s,%s,%s,%s,'not_requested','not_dispatched',%s,
                    %s,1,%s,%s)""".format(
                    qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
                ),
                (
                    attempt_id,
                    call_id,
                    b"{}",
                    _DIGEST,
                    b"{}",
                    _DIGEST,
                    b"{}",
                    _DIGEST,
                    prepared_at,
                    prepared_at,
                    event_id,
                    _DIGEST,
                ),
            )
            await execute_sql(
                cursor,
                """INSERT INTO {} (
                    event_id,tenant_id,run_id,attempt_id,call_id,revision,
                    evidence_bytes,evidence_digest,execution_outcome,usage_state,
                    totals_state,classification_state,sealed,observed_at
                ) VALUES (%s,'tenant-a1','run-a1',%s,%s,1,%s,%s,
                    'not_dispatched','unresolved','unknown','unknown',TRUE,%s)""".format(
                    qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
                ),
                (event_id, attempt_id, call_id, b"{}", _DIGEST, prepared_at),
            )
        with pytest.raises(UniqueViolation):
            async with connection.cursor() as cursor:
                await execute_sql(
                    cursor,
                    """INSERT INTO {} (
                        event_id,tenant_id,run_id,attempt_id,call_id,revision,
                        evidence_bytes,evidence_digest,execution_outcome,usage_state,
                        totals_state,classification_state,sealed,observed_at
                    ) VALUES (%s,'tenant-a1','run-a1',%s,%s,1,%s,%s,
                        'not_dispatched','unresolved','unknown','unknown',TRUE,%s)""".format(
                        qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
                    ),
                    (
                        uuid4(),
                        attempt_id,
                        call_id,
                        b"different",
                        _OTHER_DIGEST,
                        prepared_at,
                    ),
                )


@pytest.mark.parametrize(
    (
        "revision",
        "has_predecessor",
        "evidence_bytes",
        "evidence_digest",
        "outcome",
        "usage_state",
        "totals_state",
        "classification_state",
        "sealed",
    ),
    [
        (
            0,
            False,
            b"{}",
            _DIGEST,
            "not_dispatched",
            "unresolved",
            "unknown",
            "unknown",
            True,
        ),
        (
            2,
            False,
            b"{}",
            _DIGEST,
            "completed",
            "normalized",
            "known_zero",
            "complete",
            True,
        ),
        (
            1,
            True,
            b"{}",
            _DIGEST,
            "not_dispatched",
            "unresolved",
            "unknown",
            "unknown",
            True,
        ),
        (
            1,
            False,
            b"",
            _DIGEST,
            "not_dispatched",
            "unresolved",
            "unknown",
            "unknown",
            True,
        ),
        (
            1,
            False,
            b"{}",
            "not-a-digest",
            "not_dispatched",
            "unresolved",
            "unknown",
            "unknown",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "unknown",
            "unresolved",
            "known_zero",
            "complete",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "completed",
            "unresolved",
            "known_zero",
            "complete",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "not_dispatched",
            "normalized",
            "known_nonzero",
            "partial",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "completed",
            "normalized",
            "unknown",
            "complete",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "completed",
            "normalized",
            "known_zero",
            "complete",
            False,
        ),
    ],
)
async def test_evidence_table_rejects_invalid_revision_bytes_and_knowledge(
    run_chat_database_url: str,
    run_chat_schema: str,
    revision: int,
    has_predecessor: bool,
    evidence_bytes: bytes,
    evidence_digest: str,
    outcome: str,
    usage_state: str,
    totals_state: str,
    classification_state: str,
    sealed: bool,
) -> None:
    async with connect_pg(run_chat_database_url) as connection:
        with pytest.raises(CheckViolation):
            async with connection.cursor() as cursor:
                await execute_sql(
                    cursor,
                    """INSERT INTO {} (
                        event_id,tenant_id,run_id,attempt_id,call_id,revision,
                        predecessor_event_id,predecessor_digest,evidence_bytes,
                        evidence_digest,execution_outcome,usage_state,totals_state,
                        classification_state,sealed,observed_at
                    ) VALUES (%s,'tenant-a1','run-a1',%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s)""".format(
                        qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
                    ),
                    (
                        uuid4(),
                        uuid4(),
                        uuid4(),
                        revision,
                        uuid4() if has_predecessor else None,
                        _OTHER_DIGEST if has_predecessor else None,
                        evidence_bytes,
                        evidence_digest,
                        outcome,
                        usage_state,
                        totals_state,
                        classification_state,
                        sealed,
                        datetime.now(UTC),
                    ),
                )


async def test_normalized_unknown_usage_remains_a_legal_codec_projection(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    async with connect_pg(run_chat_database_url) as connection:
        async with connection.cursor() as cursor:
            await execute_sql(
                cursor,
                """INSERT INTO {} (
                    event_id,tenant_id,run_id,attempt_id,call_id,revision,
                    evidence_bytes,evidence_digest,execution_outcome,usage_state,
                    totals_state,classification_state,sealed,observed_at
                ) VALUES (%s,'tenant-a1','run-a1',%s,%s,1,%s,%s,
                    'completed','normalized','unknown','unknown',TRUE,%s)""".format(
                    qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
                ),
                (
                    uuid4(),
                    uuid4(),
                    uuid4(),
                    b"{}",
                    _DIGEST,
                    datetime.now(UTC),
                ),
            )


@pytest.mark.parametrize(
    (
        "attempts",
        "dispatched",
        "sealed",
        "unknown",
        "completeness",
        "valid",
    ),
    [
        (0, 0, 0, 0, "none", True),
        (1, 0, 1, 0, "complete", True),
        (2, 1, 2, 0, "partial", True),
        (1, 1, 0, 0, "unknown", True),
        (1, 1, 1, 1, "unknown", True),
        (0, 0, 0, 0, "unknown", False),
        (1, 0, 0, 0, "complete", False),
        (1, 0, 1, 1, "complete", False),
        (1, 2, 1, 0, "partial", False),
        (1, 0, 1, 0, "not-a-state", False),
    ],
)
async def test_run_completeness_count_constraints(
    run_chat_database_url: str,
    run_chat_schema: str,
    attempts: int,
    dispatched: int,
    sealed: int,
    unknown: int,
    completeness: str,
    valid: bool,
) -> None:
    statement = """INSERT INTO {} (
        run_id,tenant_id,model_attempt_count,model_dispatched_attempt_count,
        model_sealed_attempt_count,model_unknown_attempt_count,
        model_usage_completeness
    ) VALUES (%s,'tenant-a1',%s,%s,%s,%s,%s)""".format(
        qualified(run_chat_schema, "kokoro_agent_run")
    )
    values = (f"run-{uuid4()}", attempts, dispatched, sealed, unknown, completeness)
    async with connect_pg(run_chat_database_url) as connection:
        if valid:
            async with connection.cursor() as cursor:
                await execute_sql(cursor, statement, values)
        else:
            with pytest.raises(CheckViolation):
                async with connection.cursor() as cursor:
                    await execute_sql(cursor, statement, values)
