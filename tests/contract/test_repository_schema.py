"""The Agent schema is canonical and contains no runtime legacy migration."""

from kokoro_agent.infrastructure.schema import (
    RUN_CONTROL_COMMANDS_TABLE,
    SANDBOX_CLEANUP_INTENTS_TABLE,
    canonical_schema_sql,
)


def test_schema_has_one_agent_owned_control_command_ledger() -> None:
    sql = canonical_schema_sql()

    assert f"CREATE TABLE IF NOT EXISTS {RUN_CONTROL_COMMANDS_TABLE}" in sql
    assert "pk_kokoro_agent_run_control_command PRIMARY KEY (run_id, command_id)" in sql
    assert (
        "'admitted', 'persisted', 'applied', 'succeeded', 'failed', 'superseded'" in sql
    )
    assert "CREATE TABLE IF NOT EXISTS kokoro_agent_chat_message" in sql
    assert "CREATE TABLE IF NOT EXISTS kokoro_agent_chat_event" in sql


def test_schema_does_not_contain_legacy_rewrite_logic() -> None:
    sql = canonical_schema_sql()

    assert "information_schema" not in sql
    assert "decision_id" not in sql
    assert "ALTER TABLE" not in sql


def test_dispatch_admission_persists_replayable_request_without_silent_expiry() -> None:
    sql = " ".join(canonical_schema_sql().lower().split())

    assert "request_json text not null" in sql
    assert "deadline_at" not in sql
    assert (
        "ck_kokoro_agent_run_dispatch_status check (status in ('pending', 'claimed'))"
        in sql
    )


def test_event_indices_and_sandbox_binding_are_generation_safe() -> None:
    sql = " ".join(canonical_schema_sql().lower().split())

    assert "event_index_counter bigint not null default 0" in sql
    assert "sandbox_generation bigint" in sql
    assert "sandbox_backend_kind text" in sql
    assert "sandbox_teardown_ref text" in sql
    assert "on kokoro_agent_run_outbox (run_id, index_value)" in sql
    assert "where index_value is not null and status <> 'superseded'" in sql


def test_sandbox_cleanup_is_durable_retryable_and_has_no_database_foreign_key() -> None:
    sql = " ".join(canonical_schema_sql().lower().split())

    assert f"create table if not exists {SANDBOX_CLEANUP_INTENTS_TABLE}" in sql
    assert "status in ('pending', 'processing', 'completed')" in sql
    assert "attempt_count bigint not null default 0" in sql
    assert "next_attempt_at timestamptz(3) not null" in sql
    assert "uq_kokoro_agent_sandbox_cleanup_resource unique" in sql
    assert "FOREIGN KEY" not in sql.upper()
    assert "REFERENCES" not in sql.upper()
