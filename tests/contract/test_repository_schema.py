"""The Agent schema is canonical and contains no runtime legacy migration."""

from kokoro_agent.infrastructure.schema import (
    RUN_CONTROL_COMMANDS_TABLE,
    SANDBOX_CLEANUP_INTENTS_TABLE,
    schema_statements,
)


def test_schema_has_one_agent_owned_control_command_ledger() -> None:
    sql = "\n".join(schema_statements("kokoro_agent_test"))

    assert f'"kokoro_agent_test"."{RUN_CONTROL_COMMANDS_TABLE}"' in sql
    assert "PRIMARY KEY (run_id, command_id)" in sql
    assert (
        "'admitted', 'persisted', 'applied', 'succeeded', 'failed', 'superseded'" in sql
    )
    assert '"kokoro_agent_test"."kokoro_agent_chat_messages"' not in sql
    assert '"kokoro_agent_test"."kokoro_agent_chat_events"' not in sql


def test_schema_does_not_contain_legacy_rewrite_logic() -> None:
    sql = "\n".join(schema_statements("kokoro_agent_test"))

    assert "information_schema" not in sql
    assert "decision_id" not in sql
    assert "ALTER TABLE" not in sql


def test_dispatch_admission_persists_replayable_request_without_silent_expiry() -> None:
    sql = "\n".join(schema_statements("kokoro_agent_test"))

    assert "request_json text NOT NULL" in sql
    assert "deadline_at" not in sql
    assert "status text NOT NULL CHECK (status IN ('pending', 'claimed'))" in sql


def test_event_indices_and_sandbox_binding_are_generation_safe() -> None:
    sql = "\n".join(schema_statements("kokoro_agent_test"))

    assert "event_index_counter bigint NOT NULL DEFAULT 0" in sql
    assert "sandbox_generation bigint" in sql
    assert "sandbox_backend_kind text" in sql
    assert "sandbox_teardown_ref text" in sql
    assert (
        'ON "kokoro_agent_test"."kokoro_agent_run_outbox" (run_id, index_value)' in sql
    )
    assert "WHERE index_value IS NOT NULL AND status <> 'superseded'" in sql


def test_sandbox_cleanup_is_durable_retryable_and_has_no_database_foreign_key() -> None:
    sql = "\n".join(schema_statements("kokoro_agent_test"))

    assert f'"kokoro_agent_test"."{SANDBOX_CLEANUP_INTENTS_TABLE}"' in sql
    assert "status IN ('pending', 'processing', 'completed')" in sql
    assert "attempt_count bigint NOT NULL DEFAULT 0" in sql
    assert "next_attempt_at bigint NOT NULL" in sql
    assert "UNIQUE (run_id, lease_generation, backend_kind, sandbox_id)" in sql
    assert "FOREIGN KEY" not in sql.upper()
    assert "REFERENCES" not in sql.upper()
