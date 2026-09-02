"""The Agent schema is canonical and contains no runtime legacy migration."""

from kokoro_agent.infrastructure.schema import (
    RUN_CONTROL_COMMANDS_TABLE,
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
