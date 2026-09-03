from kokoro_agent.sandbox.workspace import workspace_key


def test_workspace_key_preserves_agent_workspace_identity() -> None:
    assert workspace_key("namespace", "session") == "namespace:session"
    assert workspace_key("other", "session") != workspace_key("namespace", "session")
