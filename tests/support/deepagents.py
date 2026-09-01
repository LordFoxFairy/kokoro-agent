# pyright: reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Typed test boundary for constructing the upstream native DeepAgents graph."""

from __future__ import annotations

from typing import Any

from deepagents import create_deep_agent

from kokoro_agent.execution.protocols import AgentRunnable, require_agent_runnable


def create_test_deep_agent(**kwargs: Any) -> AgentRunnable:
    """Build the real upstream graph and validate the call surface used by GA tests."""

    candidate: object = create_deep_agent(**kwargs)
    return require_agent_runnable(candidate)
