"""DeepAgents native runnable boundary."""

from __future__ import annotations

import pytest

from support.fakes import FakeAgent
from kokoro_agent.execution.protocols import require_agent_runnable


def test_require_agent_runnable_preserves_native_object() -> None:
    agent = FakeAgent()

    assert require_agent_runnable(agent) is agent


def test_require_agent_runnable_rejects_incomplete_object() -> None:
    with pytest.raises(TypeError, match="native call surface"):
        require_agent_runnable(object())
