"""Re-export the scripted fake chat model from the production package.

The actual implementation lives in
``kokoro_agent.infrastructure._scripted`` so ``make_agent`` can import it
without importing from the ``tests/`` tree.  This module is a thin shim for
any test that imports from ``tests._fake_chat_model`` directly.
"""
from __future__ import annotations

from kokoro_agent.infrastructure._scripted import (
    DeepAgentsFakeChatModel,
    make_scripted_model,
    scripted_planning_model,
)

__all__ = [
    "DeepAgentsFakeChatModel",
    "make_scripted_model",
    "scripted_planning_model",
]
