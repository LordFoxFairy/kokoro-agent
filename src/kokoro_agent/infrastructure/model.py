from __future__ import annotations

import os

from langchain.chat_models import init_chat_model
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from kokoro_agent.run_agent import BrainModel
from kokoro_agent.tools import TOOL_OBJECTS

DEFAULT_MODEL = "anthropic:claude-sonnet-4-6"


def _scripted_messages() -> list[AIMessage]:
    """Deterministic offline script: one thinking + tool_call turn, then text.

    Drives the full event family (``thinking.delta`` -> ``tool.invoked`` ->
    ``tool.returned`` -> ``text.delta`` -> ``text.completed``) through
    ``run_agent`` with ``execution_style="thinking"`` — no network, no key.
    """
    return [
        AIMessage(
            content=[
                {"type": "thinking", "thinking": "Let me search to be sure."},
            ],
            tool_calls=[
                {
                    "name": "echo_search",
                    "args": {"query": "kokoro"},
                    "id": "call_scripted_1",
                    "type": "tool_call",
                }
            ],
        ),
        AIMessage(
            content=[
                {"type": "text", "text": "Here is what I found for kokoro."},
            ]
        ),
    ]


def make_chat_model() -> BrainModel:
    """Build the chat model selected by ``KOKORO_MODEL``.

    ``KOKORO_MODEL=scripted`` returns a deterministic offline fake brain for
    browser demos (no network, no key). Any other value is a provider:model
    spec; the real model is built lazily and bound with ``TOOL_OBJECTS`` so the
    brain loop can issue tool calls. An invalid spec raises loudly here rather
    than degrading to a silent fallback.
    """
    spec = os.environ.get("KOKORO_MODEL", DEFAULT_MODEL)
    if spec == "scripted":
        return GenericFakeChatModel(messages=iter(_scripted_messages()))
    return init_chat_model(spec).bind_tools(  # pyright: ignore[reportUnknownMemberType]
        TOOL_OBJECTS
    )
