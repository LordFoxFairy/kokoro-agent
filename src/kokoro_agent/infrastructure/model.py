from __future__ import annotations

import os

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel

DEFAULT_MODEL = "anthropic:claude-sonnet-4-6"


def make_chat_model() -> BaseChatModel:
    """Build the chat model selected by ``KOKORO_MODEL`` (provider:model spec).

    Construction is lazy and performs no network I/O — ``init_chat_model`` only
    builds the client object. An invalid spec or a missing provider package
    raises loudly here rather than degrading to a silent fallback.
    """
    spec = os.environ.get("KOKORO_MODEL", DEFAULT_MODEL)
    return init_chat_model(spec)
