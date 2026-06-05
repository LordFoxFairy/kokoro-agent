from __future__ import annotations

import os

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel

from kokoro_agent.infrastructure.local_fake_model import make_local_fake_chat_model

DEFAULT_MODEL = "anthropic:claude-sonnet-4-6"
LOCAL_FAKE_MODEL_FLAG = "KOKORO_LOCAL_FAKE_MODEL"


def make_chat_model() -> BaseChatModel:
    """Build the configured chat model for the worker.

    When ``KOKORO_LOCAL_FAKE_MODEL=1`` is set, return a deterministic local
    fake model so the real Redis-backed three-repo chain can be exercised
    without external provider credentials. Otherwise, keep the current fail-loud
    real-provider behavior based on ``KOKORO_MODEL``.
    """
    if os.environ.get(LOCAL_FAKE_MODEL_FLAG) == "1":
        return make_local_fake_chat_model()

    spec = os.environ.get("KOKORO_MODEL", DEFAULT_MODEL)
    # Some OpenAI-compatible gateways reject streaming (concurrency caps); set
    # KOKORO_DISABLE_STREAMING=1 to run the deep-agent loop non-streaming.
    if os.environ.get("KOKORO_DISABLE_STREAMING") == "1":
        return init_chat_model(spec, disable_streaming=True)
    return init_chat_model(spec)
