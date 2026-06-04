from __future__ import annotations

from itertools import repeat

from langchain_core.language_models import BaseChatModel
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

_LOCAL_FAKE_RESPONSE = (
    "Local fallback active. Configure real model credentials to use the "
    "provider-backed agent runtime."
)


class LocalFakeChatModel(GenericFakeChatModel):
    """Deterministic offline chat model for local end-to-end runs.

    Reuse LangChain's built-in fake chat model instead of inventing a parallel
    orchestration path. The repeated static response keeps the worker able to
    serve multiple requests in local fake mode without external credentials.
    """

    def __init__(self) -> None:
        super().__init__(messages=repeat(AIMessage(content=_LOCAL_FAKE_RESPONSE)))


def make_local_fake_chat_model() -> BaseChatModel:
    return LocalFakeChatModel()
