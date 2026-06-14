from __future__ import annotations

import os

from langfuse import Langfuse
from langfuse.langchain import CallbackHandler

# Langfuse 凭据所在 env；缺任一即视为未配置 → tracing 静默关闭（离线/CI/未接入零影响）。
_REQUIRED_ENV = ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY")


def langfuse_configured() -> bool:
    return all(os.environ.get(key) for key in _REQUIRED_ENV)


def build_langfuse_handler() -> CallbackHandler | None:
    """配置齐全时返回 Langfuse 的 LangChain CallbackHandler，否则 None（tracing 关）。

    HOST 默认 Langfuse Cloud；自托管设 ``LANGFUSE_HOST``。Langfuse() 从 env 读
    public/secret/host 并维护单例，CallbackHandler() 复用该客户端。
    """
    if not langfuse_configured():
        return None
    Langfuse()
    return CallbackHandler()
