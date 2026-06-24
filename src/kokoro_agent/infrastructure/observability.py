from __future__ import annotations

from langchain_core.runnables.config import RunnableConfig
from langfuse.langchain import CallbackHandler

from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.config import AppConfig


def langfuse_configured() -> bool:
    return AppConfig.from_env().observability.langfuse_configured


def build_langfuse_handler() -> CallbackHandler | None:
    """配置齐全时返回 Langfuse 的 LangChain CallbackHandler，否则 None（tracing 关）。

    HOST 默认 Langfuse Cloud；自托管设 ``LANGFUSE_HOST``。CallbackHandler() 内部
    get_client() 从 env 读 public/secret/host 并维护单例。
    """
    if not langfuse_configured():
        return None
    return CallbackHandler()


def trace_config(req: RunRequest) -> RunnableConfig | None:
    """配置齐全时返回带 Langfuse handler 与 run 元数据的 config，否则 None（tracing 关）。"""
    handler = build_langfuse_handler()
    if handler is None:
        return None
    return {
        "callbacks": [handler],
        "metadata": {
            "langfuse_session_id": req.session_id,
            "langfuse_tags": [req.execution_style],
            "kokoro_run_id": req.run_id,
            "kokoro_conversation_id": req.conversation_id,
        },
    }
