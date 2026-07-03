"""Langfuse trace config 构造：凭据齐备与否由注入的 settings 决定。"""

from __future__ import annotations

from langchain_core.runnables.config import RunnableConfig
from langfuse.langchain import CallbackHandler
from pydantic import BaseModel, ConfigDict, SecretStr

from kokoro_agent.contract import RunRequest


class ObservabilitySettings(BaseModel):
    """Langfuse 凭据；缺任一即视为未配置 → tracing 静默关闭。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    langfuse_public_key: SecretStr | None
    langfuse_secret_key: SecretStr | None

    @property
    def configured(self) -> bool:
        return self.langfuse_public_key is not None and self.langfuse_secret_key is not None


def trace_config(settings: ObservabilitySettings, request: RunRequest) -> RunnableConfig | None:
    """配置齐全时返回带 Langfuse handler 与 run 元数据的 config，否则 None（tracing 关）。

    CallbackHandler() 内部 get_client() 自行从进程 env 读 public/secret/host 并维护单例——
    这是 langfuse SDK 的既有行为；本仓只用注入 settings 做开关判定。
    """
    if not settings.configured:
        return None
    return {
        "callbacks": [CallbackHandler()],
        "metadata": {
            "langfuse_session_id": request.context.session_id,
            "langfuse_tags": [request.runtime.model.name],
            "kokoro_run_id": request.run_id,
            "kokoro_thread_id": request.thread_id,
            "kokoro_namespace": request.context.namespace,
        },
    }
