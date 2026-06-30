"""进程级单一配置入口：一次性从环境变量解析全部稳定配置。"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StringConstraints

from kokoro_agent.infrastructure.model.settings import LOCAL_FAKE_MODEL_FLAG, ChatModelSettings

_DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"
_DEFAULT_APPROVAL_TOOLS = ("web_fetch",)
_DEFAULT_CHECKPOINT_DB = "kokoro_checkpoints.db"
_DEFAULT_RUN_STATE_DB = "kokoro_run_state.db"
_DEFAULT_MONGO_URL = "mongodb://127.0.0.1:27017"
_DEFAULT_MONGO_DB = "kokoro"

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
RunStateBackend = Literal["sqlite", "mongo"]


class ApprovalPolicy(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    # strict=False 允许 list→frozenset coercion，元素由 _NonEmpty 校验空串。
    requires_approval_tools: Annotated[frozenset[_NonEmpty], Field(strict=False)]


class StreamSettings(BaseModel):
    """事件流后端选择：memory（默认）或 redis。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    backend: str
    redis_url: str

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> StreamSettings:
        return cls(
            backend=source.get("KOKORO_STREAM_BACKEND", "memory").lower(),
            redis_url=source.get("KOKORO_REDIS_URL", _DEFAULT_REDIS_URL),
        )


class MongoSettings(BaseModel):
    """共享 Mongo 连接：checkpointer 与 run_state 的 mongo 后端共用一处 url/db（多 pod GA）。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    url: str
    db: str

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> MongoSettings:
        return cls(
            url=source.get("KOKORO_MONGO_URL", _DEFAULT_MONGO_URL),
            db=source.get("KOKORO_MONGO_DB", _DEFAULT_MONGO_DB),
        )


class CheckpointSettings(BaseModel):
    """图状态 checkpointer 后端：sqlite（默认，落盘）/ mongo（跨 pod）/ memory（易失）。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    backend: str
    db_path: str

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> CheckpointSettings:
        return cls(
            backend=source.get("KOKORO_CHECKPOINT_BACKEND", "sqlite").lower(),
            db_path=source.get("KOKORO_CHECKPOINT_DB", _DEFAULT_CHECKPOINT_DB),
        )


class RunStateSettings(BaseModel):
    """run 状态持久化后端：sqlite（默认，落盘）/ mongo（跨 pod 去重/终态认领）。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    backend: RunStateBackend
    db_path: str

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> RunStateSettings:
        return cls(
            backend=_run_state_backend(source.get("KOKORO_RUN_STATE_BACKEND", "sqlite")),
            db_path=source.get("KOKORO_RUN_STATE_DB", _DEFAULT_RUN_STATE_DB),
        )


class ObservabilitySettings(BaseModel):
    """Langfuse 凭据；缺任一即视为未配置 → tracing 静默关闭。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    langfuse_public_key: SecretStr | None = None
    langfuse_secret_key: SecretStr | None = None

    @property
    def langfuse_configured(self) -> bool:
        return self.langfuse_public_key is not None and self.langfuse_secret_key is not None

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> ObservabilitySettings:
        public_key = source.get("LANGFUSE_PUBLIC_KEY")
        secret_key = source.get("LANGFUSE_SECRET_KEY")
        return cls(
            langfuse_public_key=SecretStr(public_key) if public_key else None,
            langfuse_secret_key=SecretStr(secret_key) if secret_key else None,
        )


def _approval_from_env(source: Mapping[str, str]) -> ApprovalPolicy:
    raw = source.get("KOKORO_REQUIRES_APPROVAL_TOOLS", "")
    parsed = tuple(part for part in (s.strip() for s in raw.split(",")) if part)
    # 未设或全空白视同未配置 → 回退默认，避免空集静默放行所有工具。
    tools = parsed if parsed else _DEFAULT_APPROVAL_TOOLS
    return ApprovalPolicy(requires_approval_tools=frozenset(tools))


def _run_state_backend(raw: str) -> RunStateBackend:
    value = raw.lower()
    if value == "sqlite":
        return "sqlite"
    if value == "mongo":
        return "mongo"
    msg = f"unsupported KOKORO_RUN_STATE_BACKEND: {raw!r}"
    raise ValueError(msg)


class AppConfig(BaseModel):
    """单一配置入口：一次性读全部 env，按域分组。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    model: ChatModelSettings
    stream: StreamSettings
    observability: ObservabilitySettings
    approval: ApprovalPolicy
    checkpoint: CheckpointSettings
    run_state: RunStateSettings
    mongo: MongoSettings
    local_fake_model: bool

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AppConfig:
        source: Mapping[str, str] = env if env is not None else os.environ
        return cls(
            model=ChatModelSettings.from_env(source),
            stream=StreamSettings.from_env(source),
            observability=ObservabilitySettings.from_env(source),
            approval=_approval_from_env(source),
            checkpoint=CheckpointSettings.from_env(source),
            run_state=RunStateSettings.from_env(source),
            mongo=MongoSettings.from_env(source),
            local_fake_model=source.get(LOCAL_FAKE_MODEL_FLAG) == "1",
        )


__all__ = [
    "AppConfig",
    "ApprovalPolicy",
    "CheckpointSettings",
    "LOCAL_FAKE_MODEL_FLAG",
    "MongoSettings",
    "ObservabilitySettings",
    "RunStateSettings",
    "StreamSettings",
]
