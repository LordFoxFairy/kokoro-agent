"""AppConfig：全部环境变量的唯一解析点，仅 worker/main.py 调用一次并显式注入。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.observability import ObservabilitySettings
from kokoro_agent.sandbox import SandboxSettings
from kokoro_agent.storage.artifacts import ArtifactSettings
from kokoro_agent.storage.checkpoints import CheckpointSettings
from kokoro_agent.storage.ledger import DEFAULT_LEASE_TTL_S, LedgerSettings
from kokoro_agent.streams.factory import StreamSettings

DEFAULT_LEASE_HEARTBEAT_S = 30.0

_DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"
_DEFAULT_CHECKPOINT_DB = "kokoro_checkpoints.db"
_DEFAULT_LEDGER_DB = "kokoro_ledger.db"
_DEFAULT_MONGO_URL = "mongodb://127.0.0.1:27017"
_DEFAULT_MONGO_DB = "kokoro"
_DEFAULT_LOCAL_SHELL_TIMEOUT = 120
_DEFAULT_LOCAL_SHELL_MAX_OUTPUT_BYTES = 100000


class WebToolSettings(BaseModel):
    """web 底层工具政策：fetch 内网放行（本地开发）与 search provider（配置即挂载）。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    fetch_allow_private: bool
    search_provider: str | None
    search_api_key: SecretStr | None
    search_url: str | None


class AppConfig(BaseModel):
    """按域分组的进程配置：一次解析、全程注入。model/tools/skills/permissions 属每请求 wire。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    model: ChatModelSettings
    stream: StreamSettings
    observability: ObservabilitySettings
    checkpoint: CheckpointSettings
    ledger: LedgerSettings
    artifacts: ArtifactSettings
    sandbox: SandboxSettings
    web_tools: WebToolSettings
    custom_subagents_json: str | None
    # 内建子代理按名启用（默认全关；未知名 fail-loud）。
    enabled_builtin_subagents: frozenset[str]
    lease_heartbeat_s: Annotated[float, Field(gt=0)]
    # 失控熔断：单 run 图步数上限（无限工具循环 → GraphRecursionError → run.failed）。
    recursion_limit: Annotated[int, Field(gt=0)]
    # SIGTERM 优雅停机：限时等活跃 run 收尾（超时交 TTL 租约重拾）。
    drain_timeout_s: Annotated[float, Field(gt=0)]
    # run 级 token 预算（0=关闭）：跨 HITL 段累计，超限 run.failed(TokenBudgetExceeded)。
    run_token_budget: Annotated[int, Field(ge=0)]
    # retention（0=关，保留期限是产品决策不擅代）：终态后事件流存活期 / 终态 run 行清扫龄。
    retention_events_ttl_s: Annotated[int, Field(ge=0)]
    retention_run_ttl_s: Annotated[int, Field(ge=0)]
    # 会话 checkpoint 保留（0=关）：thread 最后活跃超龄即删（官方 adelete_thread）。
    retention_thread_ttl_s: Annotated[int, Field(ge=0)]

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> AppConfig:
        # 后端枚举经各 Settings 的 strict Literal 在构造期 fail-loud（model_validate 入口）。
        mongo_url = source.get("KOKORO_MONGO_URL", _DEFAULT_MONGO_URL)
        mongo_db = source.get("KOKORO_MONGO_DB", _DEFAULT_MONGO_DB)
        return cls(
            model=_model_from_env(source),
            stream=StreamSettings.model_validate(
                {
                    "backend": source.get("KOKORO_STREAM_BACKEND", "memory").lower(),
                    "redis_url": source.get("KOKORO_REDIS_URL", _DEFAULT_REDIS_URL),
                }
            ),
            observability=ObservabilitySettings(
                langfuse_public_key=_secret(source.get("LANGFUSE_PUBLIC_KEY")),
                langfuse_secret_key=_secret(source.get("LANGFUSE_SECRET_KEY")),
            ),
            checkpoint=CheckpointSettings.model_validate(
                {
                    "backend": source.get("KOKORO_CHECKPOINT_BACKEND", "sqlite").lower(),
                    "sqlite_path": source.get("KOKORO_CHECKPOINT_DB", _DEFAULT_CHECKPOINT_DB),
                    "mongo_url": mongo_url,
                    "mongo_db": mongo_db,
                }
            ),
            ledger=LedgerSettings.model_validate(
                {
                    "backend": source.get("KOKORO_LEDGER_BACKEND", "sqlite").lower(),
                    "sqlite_path": source.get("KOKORO_LEDGER_DB", _DEFAULT_LEDGER_DB),
                    "mongo_url": mongo_url,
                    "mongo_db": mongo_db,
                    "lease_ttl_ms": _int(source, "KOKORO_LEASE_TTL_S", DEFAULT_LEASE_TTL_S) * 1000,
                }
            ),
            artifacts=ArtifactSettings.model_validate(
                {
                    "backend": source.get("KOKORO_ARTIFACT_BACKEND", "dir").lower(),
                    "dir_root": source.get("KOKORO_ARTIFACTS_DIR", "./kokoro_artifacts"),
                    "mongo_url": mongo_url,
                    "mongo_db": mongo_db,
                }
            ),
            sandbox=SandboxSettings.model_validate(
                {
                    "local_shell_root": source.get("KOKORO_AGENT_LOCAL_SHELL_ROOT") or None,
                    "local_shell_inherit_env": source.get("KOKORO_AGENT_LOCAL_SHELL_INHERIT_ENV")
                    == "1",
                    "local_shell_timeout": _int(
                        source, "KOKORO_AGENT_LOCAL_SHELL_TIMEOUT", _DEFAULT_LOCAL_SHELL_TIMEOUT
                    ),
                    "local_shell_max_output_bytes": _int(
                        source,
                        "KOKORO_AGENT_LOCAL_SHELL_MAX_OUTPUT_BYTES",
                        _DEFAULT_LOCAL_SHELL_MAX_OUTPUT_BYTES,
                    ),
                }
            ),
            web_tools=WebToolSettings(
                fetch_allow_private=source.get("KOKORO_WEB_FETCH_ALLOW_PRIVATE") == "1",
                search_provider=source.get("KOKORO_WEB_SEARCH_PROVIDER") or None,
                search_api_key=_secret(source.get("KOKORO_WEB_SEARCH_API_KEY")),
                search_url=source.get("KOKORO_WEB_SEARCH_URL") or None,
            ),
            custom_subagents_json=source.get("KOKORO_CUSTOM_SUBAGENTS") or None,
            enabled_builtin_subagents=frozenset(
                name.strip()
                for name in source.get("KOKORO_BUILTIN_SUBAGENTS", "").split(",")
                if name.strip()
            ),
            lease_heartbeat_s=_float(source, "KOKORO_LEASE_HEARTBEAT_S", DEFAULT_LEASE_HEARTBEAT_S),
            recursion_limit=_int(source, "KOKORO_RECURSION_LIMIT", 100),
            drain_timeout_s=_float(source, "KOKORO_DRAIN_TIMEOUT_S", 60.0),
            run_token_budget=_int(source, "KOKORO_RUN_TOKEN_BUDGET", 0),
            retention_events_ttl_s=_int(source, "KOKORO_RETENTION_EVENTS_TTL_S", 0),
            retention_run_ttl_s=_int(source, "KOKORO_RETENTION_RUN_TTL_S", 0),
            retention_thread_ttl_s=_int(source, "KOKORO_RETENTION_THREAD_TTL_S", 0),
        )


def _model_from_env(source: Mapping[str, str]) -> ChatModelSettings:
    return ChatModelSettings(
        disable_streaming=source.get("KOKORO_DISABLE_STREAMING") == "1",
        local_fake=source.get("KOKORO_LOCAL_FAKE_MODEL") == "1",
        local_fake_script=source.get("KOKORO_LOCAL_FAKE_SCRIPT", "default"),
        openai_api_key=_secret(source.get("OPENAI_API_KEY")),
        openai_base_url=source.get("OPENAI_BASE_URL"),
        openai_reasoning=source.get("KOKORO_OPENAI_REASONING") == "1",
        anthropic_api_key=_secret(source.get("ANTHROPIC_API_KEY")),
        anthropic_base_url=source.get("ANTHROPIC_BASE_URL"),
    )


def _secret(raw: str | None) -> SecretStr | None:
    return SecretStr(raw) if raw else None


def _int(source: Mapping[str, str], key: str, default: int) -> int:
    raw = source.get(key)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def _float(source: Mapping[str, str], key: str, default: float) -> float:
    raw = source.get(key)
    if raw is None or not raw.strip():
        return default
    return float(raw)
