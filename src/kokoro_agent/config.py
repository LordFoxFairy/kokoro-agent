"""AppConfig：全部环境变量的唯一解析点，仅 worker/main.py 调用一次并显式注入。

设计：一个扁平 pydantic 模型，每个叶子字段声明一次（类型 + 默认值 + `validation_alias`
即 env 名），env 字符串由 pydantic lax 原生 coerce（"1"→True、"90"→90），不再手写
`_int/_float/_stringify`。域分组只对消费方保留——经 `@property` 装配既有的各子 Settings 类型。

优先级 env > yaml > 默认：`from_env` 先摊平 yaml 配置树（config_file，原生值）作底座，
再叠加注入的 env 映射（env 覆盖），交 `model_validate` 一次性 coerce 与校验。
凭据只走 env/secret（yaml 内写凭据在 config_file 阶段 fail-loud）。
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Annotated
from urllib.parse import urlsplit

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, SecretStr, field_validator

from kokoro_agent.config_file import load_config_file
from kokoro_agent.content_source import AssetSettings, LocalAssets, load_assets_config
from kokoro_agent.hub import HubRuntimeSettings
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.observability import ObservabilitySettings
from kokoro_agent.platform import MediaRuntimeSettings
from kokoro_agent.readiness import (
    MongoReadinessSettings,
    MtlsRpcReadinessSettings,
    ProcessReadinessSettings,
    RedisReadinessSettings,
)
from kokoro_agent.sandbox import SandboxSettings, load_workspace_config
from kokoro_agent.storage.checkpoints import CheckpointSettings
from kokoro_agent.storage.ledger import (
    DEFAULT_LEASE_TTL_S,
    DURABLE_OUTPUT_RETENTION_REQUIRES_CONSUMER_ACK,
    LedgerSettings,
)
from kokoro_agent.streams.factory import StreamSettings


def _blank_to_none(value: object) -> object:
    """空串归 None：env 里 `KEY=`（空值）与"未设"同义，等价旧代码的 `or None` / `_secret`。"""
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


# 可空叶子的复用别名：空串→None（before），再交目标类型 coerce。
OptStr = Annotated[str | None, BeforeValidator(_blank_to_none)]
OptSecret = Annotated[SecretStr | None, BeforeValidator(_blank_to_none)]


class WebToolSettings(BaseModel):
    """web 底层工具政策：fetch 内网放行（本地开发）与 search provider（配置即挂载）。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    fetch_allow_private: bool
    search_provider: str | None
    search_api_key: SecretStr | None
    search_url: str | None


class AppConfig(BaseModel):
    """扁平进程配置：每叶子经 `validation_alias` 从 env/yaml 底座解析一次；
    域分组经下方 `@property` 装配子 Settings 供消费方（消费 API 不变）。
    model/tools/skills/permissions 属每请求 wire，不在此。"""

    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    # --- model 域 ---
    local_fake: bool = Field(default=False, validation_alias="KOKORO_LOCAL_FAKE_MODEL")
    local_fake_script: str = Field(default="default", validation_alias="KOKORO_LOCAL_FAKE_SCRIPT")
    model_gateway_url: OptStr = Field(default=None, validation_alias="KOKORO_MODEL_GATEWAY_URL")
    model_gateway_ca_file: OptStr = Field(
        default=None, validation_alias="KOKORO_MODEL_GATEWAY_CA_FILE"
    )
    model_gateway_cert_file: OptStr = Field(
        default=None, validation_alias="KOKORO_MODEL_GATEWAY_CERT_FILE"
    )
    model_gateway_key_file: OptStr = Field(
        default=None, validation_alias="KOKORO_MODEL_GATEWAY_KEY_FILE"
    )
    model_gateway_timeout_ms: int = Field(
        default=120_000, ge=100, le=120_000, validation_alias="KOKORO_MODEL_GATEWAY_TIMEOUT_MS"
    )
    model_gateway_max_output_tokens: int = Field(
        default=65_536,
        ge=1,
        le=1_000_000,
        validation_alias="KOKORO_MODEL_GATEWAY_MAX_OUTPUT_TOKENS",
    )

    # --- Platform Media Runtime (optional process transport) ---
    media_rpc_url: OptStr = Field(default=None, validation_alias="KOKORO_MEDIA_RPC_URL")
    media_rpc_ca_file: OptStr = Field(default=None, validation_alias="KOKORO_MEDIA_RPC_CA_FILE")
    media_rpc_cert_file: OptStr = Field(
        default=None, validation_alias="KOKORO_MEDIA_RPC_CERT_FILE"
    )
    media_rpc_key_file: OptStr = Field(default=None, validation_alias="KOKORO_MEDIA_RPC_KEY_FILE")
    media_rpc_timeout_ms: int = Field(
        default=30_000, ge=100, le=30_000, validation_alias="KOKORO_MEDIA_RPC_TIMEOUT_MS"
    )

    # --- stream / mongo 域（mongo 为 checkpoint+ledger 共用真后端）---
    redis_url: str = Field(
        default="redis://127.0.0.1:6379/0", validation_alias="KOKORO_REDIS_URL"
    )
    mongo_url: str = Field(
        default="mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true",
        validation_alias="KOKORO_MONGO_URL",
    )
    mongo_db: str = Field(default="kokoro", validation_alias="KOKORO_MONGO_DB")

    # --- observability 域 ---
    langfuse_public_key: OptSecret = Field(default=None, validation_alias="LANGFUSE_PUBLIC_KEY")
    langfuse_secret_key: OptSecret = Field(default=None, validation_alias="LANGFUSE_SECRET_KEY")

    # --- ledger 域（lease_ttl_s → lease_ttl_ms×1000；gt=0 使 0 值构造期 fail-loud）---
    lease_ttl_s: int = Field(default=DEFAULT_LEASE_TTL_S, gt=0, validation_alias="KOKORO_LEASE_TTL_S")
    producer_generation: int = Field(
        default=1, gt=0, validation_alias="KOKORO_AGENT_PRODUCER_GENERATION"
    )

    # --- Agent-owned execution-evidence ConnectRPC provider ---
    evidence_host: str = Field(
        default="0.0.0.0", validation_alias="KOKORO_AGENT_EVIDENCE_HOST"
    )
    evidence_port: int = Field(
        default=8443, ge=1, le=65535, validation_alias="KOKORO_AGENT_EVIDENCE_PORT"
    )
    evidence_tls_cert: OptStr = Field(
        default=None, validation_alias="KOKORO_AGENT_EVIDENCE_TLS_CERT"
    )
    evidence_tls_key: OptStr = Field(
        default=None, validation_alias="KOKORO_AGENT_EVIDENCE_TLS_KEY"
    )
    evidence_caller_ca_bundle: OptStr = Field(
        default=None, validation_alias="KOKORO_AGENT_EVIDENCE_CALLER_CA_BUNDLE"
    )
    evidence_allowed_callers: str = Field(
        default="kokoro-session,kokoro-platform",
        validation_alias="KOKORO_AGENT_EVIDENCE_ALLOWED_CALLERS",
    )

    # --- Agent-owned Presentation ConnectRPC provider ---
    presentation_host: str = Field(
        default="0.0.0.0", validation_alias="KOKORO_AGENT_PRESENTATION_HOST"
    )
    presentation_port: int = Field(
        default=8444,
        ge=1,
        le=65535,
        validation_alias="KOKORO_AGENT_PRESENTATION_PORT",
    )
    presentation_tls_cert: OptStr = Field(
        default=None, validation_alias="KOKORO_AGENT_PRESENTATION_TLS_CERT"
    )
    presentation_tls_key: OptStr = Field(
        default=None, validation_alias="KOKORO_AGENT_PRESENTATION_TLS_KEY"
    )
    presentation_caller_ca_bundle: OptStr = Field(
        default=None,
        validation_alias="KOKORO_AGENT_PRESENTATION_CALLER_CA_BUNDLE",
    )
    presentation_allowed_callers: str = Field(
        default="kokoro-session",
        validation_alias="KOKORO_AGENT_PRESENTATION_ALLOWED_CALLERS",
    )

    # --- independent exec readiness commands ---
    readiness_timeout_ms: int = Field(
        default=2_000,
        ge=100,
        le=30_000,
        validation_alias="KOKORO_AGENT_READINESS_TIMEOUT_MS",
    )
    evidence_readiness_url: OptStr = Field(
        default=None, validation_alias="KOKORO_AGENT_EVIDENCE_READINESS_URL"
    )
    evidence_readiness_ca_file: OptStr = Field(
        default=None,
        validation_alias="KOKORO_AGENT_EVIDENCE_READINESS_CA_FILE",
    )
    evidence_readiness_cert_file: OptStr = Field(
        default=None,
        validation_alias="KOKORO_AGENT_EVIDENCE_READINESS_CERT_FILE",
    )
    evidence_readiness_key_file: OptStr = Field(
        default=None,
        validation_alias="KOKORO_AGENT_EVIDENCE_READINESS_KEY_FILE",
    )
    presentation_readiness_url: OptStr = Field(
        default=None,
        validation_alias="KOKORO_AGENT_PRESENTATION_READINESS_URL",
    )
    presentation_readiness_ca_file: OptStr = Field(
        default=None,
        validation_alias="KOKORO_AGENT_PRESENTATION_READINESS_CA_FILE",
    )
    presentation_readiness_cert_file: OptStr = Field(
        default=None,
        validation_alias="KOKORO_AGENT_PRESENTATION_READINESS_CERT_FILE",
    )
    presentation_readiness_key_file: OptStr = Field(
        default=None,
        validation_alias="KOKORO_AGENT_PRESENTATION_READINESS_KEY_FILE",
    )

    # --- sandbox 域 ---
    local_shell_root: OptStr = Field(default=None, validation_alias="KOKORO_AGENT_LOCAL_SHELL_ROOT")
    local_shell_inherit_env: bool = Field(
        default=False, validation_alias="KOKORO_AGENT_LOCAL_SHELL_INHERIT_ENV"
    )
    local_shell_timeout: int = Field(
        default=120, gt=0, validation_alias="KOKORO_AGENT_LOCAL_SHELL_TIMEOUT"
    )
    local_shell_max_output_bytes: int = Field(
        default=100000, gt=0, validation_alias="KOKORO_AGENT_LOCAL_SHELL_MAX_OUTPUT_BYTES"
    )
    # 存储形态 yaml（ADR-009）：与 session 读同一文件；缺省=local 默认档。
    workspace_config: OptStr = Field(default=None, validation_alias="KOKORO_WORKSPACE_CONFIG")
    workspace_s3_access_key: OptSecret = Field(
        default=None, validation_alias="KOKORO_WORKSPACE_S3_ACCESS_KEY"
    )
    workspace_s3_secret_key: OptSecret = Field(
        default=None, validation_alias="KOKORO_WORKSPACE_S3_SECRET_KEY"
    )
    # 沙箱空闲存活期：覆盖常规 HITL 等人窗口；超期回收，文件面由宿主/归档兜底。
    e2b_api_key: OptSecret = Field(default=None, validation_alias="KOKORO_E2B_API_KEY")
    e2b_template: OptStr = Field(default=None, validation_alias="KOKORO_E2B_TEMPLATE")
    e2b_timeout: int = Field(default=1800, gt=0, validation_alias="KOKORO_E2B_TIMEOUT")
    docker_image: OptStr = Field(default=None, validation_alias="KOKORO_DOCKER_IMAGE")
    docker_ttl: int = Field(default=1800, gt=0, validation_alias="KOKORO_DOCKER_TTL")
    custom_backend_ref: OptStr = Field(default=None, validation_alias="KOKORO_CUSTOM_BACKEND")
    custom_backend_config: OptStr = Field(
        default=None, validation_alias="KOKORO_CUSTOM_BACKEND_CONFIG"
    )

    # Agent-only Hub runtime boundary is mandatory: no HTTP/shared-secret/local-registry fallback.
    hub_rpc_url: OptStr = Field(default=None, validation_alias="KOKORO_HUB_RPC_URL")
    hub_rpc_server_name: OptStr = Field(
        default=None, validation_alias="KOKORO_HUB_RPC_SERVER_NAME"
    )
    hub_rpc_ca_file: OptStr = Field(default=None, validation_alias="KOKORO_HUB_RPC_CA_FILE")
    hub_rpc_cert_file: OptStr = Field(default=None, validation_alias="KOKORO_HUB_RPC_CERT_FILE")
    hub_rpc_key_file: OptStr = Field(default=None, validation_alias="KOKORO_HUB_RPC_KEY_FILE")
    hub_artifact_cache_dir: str = Field(
        default="/tmp/kokoro-agent/hub-artifacts",
        validation_alias="KOKORO_HUB_ARTIFACT_CACHE_DIR",
    )
    hub_rpc_timeout_ms: int = Field(
        default=30_000, ge=100, le=30_000, validation_alias="KOKORO_HUB_RPC_TIMEOUT_MS"
    )
    mcp_egress_mode: str = Field(default="strict", validation_alias="KOKORO_MCP_EGRESS_MODE")

    # --- web_tools 域 ---
    fetch_allow_private: bool = Field(
        default=False, validation_alias="KOKORO_WEB_FETCH_ALLOW_PRIVATE"
    )
    search_provider: OptStr = Field(default=None, validation_alias="KOKORO_WEB_SEARCH_PROVIDER")
    search_api_key: OptSecret = Field(default=None, validation_alias="KOKORO_WEB_SEARCH_API_KEY")
    search_url: OptStr = Field(default=None, validation_alias="KOKORO_WEB_SEARCH_URL")

    # --- deployment personas: local directory or s3 prefix ---
    assets_config: OptStr = Field(default=None, validation_alias="KOKORO_ASSETS_CONFIG")
    personas_dir: OptStr = Field(default=None, validation_alias="KOKORO_PERSONAS_DIR")
    assets_s3_access_key: OptSecret = Field(
        default=None, validation_alias="KOKORO_ASSETS_S3_ACCESS_KEY"
    )
    assets_s3_secret_key: OptSecret = Field(
        default=None, validation_alias="KOKORO_ASSETS_S3_SECRET_KEY"
    )

    # --- subagents 域 ---
    custom_subagents_json: OptStr = Field(default=None, validation_alias="KOKORO_CUSTOM_SUBAGENTS")
    # 内建子代理按名启用（env=CSV 串 / yaml=串列表；默认全关，未知名由 build_catalog fail-loud）。
    builtin_subagents_raw: str | list[str] | None = Field(
        default=None, validation_alias="KOKORO_BUILTIN_SUBAGENTS"
    )

    # --- limits 域 ---
    lease_heartbeat_s: float = Field(
        default=30.0, gt=0, validation_alias="KOKORO_LEASE_HEARTBEAT_S"
    )
    # 失控熔断：单 run 图步数上限（无限工具循环 → GraphRecursionError → run.failed）。
    recursion_limit: int = Field(default=100, gt=0, validation_alias="KOKORO_RECURSION_LIMIT")
    # SIGTERM 优雅停机：限时等活跃 run 收尾（超时交 TTL 租约重拾）。
    drain_timeout_s: float = Field(default=60.0, gt=0, validation_alias="KOKORO_DRAIN_TIMEOUT_S")
    # run 级 token 预算（0=关闭）：跨 HITL 段累计，超限 run.failed(TokenBudgetExceeded)。
    run_token_budget: int = Field(default=0, ge=0, validation_alias="KOKORO_RUN_TOKEN_BUDGET")
    # R4：critical 帧 published 后回执一直不来（events 流被修剪/丢失）→超此宽限期重发（复用固定身份）。
    outbox_republish_ms: int = Field(
        default=30_000, gt=0, validation_alias="KOKORO_OUTBOX_REPUBLISH_MS"
    )
    # OBS-1 metrics 端点端口（None=关，缺省关：worker 无常驻 HTTP 面，仅 e2e/部署显式开）。
    metrics_port: int | None = Field(
        default=None, ge=1, le=65535, validation_alias="KOKORO_AGENT_METRICS_PORT"
    )

    # --- retention 域 ---
    retention_events_ttl_s: int = Field(
        default=0, ge=0, validation_alias="KOKORO_RETENTION_EVENTS_TTL_S"
    )
    # Durable output/evidence destruction stays disabled until a consumer ACK/tombstone
    # contract exists. Both config parsing and supervisor construction enforce zero.
    retention_run_ttl_s: int = Field(
        default=0, ge=0, validation_alias="KOKORO_RETENTION_RUN_TTL_S"
    )

    @field_validator("retention_run_ttl_s")
    @classmethod
    def _reject_destructive_durable_output_retention(cls, value: int) -> int:
        if value != 0:
            raise ValueError(DURABLE_OUTPUT_RETENTION_REQUIRES_CONSUMER_ACK)
        return value

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> AppConfig:
        # Historical pre-wire runtime configuration. AppConfig otherwise ignores unknown keys,
        # which would make a stale production deployment look healthy while memory is absent.
        legacy_memory = source.get("KOKORO_AGENT_MEMORY")
        if legacy_memory is not None and legacy_memory.strip():
            raise ValueError(
                "LEGACY_STORE_MEMORY_CONFIG_DISABLED: KOKORO_AGENT_MEMORY was retired by "
                "ADR-013 M0; Product Memory is Platform-owned"
            )
        # 统一配置树（ADR-010）：yaml 摊平（原生值）作底座，env 叠加覆盖；一次 coerce+校验。
        file_layer = load_config_file(source.get("KOKORO_AGENT_CONFIG"))
        merged: dict[str, object] = {**file_layer, **dict(source)}
        return cls.model_validate(merged)

    # --- 域视图（消费方 API 不变）：由扁平叶子装配既有子 Settings 类型 ---

    @property
    def model(self) -> ChatModelSettings:
        return ChatModelSettings(
            local_fake=self.local_fake,
            local_fake_script=self.local_fake_script,
            model_gateway_url=self.model_gateway_url,
            model_gateway_ca_file=self.model_gateway_ca_file,
            model_gateway_cert_file=self.model_gateway_cert_file,
            model_gateway_key_file=self.model_gateway_key_file,
            model_gateway_timeout_ms=self.model_gateway_timeout_ms,
            model_gateway_max_output_tokens=self.model_gateway_max_output_tokens,
            producer_generation=self.producer_generation,
        )

    @property
    def stream(self) -> StreamSettings:
        return StreamSettings(redis_url=self.redis_url)

    @property
    def observability(self) -> ObservabilitySettings:
        return ObservabilitySettings(
            langfuse_public_key=self.langfuse_public_key,
            langfuse_secret_key=self.langfuse_secret_key,
        )

    @property
    def checkpoint(self) -> CheckpointSettings:
        return CheckpointSettings(mongo_url=self.mongo_url, mongo_db=self.mongo_db)

    @property
    def ledger(self) -> LedgerSettings:
        return LedgerSettings(
            mongo_url=self.mongo_url,
            mongo_db=self.mongo_db,
            lease_ttl_ms=self.lease_ttl_s * 1000,
            producer_generation=self.producer_generation,
        )

    @property
    def sandbox(self) -> SandboxSettings:
        return SandboxSettings.model_validate(
            {
                "local_shell_root": self.local_shell_root,
                "local_shell_inherit_env": self.local_shell_inherit_env,
                "local_shell_timeout": self.local_shell_timeout,
                "local_shell_max_output_bytes": self.local_shell_max_output_bytes,
                "workspace": load_workspace_config(self.workspace_config),
                "workspace_s3_access_key": self.workspace_s3_access_key,
                "workspace_s3_secret_key": self.workspace_s3_secret_key,
                "e2b": {
                    "api_key": self.e2b_api_key,
                    "template": self.e2b_template,
                    "timeout": self.e2b_timeout,
                },
                "docker": {"image": self.docker_image, "ttl": self.docker_ttl},
                "custom": {
                    "factory_ref": self.custom_backend_ref,
                    "config_path": self.custom_backend_config,
                },
            }
        )

    @property
    def hub_runtime(self) -> HubRuntimeSettings:
        rpc_url = self.hub_rpc_url
        server_name = self.hub_rpc_server_name
        ca_file = self.hub_rpc_ca_file
        cert_file = self.hub_rpc_cert_file
        key_file = self.hub_rpc_key_file
        if (
            rpc_url is None
            or server_name is None
            or ca_file is None
            or cert_file is None
            or key_file is None
        ):
            raise ValueError("HUB_RUNTIME_MTLS_CONFIGURATION_REQUIRED")
        return HubRuntimeSettings(
            rpc_url=rpc_url,
            server_name=server_name,
            ca_file=ca_file,
            cert_file=cert_file,
            key_file=key_file,
            artifact_cache_dir=self.hub_artifact_cache_dir,
            timeout_ms=self.hub_rpc_timeout_ms,
        )

    @property
    def media_runtime(self) -> MediaRuntimeSettings | None:
        values = (
            self.media_rpc_url,
            self.media_rpc_ca_file,
            self.media_rpc_cert_file,
            self.media_rpc_key_file,
        )
        if all(value is None for value in values):
            return None
        if any(value is None for value in values):
            raise ValueError("MEDIA_RUNTIME_MTLS_CONFIGURATION_INCOMPLETE")
        rpc_url, ca_file, cert_file, key_file = values
        assert rpc_url is not None
        assert ca_file is not None
        assert cert_file is not None
        assert key_file is not None
        return MediaRuntimeSettings(
            rpc_url=rpc_url,
            ca_file=ca_file,
            cert_file=cert_file,
            key_file=key_file,
            timeout_ms=self.media_rpc_timeout_ms,
        )

    @property
    def worker_readiness(self) -> ProcessReadinessSettings:
        model_values = (
            self.model_gateway_url,
            self.model_gateway_ca_file,
            self.model_gateway_cert_file,
            self.model_gateway_key_file,
        )
        hub_values = (
            self.hub_rpc_url,
            self.hub_rpc_ca_file,
            self.hub_rpc_cert_file,
            self.hub_rpc_key_file,
        )
        if self.local_fake or any(value is None for value in (*model_values, *hub_values)):
            raise ValueError("READINESS_WORKER_CONFIGURATION_REQUIRED")
        model_url, model_ca, model_cert, model_key = model_values
        hub_url, hub_ca, hub_cert, hub_key = hub_values
        assert model_url is not None and model_ca is not None
        assert model_cert is not None and model_key is not None
        assert hub_url is not None and hub_ca is not None
        assert hub_cert is not None and hub_key is not None
        hub_host = urlsplit(hub_url).hostname
        if (
            self.hub_rpc_server_name is None
            or hub_host is None
            or hub_host.casefold() != self.hub_rpc_server_name.casefold()
        ):
            raise ValueError("READINESS_WORKER_CONFIGURATION_REQUIRED")
        return ProcessReadinessSettings(
            role="worker",
            timeout_ms=self.readiness_timeout_ms,
            mongo=MongoReadinessSettings(
                url=self.mongo_url,
                database=self.mongo_db,
                timeout_ms=self.readiness_timeout_ms,
            ),
            redis=RedisReadinessSettings(
                url=self.redis_url, timeout_ms=self.readiness_timeout_ms
            ),
            hub=MtlsRpcReadinessSettings(
                url=hub_url,
                ca_file=hub_ca,
                cert_file=hub_cert,
                key_file=hub_key,
                timeout_ms=self.readiness_timeout_ms,
            ),
            model_gateway=MtlsRpcReadinessSettings(
                url=model_url,
                ca_file=model_ca,
                cert_file=model_cert,
                key_file=model_key,
                timeout_ms=self.readiness_timeout_ms,
            ),
        )

    def _provider_readiness(
        self,
        *,
        role: str,
        url: str | None,
        ca_file: str | None,
        cert_file: str | None,
        key_file: str | None,
    ) -> ProcessReadinessSettings:
        if (
            url is None
            or ca_file is None
            or cert_file is None
            or key_file is None
        ):
            raise ValueError(f"READINESS_{role.upper()}_CONFIGURATION_REQUIRED")
        if role not in ("evidence", "presentation"):
            raise AssertionError("invalid provider readiness role")
        return ProcessReadinessSettings(
            role=role,
            timeout_ms=self.readiness_timeout_ms,
            mongo=MongoReadinessSettings(
                url=self.mongo_url,
                database=self.mongo_db,
                timeout_ms=self.readiness_timeout_ms,
            ),
            listener=MtlsRpcReadinessSettings(
                url=url,
                ca_file=ca_file,
                cert_file=cert_file,
                key_file=key_file,
                timeout_ms=self.readiness_timeout_ms,
            ),
        )

    @property
    def evidence_readiness(self) -> ProcessReadinessSettings:
        return self._provider_readiness(
            role="evidence",
            url=self.evidence_readiness_url,
            ca_file=self.evidence_readiness_ca_file,
            cert_file=self.evidence_readiness_cert_file,
            key_file=self.evidence_readiness_key_file,
        )

    @property
    def presentation_readiness(self) -> ProcessReadinessSettings:
        return self._provider_readiness(
            role="presentation",
            url=self.presentation_readiness_url,
            ca_file=self.presentation_readiness_ca_file,
            cert_file=self.presentation_readiness_cert_file,
            key_file=self.presentation_readiness_key_file,
        )

    @property
    def web_tools(self) -> WebToolSettings:
        return WebToolSettings(
            fetch_allow_private=self.fetch_allow_private,
            search_provider=self.search_provider,
            search_api_key=self.search_api_key,
            search_url=self.search_url,
        )

    @property
    def assets(self) -> AssetSettings:
        # 资产源 yaml（与 workspace 同心智）优先；缺省=env 目录局部档（零配置可用）。
        return AssetSettings(
            source=load_assets_config(self.assets_config)
            or LocalAssets(
                type="local",
                personas_dir=self.personas_dir,
            ),
            s3_access_key=self.assets_s3_access_key,
            s3_secret_key=self.assets_s3_secret_key,
        )

    @property
    def enabled_builtin_subagents(self) -> frozenset[str]:
        raw = self.builtin_subagents_raw
        if raw is None:
            return frozenset()
        items = raw.split(",") if isinstance(raw, str) else raw
        return frozenset(name.strip() for name in items if name.strip())


def _mask_secret(value: object) -> object:
    """secret 只留头尾（绝不打全值）：短值整体打码，长值 head…tail。"""
    if isinstance(value, SecretStr):
        raw = value.get_secret_value()
        if not raw:
            return None
        if len(raw) <= 8:
            return "****"
        return f"{raw[:3]}…{raw[-2:]}"
    return value


def log_config_summary(config: AppConfig, logger: logging.Logger) -> None:
    """启动期打印扁平配置快照（仅叶子字段，不触发域视图装配），secret 掩码。"""
    summary = {
        name: _mask_secret(getattr(config, name)) for name in type(config).model_fields
    }
    logger.info("kokoro-agent config: %s", summary)
