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

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, SecretStr

from kokoro_agent.config_file import load_config_file
from kokoro_agent.content_source import AssetSettings, LocalAssets, load_assets_config
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.observability import ObservabilitySettings
from kokoro_agent.sandbox import SandboxSettings, load_workspace_config
from kokoro_agent.storage.checkpoints import CheckpointSettings
from kokoro_agent.storage.ledger import DEFAULT_LEASE_TTL_S, LedgerSettings
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
    disable_streaming: bool = Field(default=False, validation_alias="KOKORO_DISABLE_STREAMING")
    local_fake: bool = Field(default=False, validation_alias="KOKORO_LOCAL_FAKE_MODEL")
    local_fake_script: str = Field(default="default", validation_alias="KOKORO_LOCAL_FAKE_SCRIPT")
    openai_api_key: OptSecret = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_base_url: OptStr = Field(default=None, validation_alias="OPENAI_BASE_URL")
    openai_reasoning: bool = Field(default=False, validation_alias="KOKORO_OPENAI_REASONING")
    anthropic_api_key: OptSecret = Field(default=None, validation_alias="ANTHROPIC_API_KEY")
    anthropic_base_url: OptStr = Field(default=None, validation_alias="ANTHROPIC_BASE_URL")
    # litellm 网关档：agent 只持网关地址与网关 key（不存任何底层 provider 凭据）。
    litellm_base_url: OptStr = Field(default=None, validation_alias="KOKORO_LITELLM_BASE_URL")
    litellm_api_key: OptSecret = Field(default=None, validation_alias="KOKORO_LITELLM_API_KEY")

    # --- stream / mongo 域（mongo 为 checkpoint+ledger 共用真后端）---
    redis_url: str = Field(
        default="redis://127.0.0.1:6379/0", validation_alias="KOKORO_REDIS_URL"
    )
    mongo_url: str = Field(default="mongodb://127.0.0.1:27017", validation_alias="KOKORO_MONGO_URL")
    mongo_db: str = Field(default="kokoro", validation_alias="KOKORO_MONGO_DB")

    # --- observability 域 ---
    langfuse_public_key: OptSecret = Field(default=None, validation_alias="LANGFUSE_PUBLIC_KEY")
    langfuse_secret_key: OptSecret = Field(default=None, validation_alias="LANGFUSE_SECRET_KEY")

    # --- ledger 域（lease_ttl_s → lease_ttl_ms×1000；gt=0 使 0 值构造期 fail-loud）---
    lease_ttl_s: int = Field(default=DEFAULT_LEASE_TTL_S, gt=0, validation_alias="KOKORO_LEASE_TTL_S")

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

    # MCP server 部署注册表 yaml：wire 只传 names，定义在此解析；headers 值 ${ENV} 占位。
    mcp_config: OptStr = Field(default=None, validation_alias="KOKORO_MCP_CONFIG")
    # MCP 凭据句柄解析出口（hub runtime resolve）与连接期 egress 防线（mcp/egress.py）。
    # 声明于此供启动期配置快照可见（log_config_summary，secret 掩码）；功能消费在 mcp 层——
    # make_mcp_registry / build_connections 读同一注入 env（写区不含 worker/main，暂不经此穿透）。
    hub_base_url: OptStr = Field(default=None, validation_alias="KOKORO_HUB_BASE_URL")
    internal_secret_agent: OptSecret = Field(
        default=None, validation_alias="KOKORO_INTERNAL_SECRET_AGENT"
    )
    mcp_egress_mode: str = Field(default="strict", validation_alias="KOKORO_MCP_EGRESS_MODE")

    # --- web_tools 域 ---
    fetch_allow_private: bool = Field(
        default=False, validation_alias="KOKORO_WEB_FETCH_ALLOW_PRIVATE"
    )
    search_provider: OptStr = Field(default=None, validation_alias="KOKORO_WEB_SEARCH_PROVIDER")
    search_api_key: OptSecret = Field(default=None, validation_alias="KOKORO_WEB_SEARCH_API_KEY")
    search_url: OptStr = Field(default=None, validation_alias="KOKORO_WEB_SEARCH_URL")

    # --- assets 域（skills/prompts 从哪来）：local 目录或 s3，配置引用名称、资产统一入库。---
    assets_config: OptStr = Field(default=None, validation_alias="KOKORO_ASSETS_CONFIG")
    skills_dir: OptStr = Field(default=None, validation_alias="KOKORO_SKILLS_DIR")
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

    # --- retention 域（0=关，保留期限是产品决策不擅代）---
    retention_events_ttl_s: int = Field(
        default=0, ge=0, validation_alias="KOKORO_RETENTION_EVENTS_TTL_S"
    )
    retention_run_ttl_s: int = Field(
        default=0, ge=0, validation_alias="KOKORO_RETENTION_RUN_TTL_S"
    )

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> AppConfig:
        # 统一配置树（ADR-010）：yaml 摊平（原生值）作底座，env 叠加覆盖；一次 coerce+校验。
        file_layer = load_config_file(source.get("KOKORO_AGENT_CONFIG"))
        merged: dict[str, object] = {**file_layer, **dict(source)}
        return cls.model_validate(merged)

    # --- 域视图（消费方 API 不变）：由扁平叶子装配既有子 Settings 类型 ---

    @property
    def model(self) -> ChatModelSettings:
        return ChatModelSettings(
            disable_streaming=self.disable_streaming,
            local_fake=self.local_fake,
            local_fake_script=self.local_fake_script,
            openai_api_key=self.openai_api_key,
            openai_base_url=self.openai_base_url,
            openai_reasoning=self.openai_reasoning,
            anthropic_api_key=self.anthropic_api_key,
            anthropic_base_url=self.anthropic_base_url,
            litellm_base_url=self.litellm_base_url,
            litellm_api_key=self.litellm_api_key,
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
                skills_dir=self.skills_dir,
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
