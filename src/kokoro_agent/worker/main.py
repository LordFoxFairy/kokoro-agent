"""进程入口（纯调度域装配）：env 一次解析 → 共享服务 → 注入 Supervisor.serve。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
import logging
import os
import signal
import socket

from dotenv import load_dotenv

from kokoro_agent.config import AppConfig, log_config_summary
from kokoro_agent.clients.system import ModelResolver, SystemModelClient
from kokoro_agent.clients.storage import DeliveryClient
from kokoro_agent.clients.storage_delivery import StorageDeliveryClient
from kokoro_agent.clients.storage_transport import StorageDeliveryTransport
from kokoro_agent.application.schema import (
    apply_database_schema,
    db_apply_schema_main as _schema_db_apply_schema_main,
)
from kokoro_agent.protocol import REQUESTS_STREAM
from kokoro_agent.metrics import start_metrics_server
from kokoro_agent.observability import trace_config
from kokoro_agent.agent_factory import AgentFactory
from kokoro_agent.execution.runtime_profile_plan import RuntimeAssemblyPolicy
from kokoro_agent.execution.runtime_profile_sources import production_manifest
from kokoro_agent.agents.native_profile import prepare_native_recipe
from kokoro_agent.worker.dependencies import WorkerClients, WorkerDependencies
from kokoro_agent.worker.platform import worker_platform_runtime
from kokoro_agent.domain.run.repository import SandboxBackendKind
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.sandbox import teardown_backend_for_run
from kokoro_agent.tools.toolbox import ProcessToolbox, build_toolbox
from kokoro_agent.tools.web_search import SearchProviderSettings
from kokoro_agent.infrastructure.checkpoints import make_checkpointer
from kokoro_agent.infrastructure.checkpoint_interactions import (
    make_interaction_checkpointer,
)
from kokoro_agent.infrastructure.memory_store import make_memory_store
from kokoro_agent.infrastructure.postgres_run_repository import make_run_repository
from kokoro_agent.streams.factory import make_stream
from kokoro_agent.mcp.config import load_mcp_servers
from kokoro_agent.mcp.egress import configure_egress_mode, egress_mode_from_env
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.worker.supervisor import RunSupervisor
from kokoro_agent.infrastructure.postgres_chat_repository import (
    PostgresChatRepositorySettings,
    make_chat_repository,
)

LOGGER = logging.getLogger(__name__)

__all__ = [
    "apply_database_schema",
    "db_apply_schema_main",
    "main",
    "serve",
]


def toolbox_from_config(config: AppConfig) -> ProcessToolbox:
    # env → 领域设置的唯一翻译点（config 单点消费法则）；构建逻辑在 tools/toolbox.py。
    search = (
        None
        if config.web_tools.search_provider is None
        else SearchProviderSettings(
            provider=config.web_tools.search_provider,
            api_key=config.web_tools.search_api_key,
            base_url=config.web_tools.search_url,
        )
    )
    return build_toolbox(
        fetch_allow_private=config.web_tools.fetch_allow_private, search=search
    )


def _sandbox_teardown(
    config: AppConfig,
) -> "Callable[[SandboxBackendKind, str, str], Awaitable[None]]":
    async def teardown(
        kind: SandboxBackendKind, sandbox_id: str, teardown_ref: str
    ) -> None:
        await teardown_backend_for_run(
            kind,
            config.sandbox,
            sandbox_id,
            teardown_ref=teardown_ref,
            strict=True,
        )

    return teardown


def _consumer_name() -> str:
    # consumer-group 内的子代理身份：主机+pid 保多 pod/多进程不撞名。
    return f"{socket.gethostname()}-{os.getpid()}"


@asynccontextmanager
async def worker_model_resolver(
    config: AppConfig, injected: ModelResolver | None
) -> AsyncGenerator[ModelResolver, None]:
    """Own the CLI HTTP pool; an explicitly injected resolver remains caller-owned."""
    if injected is not None:
        yield injected
        return
    if config.system_base_url is None or config.internal_secret_agent is None:
        raise ValueError(
            "System model resolver URL and service credential are required"
        )
    if (
        not config.litellm_enabled
        or config.litellm_base_url is None
        or config.litellm_api_key is None
    ):
        raise ValueError("System model routing requires a configured LiteLLM gateway")
    async with SystemModelClient(
        config.system_base_url,
        config.internal_secret_agent,
        timeout_s=config.system_timeout_s,
    ) as resolver:
        yield resolver


@asynccontextmanager
async def worker_storage_delivery(
    config: AppConfig,
    injected: DeliveryClient | None,
    run_repository: RunRepository,
) -> AsyncGenerator[DeliveryClient | None, None]:
    """Own one Storage transport per worker; embedded client remains caller-owned."""
    if injected is not None:
        yield injected
        return
    if (
        config.storage_base_url is None
        or config.storage_object_origin is None
        or config.storage_service_secret is None
    ):
        yield None
        return
    async with StorageDeliveryTransport(
        config.storage_base_url,
        config.storage_object_origin,
        config.storage_service_secret.get_secret_value(),
    ) as transport:
        yield StorageDeliveryClient(run_repository, transport)


async def serve(config: AppConfig, clients: WorkerClients | None = None) -> None:
    """Run one worker with deployment-selected public clients.

    The standard CLI constructs a configured System resolver. Embedded deployments may
    inject that same boundary plus optional Capability/Storage adapters.
    """
    # Validate local policy/plugin provenance before opening any owner resources.
    prepare_native_recipe()
    runtime_policy = RuntimeAssemblyPolicy.from_settings(
        run_token_budget=config.run_token_budget,
        recursion_limit=config.recursion_limit,
        model=config.model,
        sandbox=config.sandbox,
    )
    manifest = production_manifest()
    owner_clients = clients or WorkerClients()
    # egress is a worker-wide connection policy. Configure it from the already
    # validated AppConfig snapshot; the MCP connection layer never reads env.
    configure_egress_mode(
        egress_mode_from_env({"KOKORO_MCP_EGRESS_MODE": config.mcp_egress_mode})
    )
    # OBS-1 metrics 端点（缺省关）：显式配置端口才起，绝不阻断 worker 主职。
    if config.metrics_port is not None:
        start_metrics_server(config.metrics_port)
    bus = make_stream(config.stream)
    subagent_catalog = build_subagent_catalog(
        config.custom_subagents_json, config.enabled_builtin_subagents
    )
    # 进程级共享 checkpointer + run 状态存储：PostgreSQL 跨 pod 共享，去重/租约/终态认领/崩溃恢复皆赖之。
    async with (
        worker_platform_runtime(config) as platform,
        worker_model_resolver(config, owner_clients.model_resolver) as model_resolver,
        make_checkpointer(config.checkpoint) as saver,
        make_checkpointer(config.checkpoint) as interaction_reader,
        make_run_repository(config.run_repository) as run_repository,
        worker_storage_delivery(
            config, owner_clients.delivery, run_repository
        ) as delivery,
        make_memory_store(config.checkpoint) as memory_store,
        make_chat_repository(
            PostgresChatRepositorySettings(
                database_url=config.database_url,
                schema_name=config.database_schema,
            )
        ) as chat_repository,
    ):
        interaction_checkpointer = make_interaction_checkpointer(
            saver=saver, reader=interaction_reader, repository=run_repository
        )
        dependencies = WorkerDependencies(
            platform=platform,
            model=config.model,
            sandbox=config.sandbox,
            runtime_policy=runtime_policy,
            manifest=manifest,
            subagent_catalog=subagent_catalog,
            toolbox=toolbox_from_config(config),
            checkpointer=interaction_checkpointer,
            run_repository=run_repository,
            memory_store=memory_store,
            # 旧部署定义仍只作为显式 client 的输入；声明存在时 client 缺席/失败
            # 直接拒绝，不以 YAML 替代 Platform 授权。typed MCP cutover 后删除旧路径。
            mcp_servers=load_mcp_servers(config.mcp_config, os.environ),
            mcp_client=owner_clients.mcp,
            delivery=delivery,
            model_resolver=model_resolver,
        )
        agent_factory = AgentFactory(dependencies)
        supervisor = RunSupervisor(
            interaction_reader=interaction_checkpointer.read_interaction,
            agent_builder=agent_factory.build,
            run_repository=run_repository,
            approval_tool_names=agent_factory.approval_names,
            backend_for=agent_factory.backend_for,
            trace_factory=lambda request: trace_config(config.observability, request),
            source_for=subagent_catalog.source_for,
            feature_for=agent_factory.feature,
            consumer=_consumer_name(),
            heartbeat_s=config.lease_heartbeat_s,
            recursion_limit=runtime_policy.recursion_limit,
            events_ttl_s=config.retention_events_ttl_s,
            run_ttl_s=config.retention_run_ttl_s,
            outbox_republish_ms=config.outbox_republish_ms,
            sandbox_teardown=_sandbox_teardown(config),
            chat_repository=chat_repository,
        )
        LOGGER.info(
            "kokoro-agent worker consuming %s as %s", REQUESTS_STREAM, _consumer_name()
        )
        serve_task = asyncio.create_task(supervisor.serve(bus))
        loop = asyncio.get_running_loop()
        # SIGTERM 优雅停机：停止消费新请求，限时等活跃 run 收尾（超时交 TTL 租约重拾）。
        loop.add_signal_handler(signal.SIGTERM, serve_task.cancel)
        try:
            await serve_task
        except asyncio.CancelledError:
            drained = await supervisor.drain(timeout_s=config.drain_timeout_s)
            LOGGER.info("graceful shutdown: drained=%s", drained)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    config = AppConfig.from_env(os.environ)
    # 启动期配置快照（secret 掩码）：一眼看清本进程实际生效的配置，便于排障。
    log_config_summary(config, LOGGER)
    asyncio.run(serve(config))


def db_apply_schema_main() -> int:
    """Install the current schema as an explicit operator command."""

    return _schema_db_apply_schema_main()


if __name__ == "__main__":
    main()
