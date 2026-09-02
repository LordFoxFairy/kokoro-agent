"""进程入口（纯调度域装配）：env 一次解析 → 共享服务 → 注入 Supervisor.serve。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import logging
import os
import signal
import socket

from dotenv import load_dotenv

from kokoro_agent.config import AppConfig, log_config_summary
from kokoro_agent.contract import REQUESTS_STREAM
from kokoro_agent.metrics import start_metrics_server
from kokoro_agent.observability import trace_config
from kokoro_agent.agent_factory import AgentFactory
from kokoro_agent.worker.dependencies import WorkerClients, WorkerDependencies
from kokoro_agent.policy import Backend
from kokoro_agent.sandbox import teardown_backend_for_run
from kokoro_agent.tools.toolbox import ProcessToolbox, build_toolbox
from kokoro_agent.tools.web_search import SearchProviderSettings
from kokoro_agent.infrastructure.checkpoints import make_checkpointer
from kokoro_agent.infrastructure.memory_store import make_memory_store
from kokoro_agent.infrastructure.postgres_run_repository import make_run_repository
from kokoro_agent.streams.factory import make_stream
from kokoro_agent.mcp.config import load_mcp_servers
from kokoro_agent.mcp.egress import configure_egress_mode, egress_mode_from_env
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.worker.supervisor import RunSupervisor
from kokoro_agent.chat.store import ChatStoreSettings, make_chat_store
from kokoro_agent.http.server import create_http_server

LOGGER = logging.getLogger(__name__)


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
    return build_toolbox(fetch_allow_private=config.web_tools.fetch_allow_private, search=search)


def _sandbox_teardown(config: AppConfig) -> "Callable[[Backend, str | None], Awaitable[None]]":
    async def teardown(kind: Backend, sandbox_id: str | None) -> None:
        await teardown_backend_for_run(kind, config.sandbox, sandbox_id)

    return teardown


def _consumer_name() -> str:
    # consumer-group 内的子代理身份：主机+pid 保多 pod/多进程不撞名。
    return f"{socket.gethostname()}-{os.getpid()}"


async def serve(config: AppConfig, clients: WorkerClients | None = None) -> None:
    """Run one worker with deployment-selected public clients.

    The standard CLI supplies none. Embedded deployments may inject Capability/Storage
    adapters here without changing Agent, Feature or Run request APIs.
    """
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
        make_checkpointer(config.checkpoint) as saver,
        make_run_repository(config.run_repository) as store,
        make_memory_store(config.checkpoint) as memory_store,
        make_chat_store(
            ChatStoreSettings(
                database_url=config.database_url,
                schema_name=config.database_schema,
            )
        ) as chat_store,
    ):
        dependencies = WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            run_token_budget=config.run_token_budget,
            subagent_catalog=subagent_catalog,
            toolbox=toolbox_from_config(config),
            checkpointer=saver,
            run_repository=store,
            memory_store=memory_store,
            skill_client=owner_clients.skill_client,
            skill_reader=owner_clients.skill_reader,
            # MCP server 定义双源：部署 yaml 启动即加载校验（含 ${ENV} 凭据展开，fail-loud）
            # 为部署基线；注册表在装配期按本次 Run 快照合并覆盖。
            mcp_servers=load_mcp_servers(config.mcp_config, os.environ),
            mcp_client=owner_clients.mcp,
            delivery=owner_clients.delivery,
        )
        agent_factory = AgentFactory(dependencies)
        supervisor = RunSupervisor(
            agent_builder=agent_factory.build,
            store=store,
            approval_tool_names=agent_factory.approval_names,
            backend_for=agent_factory.backend_for,
            trace_factory=lambda request: trace_config(config.observability, request),
            source_for=subagent_catalog.source_for,
            feature_for=agent_factory.feature,
            consumer=_consumer_name(),
            heartbeat_s=config.lease_heartbeat_s,
            recursion_limit=config.recursion_limit,
            events_ttl_s=config.retention_events_ttl_s,
            run_ttl_s=config.retention_run_ttl_s,
            outbox_republish_ms=config.outbox_republish_ms,
            sandbox_teardown=_sandbox_teardown(config),
            chat_store=chat_store,
        )
        LOGGER.info("kokoro-agent worker consuming %s as %s", REQUESTS_STREAM, _consumer_name())
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


def http_main() -> None:
    """Start only the Agent business HTTP ingress."""
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    config = AppConfig.from_env(os.environ)
    host = os.environ.get("KOKORO_AGENT_HTTP_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.environ.get("KOKORO_AGENT_HTTP_PORT", "4401"))
    log_config_summary(config, logging.getLogger(__name__))
    server = create_http_server(config, host, port)
    logging.getLogger(__name__).info("kokoro-agent HTTP ingress listening on %s:%d", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
