"""进程入口（纯调度域装配）：env 一次解析 → 共享件 → 编排配方注入 Supervisor.serve。"""

from __future__ import annotations

import asyncio
import argparse
from collections.abc import Awaitable, Callable
import functools
import logging
import os
import signal
import socket

from dotenv import load_dotenv

from kokoro_agent.config import AppConfig, log_config_summary
from kokoro_agent.contract import REQUESTS_STREAM
from kokoro_agent.metrics import start_metrics_server
from kokoro_agent.observability import trace_config
from kokoro_agent.platform import ConnectMediaOperationPort
from kokoro_agent.agents import AssembleDeps, approval_names, assemble
from kokoro_agent.contract import Backend
from kokoro_agent.sandbox import teardown_backend_for_run
from kokoro_agent.sandbox.archive import load_storage_file
from kokoro_agent.tools.toolbox import ProcessToolbox, build_toolbox
from kokoro_agent.tools.web_search import SearchProviderSettings
from kokoro_agent.storage.checkpoints import make_checkpointer
from kokoro_agent.storage.ledger import make_ledger
from kokoro_agent.storage.execution_context import ExecutionContextAuthority
from kokoro_agent.streams.factory import make_stream
from kokoro_agent.content_source import make_asset_source
from kokoro_agent.prompts import PromptLibrary
from kokoro_agent.hub import HubExecutionAssemblyClient
from kokoro_agent.mcp.egress import configure_egress_mode
from kokoro_agent.readiness import check_process_readiness
from kokoro_agent.skills.hub import (
    PackageStore,
    S3Credentials,
    make_package_store,
)
from kokoro_agent.subagents import build_catalog
from kokoro_agent.worker.supervisor import RunSupervisor

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


def deliveries_store(config: AppConfig) -> PackageStore | None:
    """交付冻结件存储位形取 ADR-009 文件 deliveries 节；缺省=None（deliver 工具降级不炸）。
    s3 凭据复用 workspace 对（同集群，env-only）。"""
    storage = load_storage_file(config.workspace_config)
    location = storage.deliveries if storage is not None else None
    if location is None:
        return None
    credentials = (
        S3Credentials(
            access_key=config.workspace_s3_access_key,
            secret_key=config.workspace_s3_secret_key,
        )
        if config.workspace_s3_access_key is not None
        and config.workspace_s3_secret_key is not None
        else None
    )
    return make_package_store(location, credentials)


def _consumer_name() -> str:
    # consumer-group 内的成员身份：主机+pid 保多 pod/多进程不撞名。
    return f"{socket.gethostname()}-{os.getpid()}"


async def _serve(config: AppConfig) -> None:
    # OBS-1 metrics 端点（缺省关）：显式配置端口才起，绝不阻断 worker 主职。
    if config.metrics_port is not None:
        start_metrics_server(config.metrics_port)
    bus = make_stream(config.stream)
    consumer = _consumer_name()
    ledger_settings = config.ledger.model_copy(
        update={"producer_instance_ref": consumer}
    )
    catalog = build_catalog(config.custom_subagents_json, config.enabled_builtin_subagents)
    configure_egress_mode(config.mcp_egress_mode)
    # Deployment assets now contain personas only. Skill packages are Hub-owned and resolved
    # exactly per run; the worker never seeds or reads Hub persistence.
    asset_source = make_asset_source(config.assets)
    prompts = PromptLibrary(asset_source.load_personas())
    capabilities = HubExecutionAssemblyClient(config.hub_runtime)
    media_settings = config.media_runtime
    media = ConnectMediaOperationPort(media_settings) if media_settings is not None else None
    # 进程级共享 checkpointer + run 状态存储：mongo 跨 pod 共享，去重/租约/终态认领/崩溃恢复皆赖之。
    async with (
        make_checkpointer(config.checkpoint) as saver,
        make_ledger(ledger_settings) as store,
    ):
        deps = AssembleDeps(
            model=config.model,
            sandbox=config.sandbox,
            run_token_budget=config.run_token_budget,
            catalog=catalog,
            toolbox=toolbox_from_config(config),
            checkpointer=saver,
            ledger=store,
            capabilities=capabilities,
            prompts=prompts,
            # 交付冻结件存储（deliveries 节）；缺省=None → deliver 工具恒挂但调用降级。
            deliveries=deliveries_store(config),
            media=media,
        )
        supervisor = RunSupervisor(
            agent_builder=functools.partial(assemble, deps),
            store=store,
            execution_context=ExecutionContextAuthority(store=store, checkpointer=saver),
            approval_tool_names=approval_names,
            trace_factory=lambda request: trace_config(config.observability, request),
            source_for=catalog.source_for,
            consumer=consumer,
            heartbeat_s=config.lease_heartbeat_s,
            recursion_limit=config.recursion_limit,
            events_ttl_s=config.retention_events_ttl_s,
            run_ttl_s=config.retention_run_ttl_s,
            outbox_republish_ms=config.outbox_republish_ms,
            sandbox_teardown=_sandbox_teardown(config),
        )
        LOGGER.info("kokoro-agent worker consuming %s as %s", REQUESTS_STREAM, consumer)
        serve_task = asyncio.create_task(supervisor.serve(bus))
        loop = asyncio.get_running_loop()
        # SIGTERM 优雅停机：停止消费新请求，限时等活跃 run 收尾（超时交 TTL 租约重拾）。
        loop.add_signal_handler(signal.SIGTERM, serve_task.cancel)
        try:
            await serve_task
        except asyncio.CancelledError:
            drained = await supervisor.drain(timeout_s=config.drain_timeout_s)
            LOGGER.info("graceful shutdown: drained=%s", drained)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="kokoro-agent-worker")
    parser.add_argument("--readiness", action="store_true")
    arguments = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    config = AppConfig.from_env(os.environ)
    if arguments.readiness:
        result = asyncio.run(check_process_readiness(config.worker_readiness))
        if not result.ready:
            LOGGER.error(
                "readiness failed: dependencies=%s",
                ",".join(result.failed_dependencies),
            )
            raise SystemExit(1)
        LOGGER.info("readiness ready")
        return
    # 启动期配置快照（secret 掩码）：一眼看清本进程实际生效的配置，便于排障。
    log_config_summary(config, LOGGER)
    asyncio.run(_serve(config))


if __name__ == "__main__":
    main()
