"""进程入口（纯调度域装配）：env 一次解析 → 共享件 → 编排配方注入 Supervisor.serve。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import functools
import logging
import os
import signal
import socket

from dotenv import load_dotenv

from kokoro_agent.config import AppConfig, log_config_summary
from kokoro_agent.contract import REQUESTS_STREAM
from kokoro_agent.observability import trace_config
from kokoro_agent.agents import AssembleDeps, approval_names, assemble
from kokoro_agent.contract import Backend
from kokoro_agent.sandbox import teardown_backend_for_run
from kokoro_agent.sandbox.archive import LocalWorkspace, load_storage_file
from kokoro_agent.tools.toolbox import ProcessToolbox, build_toolbox
from kokoro_agent.tools.web_search import SearchProviderSettings
from kokoro_agent.storage.checkpoints import make_checkpointer
from kokoro_agent.storage.memory_store import make_memory_store
from kokoro_agent.storage.ledger import make_ledger
from kokoro_agent.streams.factory import make_stream
from kokoro_agent.content_source import make_asset_source
from kokoro_agent.prompts import PromptLibrary
from kokoro_agent.mcp.config import load_mcp_servers
from kokoro_agent.skills.hub import SkillHubSettings, make_skill_hub, seed_official
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


def skill_hub_settings(config: AppConfig) -> SkillHubSettings:
    """hub 存储位形取 ADR-009 文件 hub 节；缺省=local ./kokoro_hub（dev 零配置可跑）。
    s3 凭据复用 workspace 对（同一对象存储集群，env-only）。"""
    storage = load_storage_file(config.workspace_config)
    packages = (
        storage.hub
        if storage is not None and storage.hub is not None
        else LocalWorkspace(type="local", root="./kokoro_hub")
    )
    return SkillHubSettings(
        mongo_url=config.ledger.mongo_url,
        mongo_db=config.ledger.mongo_db,
        packages=packages,
        s3_access_key=config.workspace_s3_access_key,
        s3_secret_key=config.workspace_s3_secret_key,
    )


def _consumer_name() -> str:
    # consumer-group 内的成员身份：主机+pid 保多 pod/多进程不撞名。
    return f"{socket.gethostname()}-{os.getpid()}"


async def _serve(config: AppConfig) -> None:
    bus = make_stream(config.stream)
    catalog = build_catalog(config.custom_subagents_json, config.enabled_builtin_subagents)
    # 启动装载部署资产（local/s3 同口）：prompts 进内存（部署级人格）；skills 目录只是
    # seed 输入——真源是 Mongo（多租户），启动 upsert 幂等同步（hash 未变不写）。
    asset_source = make_asset_source(config.assets)
    raw_skills = asset_source.load_skills()
    prompts = PromptLibrary(asset_source.load_personas())
    # 进程级共享 checkpointer + run 状态存储：mongo 跨 pod 共享，去重/租约/终态认领/崩溃恢复皆赖之。
    async with (
        make_checkpointer(config.checkpoint) as saver,
        make_ledger(config.ledger) as store,
        make_memory_store(config.checkpoint) as memory_store,
        make_skill_hub(skill_hub_settings(config)) as skill_hub,
    ):
        await seed_official(skill_hub, raw_skills)
        deps = AssembleDeps(
            model=config.model,
            sandbox=config.sandbox,
            run_token_budget=config.run_token_budget,
            catalog=catalog,
            toolbox=toolbox_from_config(config),
            checkpointer=saver,
            ledger=store,
            memory_store=memory_store,
            skill_hub=skill_hub,
            prompts=prompts,
            # MCP server 定义住部署侧：启动即加载校验（含 ${ENV} 凭据展开），fail-loud。
            mcp_servers=load_mcp_servers(config.mcp_config, os.environ),
        )
        supervisor = RunSupervisor(
            agent_builder=functools.partial(assemble, deps),
            store=store,
            approval_tool_names=approval_names,
            trace_factory=lambda request: trace_config(config.observability, request),
            source_for=catalog.source_for,
            consumer=_consumer_name(),
            heartbeat_s=config.lease_heartbeat_s,
            recursion_limit=config.recursion_limit,
            events_ttl_s=config.retention_events_ttl_s,
            run_ttl_s=config.retention_run_ttl_s,
            sandbox_teardown=_sandbox_teardown(config),
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
    asyncio.run(_serve(config))


if __name__ == "__main__":
    main()
