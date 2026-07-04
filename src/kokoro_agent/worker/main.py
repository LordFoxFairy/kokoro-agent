"""进程入口（纯调度域装配）：env 一次解析 → 共享件 → 编排配方注入 Supervisor.serve。"""

from __future__ import annotations

import asyncio
import functools
import logging
import os
import signal
import socket

from dotenv import load_dotenv

from kokoro_agent.config import AppConfig
from kokoro_agent.contract import REQUESTS_STREAM
from langchain_core.tools import BaseTool
from kokoro_agent.observability import trace_config
from kokoro_agent.orchestration import (
    AssembleDeps,
    approval_names,
    assemble_general,
    build_web_tools,
)
from kokoro_agent.tools.web_search import SearchProviderSettings
from kokoro_agent.storage.checkpoints import make_checkpointer
from kokoro_agent.storage.memory_store import make_memory_store
from kokoro_agent.storage.ledger import make_ledger
from kokoro_agent.streams.factory import make_stream
from kokoro_agent.subagents import build_catalog
from kokoro_agent.worker.supervisor import RunSupervisor

LOGGER = logging.getLogger(__name__)


def web_tools_from_config(config: AppConfig) -> list[BaseTool]:
    # env → 领域设置的唯一翻译点（config 单点消费法则）。
    search = (
        None
        if config.web_tools.search_provider is None
        else SearchProviderSettings(
            provider=config.web_tools.search_provider,
            api_key=config.web_tools.search_api_key,
            base_url=config.web_tools.search_url,
        )
    )
    return build_web_tools(
        fetch_allow_private=config.web_tools.fetch_allow_private, search=search
    )


def _consumer_name() -> str:
    # consumer-group 内的成员身份：主机+pid 保多 pod/多进程不撞名。
    return f"{socket.gethostname()}-{os.getpid()}"


async def _serve(config: AppConfig) -> None:
    bus = make_stream(config.stream)
    catalog = build_catalog(config.custom_subagents_json, config.enabled_builtin_subagents)
    # 进程级共享 checkpointer + run 状态存储：sqlite 落盘跨重启，多 pod 靠共享存储去重/租约/终态认领。
    async with (
        make_checkpointer(config.checkpoint) as saver,
        make_ledger(config.ledger) as store,
        make_memory_store(config.checkpoint) as memory_store,
    ):
        deps = AssembleDeps(
            model=config.model,
            sandbox=config.sandbox,
            run_token_budget=config.run_token_budget,
            catalog=catalog,
            web_tools=tuple(web_tools_from_config(config)),
            checkpointer=saver,
            ledger=store,
            memory_store=memory_store,
        )
        supervisor = RunSupervisor(
            agent_builder=functools.partial(assemble_general, deps),
            store=store,
            approval_tool_names=approval_names,
            trace_factory=lambda request: trace_config(config.observability, request),
            source_for=catalog.source_for,
            consumer=_consumer_name(),
            heartbeat_s=config.lease_heartbeat_s,
            recursion_limit=config.recursion_limit,
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
    asyncio.run(_serve(AppConfig.from_env(os.environ)))


if __name__ == "__main__":
    main()
