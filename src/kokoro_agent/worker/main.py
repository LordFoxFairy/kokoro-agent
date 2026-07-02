"""Worker 进程入口：装配 run 编排，订阅请求流并并发执行。"""

from __future__ import annotations

import asyncio
import logging

from dotenv import load_dotenv

from kokoro_agent.execution.create_agent import build_agent
from kokoro_agent.storage.checkpoints import make_checkpointer
from kokoro_agent.model import make_chat_model
from kokoro_agent.observability import trace_config
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.storage import make_run_state_store
from kokoro_agent.subagents import subagent_source_for
from kokoro_agent.streams import make_stream
from kokoro_agent.execution.run_agent import InvokableAgent
from kokoro_agent.execution.resume_agent import REQUESTS_STREAM, RunSupervisor
from kokoro_agent.worker.messages import RunRequest

LOGGER = logging.getLogger(__name__)


async def _serve() -> None:
    bus = make_stream()
    # 进程级共享 checkpointer + run 状态存储：sqlite 后端落盘跨重启，多 pod 靠共享存储去重 / 终态认领。
    async with make_checkpointer() as saver, make_run_state_store() as store:

        def build(request: RunRequest) -> InvokableAgent:
            model = make_chat_model(request.execution_style)
            return build_agent(
                model,
                request.permission_mode,
                checkpointer=saver,
            )

        supervisor = RunSupervisor(
            agent_builder=build,
            store=store,
            approval_tool_names=lambda request: frozenset(build_interrupt_on(request.permission_mode)),
            trace_factory=trace_config,
            subagent_source=subagent_source_for,
        )
        LOGGER.info("kokoro-agent worker starting on stream %s", REQUESTS_STREAM)
        await supervisor.serve(bus)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
