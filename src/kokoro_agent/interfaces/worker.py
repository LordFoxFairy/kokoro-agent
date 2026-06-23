"""Worker 进程入口：装配 run 编排，订阅请求流并并发执行。"""

from __future__ import annotations

import asyncio
import logging

from dotenv import load_dotenv
from langgraph.checkpoint.memory import InMemorySaver

from kokoro_agent.application.agent_factory import build_agent
from kokoro_agent.infrastructure.model import make_chat_model
from kokoro_agent.infrastructure.subagent import RuntimeSubagentRegistry
from kokoro_agent.infrastructure.transport import make_stream
from kokoro_agent.application.run.invoke import InvokableAgent
from kokoro_agent.application.run.supervisor import REQUESTS_STREAM, RunSupervisor
from kokoro_agent.interfaces.inbound import RunRequest

LOGGER = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    bus = make_stream()
    # 进程级共享 checkpointer：暂停态跨 request/resume 在同一图状态续跑（R1 dev-only InMemorySaver）。
    saver = InMemorySaver()

    def build(request: RunRequest) -> InvokableAgent:
        model = make_chat_model(request.execution_style)
        return build_agent(
            model,
            request.permission_mode,
            RuntimeSubagentRegistry(),
            checkpointer=saver,
        )

    supervisor = RunSupervisor(agent_builder=build, checkpointer=saver)
    LOGGER.info("kokoro-agent worker starting on stream %s", REQUESTS_STREAM)
    asyncio.run(supervisor.serve(bus))


if __name__ == "__main__":
    main()
