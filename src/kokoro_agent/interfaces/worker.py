"""Worker 进程入口：装配请求准入与 run 编排，订阅请求流并并发执行。"""

from __future__ import annotations

import asyncio
import logging

from dotenv import load_dotenv

from kokoro_agent.application.run.request_admission import RequestAdmission
from kokoro_agent.application.run.run_supervisor import (
    REQUESTS_STREAM,
    RunSupervisor,
    events_stream,
)
from kokoro_agent.infrastructure.transport import make_stream

LOGGER = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    bus = make_stream()
    supervisor = RunSupervisor(RequestAdmission(events_stream))
    LOGGER.info("kokoro-agent worker starting on stream %s", REQUESTS_STREAM)
    asyncio.run(supervisor.serve(bus))


if __name__ == "__main__":
    main()
