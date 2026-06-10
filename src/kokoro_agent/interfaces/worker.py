from __future__ import annotations

import asyncio
import logging

from dotenv import load_dotenv
from langchain_core.language_models import BaseChatModel
from pydantic import ValidationError

from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.chat_model import make_chat_model
from kokoro_agent.infrastructure.stream_port import StreamPort, make_stream_port
from kokoro_agent.application.run_agent import run_agent

LOGGER = logging.getLogger(__name__)

REQUESTS_STREAM = "kokoro:runs:requests"


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


async def _handle_request(
    port: StreamPort,
    raw: dict[str, object],
    processed: set[str],
    model: BaseChatModel | None = None,
) -> None:
    try:
        request = RunRequest.model_validate(raw)
    except ValidationError as error:
        LOGGER.warning("dropping malformed run.request: %s", error)
        return

    if request.run_id in processed:
        LOGGER.debug("skipping already-processed run_id=%s", request.run_id)
        return
    processed.add(request.run_id)

    stream = events_stream(request.run_id)
    resolved_model = model if model is not None else make_chat_model(request.execution_style)
    async for event in run_agent(request, resolved_model):
        await port.publish(stream, event.model_dump())


async def run_once(
    port: StreamPort, processed: set[str], model: BaseChatModel | None = None
) -> None:
    """Drain currently-pending run requests once and emit their event streams.

    Idempotent per ``run_id`` via the caller-owned ``processed`` set, so a
    duplicate request for a run that already ran is ignored.
    """
    for item in await port.read_all(REQUESTS_STREAM):
        await _handle_request(port, item.event, processed, model)


async def _serve(port: StreamPort) -> None:
    processed: set[str] = set()
    async for item in port.subscribe(REQUESTS_STREAM):
        await _handle_request(port, item.event, processed)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    # 本地开发便利：把 .env（gitignored）载入进程环境，供 make_chat_model 读取
    # KOKORO_MODEL / OPENAI_BASE_URL / OPENAI_API_KEY。生产由真实环境注入，无 .env 时是空操作。
    load_dotenv()
    port = make_stream_port()
    LOGGER.info("kokoro-agent worker starting on stream %s", REQUESTS_STREAM)
    asyncio.run(_serve(port))


if __name__ == "__main__":
    main()
