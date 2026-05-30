from __future__ import annotations

import asyncio
import logging

from langchain_core.language_models import BaseChatModel
from pydantic import ValidationError

from kokoro_agent.events import RunRequest
from kokoro_agent.infrastructure.model import make_chat_model
from kokoro_agent.infrastructure.stream_port import StreamPort, make_stream_port
from kokoro_agent.run_agent import run_agent

LOGGER = logging.getLogger(__name__)

REQUESTS_STREAM = "kokoro:runs:requests"


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


async def _handle_request(
    port: StreamPort,
    raw: dict[str, object],
    processed: set[str],
    model: BaseChatModel,
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
    async for event in run_agent(request, model):
        await port.publish(stream, event.model_dump())


async def run_once(
    port: StreamPort, processed: set[str], model: BaseChatModel
) -> None:
    """Drain currently-pending run requests once and emit their event streams.

    Idempotent per ``run_id`` via the caller-owned ``processed`` set, so a
    duplicate request for a run that already ran is ignored.
    """
    for item in await port.read_all(REQUESTS_STREAM):
        await _handle_request(port, item.event, processed, model)


async def _serve(port: StreamPort) -> None:
    model = make_chat_model()
    processed: set[str] = set()
    async for item in port.subscribe(REQUESTS_STREAM):
        await _handle_request(port, item.event, processed, model)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    port = make_stream_port()
    LOGGER.info("kokoro-agent worker starting on stream %s", REQUESTS_STREAM)
    asyncio.run(_serve(port))


if __name__ == "__main__":
    main()
