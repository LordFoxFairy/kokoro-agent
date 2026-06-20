"""应用层：run.request 准入——解析、按 run_id 去重、非法即发布 run.failed。"""

from __future__ import annotations

import logging
from collections.abc import Callable

from pydantic import ValidationError

from kokoro_agent.application.event_stream import StreamProtocol
from kokoro_agent.application.run_lifecycle import run_failed_event
from kokoro_agent.domain.json_payload import JsonObject
from kokoro_agent.domain.run_request import RunRequest

LOGGER = logging.getLogger(__name__)

MAX_PROCESSED_RUN_IDS = 4096


def _run_id_of(raw: JsonObject) -> str | None:
    run_id = raw.get("run_id")
    return run_id if isinstance(run_id, str) else None


def _parse_request(raw: JsonObject) -> RunRequest | None:
    try:
        return RunRequest.model_validate(raw)
    except ValidationError as error:
        run_id = _run_id_of(raw)
        if run_id is not None:
            LOGGER.warning("invalid run.request for run_id=%s: %s", run_id, error)
        else:
            LOGGER.warning("dropping malformed run.request without usable run_id: %s", error)
        return None


class ProcessedRunIds:
    """有界、按插入序的已处理 run id 集合：超过上限即逐出最旧项，限制长驻 worker 的内存增长。"""

    def __init__(self, max_size: int = MAX_PROCESSED_RUN_IDS) -> None:
        self._ids: dict[str, None] = {}
        self._max_size = max_size

    def __contains__(self, run_id: str) -> bool:
        return run_id in self._ids

    def __len__(self) -> int:
        return len(self._ids)

    def add(self, run_id: str) -> None:
        self._ids[run_id] = None
        if len(self._ids) > self._max_size:
            oldest = next(iter(self._ids))
            del self._ids[oldest]


class RequestAdmission:
    """run.request 准入闸：解析 + 去重 + 非法发布 run.failed，仅放行待执行的合法请求。"""

    def __init__(
        self, events_stream: Callable[[str], str], processed: ProcessedRunIds | None = None
    ) -> None:
        self._events_stream = events_stream
        self._processed = processed if processed is not None else ProcessedRunIds()

    async def admit(self, bus: StreamProtocol, raw: JsonObject) -> RunRequest | None:
        request = _parse_request(raw)
        if request is None:
            run_id = _run_id_of(raw)
            if run_id is not None:
                event = run_failed_event(run_id, "ValidationError", "invalid run.request")
                await bus.publish(self._events_stream(run_id), event.model_dump())
            return None
        if request.run_id in self._processed:
            LOGGER.debug("skipping already-processed run_id=%s", request.run_id)
            return None
        self._processed.add(request.run_id)
        return request
