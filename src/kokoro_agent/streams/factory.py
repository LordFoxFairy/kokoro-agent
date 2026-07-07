"""传输后端：redis（跨栈唯一真源）。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.streams.redis import RedisStream


class StreamSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    redis_url: str


def make_stream(settings: StreamSettings) -> StreamProtocol:
    return RedisStream(settings.redis_url)
