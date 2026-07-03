"""传输后端选择：按注入的 StreamSettings 实例化 memory/redis。"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.streams.redis import RedisStream


class StreamSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    backend: Literal["memory", "redis"]
    redis_url: str


def make_stream(settings: StreamSettings) -> StreamProtocol:
    if settings.backend == "redis":
        return RedisStream(settings.redis_url)
    return MemoryStream()
