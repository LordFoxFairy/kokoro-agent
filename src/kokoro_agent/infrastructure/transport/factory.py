"""传输层入口：按 KOKORO_STREAM_BACKEND 选择内存/Redis 事件流后端。"""

from __future__ import annotations

from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.infrastructure.config import AppConfig
from kokoro_agent.infrastructure.transport.memory_stream import MemoryStream
from kokoro_agent.infrastructure.transport.redis_stream import RedisStream


def make_stream() -> StreamProtocol:
    stream = AppConfig.from_env().stream
    if stream.backend == "redis":
        return RedisStream(stream.redis_url)
    if stream.backend == "memory":
        return MemoryStream()
    raise ValueError(f"unknown KOKORO_STREAM_BACKEND: {stream.backend!r}")
