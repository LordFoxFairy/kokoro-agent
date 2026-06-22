"""传输层入口：按 KOKORO_STREAM_BACKEND 选择内存/Redis 事件流后端。"""

from __future__ import annotations

import os

from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.infrastructure.transport.memory_stream import MemoryStream
from kokoro_agent.infrastructure.transport.redis_stream import RedisStream, parse_xread_response


def make_stream() -> StreamProtocol:
    backend = os.environ.get("KOKORO_STREAM_BACKEND", "memory").lower()
    if backend == "redis":
        url = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
        return RedisStream(url)
    if backend == "memory":
        return MemoryStream()
    raise ValueError(f"unknown KOKORO_STREAM_BACKEND: {backend!r}")


__all__ = [
    "MemoryStream",
    "RedisStream",
    "make_stream",
    "parse_xread_response",
]
