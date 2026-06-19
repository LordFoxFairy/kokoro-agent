"""传输层入口：按 KOKORO_STREAM_BACKEND 选择内存/Redis 事件流后端。"""

from __future__ import annotations

import os

from kokoro_agent.infrastructure.transport.memory_stream import MemoryStreamPort
from kokoro_agent.infrastructure.transport.redis_stream import RedisStreamPort, parse_xread_response
from kokoro_agent.infrastructure.transport.stream_protocol import StreamItem, StreamPort


def make_stream_port() -> StreamPort:
    backend = os.environ.get("KOKORO_STREAM_BACKEND", "memory").lower()
    if backend == "redis":
        url = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
        return RedisStreamPort(url)
    if backend == "memory":
        return MemoryStreamPort()
    raise ValueError(f"unknown KOKORO_STREAM_BACKEND: {backend!r}")


__all__ = [
    "MemoryStreamPort",
    "RedisStreamPort",
    "StreamItem",
    "StreamPort",
    "make_stream_port",
    "parse_xread_response",
]
