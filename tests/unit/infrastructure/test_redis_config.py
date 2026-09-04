"""Redis client configuration stays on the shared Agent logical database."""

from __future__ import annotations

import pytest

import kokoro_agent.streams.redis as redis_module


def test_redis_stream_sets_bounded_connection_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_url = ""
    captured_kwargs: dict[str, object] = {}

    class _FakeRedis:
        async def aclose(self) -> None:
            return None

    def fake_from_url(url: str, **kwargs: object) -> _FakeRedis:
        nonlocal captured_url
        captured_url = url
        captured_kwargs.update(kwargs)
        return _FakeRedis()

    monkeypatch.setattr(redis_module, "from_url", fake_from_url)
    redis_module.RedisStream("redis://127.0.0.1:6379/9")

    assert captured_url.endswith("/9")
    assert captured_kwargs["socket_connect_timeout"] == 10.0
    assert captured_kwargs["socket_timeout"] == 30.0
