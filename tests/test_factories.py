"""装配/工厂层：make_stream 后端选择 + build_agent 两档权限装配（此前 0% 覆盖）。"""

from __future__ import annotations

import pytest

from kokoro_agent.application.agent_factory import build_agent
from kokoro_agent.application.protocols.agent import InvokableAgent
from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.infrastructure.model import make_chat_model
from kokoro_agent.infrastructure.transport import make_stream
from kokoro_agent.infrastructure.transport.memory_stream import MemoryStream
from kokoro_agent.infrastructure.transport.redis_stream import RedisStream


# ── make_stream：KOKORO_STREAM_BACKEND 三分支 ────────────────────────────────


def test_make_stream_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOKORO_STREAM_BACKEND", "memory")
    assert isinstance(make_stream(), MemoryStream)


def test_make_stream_redis_lazy(monkeypatch: pytest.MonkeyPatch) -> None:
    # from_url 惰性建客户端、不即连，故无 live redis 也能断言类型。
    monkeypatch.setenv("KOKORO_STREAM_BACKEND", "redis")
    monkeypatch.setenv("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
    assert isinstance(make_stream(), RedisStream)


def test_make_stream_backend_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    # config 对 backend 做 .lower()：大小写不应改变选择。
    monkeypatch.setenv("KOKORO_STREAM_BACKEND", "MEMORY")
    assert isinstance(make_stream(), MemoryStream)


def test_make_stream_unknown_backend_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOKORO_STREAM_BACKEND", "bogus")
    with pytest.raises(ValueError, match="bogus"):
        make_stream()


# ── build_agent：两档权限都装配出可流式 agent ───────────────────────────────


@pytest.mark.parametrize("mode", ["auto", "default"])
def test_build_agent_assembles_invokable(mode: PermissionMode) -> None:
    # default 走 interrupt_on 工具门控分支、auto 走空映射分支；两档都须装配出满足窄契约的 agent。
    model = make_chat_model()  # 无凭证 → LocalFakeChatModel
    agent = build_agent(model, mode)
    assert isinstance(agent, InvokableAgent)
