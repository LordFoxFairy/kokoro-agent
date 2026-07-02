"""装配/工厂层：make_stream 后端选择 + build_agent 两档权限装配（此前 0% 覆盖）。"""

from __future__ import annotations

import pytest
from deepagents.middleware.subagents import SubAgent
from langchain_core.tools import StructuredTool
from pydantic import ConfigDict, TypeAdapter

import kokoro_agent.execution.build_agent as build_agent_module
from kokoro_agent.config import RuntimeSettings
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.model import make_chat_model
from kokoro_agent.run.request import PermissionMode
from kokoro_agent.streams import make_stream
from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.tools.middleware import ToolPolicyMiddleware
from kokoro_agent.tools.names import ASK_USER_TOOL_NAME

_STRUCTURED_TOOLS = TypeAdapter(
    list[StructuredTool], config=ConfigDict(arbitrary_types_allowed=True)
)
_OBJECT_TUPLE = TypeAdapter(tuple[object, ...])


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


def test_build_agent_does_not_install_runtime_custom_subagent_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeAgent:
        async def astream_events(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

        async def aget_state(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

    def fake_build_deep_agent(**kwargs: object) -> _FakeAgent:
        captured.update(kwargs)
        return _FakeAgent()

    def fake_subagent_definitions(*args: object, **kwargs: object) -> list[SubAgent]:
        return [{"name": "researcher", "description": "查资料", "system_prompt": "研究"}]

    monkeypatch.setattr(build_agent_module, "build_deep_agent", fake_build_deep_agent)
    monkeypatch.setattr(build_agent_module, "subagent_definitions", fake_subagent_definitions, raising=False)

    build_agent(make_chat_model(), "auto")

    assert _tool_names(captured["tools"]) == [ASK_USER_TOOL_NAME]
    assert captured["subagents"] == [
        {"name": "researcher", "description": "查资料", "system_prompt": "研究"}
    ]


def test_build_agent_forwards_deepagents_runtime_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeAgent:
        async def astream_events(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

        async def aget_state(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

    def fake_build_deep_agent(**kwargs: object) -> _FakeAgent:
        captured.update(kwargs)
        return _FakeAgent()

    monkeypatch.setattr(build_agent_module, "build_deep_agent", fake_build_deep_agent)

    def no_subagents(*_args: object, **_kwargs: object) -> list[SubAgent]:
        return []

    monkeypatch.setattr(build_agent_module, "subagent_definitions", no_subagents)

    runtime = RuntimeSettings(
        backend="state",
        skills=("/skills/user",),
        memory=("/memory/AGENTS.md",),
        local_shell_root=None,
        local_shell_inherit_env=False,
        local_shell_timeout=120,
        local_shell_max_output_bytes=100000,
    )

    build_agent(make_chat_model(), "auto", runtime=runtime)

    assert captured["skills"] == ("/skills/user",)
    assert captured["memory"] == ("/memory/AGENTS.md",)
    assert captured["backend"] is None


def test_build_agent_forwards_tool_policy_middleware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeAgent:
        async def astream_events(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

        async def aget_state(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

    def fake_build_deep_agent(**kwargs: object) -> _FakeAgent:
        captured.update(kwargs)
        return _FakeAgent()

    monkeypatch.setattr(build_agent_module, "build_deep_agent", fake_build_deep_agent)

    def no_subagents(*_args: object, **_kwargs: object) -> list[SubAgent]:
        return []

    monkeypatch.setattr(build_agent_module, "subagent_definitions", no_subagents)

    build_agent(make_chat_model(), "auto")

    middleware = _tuple_items(captured["middleware"])
    assert len(middleware) == 1
    assert isinstance(middleware[0], ToolPolicyMiddleware)


def test_build_agent_installs_local_shell_backend_only_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeAgent:
        async def astream_events(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

        async def aget_state(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

    def fake_build_deep_agent(**kwargs: object) -> _FakeAgent:
        captured.update(kwargs)
        return _FakeAgent()

    monkeypatch.setattr(build_agent_module, "build_deep_agent", fake_build_deep_agent)

    def no_subagents(*_args: object, **_kwargs: object) -> list[SubAgent]:
        return []

    monkeypatch.setattr(build_agent_module, "subagent_definitions", no_subagents)

    runtime = RuntimeSettings(
        backend="local_shell",
        skills=(),
        memory=(),
        local_shell_root="/tmp/kokoro-agent",
        local_shell_inherit_env=True,
        local_shell_timeout=30,
        local_shell_max_output_bytes=4096,
    )

    build_agent(make_chat_model(), "auto", runtime=runtime)

    backend = captured["backend"]
    assert backend is not None
    assert type(backend).__name__ == "LocalShellBackend"


def _tool_names(value: object) -> list[str]:
    tools = _STRUCTURED_TOOLS.validate_python(value)
    return [tool.name for tool in tools]


def _tuple_items(value: object) -> list[object]:
    return list(_OBJECT_TUPLE.validate_python(value))
