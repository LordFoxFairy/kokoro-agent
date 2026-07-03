"""装配层规格：AppConfig 解析边界 + 注入式工厂（interrupt_on/目录/沙箱/模型/传输）。"""

from __future__ import annotations

import pytest
from deepagents.backends.local_shell import LocalShellBackend
from pydantic import ValidationError

from kokoro_agent.config import AppConfig
from kokoro_agent.contract import ModelConfig, Permissions, RuntimeConfig
from kokoro_agent.model.factory import make_chat_model
from langchain_core.messages import AIMessage, HumanMessage

from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend
from kokoro_agent.streams.factory import StreamSettings, make_stream
from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.subagents import build_catalog
from kokoro_agent.tools.permissions import build_interrupt_on


def test_defaults_from_empty_env() -> None:
    config = AppConfig.from_env({})
    assert config.stream.backend == "memory"
    assert config.checkpoint.backend == "sqlite"
    assert config.run_state.backend == "sqlite"
    assert config.model.local_fake is False
    assert config.run_state.lease_ttl_ms == 90_000
    assert config.lease_heartbeat_s == 30.0
    assert config.custom_subagents_json is None


def test_env_overrides() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_STREAM_BACKEND": "redis",
            "KOKORO_REDIS_URL": "redis://example:6379/1",
            "KOKORO_LEASE_TTL_S": "10",
            "KOKORO_LEASE_HEARTBEAT_S": "2.5",
            "KOKORO_LOCAL_FAKE_MODEL": "1",
            "KOKORO_CUSTOM_SUBAGENTS": "[]",
        }
    )
    assert config.model.local_fake is True
    assert config.stream.backend == "redis"
    assert config.stream.redis_url == "redis://example:6379/1"
    assert config.run_state.lease_ttl_ms == 10_000
    assert config.lease_heartbeat_s == 2.5
    # "[]" 非空字符串照实透传，目录构建时解析。
    assert config.custom_subagents_json == "[]"


@pytest.mark.parametrize(
    "env",
    [
        {"KOKORO_STREAM_BACKEND": "kafka"},
        {"KOKORO_CHECKPOINT_BACKEND": "dynamo"},
        {"KOKORO_RUN_STATE_BACKEND": "bogus"},
        {"KOKORO_LEASE_TTL_S": "0"},
        {"KOKORO_LEASE_HEARTBEAT_S": "-1"},
    ],
)
def test_invalid_backend_enums_fail_loud(env: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        AppConfig.from_env(env)


# --- interrupt_on 矩阵（审批集合每请求经 wire 注入） ---


def test_interrupt_on_no_approval_tools_only_ask_user() -> None:
    interrupt_on = build_interrupt_on(frozenset())
    assert set(interrupt_on) == {"ask_user_question"}
    assert interrupt_on["ask_user_question"]["allowed_decisions"] == ["respond"]


def test_interrupt_on_gates_approval_tools() -> None:
    interrupt_on = build_interrupt_on(frozenset({"execute", "deploy"}))
    assert set(interrupt_on) == {"ask_user_question", "execute", "deploy"}
    assert interrupt_on["execute"]["allowed_decisions"] == ["approve", "edit", "reject"]


def test_interrupt_on_ask_user_stays_respond_only() -> None:
    # ask_user 即便被误列进审批集合，也不得降级为 approve/edit/reject。
    interrupt_on = build_interrupt_on(frozenset({"ask_user_question"}))
    assert interrupt_on["ask_user_question"]["allowed_decisions"] == ["respond"]


def test_filesystem_permissions_by_perm() -> None:
    assert build_filesystem_permissions("workspace_write") == []
    denies = build_filesystem_permissions("read_only")
    assert len(denies) == 1


# --- 子代理目录 ---


def test_catalog_builtin_and_runtime_custom_sources() -> None:
    catalog = build_catalog(None)
    assert catalog.source_for("researcher") == "built-in"
    # 目录之外的名字是运行期动态子代理：一等来源，不抛错。
    assert catalog.source_for("ephemeral-worker") == "runtime-custom"


def test_catalog_custom_from_json() -> None:
    catalog = build_catalog(
        '[{"name": "poet", "description": "写诗", "system_prompt": "你是诗人"}]'
    )
    assert catalog.source_for("poet") == "config-custom"
    names = [d["name"] for d in catalog.definitions()]
    assert names == ["researcher", "poet"]


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        '[{"name": "", "description": "d", "system_prompt": "p"}]',
        '[{"name": "poet", "description": "d", "system_prompt": "p", "extra": 1}]',
    ],
)
def test_catalog_malformed_custom_rejected(raw: str) -> None:
    with pytest.raises(ValueError):
        build_catalog(raw)


def test_catalog_duplicate_builtin_name_rejected() -> None:
    with pytest.raises(ValueError, match="researcher"):
        build_catalog('[{"name": "researcher", "description": "d", "system_prompt": "p"}]')


# --- 沙箱 / 模型 / 传输工厂（backend/model 每请求经 wire 选择） ---


def test_sandbox_state_backend_is_none() -> None:
    config = AppConfig.from_env({})
    assert make_backend("state", config.sandbox) is None


def test_sandbox_local_shell_backend() -> None:
    config = AppConfig.from_env({"KOKORO_AGENT_LOCAL_SHELL_TIMEOUT": "5"})
    backend = make_backend("local_shell", config.sandbox)
    assert isinstance(backend, LocalShellBackend)


def test_sandbox_unsupported_backend_fails_loud() -> None:
    config = AppConfig.from_env({})
    with pytest.raises(NotImplementedError):
        make_backend("e2b", config.sandbox)


def test_local_fake_model_short_circuits() -> None:
    config = AppConfig.from_env({"KOKORO_LOCAL_FAKE_MODEL": "1"})
    model = make_chat_model(config.model, ModelConfig(provider="anthropic", name="claude"))
    assert isinstance(model, LocalFakeChatModel)


def test_local_fake_hitl_script_switch() -> None:
    config = AppConfig.from_env({"KOKORO_LOCAL_FAKE_MODEL": "1", "KOKORO_LOCAL_FAKE_SCRIPT": "hitl"})
    model = make_chat_model(config.model, ModelConfig(provider="anthropic", name="claude"))
    assert isinstance(model, LocalFakeChatModel)
    first = model.invoke([HumanMessage(content="hi")])
    assert isinstance(first, AIMessage)
    assert first.tool_calls and first.tool_calls[0]["name"] == "ask_user_question"


def test_make_stream_backends() -> None:
    assert isinstance(
        make_stream(StreamSettings(backend="memory", redis_url="redis://x")), MemoryStream
    )
    assert isinstance(
        make_stream(StreamSettings(backend="redis", redis_url="redis://127.0.0.1:6379/0")),
        RedisStream,
    )


def test_runtime_system_prompt_on_wire() -> None:
    # 具名入口：RuntimeConfig 可带已解析人格；strict 契约拒绝空串。
    runtime = RuntimeConfig(
        model=ModelConfig(provider="anthropic", name="claude"),
        system_prompt="你是音乐创作人格",
        tools=[],
        skills=[],
        mcp=[],
        subagents=[],
        backend="state",
        permissions=Permissions(
            approval_tools=[], review_tools=[], subagent_create="deny", filesystem="read_only"
        ),
    )
    assert runtime.system_prompt == "你是音乐创作人格"
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate({**runtime.model_dump(), "system_prompt": ""})
