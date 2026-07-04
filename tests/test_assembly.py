"""装配层规格：AppConfig 解析边界 + 注入式工厂（interrupt_on/目录/沙箱/模型/传输）。"""

from __future__ import annotations

import pytest
from deepagents.backends.local_shell import LocalShellBackend
from langchain_deepseek import ChatDeepSeek
from pydantic import ValidationError

from kokoro_agent.config import AppConfig
from fakes import FakeLedger
from kokoro_agent.orchestration import render_tool_guidance
from kokoro_agent.tools.middleware import TerminalGuardMiddleware
from kokoro_agent.contract import (
    ModelConfig,
    Permissions,
    RunInput,
    RunRequest,
    RuntimeConfig,
    RuntimeContext,
    SubagentDef,
)
from kokoro_agent.model.factory import make_chat_model
from langchain_core.messages import AIMessage, HumanMessage

from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend
from kokoro_agent.streams.factory import StreamSettings, make_stream
from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.subagents import build_catalog
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.tools.memory import make_memory_tools
from kokoro_agent.tools.registry import resolve_tools
from kokoro_agent.orchestration import catalog_subagents, wire_subagents
from kokoro_agent.worker.main import web_tools_from_config


def test_defaults_from_empty_env() -> None:
    config = AppConfig.from_env({})
    assert config.stream.backend == "memory"
    assert config.checkpoint.backend == "sqlite"
    assert config.ledger.backend == "sqlite"
    assert config.model.local_fake is False
    assert config.ledger.lease_ttl_ms == 90_000
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
    assert config.ledger.lease_ttl_ms == 10_000
    assert config.lease_heartbeat_s == 2.5
    # "[]" 非空字符串照实透传，目录构建时解析。
    assert config.custom_subagents_json == "[]"


@pytest.mark.parametrize(
    "env",
    [
        {"KOKORO_STREAM_BACKEND": "kafka"},
        {"KOKORO_CHECKPOINT_BACKEND": "dynamo"},
        {"KOKORO_LEDGER_BACKEND": "bogus"},
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


def test_catalog_builtin_empty_until_real_capability() -> None:
    # 内建目录只收带真实工具的真能力：现阶段为空，人格预设归 namespace（wire）。
    catalog = build_catalog(None)
    assert catalog.names() == frozenset()
    # 目录之外的名字是运行期动态子代理：一等来源，不抛错。
    assert catalog.source_for("ephemeral-worker") == "runtime-custom"


def test_catalog_custom_from_json() -> None:
    catalog = build_catalog(
        '[{"name": "poet", "description": "写诗", "system_prompt": "你是诗人"}]'
    )
    assert catalog.source_for("poet") == "config-custom"
    names = [d["name"] for d in catalog.definitions()]
    assert names == ["poet"]


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


def test_catalog_duplicate_custom_name_rejected() -> None:
    with pytest.raises(ValueError, match="poet"):
        build_catalog(
            '[{"name": "poet", "description": "d", "system_prompt": "p"},'
            ' {"name": "poet", "description": "d2", "system_prompt": "p2"}]'
        )


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


def test_interrupt_on_subagent_create_ask_gates_task() -> None:
    gated = build_interrupt_on(frozenset(), subagent_create="ask")
    assert set(gated) == {"ask_user_question", "task"}
    assert gated["task"]["allowed_decisions"] == ["approve", "edit", "reject"]
    # deny/allow 不进暂停门控（deny 由 middleware fail-closed，allow 直接放行）。
    for policy in ("deny", "allow"):
        assert "task" not in build_interrupt_on(frozenset(), subagent_create=policy)


def test_core_tools_always_mounted() -> None:
    # ask_user（handbook 12 号）恒挂载；记忆工具由 worker 装配点按 run scope 创建。
    assert [tool.name for tool in resolve_tools([])] == ["ask_user_question"]
    # 名单里出现记忆工具名不报未知也不重复挂载（实例只来自 make_memory_tools）。
    assert [tool.name for tool in resolve_tools(["save_memory"])] == ["ask_user_question"]
    assert [tool.name for tool in make_memory_tools("ns")] == ["save_memory", "search_memory"]


def test_openai_reasoning_switch_selects_deepseek_wrapper() -> None:
    # GLM/DeepSeek 等 openai 兼容端点的 reasoning_content 被 ChatOpenAI 明文拒收（上游 API scope），
    # KOKORO_OPENAI_REASONING=1 切 ChatDeepSeek（官方 reasoning 抽取实现，同 openai-compat wire）。
    env = {
        "KOKORO_OPENAI_REASONING": "1",
        "OPENAI_API_KEY": "sk-test",
        "OPENAI_BASE_URL": "https://example.com/v4",
    }
    config = AppConfig.from_env(env)
    model = make_chat_model(config.model, ModelConfig(provider="openai", name="glm-5"))
    assert isinstance(model, ChatDeepSeek)
    assert model.api_base == "https://example.com/v4"
    plain = AppConfig.from_env({k: v for k, v in env.items() if k != "KOKORO_OPENAI_REASONING"})
    assert not isinstance(
        make_chat_model(plain.model, ModelConfig(provider="openai", name="glm-5")), ChatDeepSeek
    )


def test_openai_reasoning_without_base_url_fails_loud() -> None:
    config = AppConfig.from_env({"KOKORO_OPENAI_REASONING": "1", "OPENAI_API_KEY": "sk-test"})
    with pytest.raises(ValueError, match="OPENAI_BASE_URL"):
        make_chat_model(config.model, ModelConfig(provider="openai", name="glm-5"))


def test_web_tools_assembly_matrix() -> None:
    # search 配置即挂载：无 provider 只有 fetch；zhipu 配齐挂双件；半配 fail-loud。
    bare = web_tools_from_config(AppConfig.from_env({}))
    assert [tool.name for tool in bare] == ["web_fetch"]
    full = web_tools_from_config(
        AppConfig.from_env(
            {"KOKORO_WEB_SEARCH_PROVIDER": "zhipu", "KOKORO_WEB_SEARCH_API_KEY": "k"}
        )
    )
    assert [tool.name for tool in full] == ["web_fetch", "web_search"]
    searx = web_tools_from_config(AppConfig.from_env(
        {"KOKORO_WEB_SEARCH_PROVIDER": "searxng", "KOKORO_WEB_SEARCH_URL": "https://searx.local"}
    ))
    assert [tool.name for tool in searx] == ["web_fetch", "web_search"]
    with pytest.raises(ValueError, match="KOKORO_WEB_SEARCH_API_KEY"):
        web_tools_from_config(AppConfig.from_env({"KOKORO_WEB_SEARCH_PROVIDER": "zhipu"}))
    with pytest.raises(ValueError, match="unsupported"):
        web_tools_from_config(AppConfig.from_env({"KOKORO_WEB_SEARCH_PROVIDER": "bing", "KOKORO_WEB_SEARCH_API_KEY": "k"}))


def test_recursion_limit_env_parse() -> None:
    assert AppConfig.from_env({}).recursion_limit == 100
    assert AppConfig.from_env({"KOKORO_RECURSION_LIMIT": "12"}).recursion_limit == 12
    with pytest.raises(ValidationError):
        AppConfig.from_env({"KOKORO_RECURSION_LIMIT": "0"})


def testwire_subagents_tools_and_model_passthrough() -> None:
    # wire 预设声明的 tools/model 必须生效或炸——静默丢弃是最坏失效（真实缺陷回归钉）。
    fetch = web_tools_from_config(AppConfig.from_env({}))[0]
    fake_model = LocalFakeChatModel.with_script([])

    def request_with(sub: SubagentDef) -> RunRequest:
        return RunRequest(
            kind="run.request",
            run_id="r1",
            thread_id="t1",
            input=RunInput(message_id="m1", content="hi"),
            context=RuntimeContext(namespace="ns", session_id="s1"),
            runtime=RuntimeConfig(
                model=ModelConfig(provider="anthropic", name="claude"),
                tools=[],
                skills=[],
                mcp=[],
                subagents=[sub],
                backend="state",
                permissions=Permissions(
                    approval_tools=[], review_tools=[], subagent_create="deny",
                    filesystem="workspace_write",
                ),
            ),
        )

    spec = SubagentDef(
        name="poet", description="d", system_prompt="p", tools=["web_fetch"],
        model=ModelConfig(provider="anthropic", name="claude"),
    )
    subs = wire_subagents(request_with(spec), {"web_fetch": fetch}, lambda _m: fake_model)
    first = dict(subs[0])
    assert first.get("tools") == [fetch]
    assert first.get("model") is fake_model

    inherit = SubagentDef(name="poet", description="d", system_prompt="p", tools=[])
    plain = wire_subagents(request_with(inherit), {}, lambda _m: fake_model)
    assert "tools" not in plain[0]
    assert "model" not in plain[0]

    ghost = SubagentDef(name="poet", description="d", system_prompt="p", tools=["nope"])
    with pytest.raises(ValueError, match="unmounted tools"):
        wire_subagents(request_with(ghost), {"web_fetch": fetch}, lambda _m: fake_model)


def test_run_token_budget_env_parse() -> None:
    assert AppConfig.from_env({}).run_token_budget == 0  # 默认关闭：预算数值属政策，不擅代
    assert AppConfig.from_env({"KOKORO_RUN_TOKEN_BUDGET": "200000"}).run_token_budget == 200000
    with pytest.raises(ValidationError):
        AppConfig.from_env({"KOKORO_RUN_TOKEN_BUDGET": "-1"})


def test_builtin_subagents_opt_in_matrix() -> None:
    # 用户裁定：实现但默认关闭——显式点名才挂；工具缺任一即整个不挂（不设空壳）。
    assert build_catalog(None).names() == frozenset()
    with pytest.raises(ValueError, match="unknown built-in"):
        build_catalog(None, frozenset({"ghost"}))

    catalog = build_catalog(None, frozenset({"web-researcher"}))
    assert catalog.names() == {"web-researcher"}
    tools = web_tools_from_config(AppConfig.from_env(
        {"KOKORO_WEB_SEARCH_PROVIDER": "searxng", "KOKORO_WEB_SEARCH_URL": "https://searx.local"}
    ))
    index = {tool.name: tool for tool in tools}
    defs, mounted = catalog_subagents(catalog, index)
    assert mounted == {"web-researcher"}
    assert dict(defs[0]).get("tools") == [index["web_search"], index["web_fetch"]]

    # search 未配置 → web_search 缺 → 整个不挂、不进 deny 声明集。
    bare_index = {tool.name: tool for tool in web_tools_from_config(AppConfig.from_env({}))}
    defs_bare, mounted_bare = catalog_subagents(catalog, bare_index)
    assert defs_bare == [] and mounted_bare == frozenset()


def test_builtin_subagents_env_parse() -> None:
    config = AppConfig.from_env({"KOKORO_BUILTIN_SUBAGENTS": "web-researcher, "})
    assert config.enabled_builtin_subagents == {"web-researcher"}
    assert AppConfig.from_env({}).enabled_builtin_subagents == frozenset()


def test_tool_guidance_follows_mounted_tools() -> None:
    # 行为指引只提真挂载的工具：提未挂载工具=教模型调用不存在的东西。
    full = render_tool_guidance(
        frozenset({"ask_user_question", "save_memory", "search_memory", "web_fetch", "web_search"})
    )
    assert full is not None
    for token in ("ask_user_question", "save_memory", "web_fetch", "web_search"):
        assert token in full

    no_search = render_tool_guidance(
        frozenset({"ask_user_question", "save_memory", "search_memory", "web_fetch"})
    )
    assert no_search is not None and "web_search" not in no_search

    assert render_tool_guidance(frozenset()) is None


def test_guards_propagate_to_every_subagent() -> None:
    # 子代理 middleware 链独立：预算/终态闸不逐个下发 = task 委派旁路（真旁路回归钉）。
    guard = TerminalGuardMiddleware(store=FakeLedger(), run_id="r1")
    catalog = build_catalog(None, frozenset({"web-researcher"}))
    tools = web_tools_from_config(AppConfig.from_env(
        {"KOKORO_WEB_SEARCH_PROVIDER": "searxng", "KOKORO_WEB_SEARCH_URL": "https://searx.local"}
    ))
    index = {tool.name: tool for tool in tools}
    defs, _ = catalog_subagents(catalog, index, [guard])
    assert dict(defs[0]).get("middleware") == [guard]
