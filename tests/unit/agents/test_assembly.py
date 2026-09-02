"""装配层规格：AppConfig 解析边界 + 注入式工厂（interrupt_on/目录/沙箱/模型/传输）。"""

from __future__ import annotations

from pathlib import Path

import pytest
from deepagents.backends.local_shell import LocalShellBackend
from langchain_anthropic import ChatAnthropic
from langchain_deepseek import ChatDeepSeek
from langchain_openai import ChatOpenAI
from pydantic import ValidationError

from kokoro_agent.config import AppConfig
from support.fakes import FakeRunRepository
from kokoro_agent.tools.middleware import TerminalGuardMiddleware
from kokoro_agent.model.factory import make_chat_model
from kokoro_agent.policy import ModelConfig

from kokoro_agent.sandbox import build_filesystem_permissions, make_backend
from kokoro_agent.streams.factory import StreamSettings, make_stream
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.tools.memory import make_memory_tools
from kokoro_agent.tools.registry import resolve_tools
from kokoro_agent.agents.subagents import catalog_subagents, general_purpose_subagent
from kokoro_agent.worker.main import toolbox_from_config


def test_defaults_from_empty_env() -> None:
    # 存储收敛后唯一真后端：stream=redis、checkpoint/run_repository=postgres（无 backend 选择枚举）。
    config = AppConfig.from_env({})
    assert config.stream.redis_url == "redis://127.0.0.1:6379/0"
    assert config.database_url == "postgresql://localhost/postgres"
    assert config.database_schema == "kokoro_agent"
    assert config.run_repository.lease_ttl_ms == 90_000
    assert config.lease_heartbeat_s == 30.0
    assert config.custom_subagents_json is None


def test_env_overrides() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_REDIS_URL": "redis://example:6379/1",
            "KOKORO_AGENT_DATABASE_URL": "postgresql://example/postgres",
            "KOKORO_AGENT_DATABASE_SCHEMA": "kokoro_test",
            "KOKORO_LEASE_TTL_S": "10",
            "KOKORO_LEASE_HEARTBEAT_S": "2.5",
            "KOKORO_CUSTOM_SUBAGENTS": "[]",
        }
    )
    assert config.stream.redis_url == "redis://example:6379/1"
    assert config.database_url == "postgresql://example/postgres"
    assert config.database_schema == "kokoro_test"
    assert config.run_repository.lease_ttl_ms == 10_000
    assert config.lease_heartbeat_s == 2.5
    # "[]" 非空字符串照实透传，目录构建时解析。
    assert config.custom_subagents_json == "[]"


@pytest.mark.parametrize(
    "env",
    [
        {"KOKORO_LEASE_TTL_S": "0"},
        {"KOKORO_LEASE_HEARTBEAT_S": "-1"},
    ],
)
def test_invalid_config_values_fail_loud(env: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        AppConfig.from_env(env)


# --- interrupt_on 矩阵（审批集合每请求经 wire 注入） ---


def test_interrupt_on_no_approval_tools_only_ask_user() -> None:
    interrupt_on = build_interrupt_on(frozenset())
    assert set(interrupt_on) == {"ask_user_question"}
    ask_user = interrupt_on["ask_user_question"]
    assert isinstance(ask_user, dict)
    assert ask_user["allowed_decisions"] == ["respond"]


def test_interrupt_on_gates_approval_tools() -> None:
    interrupt_on = build_interrupt_on(frozenset({"execute", "deploy"}))
    assert set(interrupt_on) == {"ask_user_question", "execute", "deploy"}
    execute = interrupt_on["execute"]
    assert isinstance(execute, dict)
    assert execute["allowed_decisions"] == ["approve", "edit", "reject"]


def test_interrupt_on_ask_user_stays_respond_only() -> None:
    # ask_user 即便被误列进审批集合，也不得降级为 approve/edit/reject。
    interrupt_on = build_interrupt_on(frozenset({"ask_user_question"}))
    ask_user = interrupt_on["ask_user_question"]
    assert isinstance(ask_user, dict)
    assert ask_user["allowed_decisions"] == ["respond"]


def test_filesystem_permissions_by_perm() -> None:
    assert build_filesystem_permissions("workspace_write") == []
    denies = build_filesystem_permissions("read_only")
    assert len(denies) == 1


# --- 子代理目录 ---


def test_catalog_builtin_empty_until_real_capability() -> None:
    # 内建目录只收带真实工具的真能力：现阶段为空，prompt 预设归 namespace（wire）。
    catalog = build_subagent_catalog(None)
    assert catalog.names() == frozenset()
    # 目录之外的名字是运行期动态子代理：一等来源，不抛错。
    assert catalog.source_for("ephemeral-worker") == "runtime-custom"


def test_catalog_custom_from_json() -> None:
    catalog = build_subagent_catalog(
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
        build_subagent_catalog(raw)


def test_catalog_duplicate_custom_name_rejected() -> None:
    with pytest.raises(ValueError, match="poet"):
        build_subagent_catalog(
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


def test_sandbox_e2b_requires_run_scoped_assembly() -> None:
    # e2b 有 run 级生命周期：同步纯函数入口 fail-loud 指路 make_backend_for_run。
    config = AppConfig.from_env({})
    with pytest.raises(ValueError, match="make_backend_for_run"):
        make_backend("e2b", config.sandbox)


def test_make_stream_backends() -> None:
    # 存储收敛：传输唯一真后端是 redis（无 backend 选择字段）。
    assert isinstance(
        make_stream(StreamSettings(redis_url="redis://127.0.0.1:6379/0")),
        RedisStream,
    )


def test_interrupt_on_subagent_create_ask_gates_task() -> None:
    gated = build_interrupt_on(frozenset(), subagent_create="ask")
    assert set(gated) == {"ask_user_question", "task"}
    task = gated["task"]
    assert isinstance(task, dict)
    assert task["allowed_decisions"] == ["approve", "edit", "reject"]
    # deny/allow 不进暂停门控（deny 由 middleware fail-closed，allow 直接放行）。
    for policy in ("deny", "allow"):
        assert "task" not in build_interrupt_on(frozenset(), subagent_create=policy)


def test_core_tools_always_mounted() -> None:
    # ask_user（handbook 12 号）恒挂载；记忆工具由 worker 装配点按 run scope 创建。
    assert [tool.name for tool in resolve_tools([])] == ["ask_user_question"]
    # 名单里出现记忆工具名不报未知也不重复挂载（实例只来自 make_memory_tools）。
    assert [tool.name for tool in resolve_tools(["save_memory"])] == [
        "ask_user_question"
    ]
    assert [tool.name for tool in make_memory_tools("ns")] == [
        "save_memory",
        "search_memory",
    ]


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
    plain = AppConfig.from_env(
        {k: v for k, v in env.items() if k != "KOKORO_OPENAI_REASONING"}
    )
    assert not isinstance(
        make_chat_model(plain.model, ModelConfig(provider="openai", name="glm-5")),
        ChatDeepSeek,
    )


def test_openai_reasoning_thinking_toggle() -> None:
    # thinking=True→开 GLM 推理；False→关；None(不设)→不覆盖，随 GLM 默认（extra_body 直传 openai-compat wire）。
    env = {
        "KOKORO_OPENAI_REASONING": "1",
        "OPENAI_API_KEY": "sk-test",
        "OPENAI_BASE_URL": "https://example.com/v4",
    }
    config = AppConfig.from_env(env)
    thinking = make_chat_model(
        config.model, ModelConfig(provider="openai", name="glm-5", thinking=True)
    )
    assert isinstance(thinking, ChatDeepSeek)
    assert thinking.extra_body == {"thinking": {"type": "enabled"}}
    fast = make_chat_model(
        config.model, ModelConfig(provider="openai", name="glm-5", thinking=False)
    )
    assert isinstance(fast, ChatDeepSeek)
    assert fast.extra_body == {"thinking": {"type": "disabled"}}
    default = make_chat_model(
        config.model, ModelConfig(provider="openai", name="glm-5")
    )
    assert isinstance(default, ChatDeepSeek)
    assert default.extra_body is None


def test_openai_plain_thinking_maps_reasoning_effort() -> None:
    # 非 reasoning 的 openai：thinking 意图翻成 reasoning_effort（此前被静默丢弃）。
    config = AppConfig.from_env({"OPENAI_API_KEY": "sk-test"})

    def effort_of(thinking: bool | None, effort: str | None) -> object:
        model = make_chat_model(
            config.model,
            ModelConfig(
                provider="openai", name="gpt-5", thinking=thinking, effort=effort
            ),
        )
        assert isinstance(model, ChatOpenAI)
        return model.reasoning_effort

    assert effort_of(True, None) == "high"
    assert effort_of(False, None) == "minimal"
    # thinking 未设 → 回落 policy 的 effort（保持旧行为）。
    assert effort_of(None, "medium") == "medium"


def test_anthropic_thinking_maps_effort() -> None:
    # 默认部署模型：thinking 意图翻成 anthropic effort（此前被静默丢弃 → UI 开关无效）。
    config = AppConfig.from_env({"ANTHROPIC_API_KEY": "sk-ant-test"})

    def effort_of(thinking: bool | None, effort: str | None) -> object:
        model = make_chat_model(
            config.model,
            ModelConfig(
                provider="anthropic", name="claude-x", thinking=thinking, effort=effort
            ),
        )
        assert isinstance(model, ChatAnthropic)
        return model.effort

    assert effort_of(True, None) == "high"
    assert effort_of(False, None) == "low"
    assert effort_of(None, "medium") == "medium"


def test_openai_reasoning_without_base_url_fails_loud() -> None:
    config = AppConfig.from_env(
        {"KOKORO_OPENAI_REASONING": "1", "OPENAI_API_KEY": "sk-test"}
    )
    with pytest.raises(ValueError, match="OPENAI_BASE_URL"):
        make_chat_model(config.model, ModelConfig(provider="openai", name="glm-5"))


def test_toolbox_assembly_matrix() -> None:
    # search 配置即挂载：无 provider 只有 fetch；zhipu 配齐挂双件；半配 fail-loud。
    bare = toolbox_from_config(AppConfig.from_env({}))
    assert [tool.name for tool in bare.configured] == ["web_fetch"]
    full = toolbox_from_config(
        AppConfig.from_env(
            {"KOKORO_WEB_SEARCH_PROVIDER": "zhipu", "KOKORO_WEB_SEARCH_API_KEY": "k"}
        )
    )
    assert [tool.name for tool in full.configured] == ["web_fetch", "web_search"]
    searx = toolbox_from_config(
        AppConfig.from_env(
            {
                "KOKORO_WEB_SEARCH_PROVIDER": "searxng",
                "KOKORO_WEB_SEARCH_URL": "https://searx.local",
            }
        )
    )
    assert [tool.name for tool in searx.configured] == ["web_fetch", "web_search"]
    with pytest.raises(ValueError, match="KOKORO_WEB_SEARCH_API_KEY"):
        toolbox_from_config(AppConfig.from_env({"KOKORO_WEB_SEARCH_PROVIDER": "zhipu"}))
    with pytest.raises(ValueError, match="unsupported"):
        toolbox_from_config(
            AppConfig.from_env(
                {"KOKORO_WEB_SEARCH_PROVIDER": "bing", "KOKORO_WEB_SEARCH_API_KEY": "k"}
            )
        )
    # 恒挂底座一口出：租户态 memory 双件在前、配置态在后（挂载序即工具序）。
    assert [tool.name for tool in bare.tools_for("ns-1")] == [
        "save_memory",
        "search_memory",
        "web_fetch",
    ]


def test_recursion_limit_env_parse() -> None:
    assert AppConfig.from_env({}).recursion_limit == 100
    assert AppConfig.from_env({"KOKORO_RECURSION_LIMIT": "12"}).recursion_limit == 12
    with pytest.raises(ValidationError):
        AppConfig.from_env({"KOKORO_RECURSION_LIMIT": "0"})


def test_run_token_budget_env_parse() -> None:
    assert (
        AppConfig.from_env({}).run_token_budget == 0
    )  # 默认关闭：预算数值属政策，不擅代
    assert (
        AppConfig.from_env({"KOKORO_RUN_TOKEN_BUDGET": "200000"}).run_token_budget
        == 200000
    )
    with pytest.raises(ValidationError):
        AppConfig.from_env({"KOKORO_RUN_TOKEN_BUDGET": "-1"})


def test_builtin_subagents_opt_in_matrix() -> None:
    # 用户裁定：实现但默认关闭——显式点名才挂；工具缺任一即整个不挂（不设空壳）。
    assert build_subagent_catalog(None).names() == frozenset()
    with pytest.raises(ValueError, match="unknown built-in"):
        build_subagent_catalog(None, frozenset({"ghost"}))

    catalog = build_subagent_catalog(None, frozenset({"web-researcher"}))
    assert catalog.names() == {"web-researcher"}
    tools = toolbox_from_config(
        AppConfig.from_env(
            {
                "KOKORO_WEB_SEARCH_PROVIDER": "searxng",
                "KOKORO_WEB_SEARCH_URL": "https://searx.local",
            }
        )
    ).configured
    index = {tool.name: tool for tool in tools}
    defs, mounted = catalog_subagents(catalog, index)
    assert mounted == {"web-researcher"}
    assert dict(defs[0]).get("tools") == [index["web_search"], index["web_fetch"]]

    # Catalog 只是部署可用池；某个 Agent 未声明时不得被全局配置隐式装入。
    unselected, selected_names = catalog_subagents(
        catalog, index, selected=frozenset()
    )
    assert unselected == []
    assert selected_names == frozenset()

    # search 未配置 → web_search 缺 → 整个不挂、不进 deny 声明集。
    bare_index = {
        tool.name: tool
        for tool in toolbox_from_config(AppConfig.from_env({})).configured
    }
    defs_bare, mounted_bare = catalog_subagents(catalog, bare_index)
    assert defs_bare == [] and mounted_bare == frozenset()


def test_builtin_subagents_env_parse() -> None:
    config = AppConfig.from_env({"KOKORO_BUILTIN_SUBAGENTS": "web-researcher, "})
    assert config.enabled_builtin_subagents == {"web-researcher"}
    assert AppConfig.from_env({}).enabled_builtin_subagents == frozenset()


def test_general_purpose_override_carries_guards_and_inherits() -> None:
    # 同名覆盖内生 GP：middleware 挂守卫；不带 tools/model 键 = 继承主 agent（GP 语义）。
    guard = TerminalGuardMiddleware(store=FakeRunRepository(), run_id="r1")
    spec = general_purpose_subagent([guard])
    assert spec["name"] == "general-purpose"
    assert spec["description"] and spec["system_prompt"]
    assert dict(spec).get("middleware") == [guard]
    assert "tools" not in spec and "model" not in spec


def test_guards_propagate_to_every_subagent() -> None:
    # 子代理 middleware 链独立：预算/终态闸不逐个下发 = task 委派旁路（真旁路回归钉）。
    guard = TerminalGuardMiddleware(store=FakeRunRepository(), run_id="r1")
    catalog = build_subagent_catalog(None, frozenset({"web-researcher"}))
    tools = toolbox_from_config(
        AppConfig.from_env(
            {
                "KOKORO_WEB_SEARCH_PROVIDER": "searxng",
                "KOKORO_WEB_SEARCH_URL": "https://searx.local",
            }
        )
    ).configured
    index = {tool.name: tool for tool in tools}
    defs, _ = catalog_subagents(catalog, index, [guard])
    assert dict(defs[0]).get("middleware") == [guard]


def test_tool_usage_lives_in_tool_descriptions() -> None:
    # 工具用法经 LangChain 工具 schema 交给模型（description），不进 system prompt。
    from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL
    from kokoro_agent.tools.web_fetch import make_web_fetch_tool
    from kokoro_agent.tools.web_search import (
        SearchProviderSettings,
        make_search_provider,
        make_web_search_tool,
    )

    assert "不连环追问" in ASK_USER_TOOL.description
    assert "附 URL" in make_web_fetch_tool(allow_private=False).description
    search = make_web_search_tool(
        make_search_provider(
            SearchProviderSettings(
                provider="searxng", api_key=None, base_url="https://searx.local"
            )
        )
    )
    assert "交叉核对" in search.description


def test_local_shell_workspace_subdir(tmp_path: Path) -> None:
    # 工作区约定：{root}/{namespace:session_id}/——session files 端点按同约定直读。
    config = AppConfig.from_env({"KOKORO_AGENT_LOCAL_SHELL_ROOT": str(tmp_path)})
    backend = make_backend("local_shell", config.sandbox, workspace="ns:ses_1")
    assert isinstance(backend, LocalShellBackend)
    assert (tmp_path / "ns:ses_1").is_dir()


# --- Agent 定义：注册表分派与能力政策 ---


def test_general_package_pause_policy_includes_ask_user() -> None:
    from kokoro_agent.agents import GENERAL_AGENT

    assert GENERAL_AGENT.pause_tools == frozenset({"ask_user_question"})


def test_interrupt_on_empty_pause_tools_for_studio_types() -> None:
    # 无 chat 面的类型：不注入 ask_user 暂停点，审批工具照常门控。
    interrupt_on = build_interrupt_on(frozenset({"execute"}), pause_tools=frozenset())
    assert set(interrupt_on) == {"execute"}


def test_resolve_tools_empty_core_for_studio_types() -> None:
    from kokoro_agent.tools.registry import resolve_tools

    assert resolve_tools([], core=()) == []
