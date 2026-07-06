"""装配层规格：AppConfig 解析边界 + 注入式工厂（interrupt_on/目录/沙箱/模型/传输）。"""

from __future__ import annotations

from pathlib import Path

import pytest
from deepagents.backends.local_shell import LocalShellBackend
from langchain_deepseek import ChatDeepSeek
from pydantic import ValidationError

from kokoro_agent.config import AppConfig
from fakes import FakeLedger
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
from kokoro_agent.assets import SkillLibrary, SkillPackage
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
from kokoro_agent.subagents import catalog_subagents, general_purpose_subagent, wire_subagents
from kokoro_agent.worker.main import toolbox_from_config


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


def test_sandbox_e2b_requires_run_scoped_assembly() -> None:
    # e2b 有 run 级生命周期：同步纯函数入口 fail-loud 指路 make_backend_for_run。
    config = AppConfig.from_env({})
    with pytest.raises(ValueError, match="make_backend_for_run"):
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
        agent_type="general",
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
    searx = toolbox_from_config(AppConfig.from_env(
        {"KOKORO_WEB_SEARCH_PROVIDER": "searxng", "KOKORO_WEB_SEARCH_URL": "https://searx.local"}
    ))
    assert [tool.name for tool in searx.configured] == ["web_fetch", "web_search"]
    with pytest.raises(ValueError, match="KOKORO_WEB_SEARCH_API_KEY"):
        toolbox_from_config(AppConfig.from_env({"KOKORO_WEB_SEARCH_PROVIDER": "zhipu"}))
    with pytest.raises(ValueError, match="unsupported"):
        toolbox_from_config(AppConfig.from_env({"KOKORO_WEB_SEARCH_PROVIDER": "bing", "KOKORO_WEB_SEARCH_API_KEY": "k"}))
    # 恒挂底座一口出：租户态 memory 双件在前、配置态在后（挂载序即工具序）。
    assert [tool.name for tool in bare.tools_for("ns-1")] == [
        "save_memory", "search_memory", "web_fetch",
    ]


def test_recursion_limit_env_parse() -> None:
    assert AppConfig.from_env({}).recursion_limit == 100
    assert AppConfig.from_env({"KOKORO_RECURSION_LIMIT": "12"}).recursion_limit == 12
    with pytest.raises(ValidationError):
        AppConfig.from_env({"KOKORO_RECURSION_LIMIT": "0"})


def testwire_subagents_tools_and_model_passthrough() -> None:
    # wire 预设声明的 tools/model 必须生效或炸——静默丢弃是最坏失效（真实缺陷回归钉）。
    fetch = toolbox_from_config(AppConfig.from_env({})).configured[0]
    fake_model = LocalFakeChatModel.with_script([])

    def request_with(sub: SubagentDef) -> RunRequest:
        return RunRequest(
            kind="run.request",
            run_id="r1",
            thread_id="t1",
            input=RunInput(message_id="m1", content="hi"),
            context=RuntimeContext(namespace="ns", session_id="s1"),
            runtime=RuntimeConfig(
        agent_type="general",
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
        name="poet", description="d", system_prompt="p", tools=["web_fetch"], skills=[],
        model=ModelConfig(provider="anthropic", name="claude"),
    )
    subs = wire_subagents(request_with(spec), {"web_fetch": fetch}, lambda _m: fake_model)
    first = dict(subs[0])
    assert first.get("tools") == [fetch]
    assert first.get("model") is fake_model

    inherit = SubagentDef(name="poet", description="d", system_prompt="p", tools=[], skills=[])
    plain = wire_subagents(request_with(inherit), {}, lambda _m: fake_model)
    assert "tools" not in plain[0]
    assert "model" not in plain[0]

    ghost = SubagentDef(name="poet", description="d", system_prompt="p", tools=["nope"], skills=[])
    with pytest.raises(ValueError, match="unknown tools"):
        wire_subagents(request_with(ghost), {"web_fetch": fetch}, lambda _m: fake_model)

    # 入口对偶性：成品降格为子代理时声明的注册表工具可以不在主 agent 工具集里——
    # 主 index 优先复用（政策实例），miss 走注册表独立解析，仍未知才 fail-loud。
    dual = SubagentDef(
        name="asker", description="d", system_prompt="p", tools=["ask_user_question"], skills=[]
    )
    from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL

    resolved: list[dict[str, object]] = [dict(sub) for sub in wire_subagents(
        request_with(dual), {"web_fetch": fetch}, lambda _m: fake_model
    )]
    assert resolved[0].get("tools") == [ASK_USER_TOOL]


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
    tools = toolbox_from_config(AppConfig.from_env(
        {"KOKORO_WEB_SEARCH_PROVIDER": "searxng", "KOKORO_WEB_SEARCH_URL": "https://searx.local"}
    )).configured
    index = {tool.name: tool for tool in tools}
    defs, mounted = catalog_subagents(catalog, index)
    assert mounted == {"web-researcher"}
    assert dict(defs[0]).get("tools") == [index["web_search"], index["web_fetch"]]

    # search 未配置 → web_search 缺 → 整个不挂、不进 deny 声明集。
    bare_index = {tool.name: tool for tool in toolbox_from_config(AppConfig.from_env({})).configured}
    defs_bare, mounted_bare = catalog_subagents(catalog, bare_index)
    assert defs_bare == [] and mounted_bare == frozenset()


def test_builtin_subagents_env_parse() -> None:
    config = AppConfig.from_env({"KOKORO_BUILTIN_SUBAGENTS": "web-researcher, "})
    assert config.enabled_builtin_subagents == {"web-researcher"}
    assert AppConfig.from_env({}).enabled_builtin_subagents == frozenset()



def test_general_purpose_override_carries_guards_and_inherits() -> None:
    # 同名覆盖内生 GP：middleware 挂守卫；不带 tools/model 键 = 继承主 agent（GP 语义）。
    guard = TerminalGuardMiddleware(store=FakeLedger(), run_id="r1")
    spec = general_purpose_subagent([guard])
    assert spec["name"] == "general-purpose"
    assert spec["description"] and spec["system_prompt"]
    assert dict(spec).get("middleware") == [guard]
    assert "tools" not in spec and "model" not in spec


def test_guards_propagate_to_every_subagent() -> None:
    # 子代理 middleware 链独立：预算/终态闸不逐个下发 = task 委派旁路（真旁路回归钉）。
    guard = TerminalGuardMiddleware(store=FakeLedger(), run_id="r1")
    catalog = build_catalog(None, frozenset({"web-researcher"}))
    tools = toolbox_from_config(AppConfig.from_env(
        {"KOKORO_WEB_SEARCH_PROVIDER": "searxng", "KOKORO_WEB_SEARCH_URL": "https://searx.local"}
    )).configured
    index = {tool.name: tool for tool in tools}
    defs, _ = catalog_subagents(catalog, index, [guard])
    assert dict(defs[0]).get("middleware") == [guard]


def test_tool_usage_lives_in_tool_descriptions() -> None:
    # 工具用法经 LangChain 工具 schema 交给模型（description），不进 system prompt。
    from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL
    from kokoro_agent.tools.web_fetch import make_web_fetch_tool
    from kokoro_agent.tools.web_search import SearchProviderSettings, make_search_provider, make_web_search_tool

    assert "不连环追问" in ASK_USER_TOOL.description
    assert "附 URL" in make_web_fetch_tool(allow_private=False).description
    search = make_web_search_tool(make_search_provider(
        SearchProviderSettings(provider="searxng", api_key=None, base_url="https://searx.local")
    ))
    assert "交叉核对" in search.description


def test_local_shell_workspace_subdir(tmp_path: Path) -> None:
    # 工作区约定：{root}/{namespace:session_id}/——session files 端点按同约定直读。
    config = AppConfig.from_env({"KOKORO_AGENT_LOCAL_SHELL_ROOT": str(tmp_path)})
    backend = make_backend("local_shell", config.sandbox, workspace="ns:ses_1")
    assert isinstance(backend, LocalShellBackend)
    assert (tmp_path / "ns:ses_1").is_dir()


# --- 业务包层（agents/<type>/）：注册表分派与类型政策 ---


def test_agent_type_registry_covers_contract_enum() -> None:
    # 契约枚举扩值忘注册业务包 = 此处爆炸（装配期 NotImplementedError 提前到测试期）。
    from typing import get_args

    from kokoro_agent.agents import FACTORIES
    from kokoro_agent.contract import AgentType

    assert set(FACTORIES) == set(get_args(AgentType))


def test_general_package_pause_policy_includes_ask_user() -> None:
    from kokoro_agent.agents import FACTORIES

    assert FACTORIES["general"].pause_tools == frozenset({"ask_user_question"})


def test_approval_names_unions_wire_and_package_policy() -> None:
    from kokoro_agent.agents import approval_names

    request = RunRequest(
        kind="run.request",
        run_id="r1",
        thread_id="c1",
        input=RunInput(message_id="m1", content="hi"),
        runtime=RuntimeConfig(
            agent_type="general",
            model=ModelConfig(provider="anthropic", name="claude"),
            tools=[],
            skills=[],
            mcp=[],
            subagents=[],
            backend="state",
            permissions=Permissions(
                approval_tools=["execute"], review_tools=[],
                subagent_create="ask", filesystem="read_only",
            ),
        ),
        context=RuntimeContext(namespace="ns", session_id="s1"),
    )
    assert approval_names(request) == frozenset({"execute", "ask_user_question", "task"})


def test_interrupt_on_empty_pause_tools_for_studio_types() -> None:
    # 无 chat 面的类型：不注入 ask_user 暂停点，审批工具照常门控。
    interrupt_on = build_interrupt_on(frozenset({"execute"}), pause_tools=frozenset())
    assert set(interrupt_on) == {"execute"}


def test_resolve_tools_empty_core_for_studio_types() -> None:
    from kokoro_agent.tools.registry import resolve_tools

    assert resolve_tools([], core=()) == []


# --- 资产化：skills 按名 + persona 按名（配置引用资产，不内联资产） ---


def test_persona_library_deploy_dir_overrides_builtin(tmp_path: Path) -> None:
    from kokoro_agent.assets import LocalAssets, LocalAssetSource, PersonaLibrary

    (tmp_path / "general.md").write_text("部署覆盖人格")
    (tmp_path / "poet.md").write_text("诗人人格")
    source = LocalAssetSource(LocalAssets(type="local", personas_dir=str(tmp_path)))
    library = PersonaLibrary(source.load_personas())
    assert library.get("general") == "部署覆盖人格"
    assert library.get("poet") == "诗人人格"
    assert PersonaLibrary({}).get("poet") is None  # 内置包无此资产
    assert PersonaLibrary({}).get("general") is not None  # 内置缺省人格恒在


def test_wire_subagent_persona_resolution(tmp_path: Path) -> None:
    from kokoro_agent.assets import LocalAssets, LocalAssetSource, PersonaLibrary
    from kokoro_agent.subagents import wire_subagents

    (tmp_path / "poet.md").write_text("诗人资产人格")
    personas = PersonaLibrary(
        LocalAssetSource(LocalAssets(type="local", personas_dir=str(tmp_path))).load_personas()
    )
    request = RunRequest(
        kind="run.request", run_id="r1", thread_id="c1",
        input=RunInput(message_id="m1", content="hi"),
        runtime=RuntimeConfig(
            agent_type="general",
            model=ModelConfig(provider="anthropic", name="claude"),
            tools=[], skills=[], mcp=[],
            subagents=[
                SubagentDef(name="poet", description="诗", tools=[], skills=[]),  # 无内联 → 资产解析
                SubagentDef(name="critic", description="评", system_prompt="内联覆盖", tools=[], skills=[]),
            ],
            backend="state",
            permissions=Permissions(
                approval_tools=[], review_tools=[], subagent_create="deny", filesystem="read_only",
            ),
        ),
        context=RuntimeContext(namespace="ns", session_id="s1"),
    )
    subs = wire_subagents(request, {}, lambda spec: LocalFakeChatModel(), personas=personas)
    by_name = {sub["name"]: sub["system_prompt"] for sub in subs}
    assert by_name == {"poet": "诗人资产人格", "critic": "内联覆盖"}


def test_wire_subagent_without_any_persona_fails_loud() -> None:
    from kokoro_agent.assets import PersonaLibrary
    from kokoro_agent.subagents import wire_subagents

    request = RunRequest(
        kind="run.request", run_id="r1", thread_id="c1",
        input=RunInput(message_id="m1", content="hi"),
        runtime=RuntimeConfig(
            agent_type="general",
            model=ModelConfig(provider="anthropic", name="claude"),
            tools=[], skills=[], mcp=[],
            subagents=[SubagentDef(name="ghost", description="?", tools=[], skills=[])],
            backend="state",
            permissions=Permissions(
                approval_tools=[], review_tools=[], subagent_create="deny", filesystem="read_only",
            ),
        ),
        context=RuntimeContext(namespace="ns", session_id="s1"),
    )
    with pytest.raises(ValueError, match="no persona"):
        wire_subagents(request, {}, lambda spec: LocalFakeChatModel(), personas=PersonaLibrary({}))


# --- Skills V2 供给器（分发→供给→消费三层的中间层） ---


def _skills_runtime(main: list[str], sub: list[str]) -> RuntimeConfig:
    return RuntimeConfig(
        agent_type="general",
        model=ModelConfig(provider="anthropic", name="claude"),
        tools=[], skills=main, mcp=[],
        subagents=[
            SubagentDef(name="poet", description="诗", system_prompt="诗人", tools=[], skills=sub),
        ],
        backend="state",
        permissions=Permissions(
            approval_tools=[], review_tools=[], subagent_create="deny", filesystem="read_only",
        ),
    )


def _library_two() -> SkillLibrary:
    return SkillLibrary({
        "style": SkillPackage(
            name="style", description="风格",
            files={"SKILL.md": "---\nname: style\ndescription: 风格\n---\n正文A", "helper.md": "辅"},
        ),
        "tone": SkillPackage(
            name="tone", description="语气",
            files={"SKILL.md": "---\nname: tone\ndescription: 语气\n---\n正文B"},
        ),
    })


async def test_provision_state_backend_builds_initial_files_with_prefix_isolation() -> None:
    from kokoro_agent.agents.general.skills import provision_skills
    from kokoro_agent.assets import MAIN_SKILLS_SOURCE, subagent_skills_source

    provisioned = await provision_skills(_skills_runtime(["style"], ["tone"]), _library_two(), None)
    assert provisioned.sources == (MAIN_SKILLS_SOURCE,)
    paths = set(provisioned.initial_files)
    # 主/子代理前缀隔离：各自只见各自授权包；整包（含辅助文件）供给。
    assert f"{MAIN_SKILLS_SOURCE}style/SKILL.md" in paths
    assert f"{MAIN_SKILLS_SOURCE}style/helper.md" in paths
    assert f"{subagent_skills_source('poet')}tone/SKILL.md" in paths
    assert not any(p.startswith(f"{MAIN_SKILLS_SOURCE}tone/") for p in paths)
    # FileData 官方口径（content 字段）。
    entry = provisioned.initial_files[f"{MAIN_SKILLS_SOURCE}style/SKILL.md"]
    assert "正文A" in entry["content"]


async def test_provision_real_backend_uploads_and_leaves_initial_empty() -> None:
    from deepagents.backends.protocol import FileUploadResponse

    from kokoro_agent.agents.general.skills import provision_skills
    from kokoro_agent.assets import MAIN_SKILLS_SOURCE

    uploaded: list[tuple[str, bytes]] = []

    class FakeBackend:
        def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
            uploaded.extend(files)
            return []

    provisioned = await provision_skills(_skills_runtime(["style"], []), _library_two(), FakeBackend())
    assert provisioned.initial_files == {}
    assert provisioned.sources == (MAIN_SKILLS_SOURCE,)
    assert [p for p, _ in uploaded] == [
        f"{MAIN_SKILLS_SOURCE}style/SKILL.md",
        f"{MAIN_SKILLS_SOURCE}style/helper.md",
    ]


async def test_provision_unknown_skill_fails_loud() -> None:
    import pytest as _pytest

    from kokoro_agent.agents.general.skills import provision_skills
    from kokoro_agent.assets import SkillAssetError

    with _pytest.raises(SkillAssetError, match="ghost"):
        await provision_skills(_skills_runtime(["ghost"], []), _library_two(), None)


def test_wire_subagent_carries_native_skills_source() -> None:
    from kokoro_agent.assets import subagent_skills_source
    from kokoro_agent.subagents import wire_subagents

    request = RunRequest(
        kind="run.request", run_id="r1", thread_id="c1",
        input=RunInput(message_id="m1", content="hi"),
        runtime=_skills_runtime([], ["tone"]),
        context=RuntimeContext(namespace="ns", session_id="s1"),
    )
    subs = wire_subagents(request, {}, lambda spec: LocalFakeChatModel())
    # SubAgent.skills 是 NotRequired 键：先证存在再取值（.get 在该 TypedDict 上类型面不全）。
    assert "skills" in subs[0]
    assert subs[0]["skills"] == [subagent_skills_source("poet")]
