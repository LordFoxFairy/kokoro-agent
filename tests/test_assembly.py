"""装配层规格：AppConfig 解析边界 + 注入式工厂（interrupt_on/目录/沙箱/模型/传输）。"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from deepagents.backends.local_shell import LocalShellBackend
from pydantic import ValidationError

from kokoro_agent.config import AppConfig
from fakes import FakeLedger, request
from kokoro_agent.tools.middleware import TerminalGuardMiddleware
from kokoro_agent.contract import (
    McpGrant,
    ExecutionContextIntentRoot,
    ModelConfig,
    Permissions,
    RunInput,
    RunRequest,
    RuntimeConfig,
    RuntimeContext,
)
from kokoro_agent.model.factory import make_chat_model
from langchain_core.messages import AIMessage, HumanMessage

from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend
from kokoro_agent.streams.factory import StreamSettings, make_stream
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.subagents import build_catalog
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.tools.registry import resolve_tools
from kokoro_agent.subagents import catalog_subagents, general_purpose_subagent
from kokoro_agent.worker.main import toolbox_from_config


def test_defaults_from_empty_env() -> None:
    # 存储收敛后唯一真后端：stream=redis、checkpoint/ledger=mongo（无 backend 选择枚举）。
    config = AppConfig.from_env({})
    assert config.stream.redis_url == "redis://127.0.0.1:6379/0"
    assert config.checkpoint.mongo_url == (
        "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true"
    )
    assert config.ledger.mongo_url == (
        "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true"
    )
    assert config.model.local_fake is False
    assert config.ledger.lease_ttl_ms == 90_000
    assert config.lease_heartbeat_s == 30.0
    assert config.custom_subagents_json is None


def test_env_overrides() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_REDIS_URL": "redis://example:6379/1",
            "KOKORO_MONGO_URL": "mongodb://example:27017",
            "KOKORO_LEASE_TTL_S": "10",
            "KOKORO_LEASE_HEARTBEAT_S": "2.5",
            "KOKORO_LOCAL_FAKE_MODEL": "1",
            "KOKORO_CUSTOM_SUBAGENTS": "[]",
        }
    )
    assert config.model.local_fake is True
    assert config.stream.redis_url == "redis://example:6379/1"
    assert config.checkpoint.mongo_url == "mongodb://example:27017"
    assert config.ledger.mongo_url == "mongodb://example:27017"
    assert config.ledger.lease_ttl_ms == 10_000
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
    assert interrupt_on["ask_user_question"]["allowed_decisions"] == ["respond"]


def test_interrupt_on_gates_approval_tools() -> None:
    interrupt_on = build_interrupt_on(frozenset({"execute", "deploy"}))
    assert set(interrupt_on) == {"ask_user_question", "execute", "deploy"}
    assert interrupt_on["execute"]["allowed_decisions"] == ["approve", "edit", "reject"]


def test_interrupt_on_ask_user_stays_respond_only() -> None:
    # ask_user 即便被误列进审批集合，也不得降级为 approve/edit/reject。
    interrupt_on = build_interrupt_on(frozenset({"ask_user_question"}))
    assert interrupt_on["ask_user_question"]["allowed_decisions"] == ["respond"]


def test_interrupt_on_plan_is_forced_to_approve_or_reject() -> None:
    interrupt_on = build_interrupt_on(
        frozenset({"propose_plan"}),
        plan_tools=frozenset({"propose_plan"}),
    )
    assert interrupt_on["propose_plan"]["allowed_decisions"] == ["approve", "reject"]


def test_propose_plan_cannot_be_a_result_review_tool() -> None:
    from kokoro_agent.agents.assembly.guardrails import build_guard_chains

    deps: Any = object()
    with pytest.raises(ValueError, match="propose_plan cannot be a result-review tool"):
        build_guard_chains(deps, request("r-plan", review_tools=("propose_plan",)))


def test_filesystem_permissions_by_perm() -> None:
    writes = build_filesystem_permissions("workspace_write")
    assert len(writes) == 1
    assert writes[0].paths == ["/.skills/**"]
    denies = build_filesystem_permissions("read_only")
    assert len(denies) == 1


# --- 子代理目录 ---


def test_catalog_builtin_empty_until_real_capability() -> None:
    # 内建目录只收带真实工具的真能力：现阶段为空，prompt 预设归 namespace（wire）。
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


def test_sandbox_state_backend_is_explicit() -> None:
    from deepagents.backends.state import StateBackend

    config = AppConfig.from_env({})
    assert isinstance(make_backend("state", config.sandbox), StateBackend)


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
    model = make_chat_model(config.model, ModelConfig(provider="anthropic", name="claude", authorization_handle="model-authz:test"))
    assert isinstance(model, LocalFakeChatModel)


def test_local_fake_hitl_script_switch() -> None:
    config = AppConfig.from_env({"KOKORO_LOCAL_FAKE_MODEL": "1", "KOKORO_LOCAL_FAKE_SCRIPT": "hitl"})
    model = make_chat_model(config.model, ModelConfig(provider="anthropic", name="claude", authorization_handle="model-authz:test"))
    assert isinstance(model, LocalFakeChatModel)
    first = model.invoke([HumanMessage(content="hi")])
    assert isinstance(first, AIMessage)
    assert first.tool_calls and first.tool_calls[0]["name"] == "ask_user_question"


def test_make_stream_backends() -> None:
    # 存储收敛：传输唯一真后端是 redis（无 backend 选择字段）。
    assert isinstance(
        make_stream(StreamSettings(redis_url="redis://127.0.0.1:6379/0")),
        RedisStream,
    )


def test_wire_rejects_inline_definitions() -> None:
    # 契约负向钉（块2）：wire 只传 names+已解析上下文，一切定义/凭据/投机字段被 strict 契约拒收。
    base = RuntimeConfig(
        agent_catalog_ref=f"agent-catalog:sha256:{'a' * 64}",
        agent_type="general",
        agent="general",
        model=ModelConfig(provider="anthropic", name="claude", authorization_handle="model-authz:test"),
        tools=[],
        skills=[],
        # McpGrant 授权卡（scope,name,revision,config_hash）合法；一切内联定义/凭据被拒。
        mcp_servers=[McpGrant(option_ref="mcp:docs", scope="official", name="docs", revision=1, config_hash="a" * 64)],
        subagents=["web-researcher"],
        backend="state",
        permissions=Permissions(
            approval_tools=[], review_tools=[], subagent_create="deny", filesystem="read_only"
        ),
    ).model_dump()
    # 客户端供给系统提示词：安全洞+破坏前缀稳定，字段已删。
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate({**base, "system_prompt": "注入 prompt"})
    # 旧 McpServer 对象形状（含明文 headers）：凭据不上 wire/ledger。
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate(
            {**base, "mcp_servers": [{"name": "m", "url": "http://x", "headers": {"a": "秘"}}]}
        )
    # 子代理内联定义：定义住 agent 侧 catalog/资产库。
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate(
            {**base, "subagents": [{"name": "poet", "system_prompt": "内联"}]}
        )
    # swarm_members：P2 前的投机字段已删。
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate({**base, "swarm_members": ["poet"]})


def test_interrupt_on_subagent_create_ask_gates_task() -> None:
    gated = build_interrupt_on(frozenset(), subagent_create="ask")
    assert set(gated) == {"ask_user_question", "task"}
    assert gated["task"]["allowed_decisions"] == ["approve", "edit", "reject"]
    # deny/allow 不进暂停门控（deny 由 middleware fail-closed，allow 直接放行）。
    for policy in ("deny", "allow"):
        assert "task" not in build_interrupt_on(frozenset(), subagent_create=policy)


def test_core_tools_always_mounted_and_legacy_memory_catalog_is_rejected() -> None:
    # ask_user + 专用计划 proposal 恒挂载；ADR-013 M0 禁止旧 Mongo memory 工具进入生产目录。
    assert [tool.name for tool in resolve_tools([])] == ["ask_user_question", "propose_plan"]
    for tool_name in ("save_memory", "search_memory"):
        with pytest.raises(ValueError, match="LEGACY_STORE_MEMORY_TOOLS_DISABLED"):
            resolve_tools([tool_name])


def test_general_prompt_separates_user_plan_from_internal_todos() -> None:
    from kokoro_agent.prompts import GENERAL_PROMPT

    assert "propose_plan" in GENERAL_PROMPT
    assert "write_todos" in GENERAL_PROMPT
    assert "唯一工具调用" in GENERAL_PROMPT
    assert "不能替代" in GENERAL_PROMPT


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
    # 生产底座不再实例化旧 Mongo memory 双件；namespace 参数不扩张其能力面。
    assert [tool.name for tool in bare.tools_for("ns-1")] == ["web_fetch"]


def test_legacy_memory_process_config_fails_closed() -> None:
    # 这是历史上真实存在的启用入口；不能因 AppConfig.extra=ignore 而静默继续启动。
    with pytest.raises(ValueError, match="LEGACY_STORE_MEMORY_CONFIG_DISABLED"):
        AppConfig.from_env({"KOKORO_AGENT_MEMORY": "save_memory,search_memory"})
    # 空值等同未启用，便于 Secret/Deployment 模板保留空占位。
    assert AppConfig.from_env({"KOKORO_AGENT_MEMORY": "   "}).mongo_db == "kokoro"


async def test_legacy_memory_run_catalog_fails_before_remote_resolution() -> None:
    from kokoro_agent.agents.assembly.pipeline import assemble_agent

    class NeverResolveCapabilities:
        called = False

        async def resolve(self, *args: object) -> object:
            self.called = True
            raise AssertionError("legacy catalog must fail before Hub resolution")

    capabilities = NeverResolveCapabilities()
    policy: Any = object()
    deps: Any = SimpleNamespace(capabilities=capabilities)
    original = request("legacy-memory-catalog")
    stale = original.model_copy(
        update={"runtime": original.runtime.model_copy(update={"tools": ["save_memory"]})}
    )
    with pytest.raises(ValueError, match="LEGACY_STORE_MEMORY_TOOLS_DISABLED"):
        await assemble_agent(policy, deps, stale)
    assert capabilities.called is False


def test_recursion_limit_env_parse() -> None:
    assert AppConfig.from_env({}).recursion_limit == 100
    assert AppConfig.from_env({"KOKORO_RECURSION_LIMIT": "12"}).recursion_limit == 12
    with pytest.raises(ValidationError):
        AppConfig.from_env({"KOKORO_RECURSION_LIMIT": "0"})


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


def test_catalog_subagents_mount_only_exact_runtime_grants() -> None:
    from kokoro_agent.subagents import RegisteredSubagent, SubagentCatalog

    catalog = SubagentCatalog(
        (
            RegisteredSubagent(
                name="alpha", description="a", system_prompt="a", source="config-custom"
            ),
            RegisteredSubagent(
                name="beta", description="b", system_prompt="b", source="config-custom"
            ),
        )
    )
    definitions, mounted = catalog_subagents(
        catalog, {}, selected_names=frozenset({"alpha"})
    )
    assert [definition["name"] for definition in definitions] == ["alpha"]
    assert mounted == {"alpha"}
    with pytest.raises(ValueError, match="unknown subagent grants"):
        catalog_subagents(catalog, {}, selected_names=frozenset({"ghost"}))


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


def test_general_purpose_never_inherits_plan_owner_tool() -> None:
    from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL
    from kokoro_agent.tools.propose_plan import PROPOSE_PLAN_TOOL

    spec = general_purpose_subagent(tools=[ASK_USER_TOOL, PROPOSE_PLAN_TOOL])
    assert dict(spec).get("tools") == [ASK_USER_TOOL]


def test_catalog_subagent_cannot_mount_plan_owner_tool() -> None:
    from kokoro_agent.subagents import RegisteredSubagent, SubagentCatalog
    from kokoro_agent.tools.propose_plan import PROPOSE_PLAN_TOOL

    catalog = SubagentCatalog(
        (
            RegisteredSubagent(
                name="planner",
                description="bad nested planner",
                system_prompt="plan",
                source="config-custom",
                tools=("propose_plan",),
            ),
        )
    )
    with pytest.raises(ValueError, match="cannot mount main-agent-only tool 'propose_plan'"):
        catalog_subagents(catalog, {"propose_plan": PROPOSE_PLAN_TOOL})


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
    assert FACTORIES["general"].plan_tools == frozenset({"propose_plan"})


def test_approval_names_unions_wire_and_package_policy() -> None:
    from kokoro_agent.agents import approval_names

    request = RunRequest(
        kind="run.request",
        run_id="r1",
        thread_id="c1",
        input=RunInput(message_id="m1", content="hi"),
        runtime=RuntimeConfig(
            agent_catalog_ref=f"agent-catalog:sha256:{'a' * 64}",
            agent_type="general",
            model=ModelConfig(provider="anthropic", name="claude", authorization_handle="model-authz:test"),
            tools=[],
            skills=[],
            mcp_servers=[],
            subagents=[],
            backend="state",
            permissions=Permissions(
                approval_tools=["execute"], review_tools=[],
                subagent_create="ask", filesystem="read_only",
            ),
        ),
        context=RuntimeContext(namespace="ns", session_id="s1"),
        execution_context=ExecutionContextIntentRoot(mode="root"),
    )
    assert approval_names(request) == frozenset(
        {"execute", "ask_user_question", "propose_plan", "task"}
    )


def test_interrupt_on_empty_pause_tools_for_studio_types() -> None:
    # 无 chat 面的类型：不注入 ask_user 暂停点，审批工具照常门控。
    interrupt_on = build_interrupt_on(frozenset({"execute"}), pause_tools=frozenset())
    assert set(interrupt_on) == {"execute"}


def test_resolve_tools_empty_core_for_studio_types() -> None:
    from kokoro_agent.tools.registry import resolve_tools

    assert resolve_tools([], core=()) == []


# --- 资产化：skills 按名 + prompt 按名（配置引用资产，不内联资产） ---


def _runtime_with_agent(agent: str | None) -> RuntimeConfig:
    return RuntimeConfig(
        agent_catalog_ref=f"agent-catalog:sha256:{'a' * 64}",
        agent_type="general",
        agent=agent,
        model=ModelConfig(provider="anthropic", name="claude", authorization_handle="model-authz:test"),
        tools=[],
        skills=[],
        mcp_servers=[],
        subagents=[],
        backend="state",
        permissions=Permissions(
            approval_tools=[], review_tools=[], subagent_create="deny", filesystem="read_only"
        ),
    )


def test_named_preset_resolves_to_its_persona() -> None:
    # preset=资产库里的具名 persona；装配 prompt 取它，不取类型缺省。
    from kokoro_agent.agents.assembly import resolve_system_prompt
    from kokoro_agent.prompts import PromptLibrary

    library = PromptLibrary({"researcher": "你是严谨的研究员。"})
    resolved = resolve_system_prompt(
        _runtime_with_agent("researcher"), library, default="通用缺省"
    )
    assert resolved == "你是严谨的研究员。"


def test_unknown_preset_fails_loud_not_silent_general() -> None:
    # 名字带了却无资产 = fail-loud（supervisor 收口 assembly_failed），绝不静默回退 general。
    from kokoro_agent.agents.assembly import PresetNotFoundError, resolve_system_prompt
    from kokoro_agent.prompts import PromptLibrary

    with pytest.raises(PresetNotFoundError, match="ghost"):
        resolve_system_prompt(_runtime_with_agent("ghost"), PromptLibrary({}), default="通用缺省")


def test_absent_agent_keeps_type_default_general() -> None:
    # 缺省缺席（未带 agent 名）= 类型工厂末级缺省，general 行为不变。
    from kokoro_agent.agents.assembly import resolve_system_prompt
    from kokoro_agent.prompts import PromptLibrary

    resolved = resolve_system_prompt(_runtime_with_agent(None), PromptLibrary({}), default="通用缺省")
    assert resolved == "通用缺省"


def test_preset_file_on_disk_flows_into_assembly(tmp_path: Path) -> None:
    # 目录即配置：往 personas_dir 丢一个 <agent>.md（零代码），装配即走它的 system prompt。
    from kokoro_agent.agents.assembly import resolve_system_prompt
    from kokoro_agent.content_source import LocalAssets, LocalAssetSource
    from kokoro_agent.prompts import PromptLibrary

    (tmp_path / "researcher.md").write_text("你是严谨的研究员。", encoding="utf-8")
    source = LocalAssetSource(LocalAssets(type="local", personas_dir=str(tmp_path)))
    library = PromptLibrary(source.load_personas())
    resolved = resolve_system_prompt(
        _runtime_with_agent("researcher"), library, default="通用缺省"
    )
    assert resolved == "你是严谨的研究员。"


def test_prompt_library_deploy_dir_overrides_builtin(tmp_path: Path) -> None:
    from kokoro_agent.content_source import LocalAssets, LocalAssetSource
    from kokoro_agent.prompts import PromptLibrary

    (tmp_path / "general.md").write_text("部署覆盖 prompt")
    (tmp_path / "poet.md").write_text("诗人 prompt")
    source = LocalAssetSource(LocalAssets(type="local", personas_dir=str(tmp_path)))
    library = PromptLibrary(source.load_personas())
    assert library.get("general") == "部署覆盖 prompt"
    assert library.get("poet") == "诗人 prompt"
    assert PromptLibrary({}).get("poet") is None  # 内置包无此资产
    assert PromptLibrary({}).get("general") is not None  # 内置缺省 prompt 恒在
