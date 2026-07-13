"""会话内 swarm 移交规格：候选源 / handoff fail-closed / 人格切轨（active_agent 落 checkpoint）。

端到端「模型驱动移交 + 恢复持轨」走真实 serve 循环，见 tests/e2e/test_local_fake_run.py。
"""

from __future__ import annotations

from types import SimpleNamespace

from langgraph.types import Command

from kokoro_agent.agents.assembly.swarm import (
    HANDOFF_TOOL_NAME,
    make_handoff_tool,
    swap_persona_prompt,
    swarm_candidates,
)
from kokoro_agent.prompts import PromptLibrary
from kokoro_agent.tools.registry import JOURNAL_EXEMPT_TOOLS


# --- 候选源：目录即配置，内置包缺省不算候选（gate 单人格透明） ---


def test_candidates_are_deploy_personas_only() -> None:
    assert swarm_candidates(PromptLibrary({"poet": "p", "researcher": "r"})) == (
        "poet",
        "researcher",
    )
    # 默认部署无 personas_dir → _extra 空 → 零候选（内置 general.md 不计入）。
    assert swarm_candidates(PromptLibrary({})) == ()


def test_names_sorted_for_stable_prefix() -> None:
    assert PromptLibrary({"z": "1", "a": "2", "m": "3"}).names() == ("a", "m", "z")


def test_single_persona_deployment_has_no_swarm() -> None:
    # 候选<=1：不成 swarm（handoff 与人格中间件都不该装配，门在 toolset/pipeline）。
    assert len(swarm_candidates(PromptLibrary({"solo": "s"}))) == 1


# --- handoff 工具：Command 覆盖 active_agent / 未知名 fail-closed / schema 恒定 ---


async def test_handoff_valid_name_emits_command() -> None:
    tool = make_handoff_tool(("poet", "researcher"))
    assert tool.name == HANDOFF_TOOL_NAME
    coroutine = tool.coroutine
    assert coroutine is not None
    result = await coroutine(agent_name="researcher", runtime=SimpleNamespace(tool_call_id="tc1"))
    assert isinstance(result, Command)
    update = result.update
    assert isinstance(update, dict)
    # Command 覆盖 active_agent + 附一条回执 ToolMessage（tool_call_id 正确性由 e2e run.completed 保证）。
    assert update["active_agent"] == "researcher"
    assert "messages" in update


async def test_handoff_unknown_name_fails_closed_without_state_change() -> None:
    tool = make_handoff_tool(("poet", "researcher"))
    coroutine = tool.coroutine
    assert coroutine is not None
    result = await coroutine(agent_name="ghost", runtime=SimpleNamespace(tool_call_id="tc1"))
    # 纠错文本给模型自纠，不改 active_agent、不炸 run。
    assert isinstance(result, str)
    assert result.startswith("error:") and "ghost" in result


def test_handoff_is_journal_exempt() -> None:
    # Command 形态：不落 journal（重放归 checkpoint 的 active_agent LastValue）。
    assert HANDOFF_TOOL_NAME in JOURNAL_EXEMPT_TOOLS


# --- 人格切轨（swap_persona_prompt 纯函数）：定点换前缀，保留底座/技能清单 ---


def _library() -> PromptLibrary:
    return PromptLibrary({"poet": "你是诗人。", "researcher": "你是研究员。"})


def test_swap_passthrough_when_not_handed_off() -> None:
    # active_agent 未设：返回 None（调用方原样透传，与非 swarm 字节等价）。
    assert (
        swap_persona_prompt(
            _library(),
            [],
            initial_prompt="你是诗人。",
            initial_name="poet",
            active_name=None,
            current_prompt="你是诗人。\n\n底座段",
        )
        is None
    )


def test_swap_replaces_persona_prefix_keeps_tail() -> None:
    # 移交到 researcher：人格前缀被替换，底座尾段原样保留。
    assert (
        swap_persona_prompt(
            _library(),
            [],
            initial_prompt="你是诗人。",
            initial_name="poet",
            active_name="researcher",
            current_prompt="你是诗人。\n\n底座段",
        )
        == "你是研究员。\n\n底座段"
    )


def test_swap_noop_when_active_equals_initial() -> None:
    # 移交回自身＝无变更（返回 None）。
    assert (
        swap_persona_prompt(
            _library(),
            [],
            initial_prompt="你是诗人。",
            initial_name="poet",
            active_name="poet",
            current_prompt="你是诗人。\n\n底座段",
        )
        is None
    )


def test_swap_defensive_none_when_prefix_mismatch() -> None:
    # 前缀不匹配（不该命中的防御分支）：返回 None，不做危险的字符串手术。
    assert (
        swap_persona_prompt(
            _library(),
            [],
            initial_prompt="你是诗人。",
            initial_name="poet",
            active_name="researcher",
            current_prompt="完全不同的前缀\n\n底座段",
        )
        is None
    )
