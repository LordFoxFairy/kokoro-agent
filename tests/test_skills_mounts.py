"""skill 挂载规格：sha256 lock fail-closed + 全文渲染进 system prompt（backend 无关）。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatResult

from fakes import usage_recorder
from kokoro_agent.contract import SkillMount
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.skills.mounts import SKILL_MAX_CHARS, SkillMountError, render_skills_prompt


def _make_skill(root: Path, name: str, body: bytes) -> tuple[str, str]:
    directory = root / name
    directory.mkdir()
    (directory / "SKILL.md").write_bytes(body)
    return str(directory), hashlib.sha256(body).hexdigest()


def test_empty_returns_none() -> None:
    assert render_skills_prompt([]) is None


def test_renders_full_content_in_order(tmp_path: Path) -> None:
    path_a, lock_a = _make_skill(tmp_path, "a", "规则甲：末尾加标记".encode())
    path_b, lock_b = _make_skill(tmp_path, "b", "规则乙：简洁中文".encode())
    prompt = render_skills_prompt(
        [SkillMount(name="a", path=path_a, lock=lock_a), SkillMount(name="b", path=path_b, lock=lock_b)]
    )
    assert prompt is not None
    assert "规则甲：末尾加标记" in prompt and "规则乙：简洁中文" in prompt
    assert prompt.index("规则甲") < prompt.index("规则乙")
    assert "## Skills" in prompt


def test_dedupe_by_path_preserves_first(tmp_path: Path) -> None:
    path_a, lock_a = _make_skill(tmp_path, "a", b"alpha-body")
    prompt = render_skills_prompt(
        [SkillMount(name="a", path=path_a, lock=lock_a), SkillMount(name="a2", path=path_a, lock=lock_a)]
    )
    assert prompt is not None
    assert prompt.count("alpha-body") == 1


def test_lock_mismatch_raises(tmp_path: Path) -> None:
    path_a, _ = _make_skill(tmp_path, "a", b"alpha")
    with pytest.raises(SkillMountError, match="lock mismatch"):
        render_skills_prompt([SkillMount(name="a", path=path_a, lock="0" * 64)])


def test_missing_skill_md_raises(tmp_path: Path) -> None:
    directory = tmp_path / "empty"
    directory.mkdir()
    with pytest.raises(SkillMountError, match="missing SKILL.md"):
        render_skills_prompt([SkillMount(name="empty", path=str(directory), lock="0" * 64)])


def test_oversized_skill_fails_loud(tmp_path: Path) -> None:
    body = ("x" * (SKILL_MAX_CHARS + 1)).encode()
    path_a, lock_a = _make_skill(tmp_path, "big", body)
    with pytest.raises(SkillMountError, match="exceeds"):
        render_skills_prompt([SkillMount(name="big", path=path_a, lock=lock_a)])


async def test_skill_body_reaches_model_system_prompt(tmp_path: Path) -> None:
    # state backend 下 deepagents 渐进披露读不到宿主 SKILL.md（实证），全文注入是 V1 正解：
    # 断言真图里模型收到的 system prompt 含 skill 全文。
    path_a, lock_a = _make_skill(tmp_path, "style", "自我介绍末尾输出 via-skill:v1".encode())
    captured: list[list[BaseMessage]] = []

    class Recorder(LocalFakeChatModel):
        def _generate(
            self,
            messages: list[BaseMessage],
            stop: list[str] | None = None,
            run_manager: CallbackManagerForLLMRun | None = None,
            **kwargs: Any,
        ) -> ChatResult:
            captured.append(list(messages))
            return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)

    prompt = render_skills_prompt([SkillMount(name="style", path=path_a, lock=lock_a)])
    assert prompt is not None
    agent = build_agent(
        model=Recorder.with_script([AIMessage(content="ok")]),
        tools=[],
        system_prompt=f"base\n\n{prompt}",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on={},
    )

    async def claim() -> bool:
        return True

    terminal = await invoke_once(
        RunEmitter(MemoryStream(), "rn"),
        agent,
        "t1",
        {"messages": [HumanMessage(content="hi")]},
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        claim_terminal=claim,
        record_usage=usage_recorder()[0],
    )
    assert terminal is True
    # .text 是框架的文本收窄口（content 联合 → str），不自拆 content 块。
    system_text = "\n".join(
        message.text for message in captured[-1] if message.type == "system"
    )
    assert "via-skill:v1" in system_text
