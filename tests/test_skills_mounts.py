"""skill 挂载规格：sha256 lock fail-closed + 全文渲染进 system prompt（backend 无关）。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatResult

from fakes import usage_recorder
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.skills.mounts import SkillLibrary, SkillMountError, load_skill_library


def _library(tmp_path: Path, skills: dict[str, str]) -> SkillLibrary:
    for name, content in skills.items():
        d = tmp_path / name
        d.mkdir()
        (d / "SKILL.md").write_text(content)
    return load_skill_library(str(tmp_path))


def test_unconfigured_dir_is_empty_library() -> None:
    library = load_skill_library(None)
    assert library.names() == frozenset()
    assert library.render_prompt([]) is None


def test_renders_full_content_in_order(tmp_path: Path) -> None:
    library = _library(tmp_path, {"alpha": "A 内容", "beta": "B 内容"})
    prompt = library.render_prompt(["beta", "alpha"])
    assert prompt is not None
    assert prompt.index("### Skill: beta") < prompt.index("### Skill: alpha")
    assert "A 内容" in prompt and "B 内容" in prompt


def test_dedupe_by_name_preserves_first(tmp_path: Path) -> None:
    library = _library(tmp_path, {"alpha": "A"})
    prompt = library.render_prompt(["alpha", "alpha"])
    assert prompt is not None
    assert prompt.count("### Skill: alpha") == 1


def test_unknown_name_fails_loud(tmp_path: Path) -> None:
    library = _library(tmp_path, {"alpha": "A"})
    with pytest.raises(SkillMountError, match="ghost"):
        library.render_prompt(["ghost"])


def test_tamper_after_start_fails_loud(tmp_path: Path) -> None:
    # 启动即锁：运行期内容被改 → 装配期锁不符爆炸。
    library = _library(tmp_path, {"alpha": "原文"})
    (tmp_path / "alpha" / "SKILL.md").write_text("被篡改")
    with pytest.raises(SkillMountError, match="content changed"):
        library.render_prompt(["alpha"])


def test_dir_without_skill_md_fails_loud(tmp_path: Path) -> None:
    (tmp_path / "broken").mkdir()
    with pytest.raises(SkillMountError, match="no SKILL.md"):
        load_skill_library(str(tmp_path))


def test_oversized_skill_fails_loud(tmp_path: Path) -> None:
    library = _library(tmp_path, {"big": "x" * 33_000})
    with pytest.raises(SkillMountError, match="exceeds"):
        library.render_prompt(["big"])


@pytest.mark.asyncio
async def test_skill_body_reaches_model_system_prompt(tmp_path: Path) -> None:
    # state backend 下 deepagents 渐进披露读不到宿主 SKILL.md（实证），全文注入是 V1 正解：
    # 断言真图里模型收到的 system prompt 含 skill 全文。
    d = tmp_path / "style"
    d.mkdir()
    (d / "SKILL.md").write_text("自我介绍末尾输出 via-skill:v1")
    library = load_skill_library(str(tmp_path))
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

    prompt = library.render_prompt(["style"])
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
