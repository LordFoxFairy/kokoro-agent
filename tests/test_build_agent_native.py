"""The GA graph builder forwards native DeepAgents assembly primitives exactly."""

from __future__ import annotations

from typing import Any

from deepagents.backends.state import StateBackend
from langchain_core.messages import AIMessage

import kokoro_agent.execution.build_agent as subject
from kokoro_agent.model.local_fake import LocalFakeChatModel


class _Graph:
    async def astream_events(self, *args: object, **kwargs: object) -> Any:
        if False:
            yield None

    async def aget_state(self, *args: object, **kwargs: object) -> object:
        return object()


def test_forwards_native_skills_memory_and_explicit_backend(monkeypatch: Any) -> None:
    captured: dict[str, object] = {}

    def create(**kwargs: object) -> _Graph:
        captured.update(kwargs)
        return _Graph()

    monkeypatch.setattr(subject, "create_deep_agent", create)
    backend = StateBackend()
    subject.build_agent(
        model=LocalFakeChatModel.with_script([AIMessage(content="done")]),
        tools=(),
        system_prompt="system",
        subagents=(),
        checkpointer=None,
        permissions=(),
        interrupt_on={},
        backend=backend,
        skills=("/.skills/style-a/",),
        memory=("/.instructions/MEMORY.md",),
    )

    assert captured["backend"] is backend
    assert captured["skills"] == ["/.skills/style-a/"]
    assert captured["memory"] == ["/.instructions/MEMORY.md"]
