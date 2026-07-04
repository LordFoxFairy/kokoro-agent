"""产物面真图规格：export_artifact → 共享库落字节 → wire tool.returned.artifact 引用。"""

from __future__ import annotations

import base64
from pathlib import Path

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver

from fakes import FakeBus, usage_recorder
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.storage.artifacts import DirArtifactStore
from kokoro_agent.tools.export_artifact import make_export_artifact_tool
from kokoro_agent.tools.permissions import build_interrupt_on

_WAV = b"RIFF$\x00\x00\x00WAVEfmt "  # 头部片段足矣：全链只关心字节一致性


async def test_export_artifact_full_chain(tmp_path: Path) -> None:
    store = DirArtifactStore(str(tmp_path / "arts"))
    tool = make_export_artifact_tool(store, "r-art")
    model = LocalFakeChatModel.with_script([
        AIMessage(content="", tool_calls=[{
            "name": "export_artifact",
            "args": {"name": "track.wav", "mime": "audio/wav",
                     "content": base64.b64encode(_WAV).decode(), "encoding": "base64"},
            "id": "t-exp", "type": "tool_call"}]),
        AIMessage(content="导出完成"),
    ])
    agent = build_agent(
        model=model, tools=[tool], system_prompt="x", subagents=[],
        checkpointer=InMemorySaver(), permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
    )

    async def claim() -> bool:
        return True

    bus = FakeBus()
    terminal = await invoke_once(
        RunEmitter(bus, "r-art"), agent, "t-art",
        {"messages": [HumanMessage(content="导出", id="m1")]},
        approval_tool_names=frozenset(), source_for=lambda _n: "built-in",
        claim_terminal=claim, record_usage=usage_recorder()[0],
    )
    assert terminal is True
    returned = [e for e in bus.run_events("r-art") if e.kind == "tool.returned"]
    assert len(returned) == 1
    payload = returned[0].payload
    artifact = getattr(payload, "artifact", None)
    assert artifact is not None
    assert artifact.artifact_id == "r-art/t-exp-track.wav"
    assert artifact.mime == "audio/wav" and artifact.bytes == len(_WAV)
    # 字节真源可读回（session 端点同一后端）。
    assert await store.get(artifact.artifact_id) == ("audio/wav", _WAV)
    # 摘要走 result 通道（给模型的一句话），不含字节。
    assert "已导出产物" in payload.result
