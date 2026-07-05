"""产物面真图规格：write_file 自动镜像入共享库 → wire artifact.created 独立事件。

模型只用通用文件工具（对标 manus/codex）：产物注册零显式动作。产物诞生走自有队列
（回写 ToolMessage 是结构性竞态，实测确证弃用）。
"""

from __future__ import annotations

from pathlib import Path

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver

from fakes import FakeBus, usage_recorder
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.storage.artifacts import DirArtifactStore
from kokoro_agent.tools.middleware import ArtifactMirrorMiddleware
from kokoro_agent.tools.permissions import build_interrupt_on

_MD = "# 计划\n本地预览"


async def test_write_file_mirrors_artifact_to_wire(tmp_path: Path) -> None:
    store = DirArtifactStore(str(tmp_path / "arts"))
    mirror = ArtifactMirrorMiddleware(store=store, run_id="r-mir")
    model = LocalFakeChatModel.with_script([
        AIMessage(content="", tool_calls=[{
            "name": "write_file",
            "args": {"file_path": "/plan.md", "content": _MD},
            "id": "t-wf", "type": "tool_call"}]),
        AIMessage(content="写完了"),
    ])
    agent = build_agent(
        model=model, tools=[], system_prompt="x", subagents=[],
        checkpointer=InMemorySaver(), permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
        middleware=[mirror],
    )

    async def claim() -> bool:
        return True

    bus = FakeBus()
    terminal = await invoke_once(
        RunEmitter(bus, "r-mir"), agent, "t-mir",
        {"messages": [HumanMessage(content="写文件", id="m1")]},
        approval_tool_names=frozenset(), source_for=lambda _n: "built-in",
        claim_terminal=claim, record_usage=usage_recorder()[0],
        artifact_queue=mirror.created,
    )
    assert terminal is True
    events = bus.run_events("r-mir")
    created = [e for e in events if e.kind == "artifact.created"]
    assert len(created) == 1
    payload = created[0].payload
    assert payload.tool_id == "t-wf"  # web 按 tool_id 挂回工具步
    artifact = payload.artifact
    assert artifact.name == "plan.md"
    assert artifact.mime == "text/markdown"  # 扩展名推断
    # 事件序：诞生事件在该 run 终态之前（用户即时可点）。
    kinds = [e.kind for e in events]
    assert kinds.index("artifact.created") < kinds.index("run.completed")
    # 字节真源可读回（session 端点同一后端），内容=write_file 入参原文。
    assert await store.get(artifact.artifact_id) == ("text/markdown", _MD.encode())


async def test_failed_write_not_mirrored(tmp_path: Path) -> None:
    # 写失败（错误结果）不入库不上引用：绝不给用户一个坏文件的预览入口。
    store = DirArtifactStore(str(tmp_path / "arts"))
    bad_mirror = ArtifactMirrorMiddleware(store=store, run_id="r-bad")
    model = LocalFakeChatModel.with_script([
        AIMessage(content="", tool_calls=[{
            "name": "write_file",
            "args": {"file_path": "", "content": "x"},  # 非法路径 → 工具报错
            "id": "t-bad", "type": "tool_call"}]),
        AIMessage(content="done"),
    ])
    agent = build_agent(
        model=model, tools=[], system_prompt="x", subagents=[],
        checkpointer=InMemorySaver(), permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
        middleware=[bad_mirror],
    )

    async def claim() -> bool:
        return True

    bus = FakeBus()
    await invoke_once(
        RunEmitter(bus, "r-bad"), agent, "t-bad",
        {"messages": [HumanMessage(content="写", id="m1")]},
        approval_tool_names=frozenset(), source_for=lambda _n: "built-in",
        claim_terminal=claim, record_usage=usage_recorder()[0],
        artifact_queue=bad_mirror.created,
    )
    assert "artifact.created" not in [e.kind for e in bus.run_events("r-bad")]
