"""DeepAgents native Skill assembly: exact packages exist before graph creation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from deepagents.backends.composite import CompositeBackend
from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import FileUploadResponse
from deepagents.backends.state import StateBackend
from deepagents.middleware.skills import SkillsMiddleware, SkillsState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.runtime import Runtime

from fakes import FakeLedger, completed_execution_context, request, usage_recorder
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.skills import materialize_native_skills
from kokoro_agent.skills.hub import SkillHub
from kokoro_agent.state import KokoroAgentState
from kokoro_agent.streams.redis import RedisStream
from skill_fixtures import PDF_FILES, STYLE_FILES, make_skill_hub, snapshot_grant


def _hub() -> SkillHub:
    return make_skill_hub(
        ("official", "style", STYLE_FILES),
        ("official", "pdf", PDF_FILES),
    )


async def test_materializes_every_exact_package_before_graph_on_sandbox(
    tmp_path: Path,
) -> None:
    backend = LocalShellBackend(root_dir=tmp_path, virtual_mode=True)
    style = snapshot_grant(STYLE_FILES, "style")
    pdf = snapshot_grant(PDF_FILES, "pdf")

    assembly = await materialize_native_skills(
        grants=(style, pdf),
        hub=_hub(),
        backend=backend,
        namespace="site-a",
        run_id="run-a",
    )

    assert assembly.backend is backend
    assert len(assembly.sources) == 1
    assert all(source.startswith("/.skills/") and source.endswith("/") for source in assembly.sources)
    root = tmp_path / assembly.sources[0].lstrip("/")
    assert (root / "style" / "SKILL.md").read_text()
    assert (root / "pdf" / "make_report.py").read_text() == "print('report')"


async def test_state_mode_is_explicit_composite_with_isolated_native_skill_route() -> None:
    style = snapshot_grant(STYLE_FILES, "style")
    assembly = await materialize_native_skills(
        grants=(style,),
        hub=_hub(),
        backend=StateBackend(),
        namespace="site-a",
        run_id="run-a",
    )

    assert isinstance(assembly.backend, CompositeBackend)
    assert isinstance(assembly.backend.default, StateBackend)
    assert tuple(assembly.backend.routes) == ("/.skills/",)
    loaded = await assembly.backend.aread(f"{assembly.sources[0]}style/SKILL.md")
    assert loaded.error is None
    assert loaded.file_data is not None
    assert "先结论后论据" in str(loaded.file_data["content"])

    native = SkillsMiddleware(backend=assembly.backend, sources=assembly.sources)
    state: SkillsState = {"messages": []}
    update = native.before_agent(state, Runtime(), {})
    assert update is not None
    assert [item["name"] for item in update["skills_metadata"]] == ["style"]
    assert update["skills_metadata"][0]["path"] == f"{assembly.sources[0]}style/SKILL.md"


class _CaptureNativeSkills(AgentMiddleware[KokoroAgentState, Any]):
    state_schema = KokoroAgentState

    def __init__(self, seen: list[tuple[str, ...]]) -> None:
        super().__init__()
        self._seen = seen

    async def abefore_agent(
        self, state: KokoroAgentState, runtime: Runtime[Any]
    ) -> None:
        _ = runtime
        names = [item.get("name") for item in state.get("skills_metadata", ())]
        assert all(isinstance(name, str) for name in names)
        self._seen.append(tuple(name for name in names if isinstance(name, str)))


async def _drive(
    *,
    agent: Any,
    run_id: str,
    payload: object,
    stream: RedisStream,
    thread_id: str,
) -> None:
    ledger = FakeLedger()
    await ledger.try_claim(request(run_id))
    await invoke_once(
        RunEmitter(stream, run_id, outbox=ledger),
        agent,
        {
            "configurable": {"thread_id": thread_id},
            "metadata": {"kokoro_run_id": run_id},
        },
        payload,
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        prepare_completed=lambda: completed_execution_context(run_id),
        record_usage=usage_recorder()[0],
    )


async def test_checkpoint_roundtrip_reloads_native_metadata_on_rebuilt_worker(
    checkpointer: BaseCheckpointSaver[str], stream: RedisStream
) -> None:
    seen: list[tuple[str, ...]] = []
    thread_id = "site-a:skill-resume"

    async def assemble(grant_name: str, script: list[AIMessage]) -> Any:
        files = STYLE_FILES if grant_name == "style" else PDF_FILES
        grant = snapshot_grant(files, grant_name)
        native = await materialize_native_skills(
            grants=(grant,),
            hub=_hub(),
            backend=StateBackend(),
            namespace="site-a",
            run_id="run-skill-resume",
        )
        middleware: list[AgentMiddleware[Any, Any, Any]] = [
            _CaptureNativeSkills(seen)
        ]
        return build_agent(
            model=LocalFakeChatModel.with_script(script),
            tools=(),
            system_prompt="system",
            subagents=(),
            checkpointer=checkpointer,
            permissions=(),
            interrupt_on={},
            middleware=middleware,
            backend=native.backend,
            skills=native.sources,
        )

    first = await assemble("style", [AIMessage(content="first")])
    await _drive(
        agent=first,
        run_id="run-skill-first",
        payload={
            "messages": [HumanMessage(content="first")],
            "scope": {},
            "assembly_digest": "a" * 64,
        },
        stream=stream,
        thread_id=thread_id,
    )
    second = await assemble(
        "pdf", [AIMessage(content="historical"), AIMessage(content="second")]
    )
    await _drive(
        agent=second,
        run_id="run-skill-second",
        payload={"messages": [HumanMessage(content="second")]},
        stream=stream,
        thread_id=thread_id,
    )

    assert seen == [("style",), ("pdf",)]


async def test_snapshot_change_changes_native_source_and_package(tmp_path: Path) -> None:
    backend = LocalShellBackend(root_dir=tmp_path, virtual_mode=True)
    v1 = snapshot_grant(PDF_FILES, "pdf")
    v2_files = {**PDF_FILES, "make_report.py": "print('v2')"}
    v2 = snapshot_grant(v2_files, "pdf")
    hub = make_skill_hub(
        ("official", "pdf", PDF_FILES),
        ("official", "pdf", v2_files),
    )
    first = await materialize_native_skills(
        grants=(v1,), hub=hub, backend=backend, namespace="site-a", run_id="run-a"
    )
    second = await materialize_native_skills(
        grants=(v2,), hub=hub, backend=backend, namespace="site-a", run_id="run-a"
    )
    assert first.sources != second.sources
    assert first.package_digest != second.package_digest


async def test_fails_before_graph_when_hub_package_breaks_content_lock(tmp_path: Path) -> None:
    grant = snapshot_grant(STYLE_FILES, "style")
    corrupt = SkillHub(
        {(grant.scope, grant.name, grant.content_hash): {"SKILL.md": "corrupt"}}
    )
    with pytest.raises(RuntimeError, match="NATIVE_SKILL_CONTENT_LOCK_INVALID"):
        await materialize_native_skills(
            grants=(grant,),
            hub=corrupt,
            backend=LocalShellBackend(root_dir=tmp_path, virtual_mode=True),
            namespace="site-a",
            run_id="run-a",
        )


class _CorruptingBackend(LocalShellBackend):
    async def aupload_files(
        self, files: list[tuple[str, bytes]]
    ) -> list[FileUploadResponse]:
        corrupted = [(path, b"corrupted after transport") for path, _content in files]
        return await super().aupload_files(corrupted)


async def test_fails_before_graph_when_backend_write_does_not_roundtrip(tmp_path: Path) -> None:
    grant = snapshot_grant(STYLE_FILES, "style")
    with pytest.raises(RuntimeError, match="NATIVE_SKILL_WRITE_INTEGRITY_FAILED"):
        await materialize_native_skills(
            grants=(grant,),
            hub=_hub(),
            backend=_CorruptingBackend(root_dir=tmp_path, virtual_mode=True),
            namespace="site-a",
            run_id="run-a",
        )
