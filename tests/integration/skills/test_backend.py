"""Capability Skill references feed DeepAgents' native Skill runtime."""

from __future__ import annotations

from collections.abc import Mapping

from deepagents.backends import CompositeBackend, StateBackend
from deepagents.backends.protocol import PERMISSION_DENIED
from deepagents.middleware.skills import SkillsMiddleware, SkillsState
from langgraph.runtime import Runtime

from kokoro_agent.clients.skills import ResolvedSkill
from kokoro_agent.skills.backend import CapabilitySkillBackend, SKILLS_ROOT


class _Reader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def load_package(
        self, scope: str, name: str, content_hash: str
    ) -> Mapping[str, str]:
        self.calls.append((scope, name, content_hash))
        return {
            "SKILL.md": (
                "---\nname: style\ndescription: Writing style guide\n---\n"
                "Lead with the conclusion."
            ),
            "references/examples.md": "A concise example.",
        }


def _skill() -> ResolvedSkill:
    return ResolvedSkill(
        name="style",
        content_hash="hash-style",
        description="Writing style guide",
        scope="project:docs",
    )


async def test_native_route_lists_and_lazily_reads_authorized_skill() -> None:
    reader = _Reader()
    route = CapabilitySkillBackend((_skill(),), reader)
    backend = CompositeBackend(
        default=StateBackend(), routes={SKILLS_ROOT: route}
    )

    listed = await backend.als(SKILLS_ROOT)
    downloaded = await backend.adownload_files([f"{SKILLS_ROOT}style/SKILL.md"])
    nested = await backend.als(f"{SKILLS_ROOT}style/references")

    assert [entry["path"] for entry in listed.entries or []] == [
        f"{SKILLS_ROOT}style/"
    ]
    assert downloaded[0].content is not None
    assert b"Lead with the conclusion" in downloaded[0].content
    assert [entry["path"] for entry in nested.entries or []] == [
        f"{SKILLS_ROOT}style/references/examples.md"
    ]
    assert reader.calls == [("project:docs", "style", "hash-style")]


async def test_native_skills_middleware_loads_metadata_from_route() -> None:
    backend = CompositeBackend(
        default=StateBackend(),
        routes={SKILLS_ROOT: CapabilitySkillBackend((_skill(),), _Reader())},
    )
    middleware = SkillsMiddleware(backend=backend, sources=[SKILLS_ROOT])

    state: SkillsState = {"messages": []}
    update = await middleware.abefore_agent(state, Runtime(), {})

    assert update is not None
    assert update["skills_metadata"] == [
        {
            "name": "style",
            "description": "Writing style guide",
            "path": f"{SKILLS_ROOT}style/SKILL.md",
            "license": None,
            "compatibility": None,
            "metadata": {},
            "allowed_tools": [],
        }
    ]


async def test_route_is_read_only_and_unknown_skills_are_hidden() -> None:
    backend = CapabilitySkillBackend((_skill(),), _Reader())

    unknown = await backend.adownload_files(["/other/SKILL.md"])
    write = await backend.awrite("/style/SKILL.md", "replacement")
    upload = await backend.aupload_files([("/style/new.py", b"content")])

    assert unknown[0].content is None
    assert write.error == PERMISSION_DENIED
    assert upload[0].error == PERMISSION_DENIED
