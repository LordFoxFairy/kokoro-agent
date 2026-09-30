"""Capability Skill references feed DeepAgents' native Skill runtime."""

from __future__ import annotations

from collections.abc import Mapping

from deepagents.backends import CompositeBackend, StateBackend
from deepagents.backends.protocol import PERMISSION_DENIED
from deepagents.middleware.skills import SkillsMiddleware, SkillsState
from langgraph.runtime import Runtime

from kokoro_agent.clients.skills import ResolvedSkill
from kokoro_agent.skills.backend import TypedSkillBackend, SKILLS_ROOT


class _Reader:
    def __init__(self) -> None:
        self.calls: list[ResolvedSkill] = []

    async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]:
        self.calls.append(skill)
        return {
            "SKILL.md": (
                b"---\nname: style\ndescription: Writing style guide\n---\n"
                b"Lead with the conclusion."
            ),
            "references/examples.md": b"A concise example.",
        }


def _skill() -> ResolvedSkill:
    return ResolvedSkill(
        source_ref="skill:style",
        skill_id="style",
        revision=1,
        asset_ref="asset-1",
        content_digest="a" * 64,
        manifest_identity="zip-v1:sha256:" + "b" * 64,
    )


async def test_native_route_lists_and_lazily_reads_authorized_skill() -> None:
    reader = _Reader()
    route = TypedSkillBackend((_skill(),), reader)
    backend = CompositeBackend(default=StateBackend(), routes={SKILLS_ROOT: route})

    listed = await backend.als(SKILLS_ROOT)
    downloaded = await backend.adownload_files([f"{SKILLS_ROOT}c3R5bGU/SKILL.md"])
    nested = await backend.als(f"{SKILLS_ROOT}c3R5bGU/references")

    assert [entry["path"] for entry in listed.entries or []] == [
        f"{SKILLS_ROOT}c3R5bGU/"
    ]
    assert downloaded[0].content is not None
    assert b"Lead with the conclusion" in downloaded[0].content
    assert [entry["path"] for entry in nested.entries or []] == [
        f"{SKILLS_ROOT}c3R5bGU/references/examples.md"
    ]
    assert reader.calls == [_skill()] * 3


async def test_native_skills_middleware_loads_metadata_from_route() -> None:
    backend = CompositeBackend(
        default=StateBackend(),
        routes={SKILLS_ROOT: TypedSkillBackend((_skill(),), _Reader())},
    )
    middleware = SkillsMiddleware(backend=backend, sources=[SKILLS_ROOT])

    state: SkillsState = {"messages": []}
    update = await middleware.abefore_agent(state, Runtime(), {})

    assert update is not None
    assert update["skills_metadata"] == [
        {
            "name": "style",
            "description": "Writing style guide",
            "path": f"{SKILLS_ROOT}c3R5bGU/SKILL.md",
            "license": None,
            "compatibility": None,
            "metadata": {},
            "allowed_tools": [],
        }
    ]


async def test_route_is_read_only_and_unknown_skills_are_hidden() -> None:
    backend = TypedSkillBackend((_skill(),), _Reader())

    unknown = await backend.adownload_files(["/other/SKILL.md"])
    write = await backend.awrite("/style/SKILL.md", "replacement")
    upload = await backend.aupload_files([("/style/new.py", b"content")])

    assert unknown[0].content is None
    assert write.error == PERMISSION_DENIED
    assert upload[0].error == PERMISSION_DENIED
