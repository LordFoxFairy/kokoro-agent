"""Capability Skill references feed DeepAgents' native Skill runtime."""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from deepagents.backends import CompositeBackend, StateBackend
from deepagents.backends.protocol import PERMISSION_DENIED
from deepagents.middleware.skills import SkillsMiddleware, SkillsState
from langgraph.runtime import Runtime

from kokoro_agent.clients.skills import ResolvedSkill
from kokoro_agent.skills.backend import (
    TypedSkillBackend,
    SkillDiscoveryBackend,
    SKILLS_ROOT,
)


class _Reader:
    def __init__(self, *, closing_at_eof: bool = False) -> None:
        self.calls: list[ResolvedSkill] = []
        self.markdown = b"---\nname: style\ndescription: Writing style guide\n---" + (
            b"" if closing_at_eof else b"\nLead with the conclusion."
        )

    async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]:
        self.calls.append(skill)
        return {
            "SKILL.md": self.markdown,
            "references/examples.md": b"A concise example.",
        }


def _skill() -> ResolvedSkill:
    return ResolvedSkill(
        source_ref="skill:style",
        skill_id="style",
        revision=1,
        asset_ref="asset-1",
        content_digest="a" * 64,
        manifest_identity="skill-md-v1:sha256:" + "b" * 64,
    )


@pytest.mark.parametrize("closing_at_eof", [False, True])
async def test_native_route_lists_and_lazily_reads_authorized_skill(
    closing_at_eof: bool,
) -> None:
    reader = _Reader(closing_at_eof=closing_at_eof)
    route = TypedSkillBackend((_skill(),), reader)
    backend = CompositeBackend(default=StateBackend(), routes={SKILLS_ROOT: route})

    listed = await backend.als(SKILLS_ROOT)
    downloaded = await backend.adownload_files([f"{SKILLS_ROOT}c3R5bGU/SKILL.md"])
    nested = await backend.als(f"{SKILLS_ROOT}c3R5bGU/references")

    assert [entry["path"] for entry in listed.entries or []] == [
        f"{SKILLS_ROOT}c3R5bGU/"
    ]
    assert downloaded[0].content is not None
    assert downloaded[0].content == reader.markdown
    assert [entry["path"] for entry in nested.entries or []] == [
        f"{SKILLS_ROOT}c3R5bGU/references/examples.md"
    ]
    assert reader.calls == [_skill()] * 3

    discovery = CompositeBackend(
        default=StateBackend(),
        routes={SKILLS_ROOT: SkillDiscoveryBackend((_skill(),), reader)},
    )
    native = SkillsMiddleware(backend=discovery, sources=[SKILLS_ROOT])
    metadata = await native.abefore_agent({"messages": []}, Runtime(), {})
    assert metadata is not None
    assert [item["name"] for item in metadata["skills_metadata"]] == ["style"]
    # Discovery has no effect on progressive-disclosure reads or downloads.
    original = await backend.adownload_files([f"{SKILLS_ROOT}c3R5bGU/SKILL.md"])
    assert original[0].content == reader.markdown


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
