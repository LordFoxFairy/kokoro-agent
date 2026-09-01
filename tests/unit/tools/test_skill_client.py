"""Skill name resolution stays outside Agent/Feature declarations."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from kokoro_agent.agents.definition import Agent
from kokoro_agent.contract import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.agent_factory import resolve_declared_skills
from kokoro_agent.clients.skills import ResolvedSkill, SkillClientError
from kokoro_agent.execution.scope import runtime_namespace


class _FakeSkillClient:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], ExecutionIdentity, str]] = []

    async def resolve(
        self, selectors: Sequence[str], identity: ExecutionIdentity, namespace: str
    ) -> tuple[ResolvedSkill, ...]:
        selector_tuple = tuple(selectors)
        self.calls.append((selector_tuple, identity, namespace))
        return tuple(
            ResolvedSkill(
                name=name,
                content_hash=f"hash-{name}",
                description=f"{name} description",
                scope="official",
            )
            for name in selector_tuple
        )

    async def search(
        self,
        query: str,
        identity: ExecutionIdentity,
        namespace: str,
        *,
        limit: int = 8,
    ) -> tuple[ResolvedSkill, ...]:
        del query, identity, namespace, limit
        return ()


@pytest.mark.asyncio
async def test_declared_skill_names_resolve_through_client() -> None:
    client = _FakeSkillClient()
    agent = Agent(key="demo", prompt="demo", skills=("music", "lyrics"))
    identity = ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref="subject"),
        identity_assertion_ref="assertion",
    )
    request = RunRequest(
        kind="run.request",
        run_id="run",
        session_id="session",
        feature_key="chat",
        execution_identity=identity,
        input=RunInput(message_id="message", content="hello"),
    )

    resolved = await resolve_declared_skills(agent, client, request)

    assert [skill.name for skill in resolved] == ["music", "lyrics"]
    assert client.calls == [(('music', 'lyrics'), identity, runtime_namespace(identity))]


def test_agent_does_not_carry_resolved_skills() -> None:
    agent = Agent(key="demo", prompt="demo", skills=("music",))
    assert not hasattr(agent, "resolved_skills")


async def test_declared_skill_outage_does_not_remove_agent_core() -> None:
    class _Unavailable(_FakeSkillClient):
        async def resolve(
            self,
            selectors: Sequence[str],
            identity: ExecutionIdentity,
            namespace: str,
        ) -> tuple[ResolvedSkill, ...]:
            del selectors, identity, namespace
            raise SkillClientError("capability unavailable")

    agent = Agent(key="demo", prompt="demo", skills=("music",))
    identity = ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref="subject"),
        identity_assertion_ref="assertion",
    )
    request = RunRequest(
        kind="run.request",
        run_id="run",
        session_id="session",
        feature_key="chat",
        execution_identity=identity,
        input=RunInput(message_id="message", content="hello"),
    )

    assert await resolve_declared_skills(agent, _Unavailable(), request) == ()
