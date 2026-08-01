"""Canonical Agent assembly identity contains facts, never runtime object identity."""

from __future__ import annotations

from dataclasses import replace

from kokoro_agent.agents.assembly.identity import AgentAssemblyFacts
from kokoro_agent.contract import ModelConfig, Permissions


def _facts() -> AgentAssemblyFacts:
    return AgentAssemblyFacts(
        namespace="site-a",
        agent_catalog_ref=f"agent-catalog:sha256:{'a' * 64}",
        hub_assembly_digest="b" * 64,
        agent_type="general",
        persona_name="researcher",
        persona_prompt_sha256="c" * 64,
        model=ModelConfig(
            provider="anthropic",
            name="claude-sonnet",
            authorization_handle="model-authz:test",
            effort="high",
        ),
        skill_package_digest="d" * 64,
        tool_schema_digest="e" * 64,
        backend_kind="state",
        backend_mapping={"/": "state", "/.skills/": "run-scoped-store"},
        subagents=("researcher",),
        subagent_grants=("researcher",),
        permissions=Permissions(
            approval_tools=[],
            review_tools=[],
            subagent_create="deny",
            filesystem="read_only",
        ),
    )


def test_digest_is_stable_for_same_canonical_business_facts() -> None:
    assert _facts().digest() == _facts().digest()


def test_digest_binds_every_execution_semantic_axis() -> None:
    baseline = _facts().digest()
    variants = (
        replace(_facts(), namespace="site-b"),
        replace(_facts(), persona_prompt_sha256="f" * 64),
        replace(_facts(), hub_assembly_digest="1" * 64),
        replace(_facts(), skill_package_digest="2" * 64),
        replace(_facts(), tool_schema_digest="3" * 64),
        replace(_facts(), subagent_grants=()),
        replace(
            _facts(),
            backend_kind="docker",
            backend_mapping={"/": "docker", "/.skills/": "docker"},
        ),
        replace(
            _facts(),
            model=ModelConfig(
                provider="openai",
                name="gpt-5",
                authorization_handle="model-authz:test",
            ),
        ),
    )
    assert all(item.digest() != baseline for item in variants)


def test_authorization_handle_does_not_change_model_policy_identity() -> None:
    other_handle = ModelConfig(
        provider="anthropic",
        name="claude-sonnet",
        authorization_handle="another-model-authorization-handle",
        effort="high",
    )
    assert _facts().digest() == replace(_facts(), model=other_handle).digest()
