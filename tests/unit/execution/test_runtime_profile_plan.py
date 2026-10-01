"""The same immutable plan drives fingerprints and actual materialization."""

from dataclasses import replace
from importlib import import_module
import json
import pytest
from kokoro_agent.config import AppConfig
from kokoro_agent.agents.definition import Agent
from kokoro_agent.features.definition import Feature
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.tools.toolbox import build_toolbox


def prepare(feature: Feature, *, budget: int = 17, recursion: int = 43):
    module = import_module("kokoro_agent.execution.runtime_profile_plan")
    source = import_module("kokoro_agent.execution.runtime_profile_sources")
    config = AppConfig.from_env({})
    policy = module.RuntimeAssemblyPolicy.from_settings(
        run_token_budget=budget,
        recursion_limit=recursion,
        model=config.model,
        sandbox=config.sandbox,
    )
    return module.prepare_feature(
        feature,
        runtime_policy=policy,
        manifest=source.production_manifest(),
        toolbox=build_toolbox(fetch_allow_private=False, search=None),
        subagent_catalog=build_subagent_catalog(None),
        delivery_available=False,
    )


def test_recipe_uses_actual_declarations_and_distinct_domain() -> None:
    agent = Agent(key="one", prompt="actual prompt")
    feature = Feature(key="sample", agents=(agent,), entry_agent="one")
    plan = prepare(feature)
    recipe = json.loads(plan.recipe_bytes)
    assert recipe["domain_tag"] == "kokoro-agent:assembly-recipe:1"
    assert recipe["runtime"]["recursion_limit"] == 43
    assert recipe["runtime"]["run_token_budget"] == 17
    assert recipe["feature"]["agents"][0]["prompt"] == agent.prompt
    assert recipe["native_recipe"]["materialization"] == "post_route"
    assert "runtime_profile_digest" not in recipe
    assert prepare(feature).fingerprint == plan.fingerprint
    assert prepare(feature, budget=18).fingerprint != plan.fingerprint
    assert prepare(feature, recursion=44).fingerprint != plan.fingerprint
    changed = replace(feature, agents=(replace(agent, prompt="changed"),))
    assert prepare(changed).fingerprint != plan.fingerprint


def test_handoff_instances_are_real_ordered_and_retained() -> None:
    feature = Feature(
        key="team",
        agents=(Agent(key="one", prompt="one"), Agent(key="two", prompt="two")),
        entry_agent="one",
        handoffs=(("one", "two"), ("two", "one")),
    )
    plan = prepare(feature)
    assert len(plan.peers) == 2
    assert plan.peers[0].handoffs[0].name == "transfer_to_two"
    assert plan.peers[1].handoffs[0].name == "transfer_to_one"


def test_unknown_tool_and_illegal_guard_rejected_in_prepare() -> None:
    from kokoro_agent.policy import Permissions
    from langchain_core.tools import StructuredTool
    from pydantic import BaseModel

    class Args(BaseModel):
        pass

    tool = StructuredTool(
        func=lambda: "no", name="unknown", description="unknown", args_schema=Args
    )
    with pytest.raises(ValueError, match="source|registered"):
        prepare(
            Feature(
                key="bad",
                agents=(Agent(key="one", prompt="one", tools=(tool,)),),
                entry_agent="one",
            )
        )
    with pytest.raises(ValueError, match="result-review"):
        prepare(
            Feature(
                key="bad",
                agents=(
                    Agent(
                        key="one",
                        prompt="one",
                        permissions=Permissions(review_tools=("ask_user_question",)),
                    ),
                ),
                entry_agent="one",
            )
        )


def test_manifest_edges_participate_in_recipe() -> None:
    module = import_module("kokoro_agent.execution.runtime_profile_plan")
    sources = import_module("kokoro_agent.execution.runtime_profile_sources")
    config = AppConfig.from_env({})
    policy = module.RuntimeAssemblyPolicy.from_settings(
        run_token_budget=1,
        recursion_limit=33,
        model=config.model,
        sandbox=config.sandbox,
    )
    feature = Feature(
        key="one", agents=(Agent(key="one", prompt="one"),), entry_agent="one"
    )
    manifest = sources.production_manifest()
    kwargs = dict(
        runtime_policy=policy,
        toolbox=build_toolbox(fetch_allow_private=False, search=None),
        subagent_catalog=build_subagent_catalog(None),
        delivery_available=False,
    )
    first = module.prepare_feature(feature, manifest=manifest, **kwargs)
    changed = replace(
        manifest, dynamic_edges=(*manifest.dynamic_edges, ("new", "registered", "edge"))
    )
    assert (
        first.fingerprint
        != module.prepare_feature(feature, manifest=changed, **kwargs).fingerprint
    )


def test_materialized_tool_metadata_must_match_prepared_snapshot() -> None:
    feature = Feature(
        key="one", agents=(Agent(key="one", prompt="one"),), entry_agent="one"
    )
    plan = prepare(feature)
    with pytest.raises(ValueError, match="metadata"):
        plan.peers[0].verify_tools(())


def test_selected_toolbox_requires_explicit_factory_identity() -> None:
    module = import_module("kokoro_agent.execution.runtime_profile_plan")
    sources = import_module("kokoro_agent.execution.runtime_profile_sources")
    config = AppConfig.from_env({})
    policy = module.RuntimeAssemblyPolicy.from_settings(
        run_token_budget=0,
        recursion_limit=19,
        model=config.model,
        sandbox=config.sandbox,
    )
    feature = Feature(
        key="one", agents=(Agent(key="one", prompt="one"),), entry_agent="one"
    )
    box = build_toolbox(fetch_allow_private=False, search=None)
    for changed in (
        replace(box, source_bindings=()),
        replace(box, profile_options=None),
    ):
        with pytest.raises(ValueError, match="source|metadata"):
            module.prepare_feature(
                feature,
                runtime_policy=policy,
                manifest=sources.production_manifest(),
                toolbox=changed,
                subagent_catalog=build_subagent_catalog(None),
                delivery_available=False,
            )


def test_secrets_and_addresses_are_not_recipe_inputs() -> None:
    module = import_module("kokoro_agent.execution.runtime_profile_plan")
    config = AppConfig.from_env({})
    changed = AppConfig.from_env(
        {
            "OPENAI_API_KEY": "SENSITIVE_KEY",
            "OPENAI_BASE_URL": "https://secret.example",
            "KOKORO_LOCAL_SHELL_ROOT": "/private/workspace",
        }
    )
    first = module.RuntimeAssemblyPolicy.from_settings(
        run_token_budget=0,
        recursion_limit=19,
        model=config.model,
        sandbox=config.sandbox,
    )
    second = module.RuntimeAssemblyPolicy.from_settings(
        run_token_budget=0,
        recursion_limit=19,
        model=changed.model,
        sandbox=changed.sandbox,
    )
    assert first == second
    assert "SENSITIVE" not in str(first.as_recipe())
    assert "secret.example" not in str(second.as_recipe())


@pytest.mark.parametrize("budget,recursion", [(-1, 1), (True, 1), (1, 0), (1, True)])
def test_runtime_limits_fail_closed(budget: int, recursion: int) -> None:
    with pytest.raises(ValueError):
        prepare(
            Feature(
                key="one", agents=(Agent(key="one", prompt="one"),), entry_agent="one"
            ),
            budget=budget,
            recursion=recursion,
        )


def test_custom_backend_without_approved_source_fails_closed() -> None:
    with pytest.raises(ValueError, match="source/policy"):
        prepare(
            Feature(
                key="one",
                agents=(Agent(key="one", prompt="one", backend="custom"),),
                entry_agent="one",
            )
        )


def test_native_task_baseline_uses_selected_gp_and_catalog_metadata() -> None:
    recipe = json.loads(
        prepare(
            Feature(
                key="one", agents=(Agent(key="one", prompt="one"),), entry_agent="one"
            )
        ).recipe_bytes
    )
    from deepagents.middleware.subagents import (
        TASK_TOOL_DESCRIPTION,
        GENERAL_PURPOSE_SUBAGENT,
    )

    available = f"- {GENERAL_PURPOSE_SUBAGENT['name']}: {GENERAL_PURPOSE_SUBAGENT['description']}"
    assert recipe["feature"]["agents"][0][
        "native_task_description"
    ] == TASK_TOOL_DESCRIPTION.format(available_agents=available)
