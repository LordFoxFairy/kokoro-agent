"""Pure profile identity and explicit package provenance; no execution gate."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel
from langchain_core.tools import StructuredTool


def _api() -> Any:
    assert (
        importlib.util.find_spec("kokoro_agent.execution.runtime_profile") is not None
    ), "the approved pure runtime profile encoder is missing"
    return importlib.import_module("kokoro_agent.execution.runtime_profile")


def _source() -> dict[str, Any]:
    return {
        "source_id": "memory-v1",
        "package": "kokoro_agent",
        "symbol": "kokoro_agent.tools.memory.make_memory_tools",
        "distribution": "kokoro-agent",
        "version": "2.0.0",
        "files": [{"path": "tools/memory.py", "sha256": "a" * 64}],
    }


def _tool(name: str = "inspect") -> dict[str, Any]:
    return {
        "name": name,
        "description": "保持原文 é",
        "input_schema": {"type": "object", "properties": {}, "required": []},
        "return_direct": False,
        "response_format": "content",
        "implementation": _source(),
        "options": {},
    }


def _profile() -> dict[str, Any]:
    return {
        "profile_version": 1,
        "feature": {
            "key": "chat",
            "entry_agent": "general",
            "handoffs": [],
            "agents": [
                {
                    "key": "general",
                    "prompt": "  保持原文  ",
                    "tools": [_tool()],
                    "implicit_tools": [_tool("task")],
                    "mcp": ["declared"],
                    "subagents": [
                        {
                            "name": "helper",
                            "description": "help",
                            "system_prompt": "assist",
                            "source": "built-in",
                            "tools": ["inspect"],
                        }
                    ],
                    "delivery": {"declared": True, "mounted": False},
                    "model": {
                        "provider": "openai",
                        "name": "model",
                        "effort": None,
                        "thinking": None,
                    },
                    "backend": {
                        "kind": "state",
                        "policy_id": "state-v1",
                        "implementation": _source(),
                    },
                    "permissions": {
                        "approval_tools": [],
                        "review_tools": [],
                        "subagent_create": "deny",
                        "filesystem": "read_only",
                    },
                    "pause_tools": frozenset({"z", "a"}),
                }
            ],
        },
        "runtime": {
            "run_token_budget": 100,
            "recursion_limit": 20,
            "disable_streaming": False,
            "openai_reasoning": False,
            "litellm_enabled": False,
            "toolbox": {
                "fetch_allow_private": False,
                "search_enabled": False,
                "search_provider": None,
            },
            "delivery_available": False,
        },
    }


def test_profile_canonical_encoding_is_stable_and_preserves_null_and_text() -> None:
    api = _api()
    profile = _profile()
    actual = api.canonical_profile(profile)
    expected = copy.deepcopy(profile)
    expected["feature"]["agents"][0]["pause_tools"] = ["a", "z"]
    assert actual == json.dumps(
        expected,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    assert api.profile_digest(profile) == hashlib.sha256(actual).hexdigest()
    assert api.canonical_profile(dict(reversed(list(profile.items())))) == actual
    assert profile["feature"]["agents"][0]["pause_tools"] == frozenset({"z", "a"})


@pytest.mark.parametrize("key", ["profile_version", "feature", "runtime"])
def test_missing_profile_fields_fail_closed(key: str) -> None:
    api = _api()
    profile = _profile()
    del profile[key]
    with pytest.raises(ValueError):
        api.canonical_profile(profile)


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), object(), b"bytes", {"set"}, "\ud800"]
)
def test_non_json_values_rejected_even_in_tool_schema(value: object) -> None:
    api = _api()
    profile = _profile()
    profile["feature"]["agents"][0]["tools"][0]["input_schema"]["default"] = value
    with pytest.raises(ValueError):
        api.canonical_profile(profile)


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("runtime",),
        ("feature",),
        ("feature", "agents", 0),
        ("feature", "agents", 0, "tools", 0),
        ("feature", "agents", 0, "tools", 0, "options"),
    ],
)
def test_extra_secret_or_binding_fields_are_not_serialized(
    path: tuple[object, ...],
) -> None:
    api = _api()
    profile = _profile()
    target: Any = profile
    for key in path:
        target = target[key]
    target["api_key"] = "sentinel-not-for-profile"
    with pytest.raises(ValueError) as failure:
        api.canonical_profile(profile)
    assert "sentinel-not-for-profile" not in str(failure.value)


@pytest.mark.parametrize(
    "path,value",
    [
        (("profile_version",), 2),
        (("profile_version",), True),
        (("runtime", "recursion_limit"), True),
        (("runtime", "recursion_limit"), 0),
        (("feature", "agents", 0, "pause_tools"), {"a"}),
        (("feature", "agents", 0, "tools", 0, "implementation"), {}),
        (("runtime", "toolbox", "search_enabled"), True),
    ],
)
def test_invalid_profile_shapes_are_rejected(
    path: tuple[object, ...], value: object
) -> None:
    api = _api()
    profile = _profile()
    target: Any = profile
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        api.profile_digest(profile)


@pytest.mark.parametrize(
    "path,value",
    [
        (("feature", "agents", 0, "prompt"), "changed"),
        (("feature", "agents", 0, "model", "effort"), "high"),
        (("feature", "agents", 0, "tools", 0, "description"), "new description"),
        (
            (
                "feature",
                "agents",
                0,
                "tools",
                0,
                "implementation",
                "files",
                0,
                "sha256",
            ),
            "b" * 64,
        ),
        (("feature", "agents", 0, "tools", 0, "input_schema", "required"), ["b", "a"]),
        (("feature", "agents", 0, "subagents", 0, "system_prompt"), "new prompt"),
        (("runtime", "toolbox", "fetch_allow_private"), True),
    ],
)
def test_recipe_changes_change_digest(path: tuple[object, ...], value: object) -> None:
    api = _api()
    profile = _profile()
    before = api.profile_digest(profile)
    target: Any = profile
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    assert api.profile_digest(profile) != before


def test_tool_and_schema_array_order_are_identity() -> None:
    api = _api()
    profile = _profile()
    tools = profile["feature"]["agents"][0]["tools"]
    tools.append(_tool("second"))
    before = api.profile_digest(profile)
    tools.reverse()
    assert api.profile_digest(profile) != before


def test_registered_implementation_reads_exact_package_bytes() -> None:
    api = _api()
    source = api.ToolImplementationSource(
        source_id="memory-v1",
        package="kokoro_agent",
        symbol="kokoro_agent.tools.memory.make_memory_tools",
        distribution="kokoro-agent",
        resource_paths=("tools/memory.py",),
    )
    descriptor = api.implementation_descriptor("memory-v1", (source,))
    from importlib.resources import files

    expected = hashlib.sha256(
        files("kokoro_agent").joinpath("tools/memory.py").read_bytes()
    ).hexdigest()
    assert descriptor["files"] == [{"path": "tools/memory.py", "sha256": expected}]
    assert descriptor["version"]
    with pytest.raises(ValueError):
        api.implementation_descriptor("missing", (source,))
    with pytest.raises(ValueError):
        api.implementation_descriptor("memory-v1", (source, source))


@pytest.mark.parametrize(
    "paths",
    [
        (),
        ("../secret",),
        ("/secret",),
        ("tools\\memory.py",),
        ("not-present.py",),
        ("tools/memory.py", "tools/memory.py"),
    ],
)
def test_source_missing_or_noncanonical_resource_fails_closed(
    paths: tuple[str, ...],
) -> None:
    api = _api()
    with pytest.raises(ValueError):
        source = api.ToolImplementationSource(
            source_id="x",
            package="kokoro_agent",
            symbol="kokoro_agent.tools.memory.make_memory_tools",
            distribution="kokoro-agent",
            resource_paths=paths,
        )
        api.implementation_descriptor("x", (source,))


def test_tool_descriptor_uses_only_explicit_public_metadata() -> None:
    api = _api()

    class Args(BaseModel):
        query: str

    secret = "closure-secret-not-profile"

    def invoke(query: str) -> str:
        return secret + query

    tool = StructuredTool(
        func=invoke, name="search", description="search", args_schema=Args
    )
    descriptor = api.tool_descriptor(tool, implementation=_source(), options={})
    assert descriptor["input_schema"] == tool.get_input_schema().model_json_schema()
    assert descriptor["name"] == tool.name
    assert secret not in json.dumps(descriptor)
    with pytest.raises(ValueError):
        api.tool_descriptor(tool, implementation={}, options={})


def test_profile_tools_uses_the_actual_selection_plan_without_second_selector() -> None:
    api = _api()
    from kokoro_agent.tools.toolset import plan_toolset
    from kokoro_agent.tools.toolbox import ProcessToolbox
    from kokoro_agent.agents.definition import Agent

    assert callable(getattr(api, "profile_tools", None)), (
        "profile must consume actual selection plan"
    )
    plan = plan_toolset(
        agent=Agent(key="base", prompt="base"),
        toolbox=ProcessToolbox(configured=()),
        delivery_available=False,
    )
    descriptors = {name: _tool(name) for name in plan.names}
    descriptors["unselected"] = _tool("unselected")
    projected = api.profile_tools(plan, descriptors)
    assert tuple(item["name"] for item in projected) == plan.names
    profile = _profile()
    profile["feature"]["agents"][0]["tools"] = projected
    before = api.profile_digest(profile)
    descriptors["unselected"]["description"] = "changed but not mounted"
    profile["feature"]["agents"][0]["tools"] = api.profile_tools(plan, descriptors)
    assert api.profile_digest(profile) == before
    del descriptors[plan.names[0]]
    with pytest.raises(ValueError):
        api.profile_tools(plan, descriptors)


def test_profile_subagents_projects_only_shared_plan_and_preserves_catalog_order() -> (
    None
):
    api = _api()
    from kokoro_agent.agents.subagents import plan_subagents
    from kokoro_agent.agents.subagent_catalog import SubagentCatalog, RegisteredSubagent

    assert callable(getattr(api, "profile_subagents", None)), (
        "profile must consume actual subagent plan"
    )

    def spec(name: str, tools: tuple[str, ...] = ()) -> RegisteredSubagent:
        return RegisteredSubagent(
            name=name,
            description=name,
            system_prompt=name,
            source="built-in",
            tools=tools,
        )

    catalog = SubagentCatalog((spec("b"), spec("a", ("needed",)), spec("unselected")))
    first = plan_subagents(catalog, frozenset(), selected=frozenset({"a", "b"}))
    second = plan_subagents(
        catalog, frozenset({"needed"}), selected=frozenset({"a", "b"})
    )
    assert [item["name"] for item in api.profile_subagents(first)] == ["b"]
    assert [item["name"] for item in api.profile_subagents(second)] == ["b", "a"]
    profile = _profile()
    profile["feature"]["agents"][0]["subagents"] = api.profile_subagents(first)
    before = api.profile_digest(profile)
    profile["feature"]["agents"][0]["subagents"] = api.profile_subagents(second)
    assert api.profile_digest(profile) != before


def test_profile_toolbox_requires_explicit_nonsecret_metadata() -> None:
    api = _api()
    from kokoro_agent.tools.toolbox import ProcessToolbox, build_toolbox

    assert callable(getattr(api, "profile_toolbox", None)), (
        "explicit toolbox metadata projection missing"
    )
    with pytest.raises(ValueError):
        api.profile_toolbox(ProcessToolbox(configured=()))
    box = build_toolbox(fetch_allow_private=True, search=None)
    assert api.profile_toolbox(box) == {
        "fetch_allow_private": True,
        "search_enabled": False,
        "search_provider": None,
    }


@pytest.mark.parametrize("provider", ["unknown", "https://private.example"])
def test_profile_does_not_accept_unknown_search_provider(provider: str) -> None:
    api = _api()
    profile = _profile()
    profile["runtime"]["toolbox"] = {
        "fetch_allow_private": False,
        "search_enabled": True,
        "search_provider": provider,
    }
    with pytest.raises(ValueError):
        api.profile_digest(profile)


def test_source_resource_list_is_immutable() -> None:
    api = _api()
    with pytest.raises(ValueError):
        api.ToolImplementationSource(
            source_id="memory-v1",
            package="kokoro_agent",
            symbol="kokoro_agent.tools.memory.make_memory_tools",
            distribution="kokoro-agent",
            resource_paths=["tools/memory.py"],
        )


def test_source_content_change_changes_descriptor_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _api()
    resource = tmp_path / "implementation.py"
    resource.write_bytes(b"first implementation")

    def source_root(package: str) -> Path:
        assert package == "kokoro_agent"
        return tmp_path

    monkeypatch.setattr(api.resources, "files", source_root)
    source = api.ToolImplementationSource(
        source_id="x",
        package="kokoro_agent",
        symbol="kokoro_agent.implementation",
        distribution="kokoro-agent",
        resource_paths=("implementation.py",),
    )
    before = api.implementation_descriptor("x", (source,))
    resource.write_bytes(b"second implementation")
    after = api.implementation_descriptor("x", (source,))
    assert before["files"] != after["files"]


def test_toolbox_profile_metadata_must_match_mounted_tools() -> None:
    api = _api()
    from kokoro_agent.tools.toolbox import ProcessToolbox, ToolboxProfileOptions

    box = ProcessToolbox(
        configured=(),
        profile_options=ToolboxProfileOptions(
            fetch_allow_private=False, search_provider="tavily"
        ),
    )
    with pytest.raises(ValueError):
        api.profile_toolbox(box)


def test_toolbox_credential_rotation_does_not_change_profile() -> None:
    api = _api()
    from kokoro_agent.tools.toolbox import build_toolbox
    from kokoro_agent.tools.web_search import SearchProviderSettings
    from pydantic import SecretStr

    left = build_toolbox(
        fetch_allow_private=False,
        search=SearchProviderSettings(
            provider="tavily", api_key=SecretStr("one"), base_url="https://one.example"
        ),
    )
    right = build_toolbox(
        fetch_allow_private=False,
        search=SearchProviderSettings(
            provider="tavily", api_key=SecretStr("two"), base_url="https://two.example"
        ),
    )
    assert api.profile_toolbox(left) == api.profile_toolbox(right)


def test_selected_descriptor_identity_mismatch_is_rejected() -> None:
    api = _api()
    from kokoro_agent.tools.toolset import plan_toolset
    from kokoro_agent.tools.toolbox import ProcessToolbox
    from kokoro_agent.agents.definition import Agent

    plan = plan_toolset(
        agent=Agent(key="base", prompt="base"),
        toolbox=ProcessToolbox(configured=()),
        delivery_available=False,
    )
    descriptors = {name: _tool(name) for name in plan.names}
    descriptors[plan.names[0]]["name"] = "different"
    with pytest.raises(ValueError):
        api.profile_tools(plan, descriptors)


def test_disabled_run_token_budget_is_a_valid_distinct_recipe() -> None:
    api = _api()
    profile = _profile()
    before = api.profile_digest(profile)
    profile["runtime"]["run_token_budget"] = 0
    assert api.profile_digest(profile) != before
    profile["runtime"]["run_token_budget"] = -1
    with pytest.raises(ValueError):
        api.profile_digest(profile)


def test_unknown_selected_subagent_is_rejected_by_the_shared_plan() -> None:
    from kokoro_agent.agents.subagents import plan_subagents
    from kokoro_agent.agents.subagent_catalog import SubagentCatalog

    with pytest.raises(ValueError, match="unknown subagents"):
        plan_subagents(
            SubagentCatalog(()), frozenset(), selected=frozenset({"missing"})
        )


def test_canonical_json_shared_encoder_rejects_bound_objects() -> None:
    from kokoro_agent.execution.runtime_profile import canonical_json

    assert (
        canonical_json({"b": (2, 1), "a": "真实"}) == '{"a":"真实","b":[2,1]}'.encode()
    )
    with pytest.raises(ValueError, match="unsupported"):
        canonical_json({"bound": object()})
    with pytest.raises(ValueError, match="finite"):
        canonical_json({"value": float("nan")})


def test_hitl_transaction_sources_are_explicit_installed_resources() -> None:
    from kokoro_agent.execution.runtime_profile_sources import production_manifest

    manifest = production_manifest()
    required = {
        "domain/run/interactions.py",
        "infrastructure/postgres_run_interactions.py",
    }
    registered = {
        path
        for source in manifest.sources
        if source.package == "kokoro_agent"
        for path in source.resource_paths
    }
    assert required <= registered
    for source in manifest.sources:
        if required.intersection(source.resource_paths):
            descriptor = manifest.descriptor(source.source_id)
            assert descriptor["files"]
