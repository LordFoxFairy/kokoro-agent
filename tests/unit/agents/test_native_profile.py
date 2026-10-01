"""Native metadata approval runs before any lazy bootstrap or plugin code."""

from importlib import import_module, metadata
import pytest


def test_unknown_plugin_rejected_before_load_or_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_module("kokoro_agent.agents.native_profile")
    calls: list[str] = []
    entry = metadata.EntryPoint(
        name="unknown", value="unknown:register", group="deepagents.harness_profiles"
    )
    monkeypatch.setattr(module, "enumerate_profile_plugins", lambda: (entry,))

    def load_entry(self: metadata.EntryPoint) -> None:
        calls.append("load")

    monkeypatch.setattr(metadata.EntryPoint, "load", load_entry)
    monkeypatch.setattr(
        module, "bootstrap_native_profiles", lambda: calls.append("bootstrap")
    )
    with pytest.raises(ValueError, match="plugin"):
        module.prepare_native_recipe()
    assert calls == []


def test_native_recipe_uses_installed_templates() -> None:
    module = import_module("kokoro_agent.agents.native_profile")
    from deepagents.middleware.subagents import GENERAL_PURPOSE_SUBAGENT

    recipe = module.prepare_native_recipe()
    assert recipe["materialization"] == "post_route"
    assert (
        recipe["general_purpose"]["system_prompt"]
        == GENERAL_PURPOSE_SUBAGENT["system_prompt"]
    )
    assert tuple(t["name"] for t in recipe["filesystem"]) == (
        "ls",
        "read_file",
        "write_file",
        "edit_file",
        "glob",
        "grep",
        "execute",
    )
    module.validate_native_registry()


def test_late_registry_mutation_rejected_without_callable_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_module("kokoro_agent.agents.native_profile")
    from deepagents.profiles.harness import harness_profiles
    from deepagents.profiles.harness.harness_profiles import HarnessProfile

    module.prepare_native_recipe()
    calls: list[str] = []
    from langchain.agents.middleware import AgentMiddleware

    def dynamic_middleware() -> tuple[AgentMiddleware, ...]:
        calls.append("middleware")
        return ()

    monkeypatch.setitem(
        getattr(harness_profiles, "_HARNESS_PROFILES"),
        "unregistered",
        HarnessProfile(extra_middleware=dynamic_middleware),
    )
    with pytest.raises(ValueError, match="registry"):
        module.validate_native_registry()
    assert calls == []


@pytest.mark.parametrize(
    "names", [("openai",), ("duplicate", "duplicate"), ("one", "two"), ("two", "one")]
)
def test_all_unapproved_plugin_orders_and_duplicates_are_side_effect_free(
    names: tuple[str, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    module = import_module("kokoro_agent.agents.native_profile")
    calls: list[str] = []
    entries = tuple(
        metadata.EntryPoint(
            name=name, value="extension:register", group="deepagents.provider_profiles"
        )
        for name in names
    )
    monkeypatch.setattr(module, "enumerate_profile_plugins", lambda: entries)
    monkeypatch.setattr(
        module, "bootstrap_native_profiles", lambda: calls.append("bootstrap")
    )
    with pytest.raises(ValueError, match="plugin"):
        module.prepare_native_recipe()
    assert calls == []


@pytest.mark.parametrize("change", ["replace", "remove", "reorder"])
def test_each_registry_mutation_is_rejected(
    change: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = import_module("kokoro_agent.agents.native_profile")
    from deepagents.profiles.harness import harness_profiles

    module.prepare_native_recipe()
    original = getattr(harness_profiles, "_HARNESS_PROFILES")
    altered = dict(original)
    first = next(iter(altered))
    if change == "replace":
        altered[first] = harness_profiles.HarnessProfile(
            system_prompt_suffix="unapproved"
        )
    elif change == "remove":
        del altered[first]
    else:
        altered = dict(reversed(tuple(altered.items())))
    monkeypatch.setattr(harness_profiles, "_HARNESS_PROFILES", altered)
    with pytest.raises(ValueError, match="registry"):
        module.validate_native_registry()


def test_bootstrap_rechecks_late_plugin_inventory_before_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_module("kokoro_agent.agents.native_profile")
    from deepagents.profiles import _builtin_profiles as bootstrap
    from deepagents.profiles.harness import harness_profiles
    from deepagents.profiles.provider import provider_profiles

    monkeypatch.setattr(module, "_sealed", None)
    monkeypatch.setattr(bootstrap, "_loaded", False)
    empty_harness: dict[str, harness_profiles.HarnessProfile] = {}
    empty_provider: dict[str, provider_profiles.ProviderProfile] = {}
    monkeypatch.setattr(bootstrap, "_HARNESS_PROFILES", empty_harness)
    monkeypatch.setattr(harness_profiles, "_HARNESS_PROFILES", empty_harness)
    monkeypatch.setattr(bootstrap, "_PROVIDER_PROFILES", empty_provider)
    monkeypatch.setattr(provider_profiles, "_PROVIDER_PROFILES", empty_provider)
    monkeypatch.setattr(bootstrap, "_BOOTSTRAP_HARNESS_KEYS", frozenset[str]())
    # Initial pure inventory was empty; installation changed before upstream discovery.
    monkeypatch.setattr(module, "enumerate_profile_plugins", lambda: ())
    entry = metadata.EntryPoint(
        name="late", value="late:register", group="deepagents.provider_profiles"
    )
    calls: list[str] = []

    def entries(*, group: str) -> tuple[metadata.EntryPoint, ...]:
        return (entry,) if group == entry.group else ()

    def load(self: metadata.EntryPoint):
        calls.append("load")
        return lambda: calls.append("plugin")

    monkeypatch.setattr(metadata.EntryPoint, "load", load)
    monkeypatch.setattr(metadata, "entry_points", entries)
    monkeypatch.setattr(bootstrap, "entry_points", entries)
    with (
        pytest.warns(UserWarning, match="Failed to enumerate"),
        pytest.raises(ValueError, match="plugin"),
    ):
        module.prepare_native_recipe()
    assert calls == []
