"""Version-pinned native metadata boundary; never resolves a model or runs middleware.

Plugins are deliberately unsupported in production. Metadata is checked before the
upstream lazy bootstrap, and registry objects are sealed by identity after it. The
runtime remains DeepAgents; this module does not select a harness or build a graph.
"""

from __future__ import annotations

from importlib import metadata
from threading import RLock
from collections.abc import Mapping
from typing import TypeGuard

from deepagents.middleware import filesystem as fs
from deepagents.middleware.subagents import (
    GENERAL_PURPOSE_SUBAGENT,
    TASK_TOOL_DESCRIPTION,
)
from deepagents.profiles import _builtin_profiles as bootstrap
from deepagents.profiles.harness import harness_profiles as harness
from deepagents.profiles.provider import provider_profiles as provider
from langchain.agents.middleware.todo import (
    WRITE_TODOS_TOOL_DESCRIPTION,
    WriteTodosInput,
)

_GROUPS = ("deepagents.provider_profiles", "deepagents.harness_profiles")
_HARNESS_KEYS = (
    "anthropic:claude-opus-4-7",
    "anthropic:claude-sonnet-4-6",
    "anthropic:claude-haiku-4-5",
    "openai:gpt-5.1-codex",
    "openai:gpt-5.2-codex",
    "openai:gpt-5.3-codex",
)
_LOCK = RLock()
_sealed: tuple[tuple[str, object], ...] | None = None


def enumerate_profile_plugins() -> tuple[metadata.EntryPoint, ...]:
    """Read dist-info only, never load/import an entry point."""
    return tuple(ep for group in _GROUPS for ep in metadata.entry_points(group=group))


def _validate_plugins() -> None:
    if metadata.version("deepagents") != "0.6.6":
        raise ValueError("unapproved native profile version")
    entries = enumerate_profile_plugins()
    identities = tuple((ep.group, ep.name, ep.value) for ep in entries)
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate native profile plugin identity")
    # Approval is intentionally empty. Future extensions need a separately reviewed
    # ordered identity/distribution/version/source/key list, not a name allow-list.
    if identities:
        raise ValueError("unapproved native profile plugin")


def _is_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _registry() -> tuple[tuple[str, object], ...]:
    result: list[tuple[str, object]] = []
    for prefix, module, name, expected in (
        ("provider:", provider, "_PROVIDER_PROFILES", provider.ProviderProfile),
        ("harness:", harness, "_HARNESS_PROFILES", harness.HarnessProfile),
    ):
        value: object = getattr(module, name)
        if not _is_mapping(value):
            raise ValueError("native registry shape changed")
        for key, profile in value.items():
            if not isinstance(key, str) or not isinstance(profile, expected):
                raise ValueError("native registry entry shape changed")
            result.append((prefix + key, profile))
    return tuple(result)


def bootstrap_native_profiles() -> None:
    """Called only after the pure plugin gate, before any runtime model binding."""
    global _sealed
    _validate_plugins()
    with _LOCK:
        if _sealed is not None:
            return
        # Already initialized registries have no authenticated provenance snapshot.
        # Fail closed rather than blessing earlier arbitrary public registrations.
        if getattr(bootstrap, "_loaded") is True or _registry():
            raise ValueError("native registry initialized before approval")
        # Version-pinned private bootstrap has no public initialization API.
        initialize: object = getattr(bootstrap, "_ensure_builtin_profiles_loaded")
        if not callable(initialize):
            raise ValueError("native bootstrap shape changed")
        # Upstream discovers entry points again during bootstrap and swallows plugin
        # errors. Guard that exact enumeration too, so a late installation never
        # reaches ep.load(), and propagate rejection after its swallowed error.
        rejected: list[Exception] = []

        def guarded_entries(*, group: str) -> tuple[metadata.EntryPoint, ...]:
            try:
                if group not in _GROUPS or metadata.entry_points(group=group):
                    raise ValueError(
                        "unapproved native profile plugin during bootstrap"
                    )
                return ()
            except Exception as error:
                rejected.append(error)
                raise

        original_reader: object = getattr(bootstrap, "entry_points")
        setattr(bootstrap, "entry_points", guarded_entries)
        try:
            initialize()
        finally:
            setattr(bootstrap, "entry_points", original_reader)
        if rejected:
            raise ValueError("native profile plugin inventory rejected") from rejected[
                0
            ]
        entries = _registry()
        expected_keys = (
            "provider:openai",
            "provider:openrouter",
            *("harness:" + key for key in _HARNESS_KEYS),
        )
        if tuple(key for key, _ in entries) != expected_keys:
            raise ValueError("native builtin registry differs from approved keys")
        if any(
            isinstance(profile, harness.HarnessProfile)
            and (callable(profile.extra_middleware) or profile.extra_middleware)
            for _, profile in entries
        ):
            raise ValueError(
                "runtime-only native middleware requires post-route binding"
            )
        _sealed = _registry()


def validate_native_registry() -> None:
    """Check metadata again and reject added, replaced, removed or reordered entries."""
    _validate_plugins()
    bootstrap_native_profiles()
    current = _registry()
    expected = _sealed
    if (
        expected is None
        or len(current) != len(expected)
        or any(
            name != wanted_name or value is not wanted_value
            for (name, value), (wanted_name, wanted_value) in zip(
                current, expected, strict=True
            )
        )
    ):
        raise ValueError("native registry mutation after approval")


def prepare_native_recipe() -> dict[str, object]:
    """Installed baseline templates, not model-dependent effective descriptors."""
    _validate_plugins()
    bootstrap_native_profiles()
    validate_native_registry()
    filesystem = (
        ("ls", fs.LIST_FILES_TOOL_DESCRIPTION, fs.LsSchema),
        ("read_file", fs.READ_FILE_TOOL_DESCRIPTION, fs.ReadFileSchema),
        ("write_file", fs.WRITE_FILE_TOOL_DESCRIPTION, fs.WriteFileSchema),
        ("edit_file", fs.EDIT_FILE_TOOL_DESCRIPTION, fs.EditFileSchema),
        ("glob", fs.GLOB_TOOL_DESCRIPTION, fs.GlobSchema),
        ("grep", fs.GREP_TOOL_DESCRIPTION, fs.GrepSchema),
        ("execute", fs.EXECUTE_TOOL_DESCRIPTION, fs.ExecuteSchema),
    )
    return {
        "materialization": "post_route",
        "filesystem": tuple(
            {
                "name": name,
                "description": description,
                "input_schema": schema.model_json_schema(),
            }
            for name, description, schema in filesystem
        ),
        "todo": {
            "name": "write_todos",
            "description": WRITE_TODOS_TOOL_DESCRIPTION,
            "input_schema": WriteTodosInput.model_json_schema(),
        },
        "task_template": TASK_TOOL_DESCRIPTION,
        "general_purpose": {
            "name": GENERAL_PURPOSE_SUBAGENT["name"],
            "description": GENERAL_PURPOSE_SUBAGENT["description"],
            "system_prompt": GENERAL_PURPOSE_SUBAGENT["system_prompt"],
        },
        "injection_order": ("todo", "filesystem", "task"),
        "policy_keys": tuple(name for name, _ in _registry()),
        "approved_plugins": (),
    }


def native_task_description(selected: tuple[tuple[str, str], ...]) -> str:
    """Project the installed task template using the existing selection, not a selector."""
    descriptions = (
        (GENERAL_PURPOSE_SUBAGENT["name"], GENERAL_PURPOSE_SUBAGENT["description"]),
        *selected,
    )
    return TASK_TOOL_DESCRIPTION.format(
        available_agents="\n".join(
            f"- {name}: {description}" for name, description in descriptions
        )
    )
