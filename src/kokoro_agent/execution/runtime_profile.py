"""Pure v1 recipe identity and explicit installed-source provenance.

No runtime gate or production manifest is installed here. Callers must provide
complete, approved metadata; request, client, model and credential objects are
never serialized. The execution assembly/freeze stage is a separate slice.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import hashlib
from importlib import metadata, resources
import json
import math
from pathlib import PurePosixPath
import re
from typing import TypeAlias, TypeGuard

from langchain_core.tools import BaseTool

from kokoro_agent.agents.subagents import SubagentSelectionPlan
from kokoro_agent.tools.toolset import ToolSelectionPlan
from kokoro_agent.tools.toolbox import ProcessToolbox
from kokoro_agent.tools.web_search import (
    SUPPORTED_SEARCH_PROVIDERS,
    WEB_SEARCH_TOOL_NAME,
)
from kokoro_agent.tools.web_fetch import WEB_FETCH_TOOL_NAME

PROFILE_VERSION = 1
JsonValue: TypeAlias = (
    None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
)
Validator: TypeAlias = Callable[[object], JsonValue]


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("profile text must be a string")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError as error:
        raise ValueError("profile text must be valid UTF-8") from error
    return value


def _nonempty(value: object) -> str:
    result = _text(value)
    if not result:
        raise ValueError("profile identifier must be nonempty")
    return result


def _boolean(value: object) -> bool:
    if type(value) is not bool:
        raise ValueError("profile flag must be a boolean")
    return value


def _nonnegative(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("profile budget must be a nonnegative integer")
    return value


def _positive(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError("profile limit must be a positive integer")
    return value


def _is_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _is_array(value: object) -> TypeGuard[list[object] | tuple[object, ...]]:
    return isinstance(value, (list, tuple))


def _is_frozenset(value: object) -> TypeGuard[frozenset[object]]:
    return isinstance(value, frozenset)


def _mapping(value: object) -> dict[str, object]:
    if not _is_mapping(value):
        raise ValueError("profile object required")
    result: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("profile object keys must be strings")
        result[_text(key)] = item
    return result


def _items(value: object) -> Sequence[object]:
    if not _is_array(value):
        raise ValueError("profile ordered array required")
    return value


def _record(value: object, fields: Mapping[str, Validator]) -> dict[str, JsonValue]:
    obj = _mapping(value)
    if set(obj) != set(fields):
        raise ValueError("profile object has missing or unknown fields")
    return {key: validate(obj[key]) for key, validate in fields.items()}


def _array(value: object, validate: Validator) -> list[JsonValue]:
    return [validate(item) for item in _items(value)]


def _strings(value: object) -> list[JsonValue]:
    return _array(value, _nonempty)


def _json(value: object) -> JsonValue:
    if value is None:
        return None
    if type(value) is bool:
        return value
    if type(value) is int:
        return value
    if isinstance(value, str):
        return _text(value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("profile number must be finite")
        return value
    if _is_array(value):
        return _array(value, _json)
    if _is_mapping(value):
        return {_text(key): _json(item) for key, item in _mapping(value).items()}
    raise ValueError("unsupported profile value")


def _schema(value: object) -> JsonValue:
    return _json(_mapping(value))


def _nullable(validate: Validator) -> Validator:
    return lambda value: None if value is None else validate(value)


def _choice(*choices: str) -> Validator:
    def validate(value: object) -> str:
        text = _text(value)
        if text not in choices:
            raise ValueError("unsupported profile choice")
        return text

    return validate


def _pause_tools(value: object) -> list[JsonValue]:
    if not _is_frozenset(value):
        raise ValueError("pause_tools must use frozenset semantics")
    return list(sorted(_nonempty(item) for item in value))


def _resource_path(value: object) -> str:
    path = _nonempty(value)
    parts = PurePosixPath(path).parts
    if (
        not parts
        or path.startswith("/")
        or "\\" in path
        or any(part in ("", ".", "..") for part in path.split("/"))
        or "\x00" in path
    ):
        raise ValueError("source resource path must be canonical and relative")
    return path


def _qualified_name(value: object) -> str:
    name = _nonempty(value)
    if re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", name, flags=re.ASCII) is None:
        raise ValueError("source package/symbol must be a qualified name")
    return name


def _digest(value: object) -> str:
    text = _text(value)
    if re.fullmatch(r"[0-9a-f]{64}", text) is None:
        raise ValueError("source digest must be lowercase SHA-256")
    return text


def _source_id(value: object) -> str:
    text = _nonempty(value)
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]*", text) is None:
        raise ValueError("source identity must be an opaque local identifier")
    return text


def _implementation(value: object) -> dict[str, JsonValue]:
    result = _record(
        value,
        {
            "source_id": _source_id,
            "package": _qualified_name,
            "symbol": _qualified_name,
            "distribution": _nonempty,
            "version": _nonempty,
            "files": lambda entries: _array(
                entries,
                lambda entry: _record(
                    entry,
                    {
                        "path": _resource_path,
                        "sha256": _digest,
                    },
                ),
            ),
        },
    )
    paths = [_text(_mapping(item)["path"]) for item in _items(result["files"])]
    if not paths or len(set(paths)) != len(paths):
        raise ValueError("implementation needs unique, explicit source resources")
    return result


@dataclass(frozen=True, slots=True, kw_only=True)
class ToolImplementationSource:
    """An explicitly registered implementation and its reviewed resource closure."""

    source_id: str
    package: str
    symbol: str
    distribution: str
    resource_paths: tuple[str, ...]

    def __post_init__(self) -> None:
        _source_id(self.source_id)
        _qualified_name(self.package)
        _qualified_name(self.symbol)
        _nonempty(self.distribution)
        if type(self.resource_paths) is not tuple:
            raise ValueError("implementation resource list must be immutable")
        if not self.resource_paths or len(set(self.resource_paths)) != len(
            self.resource_paths
        ):
            raise ValueError("implementation needs unique, explicit source resources")
        for path in self.resource_paths:
            _resource_path(path)


def implementation_descriptor(
    source_id: str, registered: Sequence[ToolImplementationSource]
) -> dict[str, JsonValue]:
    """Read only registered installed resources, never Git or dynamic callables."""
    by_id = {source.source_id: source for source in registered}
    if len(by_id) != len(registered) or source_id not in by_id:
        raise ValueError("unknown or duplicate registered implementation")
    source = by_id[source_id]
    try:
        root = resources.files(source.package)
        files: list[JsonValue] = [
            {
                "path": path,
                "sha256": hashlib.sha256(root.joinpath(path).read_bytes()).hexdigest(),
            }
            for path in source.resource_paths
        ]
        version = metadata.version(source.distribution)
    except (
        OSError,
        ModuleNotFoundError,
        TypeError,
        metadata.PackageNotFoundError,
    ) as error:
        raise ValueError("registered implementation resource unavailable") from error
    return _implementation(
        {
            "source_id": source.source_id,
            "package": source.package,
            "symbol": source.symbol,
            "distribution": source.distribution,
            "version": version,
            "files": files,
        }
    )


def _tool_options(value: object) -> dict[str, JsonValue]:
    obj = _mapping(value)
    allowed: dict[str, Validator] = {
        "fetch_allow_private": _boolean,
        "search_provider": _choice(*sorted(SUPPORTED_SEARCH_PROVIDERS)),
        "policy_id": _nonempty,
    }
    if not set(obj).issubset(allowed):
        raise ValueError("unknown tool behavior option")
    return {key: allowed[key](item) for key, item in obj.items()}


def _tool(value: object) -> dict[str, JsonValue]:
    return _record(
        value,
        {
            "name": _nonempty,
            "description": _text,
            "input_schema": _schema,
            "return_direct": _boolean,
            "response_format": _choice("content", "content_and_artifact"),
            "implementation": _implementation,
            "options": _tool_options,
        },
    )


def tool_descriptor(
    tool: BaseTool,
    *,
    implementation: Mapping[str, object],
    options: Mapping[str, object],
) -> dict[str, JsonValue]:
    """Project public schema/behavior only, without model_dump or closure inspection."""
    return _tool(
        {
            "name": tool.name,
            "description": tool.description,
            "input_schema": tool.get_input_schema().model_json_schema(),
            "return_direct": tool.return_direct,
            "response_format": tool.response_format,
            "implementation": implementation,
            "options": options,
        }
    )


def _subagent(value: object) -> dict[str, JsonValue]:
    return _record(
        value,
        {
            "name": _nonempty,
            "description": _text,
            "system_prompt": _text,
            "source": _nonempty,
            "tools": _strings,
        },
    )


def _agent(value: object) -> dict[str, JsonValue]:
    return _record(
        value,
        {
            "key": _nonempty,
            "prompt": _text,
            "tools": lambda v: _array(v, _tool),
            "implicit_tools": lambda v: _array(v, _tool),
            "mcp": _strings,
            "subagents": lambda v: _array(v, _subagent),
            "delivery": lambda v: _record(
                v, {"declared": _boolean, "mounted": _boolean}
            ),
            "model": _nullable(
                lambda v: _record(
                    v,
                    {
                        "provider": _nonempty,
                        "name": _nonempty,
                        "effort": _nullable(_nonempty),
                        "thinking": _nullable(_boolean),
                    },
                )
            ),
            "backend": lambda v: _record(
                v,
                {
                    "kind": _choice("state", "local_shell", "docker", "e2b", "custom"),
                    "policy_id": _nonempty,
                    "implementation": _implementation,
                },
            ),
            "permissions": lambda v: _record(
                v,
                {
                    "approval_tools": _strings,
                    "review_tools": _strings,
                    "subagent_create": _choice("deny", "ask", "allow"),
                    "filesystem": _choice("read_only", "workspace_write"),
                },
            ),
            "pause_tools": _pause_tools,
        },
    )


def _handoff(value: object) -> list[JsonValue]:
    pair = _strings(value)
    if len(pair) != 2:
        raise ValueError("handoff must be an ordered source/target pair")
    return pair


def _toolbox_options(value: object) -> dict[str, JsonValue]:
    result = _record(
        value,
        {
            "fetch_allow_private": _boolean,
            "search_enabled": _boolean,
            "search_provider": _nullable(_choice(*sorted(SUPPORTED_SEARCH_PROVIDERS))),
        },
    )
    if result["search_enabled"] != (result["search_provider"] is not None):
        raise ValueError("search configuration must explicitly match availability")
    return result


def canonical_profile(profile: Mapping[str, object]) -> bytes:
    """Validate complete v1 metadata and encode it without mutable runtime state."""
    result = _record(
        profile,
        {
            "profile_version": _positive,
            "feature": lambda v: _record(
                v,
                {
                    "key": _nonempty,
                    "entry_agent": _nonempty,
                    "handoffs": lambda pairs: _array(pairs, _handoff),
                    "agents": lambda agents: _array(agents, _agent),
                },
            ),
            "runtime": lambda v: _record(
                v,
                {
                    "run_token_budget": _nonnegative,
                    "recursion_limit": _positive,
                    "disable_streaming": _boolean,
                    "openai_reasoning": _boolean,
                    "litellm_enabled": _boolean,
                    "toolbox": _toolbox_options,
                    "delivery_available": _boolean,
                },
            ),
        },
    )
    if result["profile_version"] != PROFILE_VERSION:
        raise ValueError("unsupported profile version")
    feature = _mapping(result["feature"])
    agents = [_mapping(item) for item in _items(feature["agents"])]
    keys = [_text(agent["key"]) for agent in agents]
    if not keys or len(set(keys)) != len(keys) or feature["entry_agent"] not in keys:
        raise ValueError("feature needs unique agents and its actual entry agent")
    for pair in _items(feature["handoffs"]):
        if any(_text(name) not in keys for name in _items(pair)):
            raise ValueError("handoff must reference selected agents")
    return canonical_json(result)


def canonical_json(value: object) -> bytes:
    """Encode explicitly projected JSON; bound objects and non-finite numbers fail."""
    return json.dumps(
        _json(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def profile_digest(profile: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_profile(profile)).hexdigest()


def profile_tools(
    plan: ToolSelectionPlan, descriptors: Mapping[str, Mapping[str, object]]
) -> list[JsonValue]:
    """Use the materializer's actual plan, not a separate profile selector."""
    if len(set(plan.names)) != len(plan.names):
        raise ValueError("duplicate tool in selection plan")
    result: list[JsonValue] = []
    for name in plan.names:
        if name not in descriptors:
            raise ValueError("selected tool descriptor is not registered")
        descriptor = _tool(descriptors[name])
        if descriptor["name"] != name:
            raise ValueError("selected tool descriptor identity mismatch")
        result.append(descriptor)
    return result


def profile_subagents(plan: SubagentSelectionPlan) -> list[JsonValue]:
    return [_subagent(spec) for spec in plan.as_profile()]


def profile_toolbox(toolbox: ProcessToolbox) -> dict[str, JsonValue]:
    if toolbox.profile_options is None:
        raise ValueError("toolbox requires explicit profile metadata")
    options = _toolbox_options(toolbox.profile_options.as_profile())
    names = tuple(tool.name for tool in toolbox.configured)
    expected = (
        (WEB_FETCH_TOOL_NAME, WEB_SEARCH_TOOL_NAME)
        if options["search_enabled"]
        else (WEB_FETCH_TOOL_NAME,)
    )
    if names != expected:
        raise ValueError("toolbox profile metadata differs from its mounted tools")
    return options
