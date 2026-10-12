"""MCP server deployment configuration (``KOKORO_MCP_CONFIG``).

The deployment registry contains only server launch/connection metadata. It is
not a Capability resource store and is never populated from browser input.
HTTP headers and explicit stdio child-process environment values accept an
entire-value ``${ENV_NAME}`` reference; secret values therefore stay outside
YAML. A missing reference fails during worker startup instead of starting a
server with a partial credential.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Literal, TypeAlias, TypeVar

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
)

_NonEmpty = Annotated[str, StringConstraints(min_length=1)]

# Recognize an entire value only. Interpolation such as ``Bearer ${TOKEN}`` is
# intentionally not supported: credentials cross this boundary as one value.
_ENV_REF = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


class McpConfigError(Exception):
    """A deployment MCP registry is malformed or refers to an absent secret."""


class _McpServerBase(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    allowed_tools: list[_NonEmpty]


class McpHttpServerConfig(_McpServerBase):
    """A remote Streamable HTTP MCP server.

    The registry uses the MCP transport name ``streamable_http`` exactly.
    """

    transport: Literal["streamable_http"]
    url: _NonEmpty
    timeout_s: int | None = Field(default=None, gt=0)
    headers: dict[str, str] | None = None


class McpStdioServerConfig(_McpServerBase):
    """A local standard MCP process launched by the upstream stdio adapter.

    There is deliberately no URL, HTTP header or egress setting here: stdio
    has no network connection to guard. ``env`` is optional; omitting it keeps
    the upstream MCP SDK's standard inherited environment behavior.
    """

    transport: Literal["stdio"] = "stdio"
    command: _NonEmpty
    args: list[str] = Field(default_factory=list)
    cwd: str | None = None
    env: dict[str, str] | None = None


McpServerConfig: TypeAlias = McpHttpServerConfig | McpStdioServerConfig
_McpServerConfigWire: TypeAlias = Annotated[
    McpServerConfig,
    Field(discriminator="transport"),
]
_MCP_SERVER_ADAPTER: TypeAdapter[McpServerConfig] = TypeAdapter(McpServerConfig)


class McpServerUnavailable(BaseModel):
    """A known server name that cannot be used for this run.

    An unavailable Capability definition masks a lower deployment definition;
    no fallback is attempted. Assembly remains available so list/describe/call
    can report the authoritative unavailable reason instead of crashing a run.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    reason: _NonEmpty


McpServerEntry: TypeAlias = McpServerConfig | McpServerUnavailable


class _McpServersFile(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    servers: dict[_NonEmpty, _McpServerConfigWire] = Field(default_factory=dict)


def parse_mcp_server_config(raw: object) -> McpServerConfig:
    """Validate one registry item through the same discriminated wire shape."""

    return _MCP_SERVER_ADAPTER.validate_python(raw)


def _resolve_env_reference(value: str, env: Mapping[str, str]) -> str:
    matched = _ENV_REF.match(value)
    if matched is None:
        return value
    resolved = env.get(matched.group(1))
    if resolved is None or resolved == "":
        raise McpConfigError(f"mcp environment reference {value!r} is not set")
    return resolved


def _resolve_values(
    values: Mapping[str, str] | None, env: Mapping[str, str]
) -> dict[str, str] | None:
    if values is None:
        return None
    return {key: _resolve_env_reference(value, env) for key, value in values.items()}


def load_mcp_servers(
    path: str | None, env: Mapping[str, str]
) -> Mapping[str, McpServerConfig]:
    """Load the deployment registry. An absent file is an empty registry."""

    if path is None or path == "":
        return {}
    raw: object = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    file = _McpServersFile.model_validate(raw if raw is not None else {})
    registry: dict[str, McpServerConfig] = {}
    for name, config in file.servers.items():
        if isinstance(config, McpHttpServerConfig):
            headers = _resolve_values(config.headers, env)
            registry[name] = (
                config
                if headers is None
                else config.model_copy(update={"headers": headers})
            )
            continue
        process_env = _resolve_values(config.env, env)
        registry[name] = (
            config
            if process_env is None
            else config.model_copy(update={"env": process_env})
        )
    return registry


_EntryT = TypeVar("_EntryT")


def select_servers(
    registry: Mapping[str, _EntryT], names: Sequence[str]
) -> dict[str, _EntryT]:
    """Resolve wire names to a de-duplicated registry subset, fail-closed."""

    unknown = sorted(set(names) - set(registry))
    if unknown:
        raise McpConfigError(f"unknown mcp servers {unknown} (check KOKORO_MCP_CONFIG)")
    return {name: registry[name] for name in dict.fromkeys(names)}
