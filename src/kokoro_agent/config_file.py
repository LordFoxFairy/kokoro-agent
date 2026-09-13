"""统一配置树（ADR-010）：KOKORO_AGENT_CONFIG yaml 按域分组，env 只做覆盖与凭据。

机制：yaml 树按映射表摊平成"env 键→原生值"的底座字典（保留 bool/int/list 原生类型，
不再 stringify）；真 env 叠加其上覆盖。映射表即配置 schema：未知键 fail-loud；
凭据（api key/secret）故意不在表内——写进 yaml 即报错，强制走 env/secret 注入。
原生值最终交 AppConfig（lax pydantic）统一 coerce，取代此前的手写 stringify/parse 往返。
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import json
from pathlib import Path
from typing import Protocol, runtime_checkable

import yaml
from pydantic import TypeAdapter
from yaml.events import (
    AliasEvent,
    DocumentEndEvent,
    DocumentStartEvent,
    Event,
    MappingEndEvent,
    MappingStartEvent,
    ScalarEvent,
    SequenceEndEvent,
    SequenceStartEvent,
    StreamEndEvent,
    StreamStartEvent,
)

# 外部 yaml 是不可信字典：TypeAdapter 运行时洗净（str 键 + object 值收窄）。
_TREE_ADAPTER: TypeAdapter[dict[str, object]] = TypeAdapter(dict[str, object])

# yaml 路径（点分域）→ env 键。加配置项 = AppConfig 字段 + 此表一行 + example 一行。
_YAML_TO_ENV: dict[str, str] = {
    "http.host": "KOKORO_AGENT_HTTP_HOST",
    "http.port": "KOKORO_AGENT_HTTP_PORT",
    "model.disable_streaming": "KOKORO_DISABLE_STREAMING",
    "model.openai_base_url": "OPENAI_BASE_URL",
    "model.openai_reasoning": "KOKORO_OPENAI_REASONING",
    "model.anthropic_base_url": "ANTHROPIC_BASE_URL",
    "model.litellm_enabled": "KOKORO_LITELLM_ENABLED",
    "model.litellm_base_url": "KOKORO_LITELLM_BASE_URL",
    "stream.redis_url": "KOKORO_REDIS_URL",
    "database.url": "KOKORO_AGENT_DATABASE_URL",
    "database.schema": "KOKORO_AGENT_DATABASE_SCHEMA",
    "run_repository.lease_ttl_s": "KOKORO_LEASE_TTL_S",
    "sandbox.local_shell.root": "KOKORO_AGENT_LOCAL_SHELL_ROOT",
    "sandbox.local_shell.inherit_env": "KOKORO_AGENT_LOCAL_SHELL_INHERIT_ENV",
    "sandbox.local_shell.timeout": "KOKORO_AGENT_LOCAL_SHELL_TIMEOUT",
    "sandbox.local_shell.max_output_bytes": "KOKORO_AGENT_LOCAL_SHELL_MAX_OUTPUT_BYTES",
    "sandbox.docker.image": "KOKORO_DOCKER_IMAGE",
    "sandbox.docker.ttl": "KOKORO_DOCKER_TTL",
    "sandbox.e2b.template": "KOKORO_E2B_TEMPLATE",
    "sandbox.e2b.timeout": "KOKORO_E2B_TIMEOUT",
    "sandbox.custom.factory": "KOKORO_CUSTOM_BACKEND",
    "sandbox.custom.config": "KOKORO_CUSTOM_BACKEND_CONFIG",
    "sandbox.custom.teardown": "KOKORO_CUSTOM_BACKEND_TEARDOWN",
    "workspace_config": "KOKORO_WORKSPACE_CONFIG",
    "mcp.config": "KOKORO_MCP_CONFIG",
    "mcp.egress_mode": "KOKORO_MCP_EGRESS_MODE",
    "web_tools.fetch_allow_private": "KOKORO_WEB_FETCH_ALLOW_PRIVATE",
    "web_tools.search.provider": "KOKORO_WEB_SEARCH_PROVIDER",
    "web_tools.search.url": "KOKORO_WEB_SEARCH_URL",
    "subagents.builtin": "KOKORO_BUILTIN_SUBAGENTS",
    "subagents.custom_json": "KOKORO_CUSTOM_SUBAGENTS",
    "limits.lease_heartbeat_s": "KOKORO_LEASE_HEARTBEAT_S",
    "limits.recursion_limit": "KOKORO_RECURSION_LIMIT",
    "limits.drain_timeout_s": "KOKORO_DRAIN_TIMEOUT_S",
    "limits.run_token_budget": "KOKORO_RUN_TOKEN_BUDGET",
    "retention.events_ttl_s": "KOKORO_RETENTION_EVENTS_TTL_S",
    "retention.run_ttl_s": "KOKORO_RETENTION_RUN_TTL_S",
}

_HTTP_YAML_TO_ENV = {
    path: env
    for path, env in _YAML_TO_ENV.items()
    if path
    in {
        "http.host",
        "http.port",
        "stream.redis_url",
        "database.url",
        "database.schema",
        "run_repository.lease_ttl_s",
    }
}


def _walk(prefix: str, node: object, out: dict[str, object]) -> None:
    if isinstance(node, Mapping):
        for key, value in _TREE_ADAPTER.validate_python(node).items():
            _walk(f"{prefix}.{key}" if prefix else key, value, out)
        return
    env_key = _YAML_TO_ENV.get(prefix)
    if env_key is None:
        # 未知键 fail-loud——含被故意排除的凭据键：凭据只走 env/secret，绝不进配置文件。
        raise KeyError(
            f"unknown config key {prefix!r} in KOKORO_AGENT_CONFIG "
            f"(credentials are env-only by design)"
        )
    if node is None:
        return
    # 原生值直接落座（bool/int/str/list）：类型收窄交 AppConfig 的 pydantic 统一处理。
    out[env_key] = node


def load_config_file(path: str | None) -> dict[str, object]:
    """yaml 配置树 → env 键→原生值底座；缺省（未配置文件）= 空底座，行为与纯 env 完全一致。"""
    if path is None or path == "":
        return {}
    raw: object = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise TypeError("KOKORO_AGENT_CONFIG must be a mapping of config domains")
    out: dict[str, object] = {}
    _walk("", _TREE_ADAPTER.validate_python(raw), out)
    return out


def _http_scalar_value(event: ScalarEvent) -> object:
    explicit = {
        "tag:yaml.org,2002:null": "null",
        "tag:yaml.org,2002:bool": "bool",
        "tag:yaml.org,2002:int": "int",
        "tag:yaml.org,2002:float": "float",
        "tag:yaml.org,2002:timestamp": "timestamp",
        "tag:yaml.org,2002:binary": "binary",
    }
    if event.tag == "tag:yaml.org,2002:str":
        return event.value
    if event.tag is not None:
        kind = explicit.get(event.tag)
        if kind is None:
            raise TypeError("HTTP configuration scalar tag is unsupported")
        return yaml.safe_load(f"!!{kind} {json.dumps(event.value)}")
    if event.style is None and event.value.lower() in {"", "null", "~"}:
        return None
    if event.style is not None:
        return event.value
    return yaml.safe_load(event.value)


def _has_descendant(path: str, candidates: Mapping[str, str]) -> bool:
    prefix = f"{path}."
    return any(candidate.startswith(prefix) for candidate in candidates)


def _next_http_event(events: Iterator[Event]) -> Event:
    try:
        return next(events)
    except StopIteration as error:
        raise TypeError("KOKORO_AGENT_CONFIG ended unexpectedly") from error


@runtime_checkable
class _ObjectIterator(Protocol):
    def __next__(self) -> object: ...


def _http_yaml_events(source: object) -> Iterator[Event]:
    parser: object = getattr(yaml, "parse")
    if not callable(parser):
        raise RuntimeError("PyYAML parser is unavailable")
    parsed: object = parser(source, Loader=yaml.SafeLoader)
    if not isinstance(parsed, _ObjectIterator):
        raise RuntimeError("PyYAML parser returned an invalid event stream")
    while True:
        try:
            event: object = next(parsed)
        except StopIteration:
            return
        if not isinstance(event, Event):
            raise RuntimeError("PyYAML parser returned an invalid event")
        yield event


def _skip_http_value(events: Iterator[Event], first: Event) -> None:
    if isinstance(first, ScalarEvent):
        return
    if isinstance(first, AliasEvent):
        raise TypeError("KOKORO_AGENT_CONFIG aliases are not supported")
    if isinstance(first, MappingStartEvent):
        while True:
            item = _next_http_event(events)
            if isinstance(item, MappingEndEvent):
                return
            _skip_http_value(events, item)
            _skip_http_value(events, _next_http_event(events))
    if isinstance(first, SequenceStartEvent):
        while True:
            item = _next_http_event(events)
            if isinstance(item, SequenceEndEvent):
                return
            _skip_http_value(events, item)
    raise TypeError("KOKORO_AGENT_CONFIG contains an invalid YAML value")


def _project_http_yaml(
    events: Iterator[Event], prefix: str, out: dict[str, object]
) -> None:
    seen: set[str] = set()
    while True:
        key_event = _next_http_event(events)
        if isinstance(key_event, MappingEndEvent):
            return
        if not isinstance(key_event, ScalarEvent):
            raise TypeError("KOKORO_AGENT_CONFIG keys must be strings")
        key = key_event.value
        if key in seen:
            raise KeyError(f"duplicate config key {key!r} in KOKORO_AGENT_CONFIG")
        seen.add(key)
        path = f"{prefix}.{key}" if prefix else key
        value_event = _next_http_event(events)
        env_key = _HTTP_YAML_TO_ENV.get(path)
        if env_key is not None:
            if not isinstance(value_event, ScalarEvent):
                raise TypeError("HTTP configuration values must be scalar")
            value = _http_scalar_value(value_event)
            if value is not None:
                out[env_key] = value
            continue
        if _has_descendant(path, _HTTP_YAML_TO_ENV):
            if not isinstance(value_event, MappingStartEvent):
                raise TypeError("HTTP configuration domains must be mappings")
            _project_http_yaml(events, path, out)
            continue
        if path in _YAML_TO_ENV or _has_descendant(path, _YAML_TO_ENV):
            # Consume but never construct or retain a worker-owned subtree.
            _skip_http_value(events, value_event)
            continue
        raise KeyError(f"unknown HTTP config key {path!r} in KOKORO_AGENT_CONFIG")


def load_http_config_file(path: str | None) -> dict[str, object]:
    """Project only HTTP business fields from the shared YAML document."""
    if path is None or path == "":
        return {}
    with Path(path).open("r", encoding="utf-8") as source:
        events = _http_yaml_events(source)
        if not isinstance(_next_http_event(events), StreamStartEvent):
            raise TypeError("KOKORO_AGENT_CONFIG is not a YAML stream")
        start = _next_http_event(events)
        if isinstance(start, StreamEndEvent):
            return {}
        if not isinstance(start, DocumentStartEvent):
            raise TypeError("KOKORO_AGENT_CONFIG is not a YAML document")
        root = _next_http_event(events)
        if not isinstance(root, MappingStartEvent):
            raise TypeError("KOKORO_AGENT_CONFIG must be a mapping of config domains")
        out: dict[str, object] = {}
        _project_http_yaml(events, "", out)
        if not isinstance(_next_http_event(events), DocumentEndEvent) or not isinstance(
            _next_http_event(events), StreamEndEvent
        ):
            raise TypeError("KOKORO_AGENT_CONFIG must contain exactly one document")
    return out
