"""统一配置树（ADR-010）：KOKORO_AGENT_CONFIG yaml 按域分组，env 只做覆盖与凭据。

机制：yaml 树按映射表摊平成"env 键→原生值"的底座字典（保留 bool/int/list 原生类型，
不再 stringify）；真 env 叠加其上覆盖。映射表即配置 schema：未知键 fail-loud；
凭据（api key/secret）故意不在表内——写进 yaml 即报错，强制走 env/secret 注入。
原生值最终交 AppConfig（lax pydantic）统一 coerce，取代此前的手写 stringify/parse 往返。
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import yaml
from pydantic import TypeAdapter

# 外部 yaml 是不可信字典：TypeAdapter 运行时洗净（str 键 + object 值收窄）。
_TREE_ADAPTER: TypeAdapter[dict[str, object]] = TypeAdapter(dict[str, object])

# yaml 路径（点分域）→ env 键。加配置项 = AppConfig 字段 + 此表一行 + example 一行。
_YAML_TO_ENV: dict[str, str] = {
    "model.disable_streaming": "KOKORO_DISABLE_STREAMING",
    "model.openai_base_url": "OPENAI_BASE_URL",
    "model.openai_reasoning": "KOKORO_OPENAI_REASONING",
    "model.anthropic_base_url": "ANTHROPIC_BASE_URL",
    "model.litellm_enabled": "KOKORO_LITELLM_ENABLED",
    "model.litellm_base_url": "KOKORO_LITELLM_BASE_URL",
    "stream.redis_url": "KOKORO_REDIS_URL",
    "storage.database_url": "KOKORO_AGENT_DATABASE_URL",
    "storage.database_schema": "KOKORO_AGENT_DATABASE_SCHEMA",
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
