"""MCP server 部署配置（KOKORO_MCP_CONFIG yaml）：wire 只传 names，定义住 agent 侧。

headers 值支持 ${ENV_NAME} 占位——凭据本体只走 env（ADR-010 凭据法则同轴），
yaml 里只出现引用名；引用缺失 fail-loud，绝不带残缺凭据连接。
env 由调用方显式注入（进程环境只在 worker/main 读取，架构规则）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Literal, TypeVar

import yaml
from pydantic import BaseModel, ConfigDict, StringConstraints

_NonEmpty = Annotated[str, StringConstraints(min_length=1)]

# 只识别整值占位 "${NAME}"：混拼（"Bearer ${X}"）不支持，凭据整值注入语义最简。
_ENV_REF = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


class McpConfigError(Exception):
    pass


class McpServerConfig(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    transport: Literal["http", "streamable_http"] = "streamable_http"
    url: _NonEmpty
    allowed_tools: list[_NonEmpty]
    timeout_s: int | None = None
    headers: dict[str, str] | None = None


class McpServerUnavailable(BaseModel):
    """已占名但不可用的定义位：名字是已知的（不触发未知名 fail-loud），定义不可用。

    来源（mcp/registry.py 双源合并）：
    - 活跃禁用文档遮蔽同名低层定义、不回退（对齐 kokoro-hub 侧语义：绝不静默回退到官方凭据）；
    - secret_ref=secret:path V1 不支持（P2：secret 管理器网关侧解析）。
    装配不炸；list 标注不可用，describe/call 返回 error 文本（不可达降级同轴）。
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    reason: _NonEmpty


# run 装配用的定义表值型：可连接配置 或 占名不可用位。
McpServerEntry = McpServerConfig | McpServerUnavailable


class _McpServersFile(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    servers: dict[_NonEmpty, McpServerConfig] = {}


def _resolve_header(value: str, env: Mapping[str, str]) -> str:
    matched = _ENV_REF.match(value)
    if matched is None:
        return value
    resolved = env.get(matched.group(1))
    if resolved is None or resolved == "":
        raise McpConfigError(f"mcp header env ref {value!r} is not set")
    return resolved


def load_mcp_servers(path: str | None, env: Mapping[str, str]) -> Mapping[str, McpServerConfig]:
    """缺省（未配置文件）= 空注册表：wire 点名任何 server 都会 fail-loud。"""
    if path is None or path == "":
        return {}
    raw: object = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    file = _McpServersFile.model_validate(raw if raw is not None else {})
    registry: dict[str, McpServerConfig] = {}
    for name, config in file.servers.items():
        if config.headers is None:
            registry[name] = config
            continue
        headers = {key: _resolve_header(value, env) for key, value in config.headers.items()}
        registry[name] = config.model_copy(update={"headers": headers})
    return registry


_EntryT = TypeVar("_EntryT")


def select_servers(registry: Mapping[str, _EntryT], names: Sequence[str]) -> dict[str, _EntryT]:
    """wire names → 配置子集：未知名 fail-loud（配置即授权边界，绝不静默跳过）。"""
    unknown = sorted(set(names) - set(registry))
    if unknown:
        raise McpConfigError(f"unknown mcp servers {unknown} (check KOKORO_MCP_CONFIG)")
    # dict.fromkeys 去重且保序：names 重复点名幂等。
    return {name: registry[name] for name in dict.fromkeys(names)}
