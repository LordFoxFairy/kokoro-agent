"""Validated MCP definitions returned by the exact Hub execution assembly."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated, Literal, TypeVar
from pydantic import BaseModel, ConfigDict, StringConstraints

_NonEmpty = Annotated[str, StringConstraints(min_length=1)]

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

    Kept as a runtime availability value for connection failures surfaced after assembly.
    装配不炸；list 标注不可用，describe/call 返回 error 文本（不可达降级同轴）。
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    reason: _NonEmpty


# run 装配用的定义表值型：可连接配置 或 占名不可用位。
McpServerEntry = McpServerConfig | McpServerUnavailable


_EntryT = TypeVar("_EntryT")


def select_servers(registry: Mapping[str, _EntryT], names: Sequence[str]) -> dict[str, _EntryT]:
    """wire names → 配置子集：未知名 fail-loud（配置即授权边界，绝不静默跳过）。"""
    unknown = sorted(set(names) - set(registry))
    if unknown:
        raise McpConfigError(f"unknown mcp servers {unknown} in resolved execution assembly")
    # dict.fromkeys 去重且保序：names 重复点名幂等。
    return {name: registry[name] for name in dict.fromkeys(names)}
