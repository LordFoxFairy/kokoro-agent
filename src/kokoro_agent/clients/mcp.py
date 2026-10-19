"""Typed trusted Connection selection and the narrow run-bound MCP tool port."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import re
from typing import Protocol
from kokoro_agent.domain.run.models import LeasedRun

from langchain_mcp_adapters.callbacks import Callbacks
from mcp.types import CallToolResult
from pydantic import JsonValue


class McpClientError(RuntimeError):
    """Fixed safe category; no endpoint, credential or upstream exception."""


@dataclass(frozen=True, slots=True, kw_only=True)
class McpConnectionRef:
    connection_id: str

    def __post_init__(self) -> None:
        if (
            type(self.connection_id) is not str
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,190}", self.connection_id)
            is None
        ):
            raise ValueError("MCP_CONNECTION_REF_INVALID")


@dataclass(frozen=True, slots=True, kw_only=True)
class McpTool:
    name: str
    description: str
    input_schema: dict[str, JsonValue]


class McpClient(Protocol):
    async def check(self, references: Sequence[McpConnectionRef]) -> None: ...
    async def list_tools(self, reference: McpConnectionRef) -> tuple[McpTool, ...]: ...
    async def call_tool(
        self,
        reference: McpConnectionRef,
        tool: str,
        arguments: dict[str, JsonValue],
        *,
        tool_call_id: str,
        callbacks: Callbacks | None = None,
    ) -> CallToolResult: ...


class RunMcpFactory(Protocol):
    def mcp_for_run(self, leased_run: LeasedRun) -> McpClient: ...
