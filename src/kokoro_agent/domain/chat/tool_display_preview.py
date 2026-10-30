"""Private, bounded tool previews; filtering is not proof that arbitrary text is secret-free."""

from __future__ import annotations

import json
import math
import re
from typing import Any, Self

from mcp.types import CallToolResult

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
    TypeAdapter,
)

from kokoro_agent.domain.chat.projection import (
    ToolDisplayDetail,
    safe_display_name,
)

from kokoro_agent.domain.chat.display_text import private_text, sensitive_display_text

_SECRET_KEY = re.compile(
    r"(?i)(password|passwd|secret|token|credential|authorization|cookie|api.?key|private.?key|"
    r"headers?|environment|^env$|transport|runtime|reasoning|chain.?of.?thought)"
)


class ToolPreview(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    text: str = Field(min_length=1, max_length=4096)
    truncated: bool
    redacted: bool

    @field_validator("text")
    @classmethod
    def valid_text(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 4096 or private_text(value):
            raise ValueError("invalid private preview")
        return value


class ActivityDetail(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    activity_id: str = Field(
        pattern=r"^act_[0-9a-f]{64}$", min_length=68, max_length=68
    )
    tool_name: str
    target: ToolDisplayDetail | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    input_preview: ToolPreview | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    output_preview: ToolPreview | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    tool_description: ToolPreview | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @field_validator("tool_name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if not safe_display_name(value) or sensitive_display_text(value):
            raise ValueError("invalid private tool name")
        return value

    @model_validator(mode="after")
    def input_bound(self) -> Self:
        if (
            self.input_preview is not None
            and len(self.input_preview.text.encode()) > 2048
        ):
            raise ValueError("private input preview too large")
        if self.tool_description is not None and (
            len(self.tool_description.text.encode()) > 1024
            or self.tool_name not in {"mcp_call", "mcp_describe_tool"}
            or self.target is None
            or self.target.kind != "mcp_tool"
        ):
            raise ValueError("description requires bounded same-Run MCP evidence")
        for name in ("target", "input_preview", "output_preview", "tool_description"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError("null optional private detail")
        return self


def clip_text(text: str, limit: int) -> str:
    return text.encode("utf-8")[:limit].decode("utf-8", errors="ignore")


def preview(value: object, *, limit: int, plain: bool = False) -> ToolPreview | None:
    """Remove suspect scalar blocks/keys before deterministic bounded formatting."""
    nodes = 0
    redacted = False
    truncated = False
    omitted = object()

    def clean(item: object, depth: int) -> object:
        nonlocal nodes, redacted, truncated
        nodes += 1
        if nodes > 128 or depth > 6:
            truncated = True
            return omitted
        if isinstance(item, str):
            if len(item) > 1024 * 1024 or private_text(item):
                redacted = True
                return omitted
            return item
        if item is None or type(item) in (bool, int):
            return item
        if isinstance(item, float):
            return item if math.isfinite(item) else omitted
        if isinstance(item, dict):
            result: dict[str, object] = {}
            for index, (key, child) in enumerate(
                TypeAdapter(dict[str, object])
                .validate_python(item, strict=True)
                .items()
            ):
                if index >= 32:
                    truncated = True
                    break
                if _SECRET_KEY.search(key) or private_text(key):
                    redacted = True
                    continue
                cleaned = clean(child, depth + 1)
                if cleaned is not omitted:
                    result[key] = cleaned
            return result if result or not item else omitted
        if isinstance(item, list):
            values: list[object] = []
            for index, child in enumerate(
                TypeAdapter(list[object]).validate_python(item, strict=True)
            ):
                if index >= 32:
                    truncated = True
                    break
                cleaned = clean(child, depth + 1)
                if cleaned is not omitted:
                    values.append(cleaned)
            return values if values or not item else omitted
        redacted = True
        return omitted

    cleaned = clean(value, 0)
    if cleaned is omitted:
        return None
    text = (
        cleaned
        if plain and isinstance(cleaned, str)
        else json.dumps(cleaned, ensure_ascii=False, indent=2, allow_nan=False)
    )
    if not text:
        return None
    truncated = truncated or len(text.encode()) > limit
    return ToolPreview(
        text=clip_text(text, limit), truncated=truncated, redacted=redacted
    )


def bounded_json(raw: str) -> object:
    """Check JSON nesting before parsing, and reject duplicates/non-finite values."""
    if len(raw) > 1024 * 1024 or len(raw.encode("utf-8")) > 1024 * 1024:
        raise ValueError("private result exceeds decode budget")
    depth = 0
    quoted = False
    escaped = False
    for c in raw:
        if quoted:
            if escaped:
                escaped = False
            elif c == "\\":
                escaped = True
            elif c == '"':
                quoted = False
        elif c == '"':
            quoted = True
        elif c in "[{":
            depth += 1
            if depth > 10:  # six content levels plus the MCP envelope
                raise ValueError("private result exceeds decode depth")
        elif c in "]}":
            depth -= 1

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate private result key")
            result[key] = value
        return result

    def constant(_value: str) -> object:
        raise ValueError("invalid private result constant")

    return json.loads(raw, object_pairs_hook=unique, parse_constant=constant)


def _plain_prose(value: str) -> bool:
    # The Web reader interprets JSON-shaped previews. Keep that representation
    # enveloped so a literal JSON text block is never decoded for a second time.
    if not value.strip() or value.lstrip().startswith(("{", "[", '"')):
        return False
    try:
        json.loads(value)
    except ValueError:
        return True
    return False


def mcp_output(raw: str) -> ToolPreview | None:
    """Only standard MCP text/structuredContent, never resource bytes or metadata."""
    try:
        result = CallToolResult.model_validate(bounded_json(raw))
        blocks = [block.text for block in result.content if block.type == "text"]
        # Filter each content block independently; unsupported content is omitted.
        if (
            len(blocks) == 1
            and result.structuredContent == {"result": blocks[0]}
            and _plain_prose(blocks[0])
        ):
            # Exact duplicate representation only: no extra keys, joining,
            # output inference or removal of distinct structured information.
            projected = preview(blocks[0], limit=4096, plain=True)
        elif result.structuredContent is not None:
            projected = preview(
                {"text": blocks, "structured_content": result.structuredContent},
                limit=4096,
            )
        elif len(blocks) == 1:
            projected = preview(blocks[0], limit=4096, plain=True)
        else:
            projected = preview(blocks, limit=4096) if blocks else None
        if projected is not None and len(blocks) != len(result.content):
            projected = projected.model_copy(update={"redacted": True})
        return projected
    except (ValueError, UnicodeError, RecursionError):
        return None


def described_tool(
    raw: str, *, server: str, tool: str
) -> tuple[str, dict[str, object]] | None:
    """Read only the exact describe wrapper, before preview clipping/filtering."""
    if len(raw) > 1024 * 1024:
        return None
    prefix = f"{server}/{tool}\n"
    if not raw.startswith(prefix):
        return None
    description, separator, schema = raw[len(prefix) :].rpartition("\n参数 schema：")
    if not separator:
        return None
    try:
        value = bounded_json(schema)
        if not isinstance(value, dict):
            return None
        return description, TypeAdapter(dict[str, object]).validate_python(
            value, strict=True
        )
    except (ValueError, UnicodeError, RecursionError):
        return None


def preparation_output(
    raw: str,
    *,
    name: str,
    connections: tuple[str, ...],
    server: str | None,
    tool: str | None,
) -> ToolPreview | None:
    """Project the stable wrappers' exact format, dropping binding IDs entirely."""
    if len(raw) > 1024 * 1024:
        return None
    if name == "mcp_describe_tool":
        if server not in connections or tool is None:
            return None
        parsed = described_tool(raw, server=server, tool=tool)
        if parsed is None:
            return None
        description, value = parsed
        return preview(
            {
                "tool": tool,
                "description": description,
                "input_schema": TypeAdapter(dict[str, object]).validate_python(
                    value, strict=True
                ),
            },
            limit=4096,
        )
    items: list[dict[str, str]] = []
    omitted = False
    truncated = False
    for index, line in enumerate(raw.splitlines()):
        if index >= 32:
            truncated = True
            break
        connection = next(
            (item for item in connections if line.startswith(item + "/")), None
        )
        if connection is None:
            omitted = True
            continue
        selector, separator, description = line[len(connection) + 1 :].partition(" — ")
        if (
            not separator
            or not safe_display_name(selector)
            or sensitive_display_text(selector)
        ):
            omitted = True
            continue
        items.append({"tool": selector, "description": description})
    result = preview(items, limit=4096) if items else None
    if result is not None and (omitted or truncated):
        result = result.model_copy(
            update={
                "redacted": result.redacted or omitted,
                "truncated": result.truncated or truncated,
            }
        )
    return result
