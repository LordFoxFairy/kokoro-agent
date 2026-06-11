from __future__ import annotations

from collections.abc import Mapping
from typing import TypeGuard

from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage


def is_str_object_mapping(obj: object) -> TypeGuard[Mapping[str, object]]:
    return isinstance(obj, Mapping)


def is_object_list(obj: object) -> TypeGuard[list[object]]:
    return isinstance(obj, list)


def as_mapping(obj: object) -> Mapping[str, object]:
    """Narrow an opaque value to a string-keyed mapping (else empty)."""
    return obj if is_str_object_mapping(obj) else {}


def message_content(message: BaseMessage) -> str | list[object]:
    # langchain stubs content as `str | list[str | dict]` (bare dict -> partially
    # unknown under strict); read via getattr (Any) and re-narrow to known shapes.
    content: object = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    return content if is_object_list(content) else ""


def _additional_kwargs(message: AIMessage) -> Mapping[str, object]:
    # additional_kwargs is a bare dict in stubs; getattr (Any) + guard re-narrows.
    extra: object = getattr(message, "additional_kwargs", None)
    return extra if is_str_object_mapping(extra) else {}


def text_of(content: str | list[object]) -> str:
    """Extract plain text from a message ``content``.

    Strings pass through. For list content (multi-modal / content blocks) only
    ``{"type": "text"}`` blocks are surfaced; thinking/tool/other blocks are
    deliberately dropped so they never leak into ``text``.
    """
    if isinstance(content, str):
        return content
    parts: list[str] = []
    for block in content:
        if not is_str_object_mapping(block):
            continue
        if block.get("type") != "text":
            continue
        text = block.get("text", "")
        if isinstance(text, str):
            parts.append(text)
    return "".join(parts)


def reasoning_of(message: AIMessage) -> str:
    """Reasoning/thinking text, when the model exposes it (reasoning models).

    Looks at ``additional_kwargs.reasoning_content`` and any ``thinking`` /
    ``reasoning`` content blocks. Returns "" for models that don't surface
    reasoning (e.g. plain chat models) — thinking then simply doesn't appear.
    """
    reasoning = _additional_kwargs(message).get("reasoning_content")
    if isinstance(reasoning, str) and reasoning:
        return reasoning
    content = message_content(message)
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not is_str_object_mapping(block):
            continue
        kind = block.get("type")
        if kind not in ("thinking", "reasoning"):
            continue
        value = block.get(kind, None) or block.get("text")
        if isinstance(value, str):
            parts.append(value)
    return "".join(parts)


def result_text(output: object) -> str:
    """Best-effort textual result of a tool call (ToolMessage/Command/str)."""
    content: object = getattr(output, "content", None)
    if isinstance(content, str):
        return content
    if content is not None:
        return str(content)
    return "" if output is None else str(output)


def as_ai_message(output: object) -> AIMessage | None:
    return output if isinstance(output, AIMessage) else None


def is_tool_call_only_chunk(chunk: AIMessageChunk) -> bool:
    """A chunk that carries only tool-call argument fragments, no answer text."""
    return bool(chunk.tool_call_chunks) and not text_of(message_content(chunk))
