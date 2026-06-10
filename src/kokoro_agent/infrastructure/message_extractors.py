from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from langchain_core.messages import AIMessage, AIMessageChunk


def text_of(content: object) -> str:
    """Extract plain text from a message ``content``.

    Strings pass through. For list content (multi-modal / content blocks) only
    ``{"type": "text"}`` blocks are surfaced; thinking/tool/other blocks are
    deliberately dropped so they never leak into ``text``.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        blocks = cast("list[object]", content)
        parts: list[str] = []
        for block in blocks:
            if not isinstance(block, Mapping):
                continue
            typed_block = cast("Mapping[object, object]", block)
            if typed_block.get("type") != "text":
                continue
            text = typed_block.get("text", "")
            if isinstance(text, str):
                parts.append(text)
        return "".join(parts)
    return ""


def reasoning_of(message: AIMessage) -> str:
    """Reasoning/thinking text, when the model exposes it (reasoning models).

    Looks at ``additional_kwargs.reasoning_content`` and any ``thinking`` /
    ``reasoning`` content blocks. Returns "" for models that don't surface
    reasoning (e.g. plain chat models) — thinking then simply doesn't appear.
    """
    extra = cast("Mapping[str, object]", message.additional_kwargs or {})  # pyright: ignore  # langchain Any field
    reasoning = extra.get("reasoning_content")
    if isinstance(reasoning, str) and reasoning:
        return reasoning
    content = cast("object", message.content)  # pyright: ignore  # langchain Any field
    if isinstance(content, list):
        parts: list[str] = []
        for block in cast("list[object]", content):
            if not isinstance(block, Mapping):
                continue
            typed = cast("Mapping[object, object]", block)
            kind = typed.get("type")
            if kind not in ("thinking", "reasoning"):
                continue
            value = typed.get(kind) or typed.get("text")
            if isinstance(value, str):
                parts.append(value)
        return "".join(parts)
    return ""


def result_text(output: object) -> str:
    """Best-effort textual result of a tool call (ToolMessage/Command/str)."""
    content = getattr(output, "content", None)
    if isinstance(content, str):
        return content
    if content is not None:
        return str(content)
    return "" if output is None else str(output)


def as_ai_message(output: object) -> AIMessage | None:
    return output if isinstance(output, AIMessage) else None


def is_tool_call_only_chunk(chunk: AIMessageChunk) -> bool:
    """A chunk that carries only tool-call argument fragments, no answer text."""
    return bool(chunk.tool_call_chunks) and not text_of(cast("object", chunk.content))  # pyright: ignore  # langchain Any field
