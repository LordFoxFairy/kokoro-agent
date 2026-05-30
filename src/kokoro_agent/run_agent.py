from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import cast

from langchain_core.language_models import BaseChatModel

from kokoro_agent.events import AgentEvent, RunRequest

ASTREAM_TIMEOUT_S = 120


def _text_of(content: object) -> str:
    """Extract plain text from a chunk's ``content``.

    Strings pass through. For list content (multi-modal / content blocks) only
    ``{"type": "text"}`` blocks are surfaced; thinking/tool/other blocks are
    deliberately dropped so they never leak into ``text.delta``.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        blocks = cast("list[object]", content)
        parts: list[str] = []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            typed_block = cast("dict[object, object]", block)
            if typed_block.get("type") != "text":
                continue
            text = typed_block.get("text", "")
            if isinstance(text, str):
                parts.append(text)
        return "".join(parts)
    return ""


async def run_agent(
    req: RunRequest, model: BaseChatModel
) -> AsyncIterator[AgentEvent]:
    """Stream a real LLM brain as raw agent events.

    Emits the agent-events contract sequence:
    ``run.started`` -> ``text.delta``* -> ``text.completed`` -> ``run.completed``.
    Any failure during streaming is caught and surfaced as a single
    ``run.failed`` event (never re-raised). ``seq`` is monotonic from 1; a single
    ``message_ref`` groups the streamed deltas. The agent fills only execution
    semantics — cursors/ids/owner belong to kokoro-session.
    """
    seq = 1
    message_ref = "m1"
    full = ""
    yield AgentEvent(kind="run.started", run_id=req.run_id, seq=seq, payload={})
    try:
        async with asyncio.timeout(ASTREAM_TIMEOUT_S):
            async for ev in model.astream_events([("user", req.input)]):
                if ev.get("event") != "on_chat_model_stream":
                    continue
                chunk = ev.get("data", {}).get("chunk")
                text = _text_of(getattr(chunk, "content", ""))
                if not text:
                    continue
                seq += 1
                full += text
                yield AgentEvent(
                    kind="text.delta",
                    run_id=req.run_id,
                    seq=seq,
                    payload={"message_ref": message_ref, "text": text},
                )
        seq += 1
        yield AgentEvent(
            kind="text.completed",
            run_id=req.run_id,
            seq=seq,
            payload={"message_ref": message_ref, "text": full},
        )
        seq += 1
        yield AgentEvent(
            kind="run.completed",
            run_id=req.run_id,
            seq=seq,
            payload={"status": "completed"},
        )
    except Exception as error:  # noqa: BLE001 — boundary: any brain failure -> run.failed
        seq += 1
        yield AgentEvent(
            kind="run.failed",
            run_id=req.run_id,
            seq=seq,
            payload={"error_kind": type(error).__name__, "message": str(error)},
        )
