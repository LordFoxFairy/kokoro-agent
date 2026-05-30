"""astream_events mapper — consume a DeepAgents graph as raw agent events.

## A1 Spike — Locked astream_events event shapes (deepagents==0.6.6)
------------------------------------------------------------------------

All events in the stream have at least ``event["event"]`` (str) and
``event["data"]`` (dict).  The ``event["run_id"]`` is unique per graph node
invocation and is the stable pairing key for tool start/end.

### Relevant event types:

``on_chat_model_stream``
  ``data["chunk"]``: an ``AIMessageChunk``.
  ``chunk.content``: ``str`` — empty ``""`` for tool-call-only turns;
  plain text for text turns; or a list of blocks for thinking turns
  (``[{"type": "thinking", "thinking": "..."}, {"type": "text", "text": "..."}]``).
  ``chunk.tool_call_chunks``: populated for tool-call-only turns (not used
  directly; we use ``on_tool_start/end`` for tool events).

``on_tool_start``
  ``event["name"]``: tool name (e.g. ``"write_todos"``, ``"echo_search"``).
  ``event["run_id"]``: stable ID, identical to the matching ``on_tool_end``.
  ``data["input"]``: the tool's argument dict (e.g. ``{"todos": [...]}``,
  ``{"query": "..."}``) — passed verbatim as ``args`` in ``tool.invoked``.

``on_tool_end``
  ``event["name"]``: same tool name as the matching ``on_tool_start``.
  ``event["run_id"]``: same as the matching ``on_tool_start`` — use as
  ``tool_call_ref`` to pair start/end.

``on_chat_model_end``
  ``data["output"].generations`` is always ``[]`` in the scripted fake (0
  count); in a real model it carries the full message.  Do NOT use this for
  text completion — instead accumulate from ``on_chat_model_stream`` chunks.

Run boundary: no explicit run event from DeepAgents.  We yield ``run.started``
before the astream loop and ``run.completed`` after it finishes normally.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any, cast

from kokoro_agent.events import AgentEvent, RunRequest

LOGGER = logging.getLogger(__name__)

ASTREAM_TIMEOUT_S = 120
RECURSION_LIMIT = 25


def _text_and_thinking(chunk: object) -> tuple[str, str]:
    """Split a streamed chat-model chunk's content into (text, thinking).

    Content is either a str (text) or a list of blocks; thinking blocks carry
    ``{"type": "thinking", "thinking": ...}``.  Shapes confirmed in the A1
    spike: real Anthropic models emit list blocks; scripted fakes emit plain
    strings.
    """
    content = getattr(chunk, "content", "")
    if isinstance(content, str):
        return content, ""
    text_parts: list[str] = []
    think_parts: list[str] = []
    if isinstance(content, list):
        for block in cast("list[object]", content):
            if not isinstance(block, dict):
                continue
            b = cast("dict[object, object]", block)
            t = b.get("type")
            if t == "text" and isinstance(b.get("text"), str):
                text_parts.append(cast("str", b["text"]))
            elif t == "thinking" and isinstance(b.get("thinking"), str):
                think_parts.append(cast("str", b["thinking"]))
    return "".join(text_parts), "".join(think_parts)


async def run_agent(  # noqa: C901 — cohesive event mapper
    req: RunRequest, agent: Any
) -> AsyncIterator[AgentEvent]:
    """Stream a DeepAgents run as raw agent events (fully generic).

    Maps ``astream_events`` (v2) to the raw family:

    - Model token chunks → ``text.delta`` / ``thinking.delta``
      (``thinking.delta`` only when ``execution_style=="thinking"``).
    - ALL tools (including ``write_todos``) → ``tool.invoked{...,args}`` /
      ``tool.returned``.  The agent does NOT know about "plan"; session
      harness recognizes ``write_todos`` by tool_name.
    - End of stream → ``run.completed``.
    - Any error → ``run.failed``.

    ``tool.invoked`` always carries ``args`` = the tool's raw input dict
    (``event["data"]["input"]`` from ``on_tool_start``), so the session
    harness can extract ``todos`` from ``write_todos`` calls without
    re-parsing.
    """
    seq = 1
    message_ref = "m1"
    thinking_mode = req.execution_style == "thinking"
    text_buf = ""
    yield AgentEvent(kind="run.started", run_id=req.run_id, seq=seq, payload={})
    try:
        async with asyncio.timeout(ASTREAM_TIMEOUT_S):
            stream = agent.astream_events(
                {"messages": [("user", req.input)]},
                version="v2",
                config={"recursion_limit": RECURSION_LIMIT},
            )
            async for event in stream:
                evt_type: str = event["event"]
                name: str = event.get("name", "")
                data = cast("dict[str, object]", event.get("data", {}))

                if evt_type == "on_chat_model_stream":
                    text, thinking = _text_and_thinking(data.get("chunk"))
                    if thinking and thinking_mode:
                        seq += 1
                        yield AgentEvent(
                            kind="thinking.delta",
                            run_id=req.run_id,
                            seq=seq,
                            payload={"text": thinking},
                        )
                    if text:
                        text_buf += text
                        seq += 1
                        yield AgentEvent(
                            kind="text.delta",
                            run_id=req.run_id,
                            seq=seq,
                            payload={"message_ref": message_ref, "text": text},
                        )

                elif evt_type == "on_tool_start":
                    # All tools treated generically (incl. write_todos).
                    # Spike confirmed: event["run_id"] is stable across
                    # on_tool_start / on_tool_end for the same invocation.
                    tool_call_ref = str(event.get("run_id", ""))
                    payload: dict[str, object] = {
                        "tool_call_ref": tool_call_ref,
                        "tool_name": name,
                    }
                    tool_input = data.get("input")
                    if isinstance(tool_input, dict):
                        payload["args"] = tool_input
                    seq += 1
                    yield AgentEvent(
                        kind="tool.invoked",
                        run_id=req.run_id,
                        seq=seq,
                        payload=payload,
                    )

                elif evt_type == "on_tool_end":
                    tool_call_ref = str(event.get("run_id", ""))
                    seq += 1
                    yield AgentEvent(
                        kind="tool.returned",
                        run_id=req.run_id,
                        seq=seq,
                        payload={
                            "tool_call_ref": tool_call_ref,
                            "tool_name": name,
                            "status": "ok",
                        },
                    )

        if text_buf:
            seq += 1
            yield AgentEvent(
                kind="text.completed",
                run_id=req.run_id,
                seq=seq,
                payload={"message_ref": message_ref, "text": text_buf},
            )
        seq += 1
        yield AgentEvent(
            kind="run.completed",
            run_id=req.run_id,
            seq=seq,
            payload={"status": "completed"},
        )
    except Exception as error:  # noqa: BLE001 — boundary: any failure -> run.failed
        LOGGER.exception("run_agent error for run_id=%s", req.run_id)
        seq += 1
        yield AgentEvent(
            kind="run.failed",
            run_id=req.run_id,
            seq=seq,
            payload={
                "error_kind": type(error).__name__,
                "message": str(error),
            },
        )
