"""Agent factory for the Kokoro agent runtime.

``make_agent`` builds a DeepAgents ``CompiledStateGraph`` (a langgraph
compiled graph with the standard ``astream_events`` interface).

``KOKORO_MODEL=scripted`` uses an offline scripted fake (no network/key).
Any other value is a ``provider:model`` spec passed to ``init_chat_model``.

## A1 Spike — Locked DeepAgents Facts (deepagents==0.6.6)
--------------------------------------------------------------

### 1. todo-only construction knob
``create_deep_agent`` always injects ``TodoListMiddleware`` (write_todos),
``FilesystemMiddleware`` (ls/read_file/write_file/edit_file/glob/grep/execute),
and ``SubAgentMiddleware`` (task).  ``FilesystemMiddleware`` and
``SubAgentMiddleware`` are **required scaffolding** and CANNOT be excluded via
``excluded_middleware``.

To expose only ``write_todos`` + user-supplied tools:
- Register a ``HarnessProfile`` for the fake model LLM type that sets
  ``excluded_tools=frozenset({fs_tools, execute, task})`` and
  ``general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False)``.
- Pass ``subagents=None`` (default) so no synchronous subagents → task tool
  not backed → not exposed.
- This tolerates FilesystemMiddleware/SubAgentMiddleware being present in
  the stack (required) but they inject no visible tools.

### 2. write_todos event shape (on_tool_start)
``event["name"]`` = ``"write_todos"``
``event["data"]["input"]`` = ``{"todos": [{"content": str, "status": str}, ...]}``
Status values confirmed: ``"pending"``, ``"in_progress"``, ``"completed"``

### 3. on_chat_model_stream chunk shapes
``event["data"]["chunk"].content`` is a ``str`` for text turns,
or ``""`` for tool-call-only turns.
``event["data"]["chunk"].tool_call_chunks`` contains tool call info (not used
directly in the mapper — we use ``on_tool_start/end`` for tool events).
Thinking/reasoning blocks: the scripted fake does not emit them; a real
Anthropic model with ``execution_style="thinking"`` would emit content blocks
``[{"type": "thinking", "thinking": "..."}, {"type": "text", "text": "..."}]``.

### 4. on_chat_model_end
``event["data"]["output"].generations`` is always an empty list (count=0) in
the scripted fake — the result is communicated through the graph state, not
the event ``output``. Do NOT rely on ``on_chat_model_end`` for text completion;
instead accumulate text from ``on_chat_model_stream`` chunks and emit
``text.completed`` at end of stream.

### 5. Run boundary events
No explicit ``on_chain_start/end`` for "run" boundaries needed.  Use:
- yield ``run.started`` before the astream loop
- yield ``run.completed`` after the astream loop

### 6. Tool pairing (tool_call_ref)
``event["run_id"]`` is **identical** for ``on_tool_start`` and ``on_tool_end``
of the same tool invocation.  Use it as the stable ``tool_call_ref``.
Example: ``on_tool_start(write_todos, run_id=X)`` → ``on_tool_end(run_id=X)``.
Different invocations of the same tool have different run_ids.
"""
from __future__ import annotations

import os

from typing import Any

from deepagents import GeneralPurposeSubagentProfile, HarnessProfile, create_deep_agent, register_harness_profile  # pyright: ignore[reportUnknownVariableType]
from langchain.chat_models import init_chat_model

from kokoro_agent.infrastructure._scripted import DeepAgentsFakeChatModel, scripted_planning_model
from kokoro_agent.tools import TOOL_OBJECTS

DEFAULT_MODEL = "anthropic:claude-sonnet-4-6"

KOKORO_AGENT_PROMPT = (
    "You are Kokoro's creation orchestrator. Plan multi-step creative tasks with "
    "the todo tool, then execute. Do not use filesystem, code execution, or "
    "sub-agents in this build."
)

# FS/shell/subagent tools to hide from the model (the middleware is still
# present — it's required scaffolding — but these tools are excluded from the
# visible tool set so the model never sees or calls them).
_KOKORO_EXCLUDED_TOOLS: frozenset[str] = frozenset(
    {"ls", "read_file", "write_file", "edit_file", "glob", "grep", "execute", "task"}
)

# Register once per process for each model provider key we use.  DeepAgents
# looks up harness profiles by the model's resolved ``ls_provider`` (lowercased,
# hyphens stripped) — NOT by ``_llm_type``.  We derive the scripted fake's
# actual provider key from the model itself rather than hardcode-guessing it,
# so the FS/execute/task exclusion is reliably applied to the scripted model.
_KOKORO_HARNESS_PROFILE = HarnessProfile(
    general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
    excluded_tools=_KOKORO_EXCLUDED_TOOLS,
)


def _resolve_fake_provider_key() -> str:
    """Return the ``ls_provider`` key DeepAgents uses to look up the harness
    profile for the scripted fake.  It is the model's resolved provider
    (lowercased, hyphens stripped), NOT its ``_llm_type`` — so we derive it
    from the model instance rather than hardcode-guessing it.
    """
    params = DeepAgentsFakeChatModel(messages=iter([]))._get_ls_params()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
    return params.get("ls_provider", "")


_FAKE_PROVIDER_KEY: str = _resolve_fake_provider_key()
register_harness_profile(_FAKE_PROVIDER_KEY, _KOKORO_HARNESS_PROFILE)
# Register for the real Anthropic provider too so the same tool exclusions
# apply when running with a live key.
register_harness_profile("anthropic", _KOKORO_HARNESS_PROFILE)


def make_agent() -> Any:
    """Build the DeepAgents engine selected by ``KOKORO_MODEL``.

    Returns a ``CompiledStateGraph`` (langgraph graph with ``astream_events``).
    Typed as ``Any`` because langgraph lacks published type stubs and the
    CompiledStateGraph generic parameters propagate Unknown through the chain.

    Only ``write_todos`` (planning) + user-registered tools (echo_search,
    clock) are visible to the model; FS/execute/task tools are excluded.
    ``KOKORO_MODEL=scripted`` uses an offline scripted fake (no network/key).
    """
    spec = os.environ.get("KOKORO_MODEL", DEFAULT_MODEL)
    if spec == "scripted":
        model: DeepAgentsFakeChatModel = scripted_planning_model()
    else:
        model = init_chat_model(spec)  # type: ignore[assignment]

    return create_deep_agent(  # pyright: ignore[reportUnknownVariableType,reportReturnType]
        model=model,  # type: ignore[arg-type]
        tools=TOOL_OBJECTS,
        system_prompt=KOKORO_AGENT_PROMPT,
    )
