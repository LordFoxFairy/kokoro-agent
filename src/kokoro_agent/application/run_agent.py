from __future__ import annotations

from collections.abc import AsyncIterator

from langchain_core.language_models import BaseChatModel
from langchain_core.runnables.config import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver

from kokoro_agent.application.agent_event_driver import ASTREAM_TIMEOUT_S, drive_agent_events
from kokoro_agent.application.agent_factory import build_agent
from kokoro_agent.domain.agent_event import AgentEvent
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.lc_adapter import AgentInvokeInput
from kokoro_agent.infrastructure.observability import build_langfuse_handler
from kokoro_agent.infrastructure.permission import blocked_tools
from kokoro_agent.infrastructure.transport import StreamPort
from kokoro_agent.infrastructure.subagent_registry import RuntimeSubagentRegistry


def trace_config(req: RunRequest) -> RunnableConfig | None:
    handler = build_langfuse_handler()
    if handler is None:
        return None
    return {
        "callbacks": [handler],
        "metadata": {
            "langfuse_session_id": req.session_id,
            "langfuse_tags": [req.execution_style],
            "kokoro_run_id": req.run_id,
            "kokoro_conversation_id": req.conversation_id,
        },
    }


def agent_config(req: RunRequest) -> RunnableConfig:
    return {"configurable": {"thread_id": req.conversation_id}}


def _user_message(input_text: str) -> AgentInvokeInput:
    return {"messages": [{"role": "user", "content": input_text}]}


async def run_agent(
    req: RunRequest,
    model: BaseChatModel,
    control_port: StreamPort | None = None,
    runtime_registry: RuntimeSubagentRegistry | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> AsyncIterator[AgentEvent]:
    registry = runtime_registry if runtime_registry is not None else RuntimeSubagentRegistry()
    agent = build_agent(
        model,
        req.permission_mode,
        req.run_id,
        control_port,
        registry,
        checkpointer=checkpointer,
    )
    awaiting_tools = (
        blocked_tools(req.permission_mode) if control_port is not None else frozenset[str]()
    )
    timeout_s = ASTREAM_TIMEOUT_S
    config = agent_config(req)
    tracing = trace_config(req)
    if tracing is not None:
        config.update(tracing)
    raw_events = agent.astream_events(
        _user_message(req.input),
        version="v2",
        config=config,
    )
    async for event in drive_agent_events(req.run_id, raw_events, awaiting_tools, timeout_s):
        yield event
