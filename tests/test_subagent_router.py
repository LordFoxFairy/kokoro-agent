from __future__ import annotations

from langchain_core.runnables.schema import StreamEvent

from kokoro_agent.application.subagent_router import SubagentRouter
from kokoro_agent.domain.stream_intent import SubagentStarted


def _event(lc_agent_name: str) -> StreamEvent:
    return {
        "event": "on_chat_model_end",
        "name": "ChatOpenAI",
        "run_id": "",
        "data": {},
        "metadata": {"lc_agent_name": lc_agent_name},
        "tags": [],
        "parent_ids": [],
    }


def _started(name: str, subagent_id: str) -> SubagentStarted:
    return SubagentStarted(
        subagent_id=subagent_id,
        name=name,
        description="查资料",
        subagent_type=name,
        source="built-in",
    )


def test_no_active_subagent_routes_nothing() -> None:
    router = SubagentRouter()
    assert router.route(_event("researcher")) is None


def test_active_subagent_routes_matching_agent_name_to_its_id() -> None:
    router = SubagentRouter()
    router.started(_started("researcher", "sa1"))
    assert router.route(_event("researcher")) == "sa1"


def test_active_subagent_ignores_non_matching_agent_name() -> None:
    router = SubagentRouter()
    router.started(_started("researcher", "sa1"))
    assert router.route(_event("ChatOpenAI")) is None


def test_finished_clears_active_routing() -> None:
    router = SubagentRouter()
    router.started(_started("researcher", "sa1"))
    router.finished()
    assert router.route(_event("researcher")) is None
