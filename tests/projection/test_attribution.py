from langchain_core.runnables.schema import StandardStreamEvent, StreamEvent

from kokoro_agent.application.projection.attribution import SubagentAttribution


def _base(metadata: dict[str, str]) -> StreamEvent:
    ev: StandardStreamEvent = {
        "event": "on_chat_model_stream",
        "run_id": "run-1",
        "parent_ids": [],
        "name": "model",
        "data": {},
        "metadata": metadata,
    }
    return ev


def _ev(agent_name: str) -> StreamEvent:
    return _base({"agent_name": agent_name})


def _ev_no_name() -> StreamEvent:
    return _base({})


def test_concurrent_subagents_do_not_cross() -> None:
    a = SubagentAttribution()
    a.started("id-alpha", "alpha")
    a.started("id-beta", "beta")
    assert a.active_id(_ev("alpha")) == "id-alpha"
    assert a.active_id(_ev("beta")) == "id-beta"
    assert a.active_id(_ev("main")) is None  # 主链路无 agent_name 命中
    a.finished("alpha")
    assert a.active_id(_ev("alpha")) is None
    assert a.active_id(_ev("beta")) == "id-beta"


def test_single_subagent_lifecycle() -> None:
    a = SubagentAttribution()
    assert a.active_id(_ev("worker")) is None
    a.started("id-worker", "worker")
    assert a.active_id(_ev("worker")) == "id-worker"
    a.finished("worker")
    assert a.active_id(_ev("worker")) is None


def test_event_without_agent_name_returns_none() -> None:
    a = SubagentAttribution()
    a.started("id-x", "x")
    assert a.active_id(_ev_no_name()) is None


def test_finished_idempotent() -> None:
    a = SubagentAttribution()
    a.started("id-z", "z")
    a.finished("z")
    # 二次 finished 不应抛异常
    a.finished("z")
    assert a.active_id(_ev("z")) is None
