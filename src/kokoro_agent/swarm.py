# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportArgumentType=false, reportUnknownArgumentType=false
"""官方 ``langgraph-swarm`` 的最薄接线。

每个 peer 已由 ``AgentFactory`` 通过 DeepAgents ``create_deep_agent`` 构造；本模块只把
这些原生 Agent 交给官方 ``create_swarm``，不定义 member/role 类、不实现 router，也不拥有
状态或 checkpoint 语义。这里的 ``.compile(...)`` 是官方 Swarm API 的调用，不是 GA 自己的
compiler 模块或编译抽象。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypeGuard

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore
from langgraph_swarm import SwarmState, create_swarm as _create_swarm

from kokoro_agent.execution.protocols import AgentRunnable


def create_swarm(
    peers: Sequence[AgentRunnable],
    *,
    entry_agent: str,
    checkpointer: BaseCheckpointSaver[str] | None = None,
    store: BaseStore | None = None,
) -> AgentRunnable:
    """把已由 DeepAgents 构造的 Agent 交给官方 Swarm，返回官方 runnable。

    Agent 的工具、middleware、sandbox 和 native subagent 都在构造阶段完成；此函数只负责
    官方的 active-agent/handoff 外层，不重新包装 Agent，也不定义 GA state。
    """
    if len(peers) < 2:
        raise ValueError("swarm requires at least two peers")
    native: object = _create_swarm(
        list(peers),
        default_active_agent=entry_agent,
        state_schema=SwarmState,
    ).compile(checkpointer=checkpointer, store=store)
    if not _is_agent_runnable(native):
        raise TypeError("official Swarm returned an object without the DeepAgents invocation surface")
    return native


def _is_agent_runnable(value: object) -> TypeGuard[AgentRunnable]:
    return callable(getattr(value, "astream_events", None)) and callable(
        getattr(value, "aget_state", None)
    )


__all__ = ["create_swarm"]
