"""官方 ``langgraph-swarm`` 的最薄接线。

每个 peer 已由 ``AgentFactory`` 通过 DeepAgents ``create_deep_agent`` 构造；本模块只把
这些原生 Agent 交给官方 ``create_swarm``，不定义 member/role 类、不实现 router，也不拥有
状态或 checkpoint 语义。这里的 ``.compile(...)`` 是官方 Swarm API 的调用，不是 GA 自己的
compiler 模块或编译抽象。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, TypeGuard

from deepagents import SubagentTransformer
from langgraph.prebuilt import ToolCallTransformer
from langgraph.stream import ProtocolEvent, StreamTransformer
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore
import langgraph_swarm
from langgraph_swarm import SwarmState

from kokoro_agent.execution.protocols import (
    AgentRunnable,
    SWARM_SCOPE_EXTENSION,
    SwarmPeerScope,
)


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
    factories = _peer_factories(peers)
    if entry_agent not in factories:
        raise ValueError("Swarm entry agent must be a declared peer")
    peer_names = frozenset(factories)

    def subagents(scope: tuple[str, ...]) -> StreamTransformer:
        if not scope:
            return SubagentTransformer(scope, subagent_names=frozenset())
        name, separator, task_id = scope[0].partition(":")
        if not separator or not task_id or name not in factories:
            raise ValueError("Swarm stream scope is not a declared peer")
        transformer = factories[name](scope)
        if not isinstance(transformer, StreamTransformer):
            raise TypeError("Native peer factory did not produce a stream transformer")
        return transformer

    def declaration(scope: tuple[str, ...]) -> StreamTransformer:
        return _PeerDeclaration(scope, peer_names)

    # langgraph-swarm exposes a Pregel-generic signature that is narrower than
    # the DeepAgents runnable accepted at runtime.  The official call is the
    # sole dynamic boundary; the result is narrowed by the guard below.
    native_constructor: Any = getattr(langgraph_swarm, "create_swarm")
    native_agents: Any = list(peers)
    builder: Any = native_constructor(
        native_agents,
        default_active_agent=entry_agent,
        state_schema=SwarmState,
    )
    native: object = builder.compile(
        checkpointer=checkpointer,
        store=store,
        transformers=[ToolCallTransformer, subagents, declaration],
    )
    if not _is_agent_runnable(native):
        raise TypeError(
            "official Swarm returned an object without the DeepAgents invocation surface"
        )
    return native


class _PeerDeclaration(StreamTransformer):
    """Passive public SDK extension; it neither filters nor changes events."""

    def __init__(self, scope: tuple[str, ...], names: frozenset[str]) -> None:
        super().__init__(scope)
        self.names = names

    def init(self) -> dict[str, Any]:
        return {SWARM_SCOPE_EXTENSION: SwarmPeerScope(self.scope, self.names)}

    def process(self, event: ProtocolEvent) -> bool:
        return True


def _peer_factories(
    peers: Sequence[AgentRunnable],
) -> dict[str, Callable[[tuple[str, ...]], object]]:
    factories: dict[str, Callable[[tuple[str, ...]], object]] = {}
    for peer in peers:
        name: object = getattr(peer, "name", None)
        recipe: object = getattr(peer, "stream_transformers", None)
        if not isinstance(name, str) or not name or ":" in name or name in factories:
            raise ValueError("Swarm peers require unique native names")
        if not _is_transformer_recipe(recipe):
            raise TypeError("Swarm peer does not expose native stream transformers")
        entries = recipe
        if len(entries) != 2 or entries[0] is not ToolCallTransformer:
            raise TypeError(
                "Swarm peer does not expose the approved native stream recipe"
            )
        factory = entries[1]
        if not callable(factory):
            raise TypeError("Native peer subagent transformer must be callable")
        factories[name] = factory
    return factories


def _is_transformer_recipe(value: object) -> TypeGuard[Sequence[object]]:
    return isinstance(value, (list, tuple))


def _is_agent_runnable(value: object) -> TypeGuard[AgentRunnable]:
    return callable(getattr(value, "astream_events", None)) and callable(
        getattr(value, "aget_state", None)
    )


__all__ = ["create_swarm"]
