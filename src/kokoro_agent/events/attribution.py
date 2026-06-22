"""子代理归属：将 StreamEvent 映射到发出它的子代理 id。"""

from typing import TypedDict


class EventMetadata(TypedDict, total=False):
    agent_name: str


class StreamEvent(TypedDict, total=False):
    metadata: EventMetadata


class SubagentAttribution:
    """按 agent_name（spec §9.3 注入的键）索引活跃子代理，支持并发多活跃。"""

    def __init__(self) -> None:
        # agent_name → subagent_id；dict 替代旧单槽，消除并发串档
        self._active: dict[str, str] = {}

    def started(self, subagent_id: str, agent_name: str) -> None:
        self._active[agent_name] = subagent_id

    def finished(self, agent_name: str) -> None:
        self._active.pop(agent_name, None)

    def active_id(self, event: StreamEvent) -> str | None:
        # agent_name 是 spec §9.3 在子代理启动时注入 metadata 的键
        metadata = event.get("metadata")
        if metadata is None:
            return None
        name = metadata.get("agent_name")
        if name is None:
            return None
        return self._active.get(name)
