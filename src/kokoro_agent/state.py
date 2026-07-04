"""RunScope（一次 run 的领域身份）与 KokoroAgentState（承载它的图状态扩展）。

身份乘 State 轴（DeepAgentState 扩展键 scope）：随初始 input 进图、落 checkpoint、
resume 不重供仍保持；工具/中间件经 ToolRuntime.state 读取。法则：图节点不得改写 scope。
"""

from __future__ import annotations

from dataclasses import dataclass

from deepagents.graph import DeepAgentState

from kokoro_agent.contract import RunRequest

SCOPE_STATE_KEY = "scope"


class KokoroAgentState(DeepAgentState):
    # 纯 dict 载荷（checkpoint 序列化安全）；读写经 RunScope.as_state/from_state 收口。
    scope: dict[str, str]


@dataclass(frozen=True, slots=True)
class RunScope:
    namespace: str
    session_id: str
    run_id: str
    thread_id: str

    @classmethod
    def of(cls, request: RunRequest) -> RunScope:
        return cls(
            namespace=request.context.namespace,
            session_id=request.context.session_id,
            run_id=request.run_id,
            thread_id=request.thread_id,
        )

    @classmethod
    def from_state(cls, scope: dict[str, str]) -> RunScope:
        return cls(
            namespace=scope["namespace"],
            session_id=scope["session_id"],
            run_id=scope["run_id"],
            thread_id=scope["thread_id"],
        )

    def as_state(self) -> dict[str, str]:
        return {
            "namespace": self.namespace,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "thread_id": self.thread_id,
        }

    @property
    def scoped_thread_id(self) -> str:
        # checkpoint 隔离键：不同 namespace 的同名 thread 绝不共享历史。
        return f"{self.namespace}:{self.thread_id}"

    @property
    def state_key(self) -> str:
        # RunLedger（去重/租约/终态）主键：namespace 前缀隔离。
        return f"{self.namespace}:{self.run_id}"
