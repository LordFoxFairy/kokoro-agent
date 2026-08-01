"""RunScope（一次 run 的领域身份）与 KokoroAgentState（承载它的图状态扩展）。

身份乘 State 轴（DeepAgentState 扩展键 scope）：随初始 input 进图、落 checkpoint、
resume 不重供仍保持；工具/中间件经 ToolRuntime.state 读取。法则：图节点不得改写 scope。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, NotRequired

from deepagents.graph import DeepAgentState
from langchain.agents.middleware.types import PrivateStateAttr
from langgraph.channels import UntrackedValue

from kokoro_agent.contract import RunRequest

SCOPE_STATE_KEY = "scope"


class KokoroAgentState(DeepAgentState):
    # 纯 dict 载荷（checkpoint 序列化安全）；读写经 RunScope.as_state/from_state 收口。
    scope: dict[str, str]
    assembly_digest: str
    # DeepAgents native SkillsMiddleware skips loading when this key exists. These
    # private channels deliberately survive within one invocation but are never
    # checkpointed, so every reconstructed graph reloads the exact current assembly.
    skills_metadata: NotRequired[
        Annotated[list[dict[str, object]], PrivateStateAttr, UntrackedValue(list)]
    ]
    skills_load_errors: NotRequired[
        Annotated[list[str], PrivateStateAttr, UntrackedValue(list)]
    ]
    # swarm 当前主导人格名（handoff 工具落此，dynamic prompt 中间件读此切 system prompt 轨）：
    # 未设=沿用装配期 preset（runtime.agent）；无 reducer→LastValue 覆盖，落 checkpoint→resume
    # 重放后仍在移交后轨（模型驱动移交，session/wire 不参与切换）。
    # NotRequired：channel 未写即缺席（未移交的 run 与 SWARM 前旧 checkpoint 都没有此键）。
    active_persona: NotRequired[str]


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
