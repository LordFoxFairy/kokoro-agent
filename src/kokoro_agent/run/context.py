"""RunContext：一次 run 的领域身份，namespace 派生 checkpoint 线程键与状态键。"""

from __future__ import annotations

from dataclasses import dataclass

from kokoro_agent.contract import RunRequest


@dataclass(frozen=True, slots=True)
class RunContext:
    namespace: str
    session_id: str
    run_id: str
    thread_id: str

    @classmethod
    def of(cls, request: RunRequest) -> RunContext:
        return cls(
            namespace=request.context.namespace,
            session_id=request.context.session_id,
            run_id=request.run_id,
            thread_id=request.thread_id,
        )

    @property
    def scoped_thread_id(self) -> str:
        # checkpoint 隔离键：不同 namespace 的同名 thread 绝不共享历史。
        return f"{self.namespace}:{self.thread_id}"

    @property
    def state_key(self) -> str:
        # RunStateStore（去重/租约/终态）主键：namespace 前缀隔离。
        return f"{self.namespace}:{self.run_id}"
