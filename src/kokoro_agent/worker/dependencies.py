"""Factory 的 leaf 上下文：worker 运行依赖。

它不依赖具体 Feature，也不读取请求之外的 transport；只由 AgentFactory 内部使用。
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
from kokoro_agent.domain.run.models import LeasedRun
from kokoro_agent.model.execution import ModelCallExecution
from kokoro_agent.worker.platform import WorkerPlatformRuntime
from kokoro_agent.execution.runtime_profile_plan import RuntimeAssemblyPolicy
from kokoro_agent.execution.runtime_profile_sources import SourceManifest

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore

from kokoro_agent.clients.system import ModelResolver
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.model.call_models import ModelCallRepository
from kokoro_agent.clients.storage import DeliveryClient
from kokoro_agent.sandbox import SandboxSettings
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.agents.subagent_catalog import SubagentCatalog
from kokoro_agent.tools.toolbox import ProcessToolbox


@dataclass(frozen=True, slots=True)
class WorkerClients:
    """Optional owner clients selected once by the deployment entrypoint."""

    delivery: DeliveryClient | None = None
    model_resolver: ModelResolver | None = None


@dataclass(frozen=True, slots=True)
class WorkerDependencies:
    """worker 启动时构建一次、由 AgentFactory 持有的运行依赖。

    该对象只存在于 Factory/worker 内部；它不是 Run input，也不会由 caller 构造或
    通过 Feature/Agent API 传递。
    """

    model: ChatModelSettings
    sandbox: SandboxSettings
    runtime_policy: RuntimeAssemblyPolicy
    manifest: SourceManifest
    subagent_catalog: SubagentCatalog
    toolbox: ProcessToolbox
    checkpointer: BaseCheckpointSaver[str]
    run_repository: RunRepository
    model_call_repository: ModelCallRepository
    model_execution: Callable[[LeasedRun], ModelCallExecution]
    memory_store: BaseStore
    platform: WorkerPlatformRuntime | None = None
    # Storage Artifact public facade；缺省时不装配 deliver，Agent 基础循环不受影响。
    delivery: DeliveryClient | None = None
    model_resolver: ModelResolver | None = None

    def __post_init__(self) -> None:
        expected = RuntimeAssemblyPolicy.from_settings(
            run_token_budget=self.runtime_policy.run_token_budget,
            recursion_limit=self.runtime_policy.recursion_limit,
            model=self.model,
            sandbox=self.sandbox,
        )
        if expected != self.runtime_policy:
            raise ValueError(
                "runtime assembly policy differs from actual worker settings"
            )
