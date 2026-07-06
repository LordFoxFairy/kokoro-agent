"""AgentFactory 模板（Factory Method）：一个类型一个 py 一个工厂类，政策即类属性。

看一个文件懂一个类型：挂什么核心工具（core_tools）、哪些工具是 respond 语义暂停点
（pause_tools）、怎么装配（create）。未来 swarm/handoff（langgraph-swarm）在注册表
之上组合各工厂产物，工厂形状不变。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import ClassVar

from deepagents.backends.protocol import FileData
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore

from kokoro_agent.contract import AgentType, RunRequest
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.prompts import PromptLibrary
from kokoro_agent.skills import SkillLibrary
from kokoro_agent.sandbox import SandboxSettings
from kokoro_agent.storage.ledger import RunLedger
from kokoro_agent.subagents import SubagentCatalog
from kokoro_agent.tools.toolbox import ProcessToolbox


@dataclass(frozen=True, slots=True)
class AssembledAgent:
    """装配产物：可运行图 + wire 面元数据（审批卡的工具自述查询）。"""

    agent: InvokableAgent
    tool_descriptions: Mapping[str, str]
    # Skills V2 state 档：随首次 invoke 注入的初始文件（官方 FileData 口径）；真实 backend 恒空。
    initial_files: Mapping[str, FileData] = field(default_factory=dict[str, FileData])

    def describe_tool(self, name: str) -> str | None:
        return self.tool_descriptions.get(name)


@dataclass(frozen=True, slots=True)
class AssembleDeps:
    """进程级共享件：worker 启动时构建一次，逐请求复用。
    只收领域设置，不收整个 AppConfig（config 单点消费法则）。"""

    model: ChatModelSettings
    sandbox: SandboxSettings
    run_token_budget: int
    catalog: SubagentCatalog
    toolbox: ProcessToolbox
    checkpointer: BaseCheckpointSaver[str]
    ledger: RunLedger
    memory_store: BaseStore
    skills: SkillLibrary
    prompts: PromptLibrary


class AgentFactory(ABC):
    """类型工厂：name=契约分派键；工具面政策为类属性；create=每请求装配。"""

    name: ClassVar[AgentType]
    # 类型恒挂的核心工具（对话型含 ask_user；无 chat 面的 studio 类型为空）。
    core_tools: ClassVar[tuple[StructuredTool, ...]]
    # respond 语义暂停点（进 interrupt 与 pending 识别集；studio 类型为空集）。
    pause_tools: ClassVar[frozenset[str]]

    @abstractmethod
    async def create(self, deps: AssembleDeps, request: RunRequest) -> AssembledAgent: ...
