"""装配共享契约（leaf）：进程级依赖束、装配产物与类型政策接口。

绝不 import agents.base / agents.assembly，作为无环 DAG 的根叶。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import ClassVar, Protocol

from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore

from kokoro_agent.contract import AgentType
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.mcp.config import McpServerConfig
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
    # MCP server 部署注册表（KOKORO_MCP_CONFIG）：wire names 在此解析，凭据不上 wire。
    mcp_servers: Mapping[str, McpServerConfig] = field(default_factory=dict[str, McpServerConfig])


class AgentPolicy(Protocol):
    """类型政策接口：装配管线只读这几项声明，其余全共享。"""

    name: ClassVar[AgentType]
    # 类型恒挂的核心工具（对话型含 ask_user；无 chat 面的 studio 类型为空）。
    core_tools: ClassVar[tuple[StructuredTool, ...]]
    # respond 语义暂停点（进 interrupt 与 pending 识别集；studio 类型为空集）。
    pause_tools: ClassVar[frozenset[str]]
    # system prompt 三级解析的末级缺省。
    default_prompt: ClassVar[str]
