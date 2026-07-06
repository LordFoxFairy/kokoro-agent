"""AgentFactory 模板（Template Method）：一个类型一个 py 一个工厂类，政策即类属性。

看一个文件懂一个类型：挂什么核心工具（core_tools）、哪些工具是 respond 语义暂停点
（pause_tools）、末级缺省 prompt（default_prompt）。装配管线共享（create=模板方法委派
assemble_agent），未来 swarm/handoff（langgraph-swarm）在注册表之上组合各工厂产物，
工厂形状不变。
"""

from __future__ import annotations

from abc import ABC
from typing import ClassVar

from langchain_core.tools import StructuredTool

from kokoro_agent.agents.assembly.pipeline import assemble_agent
from kokoro_agent.agents.deps import AgentPolicy, AssembleDeps, AssembledAgent
from kokoro_agent.contract import AgentType, RunRequest

__all__ = ["AgentFactory", "AgentPolicy", "AssembleDeps", "AssembledAgent"]


class AgentFactory(ABC):
    """类型工厂：name=契约分派键；工具面政策为类属性；create=模板方法（共享管线装配）。"""

    name: ClassVar[AgentType]
    # 类型恒挂的核心工具（对话型含 ask_user；无 chat 面的 studio 类型为空）。
    core_tools: ClassVar[tuple[StructuredTool, ...]]
    # respond 语义暂停点（进 interrupt 与 pending 识别集；studio 类型为空集）。
    pause_tools: ClassVar[frozenset[str]]
    # system prompt 三级解析的末级缺省。
    default_prompt: ClassVar[str]

    async def create(self, deps: AssembleDeps, request: RunRequest) -> AssembledAgent:
        return await assemble_agent(self, deps, request)
