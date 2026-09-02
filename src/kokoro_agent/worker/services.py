"""Factory 的 leaf 上下文：worker 共享服务。

它不依赖具体 Feature，也不读取请求之外的 transport；只由 AgentFactory 内部使用。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore

from kokoro_agent.mcp.config import McpServerConfig
from kokoro_agent.clients.mcp import McpClient
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.clients.skills import NoSkillsClient, SkillClient, SkillReader
from kokoro_agent.clients.storage import DeliveryClient
from kokoro_agent.sandbox import SandboxSettings
from kokoro_agent.persistence.repository import RunRepository
from kokoro_agent.agents.subagent_catalog import SubagentCatalog
from kokoro_agent.tools.toolbox import ProcessToolbox


@dataclass(frozen=True, slots=True)
class WorkerClients:
    """Optional owner clients selected once by the deployment entrypoint."""

    skill_client: SkillClient = field(default_factory=NoSkillsClient)
    skill_reader: SkillReader = field(default_factory=NoSkillsClient)
    mcp: McpClient | None = None
    delivery: DeliveryClient | None = None


@dataclass(frozen=True, slots=True)
class WorkerServices:
    """worker 启动时构建一次、由 AgentFactory 持有的共享服务。

    该对象只存在于 Factory/worker 内部；它不是 Run input，也不会由 caller 构造或
    通过 Feature/Agent API 传递。
    """

    model: ChatModelSettings
    sandbox: SandboxSettings
    run_token_budget: int
    subagent_catalog: SubagentCatalog
    toolbox: ProcessToolbox
    checkpointer: BaseCheckpointSaver[str]
    run_repository: RunRepository
    memory_store: BaseStore
    # Skill public contract 的名称解析面与内容读取面分开；本地 fixture 可由同一对象实现二者。
    skill_client: SkillClient
    skill_reader: SkillReader
    # MCP server 部署注册表（KOKORO_MCP_CONFIG）：wire names 在此解析，凭据不上 wire。
    mcp_servers: Mapping[str, McpServerConfig] = field(
        default_factory=dict[str, McpServerConfig]
    )
    # MCP public client：per-run 快照与部署 yaml 合并；None=仅 yaml（无外部 client）。
    mcp_client: McpClient | None = None
    # Storage Artifact public facade；缺省时不装配 deliver，Agent 基础循环不受影响。
    delivery: DeliveryClient | None = None
