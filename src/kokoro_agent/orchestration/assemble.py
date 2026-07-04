"""编排共享装配件：AssembleDeps/AssembledAgent 形状 + 工具/守卫/子代理装配函数。

各 agent 类型的配方在同目录 <type>.py（现有 general.py；新增类型即新增配方文件），
政策全部在配方内注入（租户 scope/审批集/预算/后端），工具与执行层保持通用原语。
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from deepagents.middleware.subagents import GENERAL_PURPOSE_SUBAGENT, SubAgent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore

from kokoro_agent.contract import ModelConfig, RunRequest
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.sandbox import SandboxSettings
from kokoro_agent.storage.ledger import RunLedger
from kokoro_agent.subagents import SubagentCatalog
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME
from kokoro_agent.tools.web_fetch import make_web_fetch_tool
from kokoro_agent.tools.web_search import (
    SearchProviderSettings,
    make_search_provider,
    make_web_search_tool,
)

LOGGER = logging.getLogger(__name__)


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
    web_tools: tuple[BaseTool, ...]
    checkpointer: BaseCheckpointSaver[str]
    ledger: RunLedger
    memory_store: BaseStore


def build_web_tools(
    *, fetch_allow_private: bool, search: SearchProviderSettings | None
) -> list[BaseTool]:
    # fetch 恒挂载（SSRF 政策来自进程配置）；search 配置即挂载——无 provider 不挂空壳。
    tools: list[BaseTool] = [make_web_fetch_tool(allow_private=fetch_allow_private)]
    if search is None:
        return tools
    tools.append(make_web_search_tool(make_search_provider(search)))
    return tools


def approval_names(request: RunRequest) -> frozenset[str]:
    # ask_user 恒为语义暂停点，须与审批工具一同纳入 pending 识别集合；
    # 委派策略为 ask 时 task 同样是暂停点。
    names = frozenset(request.runtime.permissions.approval_tools) | {ASK_USER_TOOL_NAME}
    if request.runtime.permissions.subagent_create == "ask":
        names |= {SUBAGENT_TOOL_NAME}
    return names


def general_purpose_subagent(guards: Sequence[AgentMiddleware] = ()) -> SubAgent:
    """deepagents 自动注入的 general-purpose 不带本仓守卫（allow 档可达即旁路预算/终态/审核）
    ——传同名 spec 显式覆盖：tools/model 缺省即继承主 agent（GP 语义不变），middleware 挂满守卫。"""
    sub: SubAgent = {
        "name": GENERAL_PURPOSE_SUBAGENT["name"],
        "description": GENERAL_PURPOSE_SUBAGENT["description"],
        "system_prompt": GENERAL_PURPOSE_SUBAGENT["system_prompt"],
    }
    if guards:
        sub["middleware"] = list(guards)
    return sub


def wire_subagents(
    request: RunRequest,
    tool_index: Mapping[str, BaseTool],
    make_model: Callable[[ModelConfig], BaseChatModel],
    guards: Sequence[AgentMiddleware] = (),
) -> list[SubAgent]:
    """wire 子代理 → deepagents 定义：tools 按名解析为已挂载实例（未知名 fail-loud，
    绝不静默丢弃），model 经工厂实例化；二者缺省即继承主 agent。"""
    out: list[SubAgent] = []
    for spec in request.runtime.subagents:
        sub: SubAgent = {
            "name": spec.name,
            "description": spec.description,
            "system_prompt": spec.system_prompt,
        }
        if spec.tools:
            missing = sorted(set(spec.tools) - set(tool_index))
            if missing:
                raise ValueError(f"subagent {spec.name!r} declares unmounted tools: {missing}")
            sub["tools"] = [tool_index[name] for name in spec.tools]
        if spec.model is not None:
            sub["model"] = make_model(spec.model)
        if guards:
            # 子代理 middleware 链独立于主 agent：预算/终态闸必须逐个下发，否则 task 委派即旁路。
            sub["middleware"] = list(guards)
        out.append(sub)
    return out


def catalog_subagents(
    catalog: SubagentCatalog,
    tool_index: Mapping[str, BaseTool],
    guards: Sequence[AgentMiddleware] = (),
) -> tuple[list[SubAgent], frozenset[str]]:
    """内建/配置子代理 → deepagents 定义：声明工具缺任一即整个不挂（不设空壳），
    返回 (定义, 实际可委派名集)——deny 声明集只含真挂载者。"""
    subs: list[SubAgent] = []
    mounted: set[str] = set()
    for spec in catalog.specs():
        missing = sorted(set(spec.tools) - set(tool_index))
        if missing:
            LOGGER.info(
                "built-in subagent %r not mounted (tools unavailable: %s)", spec.name, missing
            )
            continue
        sub: SubAgent = {
            "name": spec.name,
            "description": spec.description,
            "system_prompt": spec.system_prompt,
        }
        if spec.tools:
            sub["tools"] = [tool_index[name] for name in spec.tools]
        if guards:
            sub["middleware"] = list(guards)
        subs.append(sub)
        mounted.add(spec.name)
    return subs, frozenset(mounted)
