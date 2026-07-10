"""子代理装配件：catalog 声明 → deepagents SubAgent 定义（守卫逐个下发）。

wire 不再携带子代理定义（names 只作声明性白名单）；定义住 catalog/资产库。
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

from deepagents.middleware.subagents import GENERAL_PURPOSE_SUBAGENT, SubAgent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.tools import BaseTool

from kokoro_agent.subagents.catalog import SubagentCatalog

LOGGER = logging.getLogger(__name__)


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
