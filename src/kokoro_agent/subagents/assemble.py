"""子代理装配件：catalog/wire 声明 → deepagents SubAgent 定义（守卫逐个下发）。"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence

from deepagents.middleware.subagents import GENERAL_PURPOSE_SUBAGENT, SubAgent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool

from kokoro_agent.contract import ModelConfig, RunRequest
from kokoro_agent.prompts import PersonaLibrary
from kokoro_agent.subagents.catalog import SubagentCatalog
from kokoro_agent.tools.registry import KOKORO_TOOLS

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


def wire_subagents(
    request: RunRequest,
    tool_index: Mapping[str, BaseTool],
    make_model: Callable[[ModelConfig], BaseChatModel],
    guards: Sequence[AgentMiddleware] = (),
    personas: PersonaLibrary | None = None,
) -> list[SubAgent]:
    """wire 子代理 → deepagents 定义：tools 主 index 优先（复用政策实例）、注册表兜底
    （入口对偶性：成品降格为子代理时，其专属注册表工具可不在主 agent 工具集里），
    仍未知即 fail-loud——绝不静默丢弃；model 经工厂实例化；二者缺省即继承主 agent。"""
    out: list[SubAgent] = []
    for spec in request.runtime.subagents:
        # 人格：内联覆盖 → 按名资产（prompts/<name>.md）；两者皆无即 fail-loud（不设无人格下属）。
        persona = spec.system_prompt or (personas.get(spec.name) if personas else None)
        if persona is None:
            raise ValueError(f"subagent {spec.name!r} has no persona (inline or prompts/{spec.name}.md)")
        sub: SubAgent = {
            "name": spec.name,
            "description": spec.description,
            "system_prompt": persona,
        }
        if spec.tools:
            resolved: list[BaseTool] = []
            unknown: list[str] = []
            for name in spec.tools:
                tool = tool_index.get(name) or KOKORO_TOOLS.get(name)
                if tool is None:
                    unknown.append(name)
                else:
                    resolved.append(tool)
            if unknown:
                raise ValueError(
                    f"subagent {spec.name!r} declares unknown tools: {sorted(unknown)}"
                )
            sub["tools"] = resolved
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
