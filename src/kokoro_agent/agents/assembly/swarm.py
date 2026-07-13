"""会话内 swarm 移交（handbook 20 D6「功能层」）：模型自判调 handoff 换主导人格。

无契约变更：候选=本部署 personas 资产全集（PromptLibrary.names），移交=切 system prompt 轨
（active_agent 进 graph state/checkpoint），工具面/skills/MCP 快照不变（换人格不换授权）。
候选<=1 时不挂 handoff、不挂人格中间件——单人格链路与移交前字节等价（gate 透明）。
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain.tools import ToolRuntime
from langchain_core.messages import SystemMessage, ToolMessage
from langchain_core.tools import StructuredTool
from langgraph.types import Command
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.agents.assembly.prompt import render_skill_manifest
from kokoro_agent.contract import RunRequest, SkillGrant
from kokoro_agent.prompts import PromptLibrary
from kokoro_agent.state import KokoroAgentState

LOGGER = logging.getLogger(__name__)

HANDOFF_TOOL_NAME = "handoff"


def swarm_candidates(prompts: PromptLibrary) -> tuple[str, ...]:
    """本 run 的 handoff 候选人格全集（目录即配置）；<=1 即不成 swarm。"""
    return prompts.names()


class HandoffArgs(BaseModel):
    # extra="allow"（同 mcp_call）：图内 ToolNode 把注入的 ToolRuntime 并入 args 后再过本模型校验，
    # forbid 会把注入形参误当禁止的额外字段拒收（from __future__ annotations 令 StructuredTool
    # 无法在构造期识别注入形参，故落回 schema 放行）。模型面只声明 agent_name。
    model_config = ConfigDict(strict=True, extra="allow")

    agent_name: str = Field(description="要移交主导权给哪个人格（见工具说明里的候选清单）。")


def make_handoff_tool(candidates: Sequence[str]) -> StructuredTool:
    """handoff 工具（仅候选>1 时挂载）：把主导权移交给另一具名人格。

    未知名→纠错文本 fail-closed（不改 active_agent、不炸 run，模型据此自纠）；
    合法名→Command 覆盖 active_agent（LastValue 落 checkpoint）+ 一条 ToolMessage 回执。
    schema 恒定（候选集在描述里，args 只一个 name），换人格不换 schema（D9）。
    """
    roster = tuple(dict.fromkeys(candidates))
    catalog = set(roster)
    listing = "、".join(roster)

    async def handoff(
        agent_name: str,
        *,
        runtime: ToolRuntime[None, KokoroAgentState] | None = None,
        **_injected: object,
    ) -> Command[Any] | str:
        if agent_name not in catalog:
            return f"error: 未知人格 {agent_name!r}；可移交对象：{listing}。"
        # 观测：主导人格变迁记进日志（wire 不新增 kind，浏览器由模型自然语言告知）。
        LOGGER.info("swarm handoff active_agent -> %s", agent_name)
        # ToolRuntime 注入同 mcp_call（可选形参 + **_injected：图内恒被注入且 schema 干净，
        # 不把 runtime 当禁止的额外字段拒收）。tool_call_id 图内恒有值，收窄空值兜底。
        tool_call_id = runtime.tool_call_id if runtime is not None else ""
        return Command(
            update={
                "active_agent": agent_name,
                "messages": [
                    ToolMessage(
                        content=f"已移交主导权给 {agent_name}；后续以该人格继续。",
                        tool_call_id=tool_call_id or "",
                    )
                ],
            }
        )

    return StructuredTool(
        name=HANDOFF_TOOL_NAME,
        description=(
            "把当前对话的主导权移交给另一具名人格（换的是应答人格/system prompt，"
            f"工具与技能授权不变）。候选：{listing}。判断当前任务更适合某个人格时才调用。"
        ),
        args_schema=HandoffArgs,
        coroutine=handoff,
    )


def swap_persona_prompt(
    library: PromptLibrary,
    grants: Sequence[SkillGrant],
    *,
    initial_prompt: str,
    initial_name: str | None,
    active_name: str | None,
    current_prompt: str,
) -> str | None:
    """移交后 system prompt（纯函数）：把装配期人格前缀定点换成移交后人格前缀，其余原样。

    返回 None＝无需换轨（未移交 / 移交回自身 / 候选无资产 / 前缀不匹配的防御分支），
    调用方据此原样透传。装配期前缀=deepagents 追加底座前的那段（人格+技能清单）。
    """
    if not active_name or active_name == initial_name:
        return None
    base = library.get(active_name)
    if base is None or not current_prompt.startswith(initial_prompt):
        return None  # 防御：候选已在 handoff 校验，此处不该命中
    return render_skill_manifest(base, grants) + current_prompt[len(initial_prompt) :]


class SwarmPersonaMiddleware(AgentMiddleware):
    """按 graph state 的 active_agent 动态切 system prompt 轨（模型每一步都据当前人格作答）。

    只在候选>1 时挂载。未移交（active_agent 未设或＝装配期 preset）→原样透传（与非 swarm 字节等价）。
    移交后→把装配期人格前缀替换成移交后人格前缀，deepagents 追加的底座段与技能清单原样保留。
    """

    state_schema = KokoroAgentState

    def __init__(
        self,
        *,
        library: PromptLibrary,
        grants: Sequence[SkillGrant],
        initial_prompt: str,
        initial_name: str | None,
    ) -> None:
        super().__init__()
        self._library = library
        self._grants = grants
        self._initial_prompt = initial_prompt
        self._initial_name = initial_name

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        new_prompt = swap_persona_prompt(
            self._library,
            self._grants,
            initial_prompt=self._initial_prompt,
            initial_name=self._initial_name,
            active_name=request.state.get("active_agent"),
            current_prompt=request.system_prompt or "",
        )
        if new_prompt is None:
            return await handler(request)
        return await handler(request.override(system_message=SystemMessage(content=new_prompt)))


def build_swarm_middleware(
    request: RunRequest,
    library: PromptLibrary,
    *,
    initial_prompt: str,
) -> SwarmPersonaMiddleware:
    """装配期构建人格中间件（捕获装配期人格前缀=切轨定点）。仅调用方确认候选>1 时构建。"""
    return SwarmPersonaMiddleware(
        library=library,
        grants=request.runtime.skills,
        initial_prompt=initial_prompt,
        initial_name=request.runtime.agent,
    )
