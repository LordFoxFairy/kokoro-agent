"""上下文构造器：模型可见面的唯一拼装点（静态=人格/指引/skills 组合，动态=steering 注入）。"""

from __future__ import annotations

from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import AgentState
from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime

from kokoro_agent.storage.ledger import RunLedger
from kokoro_agent.tools import ask_user_question, memory, web_fetch, web_search

# 指引文案随工具本体维护（tools/*.GUIDANCE）；此处只做拼装序——prompt 段落顺序是 context 关切。
_SECTIONS = (
    ask_user_question.GUIDANCE,
    memory.GUIDANCE,
    web_fetch.GUIDANCE,
    web_search.GUIDANCE,
)


def render_tool_guidance(mounted: frozenset[str]) -> str | None:
    """段落仅在其全部所需工具真挂载时出现；全缺返回 None（不占 prompt）。"""
    parts = [g.text for g in _SECTIONS if g.requires <= mounted]
    return "\n\n".join(parts) if parts else None


def compose_system_prompt(
    persona: str, mounted_tools: frozenset[str], skills_prompt: str | None
) -> str:
    """三段组合：人格（入口预设或成品缺省）+ 条件行为指引 + skills 全文。"""
    guidance = render_tool_guidance(mounted_tools)
    return "\n\n".join(part for part in (persona, guidance, skills_prompt) if part)


class SteeringMiddleware(AgentMiddleware):
    """运行中插话：模型轮前排空信箱，按到达序注入 HumanMessage——协作式转向，
    不打断进行中的工具；稳定 id=message_id 保 checkpoint 重放幂等。只挂主链。"""

    def __init__(self, *, store: RunLedger, run_id: str) -> None:
        super().__init__()
        self._store = store
        self._run_id = run_id

    async def abefore_model(
        self, state: AgentState[Any], runtime: Runtime[Any]
    ) -> dict[str, Any] | None:
        steers = await self._store.drain_steers(self._run_id)
        if not steers:
            return None
        for message_id, content in steers:
            if not content:
                raise ValueError(f"empty steer content: message_id={message_id!r}")
        return {
            "messages": [
                HumanMessage(content=content, id=message_id) for message_id, content in steers
            ]
        }
