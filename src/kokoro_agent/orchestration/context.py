"""上下文构造器：模型可见面的唯一拼装点（静态=人格/指引/skills 组合，动态=steering 注入）。"""

from __future__ import annotations

from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import AgentState
from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime

from kokoro_agent.storage.ledger import RunLedger
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.memory import SAVE_MEMORY_TOOL_NAME, SEARCH_MEMORY_TOOL_NAME
from kokoro_agent.tools.web_fetch import WEB_FETCH_TOOL_NAME
from kokoro_agent.tools.web_search import WEB_SEARCH_TOOL_NAME

_ASK_USER = """## 何时向用户提问（ask_user_question）
- 只在缺少"只有用户知道"的关键信息（偏好、目标、指代不明）时提问；能自己查证的不要问。
- 一次问清并给出可选项，不连环追问。"""

_MEMORY = """## 记忆（save_memory / search_memory）
- 请求可能涉及用户偏好或长期背景时，先 search_memory 查一下再动手。
- 用户表达持久偏好、纠正你的做法、给出可复用事实时，及时 save_memory（key 用短横线小写）。
- 一次性/临时信息与任何密钥密码，绝不入记忆。"""

_WEB_FETCH = """## 读网页（web_fetch）
- 用户给了 URL 或结论依赖某个页面内容时，用 web_fetch 读正文再回答；引用时附 URL。"""

_WEB_SEARCH = """## 联网检索（web_search）
- 需要时效信息或外部事实时先 web_search，再对关键来源 web_fetch 读原文，交叉核对后作答并附来源。
- 检索不到就明说。"""

_SECTIONS: tuple[tuple[frozenset[str], str], ...] = (
    (frozenset({ASK_USER_TOOL_NAME}), _ASK_USER),
    (frozenset({SAVE_MEMORY_TOOL_NAME, SEARCH_MEMORY_TOOL_NAME}), _MEMORY),
    (frozenset({WEB_FETCH_TOOL_NAME}), _WEB_FETCH),
    (frozenset({WEB_SEARCH_TOOL_NAME, WEB_FETCH_TOOL_NAME}), _WEB_SEARCH),
)


def render_tool_guidance(mounted: frozenset[str]) -> str | None:
    """段落仅在其全部所需工具真挂载时出现；全缺返回 None（不占 prompt）。"""
    parts = [text for required, text in _SECTIONS if required <= mounted]
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
