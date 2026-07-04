"""按挂载工具条件拼装的行为指引：不提未挂载的工具（避免模型自我混乱）。"""

from __future__ import annotations

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
