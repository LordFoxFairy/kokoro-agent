"""LocalFakeChatModel 的同步路径：_generate / bind_tools（agent 走 async，此前 72%）。"""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage

from kokoro_agent.infrastructure.model import LocalFakeChatModel
from kokoro_agent.infrastructure.tools import BUILT_IN_TOOLS


def test_sync_invoke_follows_script() -> None:
    # 同步 invoke → _generate：本轮首调（messages 无 AIMessage）重启脚本游标，返回脚本首帧。
    model = LocalFakeChatModel()
    first = model.invoke([HumanMessage(content="hi")])
    assert isinstance(first, AIMessage)


def test_sync_generate_exhausts_script_to_empty_message() -> None:
    # 带 AIMessage 的后续轮次不重启游标，逐帧推进；超出脚本长度后返回空内容 AIMessage（终止循环）。
    model = LocalFakeChatModel()
    model.invoke([HumanMessage(content="hi")])
    last: AIMessage = AIMessage(content="seed")
    for _ in range(6):
        result = model.invoke([HumanMessage(content="hi"), AIMessage(content="prev")])
        assert isinstance(result, AIMessage)
        last = result
    assert last.content == ""


def test_bind_tools_accepts_and_returns_runnable() -> None:
    # deepagents 会调 bind_tools；fake 接受但忽略绑定（脚本固定），返回可继续 invoke 的 Runnable。
    model = LocalFakeChatModel()
    bound = model.bind_tools(list(BUILT_IN_TOOLS))
    assert bound is not None
