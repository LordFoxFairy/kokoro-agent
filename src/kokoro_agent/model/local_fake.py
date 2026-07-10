"""离线确定性假聊天模型：无需凭证即可驱动真实 DeepAgents 循环。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool
from pydantic import PrivateAttr

# 离线确定性脚本（先 write_todos，再给最终答案），驱动真实 DeepAgents 循环；
# GenericFakeChatModel 没有 bind_tools，做不到这点。
_PLAN: list[dict[str, str]] = [
    {"content": "理解请求并规划", "status": "completed"},
    {"content": "用本地预览作答", "status": "in_progress"},
]
_FINAL_TEXT = (
    "本地预览：DeepAgents 活动流已接通（思考 / todo）。"
    "在 session 的 namespace profile 配置真实模型（model_policy + 凭证）后，这里会是真实的多步回答。"
)


def _script() -> list[AIMessage]:
    return [
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "write_todos",
                    "args": {"todos": _PLAN},
                    "id": "local_todos",
                    "type": "tool_call",
                }
            ],
        ),
        AIMessage(content=_FINAL_TEXT),
    ]


def hitl_script() -> list[AIMessage]:
    # 跨栈 e2e 脚本：一次 ask_user_question 暂停 → 一次审批工具（write_file）暂停 → 正常文本流。
    # 尾部 deliver 轮只被同线程的第二次 run 消费（单 run 在首个纯文本帧收束）——驱动 E2E-31 成果交付链。
    return [
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "ask_user_question",
                    "args": {"question": "偏好中文还是英文？", "choices": ["中文", "英文"]},
                    "id": "local_ask",
                    "type": "tool_call",
                }
            ],
        ),
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "write_file",
                    "args": {"file_path": "/plan.md", "content": "# 计划\n本地预览"},
                    "id": "local_write",
                    "type": "tool_call",
                }
            ],
        ),
        AIMessage(content=_FINAL_TEXT),
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "deliver",
                    "args": {"path": "/plan.md", "title": "执行计划"},
                    "id": "local_deliver",
                    "type": "tool_call",
                }
            ],
        ),
        AIMessage(content=_FINAL_TEXT),
        # 第三次 run（E2E-32）：mcp_call 中途被服务器 elicit → kind=input 暂停 → submit 回灌续跑。
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "mcp_call",
                    "args": {"server": "e2e-elicit", "tool": "verify", "arguments": {}},
                    "id": "local_mcp",
                    "type": "tool_call",
                }
            ],
        ),
        AIMessage(content=_FINAL_TEXT),
    ]


class LocalFakeChatModel(BaseChatModel):
    """支持工具调用的确定性、免凭证假模型。

    让真实 DeepAgents 循环离线可跑：返回固定脚本（先一个 ``write_todos`` 工具调用，
    再给最终答案）。``bind_tools`` 被接受但忽略（脚本固定）。游标在一轮的首个模型调用
    （输入里尚无 ``AIMessage``）时归零，因此长驻 worker 复用同一实例也能跨轮正常工作。
    """

    _turns: list[AIMessage] = PrivateAttr(default_factory=_script)

    @classmethod
    def with_script(cls, script: Sequence[AIMessage]) -> LocalFakeChatModel:
        model = cls()
        model._turns = list(script)
        return model

    @property
    def _llm_type(self) -> str:
        return "kokoro-local-fake"

    def bind_tools(
        self,
        tools: Sequence[dict[str, Any] | type | Callable[..., Any] | BaseTool],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        # 接受但忽略绑定（脚本固定）；deepagents 会调用 bind_tools，缺失则基类抛 NotImplementedError。
        return self.with_types(output_type=AIMessage)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        # 从历史中已有的 AIMessage 数推断下一步：状态派生，跨 build/resume（新实例）稳定，
        # 实例计数器做不到（resume 重建 model 会归零）。
        turn = sum(1 for message in messages if isinstance(message, AIMessage))
        reply = self._turns[turn] if turn < len(self._turns) else AIMessage(content="")
        return ChatResult(generations=[ChatGeneration(message=reply)])


def make_local_fake_chat_model(script: Sequence[AIMessage] | None = None) -> BaseChatModel:
    if script is not None:
        return LocalFakeChatModel.with_script(script)
    return LocalFakeChatModel()
