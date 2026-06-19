"""离线确定性假聊天模型：无需凭证即可驱动真实 DeepAgents 循环。"""

from __future__ import annotations

from collections.abc import Sequence

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable
from pydantic import PrivateAttr

# 离线确定性脚本（先 write_todos，再给最终答案），驱动真实 DeepAgents 循环；
# GenericFakeChatModel 没有 bind_tools，做不到这点。
_PLAN: list[dict[str, str]] = [
    {"content": "理解请求并规划", "status": "completed"},
    {"content": "用本地预览作答", "status": "in_progress"},
]
_FINAL_TEXT = (
    "本地预览：DeepAgents 活动流已接通（思考 / 工具 / todo / 子智能体）。"
    "配置真实模型（KOKORO_MODEL + 凭证）后，这里会是真实的多步回答。"
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
        # 调一次内置 now 工具：让离线 e2e/门禁确定性地覆盖 tool.invoked/tool.returned 链路。
        AIMessage(
            content="",
            tool_calls=[
                {"name": "now", "args": {}, "id": "local_now", "type": "tool_call"}
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

    _cursor: int = PrivateAttr(default=0)
    _turns: list[AIMessage] = PrivateAttr(default_factory=_script)

    @property
    def _llm_type(self) -> str:
        return "kokoro-local-fake"

    def bind_tools(
        self,
        tools: Sequence[object],
        *,
        tool_choice: str | None = None,
        **kwargs: object,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        # 接受但忽略绑定（脚本固定）；deepagents 会调用 bind_tools，缺失则基类抛 NotImplementedError。
        return self.with_types(output_type=AIMessage)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: object,
    ) -> ChatResult:
        if not any(isinstance(message, AIMessage) for message in messages):
            self._cursor = 0  # 一轮的首个调用 → 重启脚本
        if self._cursor < len(self._turns):
            reply = self._turns[self._cursor]
            self._cursor += 1
        else:
            reply = AIMessage(content="")  # 多出的循环轮次直接终止
        return ChatResult(generations=[ChatGeneration(message=reply)])


def make_local_fake_chat_model() -> BaseChatModel:
    return LocalFakeChatModel()
