from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable
from pydantic import PrivateAttr

# Deterministic offline script (write_todos call, then a final answer) that drives
# the real DeepAgents loop, which GenericFakeChatModel can't (no bind_tools).
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
    """Deterministic, credential-free chat model that supports tool-calling.

    Lets the real DeepAgents loop run offline: returns a fixed script (a
    ``write_todos`` tool call, then a final answer). ``bind_tools`` is accepted
    and ignored (the script is fixed). The cursor resets on the first model call
    of a run (no prior ``AIMessage`` in the input), so a long-lived worker reusing
    one instance still works across runs.
    """

    _cursor: int = PrivateAttr(default=0)
    _turns: list[AIMessage] = PrivateAttr(default_factory=_script)

    @property
    def _llm_type(self) -> str:
        return "kokoro-local-fake"

    def bind_tools(
        self,
        tools: Sequence[Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        # Binding is ignored (script is fixed), but the method must exist: deep
        # agents call bind_tools and the base class raises NotImplementedError.
        return self.with_types(output_type=AIMessage)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        if not any(isinstance(message, AIMessage) for message in messages):
            self._cursor = 0  # first call of a fresh run → restart the script
        if self._cursor < len(self._turns):
            reply = self._turns[self._cursor]
            self._cursor += 1
        else:
            reply = AIMessage(content="")  # terminate any extra loop turn
        return ChatResult(generations=[ChatGeneration(message=reply)])


def make_local_fake_chat_model() -> BaseChatModel:
    return LocalFakeChatModel()
