"""AIMessage 中 text 与 reasoning 的提取；唯一保留的 provider 补丁是裸消息兜底。"""

from collections.abc import Mapping
from typing import TypeGuard

from langchain_core.messages import BaseMessage
from langchain_core.messages.content import ReasoningContentBlock


def _is_object_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    # 把未类型化值收口到 Mapping[object, object]，避免裸 isinstance 收窄成 Unknown
    return isinstance(value, Mapping)


def message_text_and_reasoning(msg: BaseMessage) -> tuple[str, str]:
    """提取 (text, reasoning)。

    优先遍历 content_blocks 收集原生 reasoning 块；
    若 reasoning 仍为空则兜底读 additional_kwargs["reasoning_content"]（裸消息/replay 场景）。
    """
    text = str(msg.text)
    reasoning_parts: list[str] = []

    for block in msg.content_blocks:
        # TypedDict 判别联合：type=="reasoning" 收窄到 ReasoningContentBlock
        if block["type"] == "reasoning":
            r_block: ReasoningContentBlock = block
            value = r_block.get("reasoning", "")
            if value:
                reasoning_parts.append(value)

    reasoning = "".join(reasoning_parts)

    # 裸消息兜底：仅当 content_blocks 未给出 reasoning 时才读 additional_kwargs
    if not reasoning:
        # langchain 将 additional_kwargs 声明为无类型 dict；getattr 把未知值收口到 object 边界
        kwargs: object = getattr(msg, "additional_kwargs", None)
        if _is_object_mapping(kwargs):
            fallback = kwargs.get("reasoning_content")
            if isinstance(fallback, str) and fallback:
                reasoning = fallback

    return text, reasoning
