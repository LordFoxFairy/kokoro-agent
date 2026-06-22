from langchain_core.messages import AIMessage

from kokoro_agent.events.reasoning_shim import message_text_and_reasoning


def test_native_reasoning_block() -> None:
    """原生 content_blocks 含 reasoning 类型时正确分离 text 与 reasoning。"""
    msg = AIMessage(content=[
        {"type": "text", "text": "answer"},
        {"type": "reasoning", "reasoning": "step by step"},
    ])
    text, reasoning = message_text_and_reasoning(msg)
    assert text == "answer"
    assert reasoning == "step by step"


def test_bare_message_additional_kwargs_fallback() -> None:
    """裸消息（无 content_blocks reasoning）兜底读 additional_kwargs.reasoning_content。"""
    msg = AIMessage(content="bare", additional_kwargs={"reasoning_content": "r"})
    text, reasoning = message_text_and_reasoning(msg)
    assert text == "bare"
    assert reasoning == "r"


def test_no_reasoning_returns_empty_string() -> None:
    """无任何 reasoning 来源时 reasoning 位为空串。"""
    msg = AIMessage(content="plain")
    text, reasoning = message_text_and_reasoning(msg)
    assert text == "plain"
    assert reasoning == ""
