from langchain_core.messages import AIMessage

from kokoro_agent.application.projection.reasoning_shim import message_text_and_reasoning


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


def test_non_standard_thinking_block_merges_into_reasoning() -> None:
    """non_standard.value.type==thinking（thinking 键）并入 reasoning。"""
    msg = AIMessage(content=[
        {"type": "text", "text": "answer"},
        {"type": "non_standard", "value": {"type": "thinking", "thinking": "deep"}},
    ])
    text, reasoning = message_text_and_reasoning(msg)
    assert text == "answer"
    assert "deep" in reasoning


def test_non_standard_thinking_block_text_key_merges_into_reasoning() -> None:
    """non_standard.value.type==thinking（text 键变体）并入 reasoning。"""
    msg = AIMessage(content=[
        {"type": "non_standard", "value": {"type": "thinking", "text": "via-text"}},
    ])
    _, reasoning = message_text_and_reasoning(msg)
    assert "via-text" in reasoning


def test_reasoning_block_empty_string() -> None:
    """空串 reasoning block 不追加，reasoning 位仍为空串。"""
    msg = AIMessage(content=[
        {"type": "text", "text": "answer"},
        {"type": "reasoning", "reasoning": ""},
    ])
    text, reasoning = message_text_and_reasoning(msg)
    assert text == "answer"
    assert reasoning == ""
