"""HITL 通用暂停原语的公开入口：request_human + request_input + HumanRequest + 预设决策词汇。"""

from __future__ import annotations

from kokoro_agent.hitl.input import (
    HumanInput,
    InputRejected,
    InputSubmitted,
    request_input,
)
from kokoro_agent.hitl.presets import (
    APPROVAL_DECISIONS,
    INPUT_DECISIONS,
    QUESTION_DECISIONS,
    REVIEW_DECISIONS,
)
from kokoro_agent.hitl.request import HumanKind, HumanRequest, request_human

__all__ = [
    "APPROVAL_DECISIONS",
    "INPUT_DECISIONS",
    "QUESTION_DECISIONS",
    "REVIEW_DECISIONS",
    "HumanInput",
    "HumanKind",
    "HumanRequest",
    "InputRejected",
    "InputSubmitted",
    "request_human",
    "request_input",
]
