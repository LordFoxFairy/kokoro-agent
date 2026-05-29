from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from uuid import uuid4

import kokoro_agent.events as events


@dataclass(frozen=True)
class RunAgentInput:
    session_id: str
    conversation_id: str
    user_input: str


# 先产出最小可回放事件序列，后续再把真实 DeepAgents 输出映射进来。
def run_agent(input: RunAgentInput) -> Iterator[events.SessionEvent]:
    run_id = f"run_{uuid4().hex[:8]}"
    message_id = f"msg_{uuid4().hex[:8]}"
    content = f"Kokoro received: {input.user_input}"

    yield events.session_created(
        session_id=input.session_id,
        conversation_id=input.conversation_id,
        run_id=run_id,
        sequence=1,
        title="Kokoro Session",
        owner_id="kokoro-agent",
    )
    yield events.message_delta(
        session_id=input.session_id,
        conversation_id=input.conversation_id,
        run_id=run_id,
        sequence=2,
        message_id=message_id,
        delta=content,
        role="assistant",
    )
    yield events.message_completed(
        session_id=input.session_id,
        conversation_id=input.conversation_id,
        run_id=run_id,
        sequence=3,
        message_id=message_id,
        content=content,
        role="assistant",
    )
    yield events.run_completed(
        session_id=input.session_id,
        conversation_id=input.conversation_id,
        run_id=run_id,
        sequence=4,
        final_message_id=message_id,
    )
