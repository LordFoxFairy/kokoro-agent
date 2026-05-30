"""Scripted fake chat model for offline deep-agent testing.

Inspired by the bind_tools-override pattern used in DeepAgents' own test suite
(langchain-ai/deepagents tests/unit_tests/chat_model.py). The key insight is
that ``create_deep_agent`` calls ``model.bind_tools(tools)`` internally during
graph construction. A plain ``GenericFakeChatModel`` raises because it doesn't
override ``bind_tools``. This subclass returns ``self`` from ``bind_tools`` so
the same scripted-messages iterator drives the full graph.

Critical streaming fix: ``BaseChatModel.ainvoke`` goes through
``_agenerate_with_cache`` which calls ``generate_from_stream(iter(chunks))``
by reading from ``_astream``. ``GenericFakeChatModel._stream`` (used by
``_astream``) only handles plain-string ``content`` and crashes with
``ValueError: No generations found in stream`` when content is empty
(tool-call-only turns). This subclass overrides ``_astream`` to delegate
directly to ``_generate`` so tool_calls are returned faithfully.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any

from langchain_core.callbacks import (
    AsyncCallbackManagerForLLMRun,
    CallbackManagerForLLMRun,
)
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_core.runnables import Runnable
from typing_extensions import override


class DeepAgentsFakeChatModel(GenericFakeChatModel):
    """GenericFakeChatModel with overrides required by DeepAgents graph assembly:

    1. ``bind_tools`` returns ``self`` so DeepAgents' ``model.bind_tools(...)``
       call during graph construction succeeds without ``NotImplementedError``.

    2. ``_stream`` / ``_astream`` delegate to ``_generate`` so that tool-call-only
       turns (``content=""``) are returned faithfully as a single chunk.  The
       parent's ``_stream`` splits on whitespace and raises
       ``ValueError: No generations found in stream`` when content is empty.
    """

    @override
    def bind_tools(  # type: ignore[override]
        self, tools: Any, **kwargs: Any
    ) -> Runnable[Any, Any]:
        """Return self; the scripted iterator already encodes tool_calls."""
        return self  # type: ignore[return-value]

    @override
    def _stream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> Iterator[ChatGenerationChunk]:
        """Yield the full scripted message as a single chunk."""
        result: ChatResult = self._generate(messages, stop=stop, **kwargs)
        message = result.generations[0].message
        if not isinstance(message, AIMessage):
            yield ChatGenerationChunk(message=AIMessageChunk(content=str(message.content)))
            return
        chunk = AIMessageChunk(
            content=message.content,
            tool_calls=message.tool_calls,  # type: ignore[arg-type]
            id=message.id,
        )
        chunk.chunk_position = "last"
        yield ChatGenerationChunk(message=chunk)

    @override
    async def _astream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        """Async version of _stream — yield the full scripted message as one chunk."""
        for chunk in self._stream(messages, stop=stop, **kwargs):
            yield chunk

    @property
    @override
    def _llm_type(self) -> str:
        return "deepagents-fake-chat-model"


def make_scripted_model(messages: list[AIMessage]) -> DeepAgentsFakeChatModel:
    """Build a scripted fake that replays ``messages`` in order."""
    return DeepAgentsFakeChatModel(messages=iter(messages))


def scripted_planning_model() -> DeepAgentsFakeChatModel:
    """Offline scripted model: write_todos -> echo_search -> write_todos(done) -> text.

    Exercises:
    - ``write_todos`` emitted as a generic tool (session recognizes it)
    - ``echo_search`` emitted as a generic user tool
    - A second ``write_todos`` updating status to completed
    - Final text answer
    """
    return make_scripted_model(
        [
            # Turn 1: write_todos (creates plan)
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_todos",
                        "args": {
                            "todos": [
                                {"content": "search for info", "status": "in_progress"},
                                {"content": "summarize results", "status": "pending"},
                            ]
                        },
                        "id": "call_wt_1",
                        "type": "tool_call",
                    }
                ],
            ),
            # Turn 2: echo_search (use a regular tool)
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "echo_search",
                        "args": {"query": "kokoro"},
                        "id": "call_es_1",
                        "type": "tool_call",
                    }
                ],
            ),
            # Turn 3: write_todos (update status to completed)
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_todos",
                        "args": {
                            "todos": [
                                {"content": "search for info", "status": "completed"},
                                {"content": "summarize results", "status": "completed"},
                            ]
                        },
                        "id": "call_wt_2",
                        "type": "tool_call",
                    }
                ],
            ),
            # Turn 4: final text answer
            AIMessage(content="Here is what I found for kokoro."),
        ]
    )
