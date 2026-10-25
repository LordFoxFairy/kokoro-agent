"""Authorize native summary subcalls without replacing ChatOpenAI's SDK behavior.

DeepAgents 0.6.6 invokes summaries outside user model middleware and LangChain's
summary helper converts Exceptions to text. Retain a failed subcall under the
actual execution locator and rethrow before the following ordinary SDK call, so
that error text can never become a committed native summarization event.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, Self

from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.language_models import LanguageModelInput
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from langgraph.runtime import get_runtime
from pydantic import JsonValue, PrivateAttr, TypeAdapter

from kokoro_agent.model.call_models import ModelCallAuthorizationUnavailable
from kokoro_agent.model.call_runtime import ModelCallRuntimeMiddleware
from kokoro_agent.model.request_profile import REQUEST_PROFILE
from kokoro_agent.model.summary_result import summary_prompt_digest
from kokoro_agent.tools.middleware import TokenBudgetExceeded


class SummaryAuthorizedChatOpenAI(ChatOpenAI):
    """The original routed SDK model; only native summary ``ainvoke`` is adapted."""

    _summary_budget: int = PrivateAttr(default=0)
    _summary_gate: ModelCallRuntimeMiddleware | None = PrivateAttr(default=None)
    _summary_guards: tuple[AgentMiddleware, ...] = PrivateAttr(default=())
    _summary_failures: dict[tuple[str, str, str], Exception] = PrivateAttr(
        default_factory=lambda: dict[tuple[str, str, str], Exception]()
    )

    def with_summary_gate(
        self,
        gate: ModelCallRuntimeMiddleware,
        guards: tuple[AgentMiddleware, ...],
        token_budget: int = 0,
    ) -> Self:
        """Give each existing peer its own gate without copying transports/tools."""
        model = self.model_copy()
        model._summary_budget = token_budget
        model._summary_gate = gate
        model._summary_guards = guards
        model._summary_failures = {}
        return model

    async def ainvoke(
        self,
        input: LanguageModelInput,
        config: RunnableConfig | None = None,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> AIMessage:
        is_summary = bool(
            config and config.get("metadata", {}).get("lc_source") == "summarization"
        )
        # Unbound direct SDK mechanism tests retain their existing behavior. The
        # production factory binds every reachable main/declared/native peer.
        if self._summary_gate is None and not is_summary:
            return await super().ainvoke(input, config, stop=stop, **kwargs)
        runtime = get_runtime()
        info = runtime.execution_info
        if info is None:
            raise ModelCallAuthorizationUnavailable(
                "native summary execution locator is unavailable"
            )
        key = (info.checkpoint_id, info.checkpoint_ns, info.task_id)
        if not is_summary:
            failure = self._summary_failures.pop(key, None)
            if failure is not None:
                raise failure
            return await super().ainvoke(input, config, stop=stop, **kwargs)
        try:
            if self._summary_gate is None:
                raise ModelCallAuthorizationUnavailable(
                    "native summary authorization binding is unavailable"
                )
            if not isinstance(input, str):
                raise ModelCallAuthorizationUnavailable(
                    "native summary input must be text"
                )
            parameters = self.model_dump(
                mode="json",
                include={
                    "model_name",
                    "max_tokens",
                    "n",
                    "temperature",
                    "top_p",
                    "frequency_penalty",
                    "presence_penalty",
                    "seed",
                    "reasoning_effort",
                    "model_kwargs",
                    "extra_body",
                    "stop",
                    "disable_streaming",
                    "stream_usage",
                    "streaming",
                    "use_responses_api",
                    "profile",
                },
            )
            parameters["invoke_kwargs"] = kwargs
            digest = summary_prompt_digest(
                input,
                stop,
                TypeAdapter(dict[str, JsonValue]).validate_python(
                    parameters, strict=True
                ),
                REQUEST_PROFILE,
            )
            request = ModelRequest(
                model=self,
                messages=[HumanMessage(content=input)],
                runtime=runtime,
                model_settings={"kokoro_summary_prompt_digest": digest},
            )

            async def invoke(_request: ModelRequest) -> ModelResponse:
                answer = await super(SummaryAuthorizedChatOpenAI, self).ainvoke(
                    input, config, stop=stop, **kwargs
                )
                return ModelResponse(result=[answer])

            async def execute(current: ModelRequest) -> ModelResponse:
                assert self._summary_gate is not None
                stored = await self._summary_gate.areplay_summary(current)
                if stored is not None:
                    self._check_summary_budget(stored.token_total)
                    return ModelResponse(
                        result=[AIMessage(content=stored.result.message.content)]
                    )
                response = await self._summary_gate.awrap_model_call(current, invoke)
                stored = await self._summary_gate.areplay_summary(current)
                if stored is None:
                    raise ModelCallAuthorizationUnavailable(
                        "native summary result was not sealed"
                    )
                self._check_summary_budget(stored.token_total)
                return response

            handler: Callable[[ModelRequest], Awaitable[ModelResponse]] = execute
            for guard in reversed(self._summary_guards):
                handler = partial(_run_guard, guard=guard, handler=handler)
            response = await handler(request)
            answer = response.result[0]
            if not isinstance(answer, AIMessage):
                raise ModelCallAuthorizationUnavailable(
                    "native summary response is unavailable"
                )
            return answer
        except Exception as error:
            self._summary_failures[key] = error
            raise

    def _check_summary_budget(self, token_total: int) -> None:
        if self._summary_budget > 0 and token_total > self._summary_budget:
            raise TokenBudgetExceeded(
                f"run token budget exceeded: spent {token_total} > budget {self._summary_budget}"
            )


async def _run_guard(
    request: ModelRequest,
    *,
    guard: AgentMiddleware,
    handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
) -> ModelResponse:
    response = await guard.awrap_model_call(request, handler)
    if not isinstance(response, ModelResponse):
        raise ModelCallAuthorizationUnavailable(
            "native summary guard returned an invalid response"
        )
    return response
