from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, TypeGuard

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field, StringConstraints

from kokoro_agent.infrastructure.agent_adapter import AsyncRunner, make_subagent_runner
from kokoro_agent.infrastructure.stream_events import RUNTIME_SUBAGENT_TOOL_NAME, message_parts, result_messages
from kokoro_agent.infrastructure.subagent_registry import RuntimeSubagentRegistry, load_custom_subagents_from_env

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
_RunnerResult = Mapping[str, object]


def _is_object_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _is_runner_result(value: object) -> TypeGuard[_RunnerResult]:
    if not _is_object_mapping(value):
        return False
    return all(isinstance(key, str) for key in value)


def _runtime_result_messages(result: object) -> list[BaseMessage]:
    if not _is_runner_result(result):
        return []
    return result_messages(result)


class RuntimeSubagentToolInput(BaseModel):
    name: _NonEmpty = Field(description="Runtime custom subagent name")
    description: _NonEmpty = Field(description="Short role or responsibility summary")
    system_prompt: _NonEmpty = Field(description="System prompt for the runtime custom subagent")
    task: _NonEmpty = Field(description="The concrete task the runtime custom subagent should perform")


def _make_runner(model: BaseChatModel, system_prompt: str, name: str) -> AsyncRunner:
    return make_subagent_runner(model, system_prompt=system_prompt, name=name)


def _runtime_messages(task: str) -> dict[str, list[dict[str, str]]]:
    return {"messages": [{"role": "user", "content": task}]}


def build_runtime_custom_subagent_tool(
    model: BaseChatModel,
    runtime_registry: RuntimeSubagentRegistry,
) -> StructuredTool:
    async def agent_runtime(
        name: str,
        description: str,
        system_prompt: str,
        task: str,
    ) -> str:
        normalized_name = name.strip()
        normalized_description = description.strip()
        normalized_system_prompt = system_prompt.strip()
        normalized_task = task.strip()

        if normalized_name in {spec.name for spec in load_custom_subagents_from_env()}:
            msg = f"duplicate or reserved subagent name: {normalized_name}"
            raise ValueError(msg)

        spec = runtime_registry.get(normalized_name)
        if spec is not None and (
            spec.description != normalized_description
            or spec.system_prompt != normalized_system_prompt
        ):
            msg = f"conflicting runtime subagent definition: {normalized_name}"
            raise ValueError(msg)
        if spec is None:
            spec = runtime_registry.register(
                normalized_name,
                normalized_description,
                normalized_system_prompt,
            )

        runner = _make_runner(model, spec.system_prompt, spec.name)
        result_obj = await runner.ainvoke(_runtime_messages(normalized_task))
        for message in reversed(_runtime_result_messages(result_obj)):
            if isinstance(message, AIMessage):
                text = message_parts(message).text.rstrip()
                if text:
                    return text
        return ""

    def agent_runtime_sync(
        name: str,
        description: str,
        system_prompt: str,
        task: str,
    ) -> str:
        msg = "runtime custom subagent tool requires async execution"
        raise RuntimeError(msg)

    return StructuredTool(
        name=RUNTIME_SUBAGENT_TOOL_NAME,
        description=(
            "Create and run a runtime custom subagent. Use this when you need an ad-hoc"
            " specialized helper that is not part of the built-in or config-defined"
            " subagent set."
        ),
        args_schema=RuntimeSubagentToolInput,
        func=agent_runtime_sync,
        coroutine=agent_runtime,
    )
