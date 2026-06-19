"""运行时自定义子代理工具：模型按需临时创建并运行一个专用子代理。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, TypeGuard

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field, StringConstraints

from kokoro_agent.infrastructure.agent_builder import AsyncRunner, make_subagent_runner
from kokoro_agent.infrastructure.stream_events import message_parts, result_messages
from kokoro_agent.infrastructure.tool_names import RUNTIME_SUBAGENT_TOOL_NAME
from kokoro_agent.infrastructure.subagent import RuntimeSubagentRegistry, load_custom_subagents_from_env

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
# deepagents runner 返回的是含 BaseMessage 的进程内对象（非 JSON），故在此以 object 边界收口。
_RunnerResult = Mapping[str, object]


# 两段式 TypeGuard：先把 object 收窄到键值均为 object 的 Mapping（避免 pyright 把内容判为 Unknown），
# 再校验键全为 str，方能安全交给 result_messages。
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
    name: _NonEmpty = Field(description="运行时自定义子代理的名称")
    description: _NonEmpty = Field(description="角色或职责的简短描述")
    system_prompt: _NonEmpty = Field(description="该运行时自定义子代理的系统提示词")
    task: _NonEmpty = Field(description="要交给该运行时自定义子代理执行的具体任务")


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

    # 纯异步工具：只给 coroutine、不给 func，sync 调用由 langchain 原生 NotImplementedError 拒绝。
    return StructuredTool(
        name=RUNTIME_SUBAGENT_TOOL_NAME,
        description=(
            "创建并运行一个运行时自定义子代理。当你需要一个临时的、专门的助手，"
            "且它不属于内建或配置定义的子代理集合时使用。"
        ),
        args_schema=RuntimeSubagentToolInput,
        coroutine=agent_runtime,
    )
