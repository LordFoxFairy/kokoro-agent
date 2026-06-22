"""运行时自定义子代理工具：模型按需临时创建并运行一个专用子代理。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, TypeGuard

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from kokoro_agent.infrastructure.agent_builder import AsyncRunner, make_subagent_runner
from kokoro_agent.infrastructure.stream_events import message_parts, result_messages
from kokoro_agent.infrastructure.constants import RUNTIME_SUBAGENT_TOOL_NAME
from kokoro_agent.infrastructure.subagent import RuntimeSubagentRegistry

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
# deepagents runner 结果是进程内不透明对象（非 JSON），值类型在此真实收口为 object。
_RunnerResult = Mapping[str, object]


# 两段收窄：先到 object-keyed Mapping 避免 pyright 判 Unknown，再断言键全为 str。
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
    # LLM 外部边界：拒收未知字段、禁止隐式类型转换。
    model_config = ConfigDict(strict=True, extra="forbid")

    name: _NonEmpty = Field(description="运行时自定义子代理的名称")
    description: _NonEmpty = Field(description="角色或职责的简短描述")
    system_prompt: _NonEmpty = Field(description="该运行时自定义子代理的系统提示词")
    task: _NonEmpty = Field(description="要交给该运行时自定义子代理执行的具体任务")


def _make_runner(model: BaseChatModel, system_prompt: str, name: str) -> AsyncRunner:
    return make_subagent_runner(model, system_prompt=system_prompt, name=name)


def _runtime_messages(task: str) -> dict[str, list[BaseMessage]]:
    return {"messages": [HumanMessage(content=task)]}


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
        spec = runtime_registry.register_or_get(name, description, system_prompt)
        runner = _make_runner(model, spec.system_prompt, spec.name)
        result_obj = await runner.ainvoke(_runtime_messages(task.strip()))
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
