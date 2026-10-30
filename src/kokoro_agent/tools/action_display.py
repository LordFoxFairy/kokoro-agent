"""Same-call public action intent; never an execution argument or authority.

The mounted tool and the model's original message stay untouched. A per-Run
publisher must bind observations to the exact started call and persist them in
the existing activity. This module is not a history store or an activation gate.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from copy import deepcopy
from dataclasses import replace
from typing import Any, TypeGuard

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import (
    ModelRequest,
    ModelResponse,
    ToolCallRequest,
)
from langchain_core.messages import AIMessage, BaseMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool
from langchain_core.utils import function_calling
from langgraph.types import Command
from pydantic import TypeAdapter, ValidationError
from kokoro_agent.domain.chat.display_text import public_action_title

from kokoro_agent.domain.chat.action_intent import (
    ActionObservation,
    arguments_digest,
)

ACTION_FIELD = "_kokoro_action"
_CHECKPOINT_KEY = "kokoro_action_source_v1"
_ACTION_GUIDANCE = (
    "For each tool call whose definition includes _kokoro_action, fill that field "
    "with a brief public action and its object in the user's language. This is the "
    "tool row's action title, not an extra prose message, internal reasoning, the "
    "tool's general description, or a claim that the action succeeded. Do not include "
    "secrets or raw arguments. Do not add this field to tools that do not define it."
)
_ACTION_SCHEMA = {
    "type": "string",
    "minLength": 1,
    "maxLength": 256,
    "description": (
        "Brief public action title in the user's language: what this specific call "
        "will do and to which object (for example, 'Check the project file structure'). "
        "Not the tool's general description, reasoning, credentials, raw arguments "
        "or a claim of success. At most 256 UTF-8 bytes. Display metadata only."
    ),
}


_OBJECT = TypeAdapter(dict[str, Any])


def _object(value: object) -> TypeGuard[dict[str, Any]]:
    try:
        _OBJECT.validate_python(value, strict=True)
    except ValidationError:
        return False
    return True


def _decorated(
    tool: BaseTool | dict[str, Any], selected: frozenset[str]
) -> dict[str, Any] | None:
    # The installed SDK includes an unparameterized Callable overload. Keep
    # that third-party typing boundary here; validate the actual schema below.
    convert: Callable[[BaseTool | dict[str, Any]], dict[str, Any]] = getattr(
        function_calling, "convert_to_openai_tool"
    )
    schema = convert(tool)
    function = schema.get("function")
    if not _object(function) or function.get("name") not in selected:
        return None
    parameters = function.get("parameters")
    if (
        function.get("strict") is True
        or not _object(parameters)
        or parameters.get("type") != "object"
        or not _object(parameters.get("properties"))
        or ACTION_FIELD in parameters["properties"]
    ):
        return None
    try:
        required = TypeAdapter(list[str]).validate_python(
            parameters.get("required", []), strict=True
        )
    except ValidationError:
        return None
    result = deepcopy(schema)
    result["function"]["parameters"]["properties"][ACTION_FIELD] = dict(_ACTION_SCHEMA)
    result["function"]["parameters"]["required"] = [*required, ACTION_FIELD]
    return result


def model_action_tools(
    tools: Sequence[BaseTool | dict[str, Any]], selected: frozenset[str]
) -> list[BaseTool | dict[str, Any]]:
    """Decorate a model-only copy, preserving order, names and business schemas."""
    return [_decorated(tool, selected) or tool for tool in tools]


class ActionDisplayMiddleware(AgentMiddleware):
    """Preserve native calls/checkpoints; strip only our selected UI extension."""

    def __init__(
        self,
        selected: frozenset[str],
        observe: Callable[[ActionObservation], None],
        *,
        run_id: str,
    ) -> None:
        super().__init__()
        if not run_id:
            raise ValueError("An action display source requires its Run ID")
        self._run_id = run_id
        self._selected = selected
        self._observe = observe

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        visible: list[BaseTool | dict[str, Any]] = []
        qualified: set[str] = set()
        for tool in request.tools:
            decorated = _decorated(tool, self._selected)
            visible.append(decorated if decorated is not None else tool)
            if decorated is not None:
                qualified.add(decorated["function"]["name"])
        system = request.system_message
        if qualified:
            if system is None:
                system = SystemMessage(content=_ACTION_GUIDANCE)
            else:
                content_adapter: TypeAdapter[str | list[str | dict[str, Any]]] = (
                    TypeAdapter(str | list[str | dict[str, Any]])
                )
                content = content_adapter.validate_python(
                    getattr(system, "content"), strict=True
                )
                guided = (
                    content + "\n\n" + _ACTION_GUIDANCE
                    if isinstance(content, str)
                    else [
                        *deepcopy(content),
                        {"type": "text", "text": _ACTION_GUIDANCE},
                    ]
                )
                system = system.model_copy(update={"content": guided})
        response = await handler(request.override(tools=visible, system_message=system))
        result: list[BaseMessage] = []
        for message in response.result:
            if not isinstance(message, AIMessage):
                result.append(message)
                continue
            calls: dict[str, Any] = {}
            ids = [call["id"] for call in message.tool_calls]
            for call in message.tool_calls:
                if (
                    not call["id"]
                    or ids.count(call["id"]) != 1
                    or call["name"] not in qualified
                    or ACTION_FIELD not in call["args"]
                ):
                    continue
                arguments = dict(call["args"])
                title = public_action_title(arguments.pop(ACTION_FIELD))
                calls[call["id"]] = {
                    "name": call["name"],
                    "digest": arguments_digest(arguments),
                    "title": title,
                }
            # Freeze before after_model/HITL. Native checkpoint serialization
            # retains response metadata, whereas model wire serialization does
            # not send it as content or tool arguments. Replace any provider key.
            metadata = _OBJECT.validate_python(
                getattr(message, "response_metadata"), strict=True
            )
            metadata[_CHECKPOINT_KEY] = {"run_id": self._run_id, "calls": calls}
            result.append(message.model_copy(update={"response_metadata": metadata}))
        return replace(response, result=result)

    def _source(self, request: ToolCallRequest) -> dict[str, Any] | None:
        state = request.state
        if not _object(state) or not isinstance(state.get("messages"), list):
            return None
        # Only the currently executing model response qualifies; never find an
        # older call with a recycled ID in the conversation history.
        for message in reversed(state["messages"]):
            if not isinstance(message, AIMessage):
                continue
            call_id = request.tool_call["id"]
            if (
                not call_id
                or sum(call["id"] == call_id for call in message.tool_calls) != 1
            ):
                return None
            metadata: object = getattr(message, "response_metadata")
            if not _object(metadata):
                return None
            stamp = metadata.get(_CHECKPOINT_KEY)
            if not _object(stamp) or stamp.get("run_id") != self._run_id:
                return None
            calls = stamp.get("calls")
            source = calls.get(call_id) if _object(calls) else None
            return source if _object(source) else None
        return None

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        call = request.tool_call
        if (
            request.tool is None
            or call["name"] != request.tool.name
            or _decorated(request.tool, self._selected) is None
            or ACTION_FIELD not in call["args"]
        ):
            return await handler(request)
        source = self._source(request)
        if source is None or source.get("name") != call["name"]:
            return await handler(request)
        arguments = dict(call["args"])
        supplied_title = arguments.pop(ACTION_FIELD)
        title = public_action_title(source.get("title"))
        digest = arguments_digest(arguments)
        if (
            title is not None
            and supplied_title == title
            and digest is not None
            and digest == source.get("digest")
            and call["id"]
        ):
            self._observe(ActionObservation(call["id"], call["name"], digest, title))
        # Do not mutate the AIMessage's call: native checkpoint/replay owns it.
        return await handler(request.override(tool_call={**call, "args": arguments}))
