"""Verify exact native workspace-tool provenance; State stores the proof in its checkpoint."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypeGuard

from deepagents.middleware.filesystem import EditFileSchema, WriteFileSchema
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.prebuilt import ToolRuntime
from langgraph.types import Command
from pydantic import BaseModel

from kokoro_agent.domain.run.workspace_output import OUTPUT_PROOF, output_path_digest

_ToolHandler = Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]]


def _is_mapping(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def native_output_path(
    request: ToolCallRequest,
    thread_id: str,
    schema: type[BaseModel] | None,
    *,
    field: str = "file_path",
) -> str | None:
    call = request.tool_call
    name, call_id, path = call["name"], call.get("id"), call["args"].get(field)
    runtime: object = getattr(request, "runtime", None)
    runtime_call_id: object = getattr(runtime, "tool_call_id", None)
    config: object = getattr(runtime, "config", None)
    configurable = config.get("configurable") if _is_mapping(config) else None
    if (
        schema is None
        or request.tool is None
        or request.tool.name != name
        or request.tool.args_schema is not schema
        or not isinstance(call_id, str)
        or not call_id
        or not isinstance(runtime, ToolRuntime)
        or runtime_call_id != call_id
        or not _is_mapping(configurable)
        or configurable.get("thread_id") != thread_id
        or not isinstance(path, str)
        or output_path_digest(path) is None
    ):
        return None
    return path


class WorkspaceOutputProofMiddleware(AgentMiddleware):
    """Mark only the actual pinned native tools on this assembled State peer."""

    def __init__(self, thread_id: str) -> None:
        super().__init__()
        self._thread_id = thread_id

    async def awrap_tool_call(
        self, request: ToolCallRequest, handler: _ToolHandler
    ) -> ToolMessage | Command[Any]:
        call = request.tool_call
        name, call_id = call["name"], call.get("id")
        path = native_output_path(
            request,
            self._thread_id,
            {"write_file": WriteFileSchema, "edit_file": EditFileSchema}.get(name),
        )
        result = await handler(request)
        if (
            path is None
            or not isinstance(result, ToolMessage)
            or (result.name, result.tool_call_id, result.status)
            != (name, call_id, "success")
        ):
            return result
        metadata: object = getattr(result, "response_metadata")
        if not _is_mapping(metadata):
            return result
        return result.model_copy(
            update={
                "response_metadata": {
                    **metadata,
                    OUTPUT_PROOF: output_path_digest(path),
                }
            }
        )
