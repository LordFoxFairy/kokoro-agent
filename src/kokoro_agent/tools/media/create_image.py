"""Thin create_image tool: the model supplies intent; runtime supplies authority."""

from __future__ import annotations

import hashlib
from typing import Literal

from langchain_core.tools import StructuredTool
from langgraph.prebuilt.tool_node import ToolRuntime
from pydantic import BaseModel, ConfigDict

from kokoro_agent.platform import (
    AgentImageCreateCommand,
    ImageCreateIntent,
    MediaCommandResult,
    MediaOperationPort,
    MediaOperationSafeView,
    MediaRuntimeSafeError,
)
from kokoro_agent.state import KokoroAgentState, RunScope

CREATE_IMAGE_TOOL_NAME = "create_image"


class CreateImageArgs(ImageCreateIntent):
    """Exact model-visible product intent; no Platform authority is accepted."""


class CreateImageToolResult(BaseModel):
    """Closed model-facing projection of the owner command receipt."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    outcome: Literal["accepted", "rejected", "outcome_unknown"]
    media_command_ref: str
    recovery_action: Literal["get_operation", "recover_command", "contact_support"] | None
    operation_ref: str | None = None
    operation: MediaOperationSafeView | None
    error: MediaRuntimeSafeError | None


def make_create_image_tool(
    port: MediaOperationPort,
    *,
    media_access_handle: str,
    media_projection_reservation_handle: str,
) -> StructuredTool:
    async def create_image(
        prompt: str,
        aspect_ratio: Literal[
            "square_1_1",
            "landscape_4_3",
            "landscape_16_9",
            "portrait_3_4",
            "portrait_9_16",
        ],
        candidate_count: int,
        output_format: Literal["png", "jpeg", "webp"],
        runtime: ToolRuntime[None, KokoroAgentState],
    ) -> str:
        intent = CreateImageArgs(
            prompt=prompt,
            aspect_ratio=aspect_ratio,
            candidate_count=candidate_count,
            output_format=output_format,
        )
        scope = RunScope.from_state(runtime.state["scope"])
        tool_call_id = runtime.tool_call_id
        if tool_call_id is None or not tool_call_id:
            raise ValueError("MEDIA_COMMAND_IDENTITY_REQUIRED")
        command = AgentImageCreateCommand(
            media_access_handle=media_access_handle,
            media_projection_reservation_handle=media_projection_reservation_handle,
            stable_output_slot_ref=_stable_reference(
                "media-output-slot",
                "kokoro.ga.media-output-slot.v1",
                scope.namespace,
                scope.run_id,
                tool_call_id,
            ),
            agent_media_command_ref=_stable_reference(
                "media-command",
                "kokoro.ga.media-command.v1",
                scope.namespace,
                scope.run_id,
                tool_call_id,
            ),
            image=intent,
        )
        result: MediaCommandResult = await port.create_image(command)
        return result.model_dump_json()

    return StructuredTool(
        name=CREATE_IMAGE_TOOL_NAME,
        description=(
            "Create one or more images from a visual prompt. Use it when the user asks "
            "to generate an image; choose only aspect ratio, candidate count, and format."
        ),
        args_schema=CreateImageArgs,
        coroutine=create_image,
    )


def _stable_reference(prefix: str, domain: str, *parts: str) -> str:
    digest = hashlib.sha256(domain.encode("utf-8"))
    for part in parts:
        digest.update(b"\0")
        digest.update(part.encode("utf-8"))
    return f"{prefix}:sha256:{digest.hexdigest()}"
