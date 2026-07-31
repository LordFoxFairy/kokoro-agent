"""Thin create_image product tool: model input stays separate from owner authority."""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import Sequence

import pytest
from langgraph.prebuilt.tool_node import ToolRuntime
from pydantic import ValidationError

from kokoro_agent.platform import (
    AgentImageCreateCommand,
    ArtifactVersionHandle,
    MediaCommandAccepted,
    MediaOperationPort,
    MediaOperationSafeView,
)
from kokoro_agent.state import KokoroAgentState
from kokoro_agent.tools.media import (
    CREATE_IMAGE_TOOL_NAME,
    CreateImageArgs,
    CreateImageToolResult,
    make_create_image_tool,
)


class RecordingMediaPort(MediaOperationPort):
    def __init__(self, results: Sequence[MediaCommandAccepted]) -> None:
        self._results = list(results)
        self.commands: list[AgentImageCreateCommand] = []

    async def create_image(self, command: AgentImageCreateCommand) -> MediaCommandAccepted:
        self.commands.append(command)
        return self._results.pop(0).model_copy(
            update={"media_command_ref": command.agent_media_command_ref}
        )


def _runtime(tool_call_id: str = "call-image-1") -> ToolRuntime[None, KokoroAgentState]:
    state: KokoroAgentState = {
        "scope": {
            "namespace": "opaque-ns-a",
            "session_id": "session-a",
            "run_id": "run-a",
            "thread_id": "thread-a",
        },
        "messages": [],
        "skills_materialized": {},
    }
    return ToolRuntime(
        state=state,
        context=None,
        config={},
        stream_writer=lambda _chunk: None,
        tool_call_id=tool_call_id,
        store=None,
    )


def _accepted() -> MediaCommandAccepted:
    operation = MediaOperationSafeView(
        media_operation_handle="operation-handle:" + "a" * 48,
        operation_ref="operation-1",
        owner_version=1,
        state="queued",
        safe_progress_bps=0,
        artifacts=(
            ArtifactVersionHandle(
                candidate_ref="candidate-1",
                candidate_owner_version=2,
                artifact_version_handle="artifact-handle:" + "b" * 48,
            ),
        ),
    )
    return MediaCommandAccepted(
        outcome="accepted",
        media_command_ref="media-command:placeholder",
        recovery_action="get_operation",
        operation_ref="operation-1",
        operation=operation,
    )


def test_create_image_schema_exposes_only_product_intent() -> None:
    schema = CreateImageArgs.model_json_schema()
    assert set(schema["properties"]) == {
        "prompt",
        "aspect_ratio",
        "candidate_count",
        "output_format",
    }
    for forbidden in (
        "media_access_handle",
        "media_projection_reservation_handle",
        "stable_output_slot_ref",
        "agent_media_command_ref",
        "definition_revision_ref",
        "model_option_revision_ref",
        "site_id",
        "account_ref",
        "provider",
        "storage_url",
    ):
        assert forbidden not in schema["properties"]


@pytest.mark.parametrize(
    "pollution",
    [
        {"media_access_handle": "forged"},
        {"definition_revision_ref": "forged"},
        {"model_option_revision_ref": "forged"},
        {"provider": "forged"},
        {"site_id": "forged"},
    ],
)
def test_create_image_args_reject_authority_and_provider_pollution(
    pollution: dict[str, str],
) -> None:
    with pytest.raises(ValidationError):
        CreateImageArgs.model_validate(
            {
                "prompt": "draw a fox",
                "aspect_ratio": "square_1_1",
                "candidate_count": 1,
                "output_format": "png",
                **pollution,
            }
        )


@pytest.mark.parametrize(
    "prompt",
    ["", "x" * 32769, "🌙" * 8193, "\ud800"],
)
def test_create_image_prompt_uses_the_root_utf8_budget(prompt: str) -> None:
    with pytest.raises(ValidationError):
        CreateImageArgs(
            prompt=prompt,
            aspect_ratio="square_1_1",
            candidate_count=1,
            output_format="png",
        )


async def test_create_image_injects_handles_and_stable_identities_outside_model_schema() -> None:
    port = RecordingMediaPort((_accepted(), _accepted()))
    tool = make_create_image_tool(
        port,
        media_access_handle="media-access:" + "a" * 48,
        media_projection_reservation_handle="projection-reservation:" + "b" * 48,
    )
    coroutine = tool.coroutine
    assert coroutine is not None
    first_raw = await coroutine(
        prompt="draw a fox",
        aspect_ratio="landscape_16_9",
        candidate_count=2,
        output_format="webp",
        runtime=_runtime(),
    )
    second_raw = await coroutine(
        prompt="draw a fox",
        aspect_ratio="landscape_16_9",
        candidate_count=2,
        output_format="webp",
        runtime=_runtime(),
    )

    assert len(port.commands) == 2
    first, second = port.commands
    assert first == second
    assert first.media_access_handle.startswith("media-access:")
    assert first.media_projection_reservation_handle.startswith("projection-reservation:")
    assert first.image.prompt == "draw a fox"
    assert first.image.aspect_ratio == "landscape_16_9"
    assert first.image.candidate_count == 2
    assert first.image.output_format == "webp"
    assert first.agent_media_command_ref.startswith("media-command:sha256:")
    assert first.stable_output_slot_ref.startswith("media-output-slot:sha256:")

    first_result = CreateImageToolResult.model_validate_json(first_raw)
    second_result = CreateImageToolResult.model_validate_json(second_raw)
    assert first_result == second_result
    assert first_result.media_command_ref == first.agent_media_command_ref
    raw = json.loads(first_raw)
    assert "media_access_handle" not in raw
    assert "media_projection_reservation_handle" not in raw
    assert "provider" not in first_raw
    assert "storage" not in first_raw


async def test_create_image_tool_call_id_changes_hidden_command_identity() -> None:
    port = RecordingMediaPort((_accepted(), _accepted()))
    tool = make_create_image_tool(
        port,
        media_access_handle="media-access:" + "a" * 48,
        media_projection_reservation_handle="projection-reservation:" + "b" * 48,
    )
    coroutine = tool.coroutine
    assert coroutine is not None
    kwargs = {
        "prompt": "draw a fox",
        "aspect_ratio": "square_1_1",
        "candidate_count": 1,
        "output_format": "png",
    }
    await coroutine(**kwargs, runtime=_runtime("call-image-1"))
    await coroutine(**kwargs, runtime=_runtime("call-image-2"))
    assert port.commands[0].agent_media_command_ref != port.commands[1].agent_media_command_ref
    assert port.commands[0].stable_output_slot_ref != port.commands[1].stable_output_slot_ref


def test_stable_reference_uses_injective_part_framing() -> None:
    module = importlib.import_module("kokoro_agent.tools.media.create_image")
    stable_reference = getattr(module, "_stable_reference")
    left = stable_reference("media-command", "domain", "a\0b", "c")
    right = stable_reference("media-command", "domain", "a", "b\0c")

    assert left != right


async def test_create_image_does_not_swallow_graph_cancellation() -> None:
    class CanceledPort(MediaOperationPort):
        async def create_image(
            self, command: AgentImageCreateCommand
        ) -> MediaCommandAccepted:
            del command
            raise asyncio.CancelledError

    tool = make_create_image_tool(
        CanceledPort(),
        media_access_handle="media-access:" + "a" * 48,
        media_projection_reservation_handle="projection-reservation:" + "b" * 48,
    )
    coroutine = tool.coroutine
    assert coroutine is not None
    with pytest.raises(asyncio.CancelledError):
        await coroutine(
            prompt="draw a fox",
            aspect_ratio="square_1_1",
            candidate_count=1,
            output_format="png",
            runtime=_runtime(),
        )


def test_create_image_has_one_stable_catalog_name() -> None:
    assert CREATE_IMAGE_TOOL_NAME == "create_image"
