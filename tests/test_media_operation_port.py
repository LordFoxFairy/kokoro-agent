"""Platform Media runtime adapter: receipt recovery, validation and safe projections."""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from google.protobuf.timestamp_pb2 import Timestamp

from kokoro.platform.media.v1 import media_canonical_pb2 as canonical_pb
from kokoro.platform.media.v1 import media_runtime_pb2 as media_pb
from kokoro_agent.platform import (
    AgentImageCreateCommand,
    ConnectMediaOperationPort,
    ImageCreateIntent,
    MediaCommandAccepted,
    MediaCommandRejected,
    MediaCommandUnknown,
    MediaOperationProtocolError,
    MediaRuntimeSettings,
)


def _timestamp() -> Timestamp:
    value = Timestamp()
    value.FromMilliseconds(1_700_000_000_000)
    return value


def _command() -> AgentImageCreateCommand:
    return AgentImageCreateCommand(
        media_access_handle="media-access:" + "a" * 48,
        media_projection_reservation_handle="projection-reservation:" + "b" * 48,
        stable_output_slot_ref="media-output-slot:sha256:" + "c" * 64,
        agent_media_command_ref="media-command:sha256:" + "d" * 64,
        image=ImageCreateIntent(
            prompt="draw a fox",
            aspect_ratio="square_1_1",
            candidate_count=1,
            output_format="png",
        ),
    )


def _operation() -> media_pb.AgentMediaOperationView:
    return media_pb.AgentMediaOperationView(
        media_operation_handle="media-operation:" + "e" * 48,
        operation_ref="operation-1",
        owner_version=3,
        state=media_pb.MEDIA_OPERATION_STATE_QUEUED,
        safe_progress_bps=100,
        candidates=[
            media_pb.AgentMediaCandidateView(
                candidate_ref="candidate-1",
                owner_version=2,
                state=media_pb.MEDIA_CANDIDATE_STATE_READY,
                artifact_version_handle="artifact-version:" + "f" * 48,
            )
        ],
        observed_at=_timestamp(),
    )


def _missing(command_ref: str) -> media_pb.RecoverMediaOperationByCommandResponse:
    return media_pb.RecoverMediaOperationByCommandResponse(
        receipt=media_pb.MediaCommandReceipt(
            submit_rejected=media_pb.SubmitMediaCommandRejected(
                media_command_ref=command_ref,
                caller_request_fingerprint="0" * 64,
                error=media_pb.MediaRuntimeError(
                    code=media_pb.MEDIA_RUNTIME_ERROR_CODE_OPERATION_NOT_FOUND,
                    safe_message="command not found",
                ),
                receipt_version=1,
                recorded_at=_timestamp(),
            )
        )
    )


def _accepted(
    command_ref: str, fingerprint: str
) -> media_pb.CreateAgentImageOperationResponse:
    return media_pb.CreateAgentImageOperationResponse(
        receipt=media_pb.MediaCommandReceipt(
            submit_accepted=media_pb.SubmitMediaCommandAccepted(
                media_command_ref=command_ref,
                caller_request_fingerprint=fingerprint,
                operation_ref="operation-1",
                receipt_version=1,
                recorded_at=_timestamp(),
                recovery_action=media_pb.MEDIA_COMMAND_RECOVERY_ACTION_GET_OPERATION,
            )
        ),
        operation=_operation(),
    )


class ScriptedClient:
    def __init__(self, *, recover: list[object], create: list[object]) -> None:
        self.recover = recover
        self.create = create
        self.calls: list[tuple[str, object, int | None]] = []

    async def recover_media_operation_by_command(
        self,
        request: media_pb.RecoverMediaOperationByCommandRequest,
        *,
        timeout_ms: int | None = None,
    ) -> media_pb.RecoverMediaOperationByCommandResponse:
        self.calls.append(("recover", request, timeout_ms))
        value = self.recover.pop(0)
        if isinstance(value, BaseException):
            raise value
        assert isinstance(value, media_pb.RecoverMediaOperationByCommandResponse)
        return value

    async def create_agent_image_operation(
        self,
        request: media_pb.CreateAgentImageOperationRequest,
        *,
        timeout_ms: int | None = None,
    ) -> media_pb.CreateAgentImageOperationResponse:
        self.calls.append(("create", request, timeout_ms))
        value = self.create.pop(0)
        if isinstance(value, BaseException):
            raise value
        assert isinstance(value, media_pb.CreateAgentImageOperationResponse)
        return value


def _port(client: ScriptedClient) -> ConnectMediaOperationPort:
    return ConnectMediaOperationPort(
        MediaRuntimeSettings(
            rpc_url="https://media.internal:9443",
            ca_file="/not/read/with/injected/client",
            cert_file="/not/read/with/injected/client",
            key_file="/not/read/with/injected/client",
            timeout_ms=30_000,
        ),
        client=client,
    )


async def test_new_command_recovers_first_then_creates_once_with_root_fingerprint() -> None:
    command = _command()
    client = ScriptedClient(recover=[_missing(command.agent_media_command_ref)], create=[])
    port = _port(client)
    expected = port.fingerprint(command)
    independent_preimage = media_pb.AgentImageSubmissionFingerprintInputV1(
        stable_output_slot_ref=command.stable_output_slot_ref,
        image_intent=media_pb.AgentImageIntentV1(
            prompt_intent="draw a fox",
            aspect_ratio=canonical_pb.CANONICAL_IMAGE_ASPECT_RATIO_SQUARE_1_1,
            candidate_count=1,
            output_format=canonical_pb.CANONICAL_IMAGE_OUTPUT_FORMAT_PNG,
        ),
    )
    assert expected == hashlib.sha256(
        b"kokoro.platform.media.agent-image-submit.v1\0"
        + independent_preimage.SerializeToString(deterministic=True)
    ).hexdigest()
    client.create.append(_accepted(command.agent_media_command_ref, expected))

    result = await port.create_image(command)

    assert isinstance(result, MediaCommandAccepted)
    assert [name for name, _request, _timeout in client.calls] == ["recover", "create"]
    assert all(timeout == 30_000 for _name, _request, timeout in client.calls)
    create_request = client.calls[1][1]
    assert isinstance(create_request, media_pb.CreateAgentImageOperationRequest)
    assert create_request.media_access_handle == command.media_access_handle
    assert (
        create_request.media_projection_reservation_handle
        == command.media_projection_reservation_handle
    )
    assert create_request.agent_media_command_ref == command.agent_media_command_ref
    assert create_request.stable_output_slot_ref == command.stable_output_slot_ref
    assert create_request.caller_request_fingerprint == expected
    assert create_request.image_intent.prompt_intent == command.image.prompt
    assert (
        create_request.image_intent.aspect_ratio
        == canonical_pb.CANONICAL_IMAGE_ASPECT_RATIO_SQUARE_1_1
    )
    assert create_request.image_intent.candidate_count == 1
    assert (
        create_request.image_intent.output_format
        == canonical_pb.CANONICAL_IMAGE_OUTPUT_FORMAT_PNG
    )
    assert result.operation is not None
    assert result.operation.operation_ref == "operation-1"
    artifact_handle = result.operation.artifacts[0].artifact_version_handle
    assert artifact_handle is not None
    assert artifact_handle.startswith("artifact-version:")


def test_fingerprint_excludes_rotatable_handles_and_command_ref() -> None:
    command = _command()
    port = _port(ScriptedClient(recover=[], create=[]))
    rotated = command.model_copy(
        update={
            "media_access_handle": "media-access:" + "1" * 48,
            "media_projection_reservation_handle": "projection-reservation:" + "2" * 48,
            "agent_media_command_ref": "media-command:sha256:" + "3" * 64,
        }
    )
    assert port.fingerprint(command) == port.fingerprint(rotated)


async def test_replayed_command_uses_owner_receipt_without_second_create() -> None:
    command = _command()
    port_seed = _port(ScriptedClient(recover=[], create=[]))
    fingerprint = port_seed.fingerprint(command)
    accepted = _accepted(command.agent_media_command_ref, fingerprint)
    recovered = media_pb.RecoverMediaOperationByCommandResponse(
        receipt=accepted.receipt,
        operation=accepted.operation,
    )
    client = ScriptedClient(recover=[recovered], create=[])
    result = await _port(client).create_image(command)
    assert isinstance(result, MediaCommandAccepted)
    assert [name for name, _request, _timeout in client.calls] == ["recover"]


async def test_accepted_receipt_without_optional_operation_view_remains_accepted() -> None:
    command = _command()
    port_seed = _port(ScriptedClient(recover=[], create=[]))
    fingerprint = port_seed.fingerprint(command)
    accepted = _accepted(command.agent_media_command_ref, fingerprint)
    recovered = media_pb.RecoverMediaOperationByCommandResponse(receipt=accepted.receipt)
    client = ScriptedClient(recover=[recovered], create=[])

    result = await _port(client).create_image(command)

    assert isinstance(result, MediaCommandAccepted)
    assert result.operation_ref == "operation-1"
    assert result.operation is None


async def test_create_transport_ambiguity_recovers_same_command_instead_of_resending() -> None:
    command = _command()
    port_seed = _port(ScriptedClient(recover=[], create=[]))
    fingerprint = port_seed.fingerprint(command)
    accepted = _accepted(command.agent_media_command_ref, fingerprint)
    recovered = media_pb.RecoverMediaOperationByCommandResponse(
        receipt=accepted.receipt,
        operation=accepted.operation,
    )
    client = ScriptedClient(
        recover=[_missing(command.agent_media_command_ref), recovered],
        create=[ConnectError(Code.UNAVAILABLE, "unavailable")],
    )
    result = await _port(client).create_image(command)
    assert isinstance(result, MediaCommandAccepted)
    assert [name for name, _request, _timeout in client.calls] == [
        "recover",
        "create",
        "recover",
    ]


async def test_owner_outcome_unknown_is_typed_and_never_inferred_terminal() -> None:
    command = _command()
    port_seed = _port(ScriptedClient(recover=[], create=[]))
    fingerprint = port_seed.fingerprint(command)
    response = media_pb.CreateAgentImageOperationResponse(
        receipt=media_pb.MediaCommandReceipt(
            submit_outcome_unknown=media_pb.SubmitMediaCommandOutcomeUnknown(
                media_command_ref=command.agent_media_command_ref,
                caller_request_fingerprint=fingerprint,
                error=media_pb.MediaRuntimeError(
                    code=media_pb.MEDIA_RUNTIME_ERROR_CODE_OUTCOME_UNKNOWN,
                    safe_message="owner is reconciling",
                ),
                receipt_version=1,
                recorded_at=_timestamp(),
                recovery_action=media_pb.MEDIA_COMMAND_RECOVERY_ACTION_RECOVER_COMMAND,
            )
        )
    )
    client = ScriptedClient(
        recover=[_missing(command.agent_media_command_ref)],
        create=[response],
    )
    result = await _port(client).create_image(command)
    assert isinstance(result, MediaCommandUnknown)
    assert result.outcome == "outcome_unknown"
    assert result.operation is None
    assert result.recovery_action == "recover_command"


async def test_create_and_recovery_transport_loss_returns_local_unknown_not_retry() -> None:
    command = _command()
    client = ScriptedClient(
        recover=[
            _missing(command.agent_media_command_ref),
            ConnectError(Code.UNAVAILABLE, "unavailable"),
        ],
        create=[ConnectError(Code.DEADLINE_EXCEEDED, "deadline")],
    )
    result = await _port(client).create_image(command)
    assert isinstance(result, MediaCommandUnknown)
    assert result.error.code == "transport_outcome_unknown"
    assert [name for name, _request, _timeout in client.calls] == [
        "recover",
        "create",
        "recover",
    ]


async def test_recover_unavailable_before_any_effect_fails_closed_without_create() -> None:
    command = _command()
    client = ScriptedClient(
        recover=[ConnectError(Code.UNAVAILABLE, "unavailable")],
        create=[],
    )
    result = await _port(client).create_image(command)
    assert isinstance(result, MediaCommandUnknown)
    assert [name for name, _request, _timeout in client.calls] == ["recover"]


async def test_protocol_mismatch_fails_closed_without_leaking_response() -> None:
    command = _command()
    port_seed = _port(ScriptedClient(recover=[], create=[]))
    response = _accepted("another-command", port_seed.fingerprint(command))
    client = ScriptedClient(
        recover=[_missing(command.agent_media_command_ref)],
        create=[response],
    )
    with pytest.raises(MediaOperationProtocolError) as captured:
        await _port(client).create_image(command)
    assert str(captured.value) == "MEDIA_RUNTIME_RESPONSE_INVALID"


async def test_out_of_range_receipt_timestamp_is_a_protocol_error() -> None:
    command = _command()
    fingerprint = _port(ScriptedClient(recover=[], create=[])).fingerprint(command)
    response = _accepted(command.agent_media_command_ref, fingerprint)
    response.receipt.submit_accepted.recorded_at.seconds = 253_402_300_800
    client = ScriptedClient(recover=[_missing(command.agent_media_command_ref)], create=[response])
    with pytest.raises(MediaOperationProtocolError):
        await _port(client).create_image(command)


async def test_rejected_owner_receipt_is_closed_and_safe() -> None:
    command = _command()
    fingerprint = _port(ScriptedClient(recover=[], create=[])).fingerprint(command)
    rejected = media_pb.CreateAgentImageOperationResponse(
        receipt=media_pb.MediaCommandReceipt(
            submit_rejected=media_pb.SubmitMediaCommandRejected(
                media_command_ref=command.agent_media_command_ref,
                caller_request_fingerprint=fingerprint,
                error=media_pb.MediaRuntimeError(
                    code=media_pb.MEDIA_RUNTIME_ERROR_CODE_POLICY_REJECTED,
                    safe_message="not available for this run",
                ),
                receipt_version=1,
                recorded_at=_timestamp(),
            )
        )
    )
    client = ScriptedClient(
        recover=[_missing(command.agent_media_command_ref)],
        create=[rejected],
    )
    result = await _port(client).create_image(command)
    assert isinstance(result, MediaCommandRejected)
    assert result.error.code == "policy_rejected"
    assert result.operation is None


async def test_connect_cancellation_propagates_without_recovery_or_create() -> None:
    command = _command()
    client = ScriptedClient(recover=[asyncio.CancelledError()], create=[])
    with pytest.raises(asyncio.CancelledError):
        await _port(client).create_image(command)
    assert [name for name, _request, _timeout in client.calls] == ["recover"]


async def test_non_transport_client_bug_is_not_reclassified_as_unknown() -> None:
    command = _command()
    client = ScriptedClient(recover=[ValueError("client bug")], create=[])
    with pytest.raises(ValueError, match="client bug"):
        await _port(client).create_image(command)
