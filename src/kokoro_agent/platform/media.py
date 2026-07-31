"""Thin, owner-journal-aware adapter for Platform Media Runtime.

GA owns only agent orchestration and product intent. Platform owns Site policy,
model selection, credit admission, provider dispatch, artifacts and operation state.
Opaque grants cross this boundary without being decoded or logged.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
from pathlib import Path
from typing import Annotated, Literal, Protocol
from urllib.parse import urlsplit, urlunsplit

import pyqwest
from connectrpc.errors import ConnectError
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from kokoro.platform.media.v1 import media_canonical_pb2 as canonical_pb
from kokoro.platform.media.v1 import media_runtime_pb2 as media_pb
from kokoro.platform.media.v1.media_runtime_connect import MediaRuntimeServiceClient

_FINGERPRINT_DOMAIN = b"kokoro.platform.media.agent-image-submit.v1\0"
_MAX_TLS_BYTES = 256 * 1024
_MAX_RESPONSE_BYTES = 256 * 1024
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_REFERENCE = Annotated[
    str,
    StringConstraints(min_length=1, max_length=256),
    AfterValidator(lambda value: _trimmed(value, "reference")),
]
_OPAQUE_HANDLE = Annotated[
    str,
    StringConstraints(min_length=32, max_length=8192),
    AfterValidator(lambda value: _trimmed(value, "opaque handle")),
]
ImageAspectRatio = Literal[
    "square_1_1",
    "landscape_4_3",
    "landscape_16_9",
    "portrait_3_4",
    "portrait_9_16",
]
ImageOutputFormat = Literal["png", "jpeg", "webp"]
MediaOperationState = Literal[
    "admission_pending",
    "authorized",
    "queued",
    "active",
    "finalizing",
    "cancel_requested",
    "reconciling",
    "completed",
    "partial",
    "failed",
    "canceled",
]
MediaCandidateState = Literal[
    "allocated",
    "producing",
    "output_received",
    "validating",
    "ready",
    "restricted",
    "failed",
    "unknown",
    "cancel_requested",
    "canceled",
]
MediaOutcomeClass = Literal["canonical", "irreconcilable"]
MediaRecoveryAction = Literal["get_operation", "recover_command", "contact_support"]
MediaErrorCode = Literal[
    "access_denied",
    "access_expired",
    "idempotency_conflict",
    "operation_not_found",
    "operation_version_conflict",
    "projection_binding_rejected",
    "policy_rejected",
    "outcome_unknown",
    "transport_outcome_unknown",
]


def _trimmed(value: str, kind: str) -> str:
    if value.strip() != value:
        raise ValueError(f"{kind} must not have surrounding whitespace")
    return value


class _FrozenModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class ImageCreateIntent(_FrozenModel):
    prompt: str
    aspect_ratio: ImageAspectRatio
    candidate_count: int = Field(ge=1, le=4)
    output_format: ImageOutputFormat

    @field_validator("prompt")
    @classmethod
    def _prompt_budget(cls, value: str) -> str:
        try:
            size = len(value.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise ValueError("prompt must be valid UTF-8") from error
        if not 1 <= size <= 32_768:
            raise ValueError("prompt must be 1..32768 UTF-8 bytes")
        return value


class AgentImageCreateCommand(_FrozenModel):
    media_access_handle: _OPAQUE_HANDLE
    media_projection_reservation_handle: _OPAQUE_HANDLE
    stable_output_slot_ref: _REFERENCE
    agent_media_command_ref: _REFERENCE
    image: ImageCreateIntent


class ArtifactVersionHandle(_FrozenModel):
    candidate_ref: _REFERENCE
    candidate_owner_version: int = Field(gt=0)
    state: MediaCandidateState = "ready"
    artifact_version_handle: _OPAQUE_HANDLE | None = None

    @model_validator(mode="after")
    def _artifact_only_for_ready(self) -> ArtifactVersionHandle:
        if (self.artifact_version_handle is not None) != (self.state == "ready"):
            raise ValueError("artifact handle is present exactly when candidate is ready")
        return self


class MediaOperationSafeView(_FrozenModel):
    media_operation_handle: _OPAQUE_HANDLE
    operation_ref: _REFERENCE
    owner_version: int = Field(gt=0)
    state: MediaOperationState
    outcome_class: MediaOutcomeClass | None = None
    safe_progress_bps: int = Field(ge=0, le=10_000)
    artifacts: tuple[ArtifactVersionHandle, ...] = Field(max_length=4)
    cost_projection_ref: _REFERENCE | None = None
    cost_projection_owner_version: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _closed_projection(self) -> MediaOperationSafeView:
        terminal = self.state in {"completed", "partial", "failed", "canceled"}
        if terminal != (self.outcome_class is not None):
            raise ValueError("operation outcome is present exactly for terminal state")
        if (self.cost_projection_ref is None) != (self.cost_projection_owner_version is None):
            raise ValueError("cost projection ref and version are an atomic pair")
        return self


class MediaRuntimeSafeError(_FrozenModel):
    code: MediaErrorCode
    message: str = Field(max_length=512)


class MediaCommandAccepted(_FrozenModel):
    outcome: Literal["accepted"]
    media_command_ref: _REFERENCE
    recovery_action: MediaRecoveryAction
    operation_ref: _REFERENCE
    operation: MediaOperationSafeView | None = None
    error: None = None


class MediaCommandRejected(_FrozenModel):
    outcome: Literal["rejected"]
    media_command_ref: _REFERENCE
    recovery_action: None = None
    operation: None = None
    error: MediaRuntimeSafeError


class MediaCommandUnknown(_FrozenModel):
    outcome: Literal["outcome_unknown"]
    media_command_ref: _REFERENCE
    recovery_action: MediaRecoveryAction
    operation: MediaOperationSafeView | None = None
    error: MediaRuntimeSafeError


MediaCommandResult = Annotated[
    MediaCommandAccepted | MediaCommandRejected | MediaCommandUnknown,
    Field(discriminator="outcome"),
]


class MediaOperationPort(Protocol):
    async def create_image(self, command: AgentImageCreateCommand) -> MediaCommandResult: ...


class AsyncMediaRuntimeClient(Protocol):
    async def recover_media_operation_by_command(
        self,
        request: media_pb.RecoverMediaOperationByCommandRequest,
        *,
        timeout_ms: int | None = None,
    ) -> media_pb.RecoverMediaOperationByCommandResponse: ...

    async def create_agent_image_operation(
        self,
        request: media_pb.CreateAgentImageOperationRequest,
        *,
        timeout_ms: int | None = None,
    ) -> media_pb.CreateAgentImageOperationResponse: ...


class MediaRuntimeSettings(_FrozenModel):
    rpc_url: str
    ca_file: str
    cert_file: str
    key_file: str
    timeout_ms: int = Field(default=30_000, ge=100, le=30_000)


class MediaOperationProtocolError(RuntimeError):
    """Stable boundary error; remote response data is deliberately omitted."""

    def __init__(self) -> None:
        super().__init__("MEDIA_RUNTIME_RESPONSE_INVALID")


class ConnectMediaOperationPort:
    """Exactly-once-intent client backed by Platform's command owner journal."""

    def __init__(
        self,
        settings: MediaRuntimeSettings,
        *,
        client: AsyncMediaRuntimeClient | None = None,
    ) -> None:
        self._timeout_ms = settings.timeout_ms
        if client is None:
            address = _media_address(settings.rpc_url)
            transport = pyqwest.HTTPTransport(
                tls_ca_cert=_tls_file(settings.ca_file, "CA"),
                tls_include_system_certs=False,
                tls_key=_tls_file(settings.key_file, "KEY", private=True),
                tls_cert=_tls_file(settings.cert_file, "CERT"),
                http_version=pyqwest.HTTPVersion.HTTP2,
                enable_cookie_store=False,
            )
            client = MediaRuntimeServiceClient(
                address,
                accept_compression=(),
                send_compression=None,
                timeout_ms=settings.timeout_ms,
                read_max_bytes=_MAX_RESPONSE_BYTES,
                http_client=pyqwest.Client(transport),
            )
        self._client = client

    def fingerprint(self, command: AgentImageCreateCommand) -> str:
        preimage = media_pb.AgentImageSubmissionFingerprintInputV1(
            stable_output_slot_ref=command.stable_output_slot_ref,
            image_intent=_image_intent(command.image),
        )
        return hashlib.sha256(
            _FINGERPRINT_DOMAIN + preimage.SerializeToString(deterministic=True)
        ).hexdigest()

    async def create_image(self, command: AgentImageCreateCommand) -> MediaCommandResult:
        fingerprint = self.fingerprint(command)
        try:
            recovered_response = await self._client.recover_media_operation_by_command(
                media_pb.RecoverMediaOperationByCommandRequest(
                    media_access_handle=command.media_access_handle,
                    media_command_ref=command.agent_media_command_ref,
                ),
                timeout_ms=self._timeout_ms,
            )
        except (ConnectError, OSError, TimeoutError):
            return _transport_unknown(command.agent_media_command_ref)

        recovered = _map_response(
            recovered_response,
            command_ref=command.agent_media_command_ref,
            fingerprint=fingerprint,
            allow_not_found=True,
        )
        if not (
            isinstance(recovered, MediaCommandRejected)
            and recovered.error.code == "operation_not_found"
        ):
            return recovered

        request = media_pb.CreateAgentImageOperationRequest(
            media_access_handle=command.media_access_handle,
            media_projection_reservation_handle=command.media_projection_reservation_handle,
            stable_output_slot_ref=command.stable_output_slot_ref,
            agent_media_command_ref=command.agent_media_command_ref,
            caller_request_fingerprint=fingerprint,
            image_intent=_image_intent(command.image),
        )
        try:
            response = await self._client.create_agent_image_operation(
                request, timeout_ms=self._timeout_ms
            )
        except (ConnectError, OSError, TimeoutError):
            return await self._recover_after_ambiguous_transport(command, fingerprint)
        return _map_response(
            response,
            command_ref=command.agent_media_command_ref,
            fingerprint=fingerprint,
            allow_not_found=False,
        )

    async def _recover_after_ambiguous_transport(
        self, command: AgentImageCreateCommand, fingerprint: str
    ) -> MediaCommandResult:
        try:
            response = await self._client.recover_media_operation_by_command(
                media_pb.RecoverMediaOperationByCommandRequest(
                    media_access_handle=command.media_access_handle,
                    media_command_ref=command.agent_media_command_ref,
                ),
                timeout_ms=self._timeout_ms,
            )
        except (ConnectError, OSError, TimeoutError):
            return _transport_unknown(command.agent_media_command_ref)
        recovered = _map_response(
            response,
            command_ref=command.agent_media_command_ref,
            fingerprint=fingerprint,
            allow_not_found=True,
        )
        if (
            isinstance(recovered, MediaCommandRejected)
            and recovered.error.code == "operation_not_found"
        ):
            return _transport_unknown(command.agent_media_command_ref)
        return recovered


def _image_intent(intent: ImageCreateIntent) -> media_pb.AgentImageIntentV1:
    aspects = {
        "square_1_1": canonical_pb.CANONICAL_IMAGE_ASPECT_RATIO_SQUARE_1_1,
        "landscape_4_3": canonical_pb.CANONICAL_IMAGE_ASPECT_RATIO_LANDSCAPE_4_3,
        "landscape_16_9": canonical_pb.CANONICAL_IMAGE_ASPECT_RATIO_LANDSCAPE_16_9,
        "portrait_3_4": canonical_pb.CANONICAL_IMAGE_ASPECT_RATIO_PORTRAIT_3_4,
        "portrait_9_16": canonical_pb.CANONICAL_IMAGE_ASPECT_RATIO_PORTRAIT_9_16,
    }
    formats = {
        "png": canonical_pb.CANONICAL_IMAGE_OUTPUT_FORMAT_PNG,
        "jpeg": canonical_pb.CANONICAL_IMAGE_OUTPUT_FORMAT_JPEG,
        "webp": canonical_pb.CANONICAL_IMAGE_OUTPUT_FORMAT_WEBP,
    }
    return media_pb.AgentImageIntentV1(
        prompt_intent=intent.prompt,
        aspect_ratio=aspects[intent.aspect_ratio],
        candidate_count=intent.candidate_count,
        output_format=formats[intent.output_format],
    )


def _map_response(
    response: media_pb.CreateAgentImageOperationResponse
    | media_pb.RecoverMediaOperationByCommandResponse,
    *,
    command_ref: str,
    fingerprint: str,
    allow_not_found: bool,
) -> MediaCommandResult:
    try:
        if not response.HasField("receipt"):
            raise ValueError
        receipt = response.receipt
        arm = receipt.WhichOneof("outcome")
        operation = _operation(response.operation) if response.HasField("operation") else None
        if arm == "submit_accepted":
            accepted = receipt.submit_accepted
            _validate_submit_identity(accepted, command_ref, fingerprint)
            _validate_receipt(accepted)
            recovery_action = _recovery_action(accepted.recovery_action)
            if operation is not None and operation.operation_ref != accepted.operation_ref:
                raise ValueError
            return MediaCommandAccepted(
                outcome="accepted",
                media_command_ref=command_ref,
                recovery_action=recovery_action,
                operation_ref=accepted.operation_ref,
                operation=operation,
            )
        if arm == "submit_rejected":
            rejected = receipt.submit_rejected
            _validate_receipt(rejected)
            error = _safe_error(rejected.error)
            if rejected.media_command_ref != command_ref or operation is not None:
                raise ValueError
            if _DIGEST_PATTERN.fullmatch(rejected.caller_request_fingerprint) is None:
                raise ValueError
            if error.code != "operation_not_found" or not allow_not_found:
                if rejected.caller_request_fingerprint != fingerprint:
                    raise ValueError
            return MediaCommandRejected(
                outcome="rejected", media_command_ref=command_ref, error=error
            )
        if arm == "submit_outcome_unknown":
            unknown = receipt.submit_outcome_unknown
            _validate_submit_identity(unknown, command_ref, fingerprint)
            _validate_receipt(unknown)
            return MediaCommandUnknown(
                outcome="outcome_unknown",
                media_command_ref=command_ref,
                recovery_action=_recovery_action(unknown.recovery_action),
                operation=operation,
                error=_safe_error(unknown.error),
            )
        raise ValueError
    except (ValueError, TypeError, UnicodeError):
        raise MediaOperationProtocolError() from None


def _validate_submit_identity(receipt: object, command_ref: str, fingerprint: str) -> None:
    if (
        getattr(receipt, "media_command_ref", None) != command_ref
        or getattr(receipt, "caller_request_fingerprint", None) != fingerprint
    ):
        raise ValueError


_SubmitReceipt = (
    media_pb.SubmitMediaCommandAccepted
    | media_pb.SubmitMediaCommandRejected
    | media_pb.SubmitMediaCommandOutcomeUnknown
)


def _validate_receipt(receipt: _SubmitReceipt) -> None:
    if receipt.receipt_version <= 0 or not receipt.HasField("recorded_at"):
        raise ValueError
    try:
        receipt.recorded_at.ToDatetime()
    except (OverflowError, ValueError):
        raise ValueError


def _operation(value: media_pb.AgentMediaOperationView) -> MediaOperationSafeView:
    states: dict[int, MediaOperationState] = {
        media_pb.MEDIA_OPERATION_STATE_ADMISSION_PENDING: "admission_pending",
        media_pb.MEDIA_OPERATION_STATE_AUTHORIZED: "authorized",
        media_pb.MEDIA_OPERATION_STATE_QUEUED: "queued",
        media_pb.MEDIA_OPERATION_STATE_ACTIVE: "active",
        media_pb.MEDIA_OPERATION_STATE_FINALIZING: "finalizing",
        media_pb.MEDIA_OPERATION_STATE_CANCEL_REQUESTED: "cancel_requested",
        media_pb.MEDIA_OPERATION_STATE_RECONCILING: "reconciling",
        media_pb.MEDIA_OPERATION_STATE_COMPLETED: "completed",
        media_pb.MEDIA_OPERATION_STATE_PARTIAL: "partial",
        media_pb.MEDIA_OPERATION_STATE_FAILED: "failed",
        media_pb.MEDIA_OPERATION_STATE_CANCELED: "canceled",
    }
    candidate_states: dict[int, MediaCandidateState] = {
        media_pb.MEDIA_CANDIDATE_STATE_ALLOCATED: "allocated",
        media_pb.MEDIA_CANDIDATE_STATE_PRODUCING: "producing",
        media_pb.MEDIA_CANDIDATE_STATE_OUTPUT_RECEIVED: "output_received",
        media_pb.MEDIA_CANDIDATE_STATE_VALIDATING: "validating",
        media_pb.MEDIA_CANDIDATE_STATE_READY: "ready",
        media_pb.MEDIA_CANDIDATE_STATE_RESTRICTED: "restricted",
        media_pb.MEDIA_CANDIDATE_STATE_FAILED: "failed",
        media_pb.MEDIA_CANDIDATE_STATE_UNKNOWN: "unknown",
        media_pb.MEDIA_CANDIDATE_STATE_CANCEL_REQUESTED: "cancel_requested",
        media_pb.MEDIA_CANDIDATE_STATE_CANCELED: "canceled",
    }
    state = states.get(value.state)
    if state is None or not value.HasField("observed_at") or len(value.candidates) > 4:
        raise ValueError
    try:
        value.observed_at.ToDatetime()
    except (OverflowError, ValueError):
        raise ValueError
    terminal = value.state >= media_pb.MEDIA_OPERATION_STATE_COMPLETED
    outcomes: dict[int, MediaOutcomeClass] = {
        media_pb.MEDIA_OPERATION_OUTCOME_CLASS_CANONICAL: "canonical",
        media_pb.MEDIA_OPERATION_OUTCOME_CLASS_IRRECONCILABLE: "irreconcilable",
    }
    outcome = outcomes.get(value.outcome_class)
    if terminal != (outcome is not None):
        raise ValueError
    has_cost_ref = value.HasField("cost_projection_ref")
    has_cost_version = value.HasField("cost_projection_owner_version")
    if has_cost_ref != has_cost_version:
        raise ValueError
    candidates: list[ArtifactVersionHandle] = []
    for candidate in value.candidates:
        candidate_state = candidate_states.get(candidate.state)
        if candidate_state is None:
            raise ValueError
        has_artifact = candidate.HasField("artifact_version_handle")
        if has_artifact != (candidate.state == media_pb.MEDIA_CANDIDATE_STATE_READY):
            raise ValueError
        candidates.append(
            ArtifactVersionHandle(
                candidate_ref=candidate.candidate_ref,
                candidate_owner_version=candidate.owner_version,
                state=candidate_state,
                artifact_version_handle=(candidate.artifact_version_handle if has_artifact else None),
            )
        )
    return MediaOperationSafeView(
        media_operation_handle=value.media_operation_handle,
        operation_ref=value.operation_ref,
        owner_version=value.owner_version,
        state=state,
        outcome_class=outcome,
        safe_progress_bps=value.safe_progress_bps,
        artifacts=tuple(candidates),
        cost_projection_ref=value.cost_projection_ref if has_cost_ref else None,
        cost_projection_owner_version=(
            value.cost_projection_owner_version if has_cost_version else None
        ),
    )


def _safe_error(value: media_pb.MediaRuntimeError) -> MediaRuntimeSafeError:
    codes: dict[int, MediaErrorCode] = {
        media_pb.MEDIA_RUNTIME_ERROR_CODE_ACCESS_DENIED: "access_denied",
        media_pb.MEDIA_RUNTIME_ERROR_CODE_ACCESS_EXPIRED: "access_expired",
        media_pb.MEDIA_RUNTIME_ERROR_CODE_IDEMPOTENCY_CONFLICT: "idempotency_conflict",
        media_pb.MEDIA_RUNTIME_ERROR_CODE_OPERATION_NOT_FOUND: "operation_not_found",
        media_pb.MEDIA_RUNTIME_ERROR_CODE_OPERATION_VERSION_CONFLICT: "operation_version_conflict",
        media_pb.MEDIA_RUNTIME_ERROR_CODE_PROJECTION_BINDING_REJECTED: (
            "projection_binding_rejected"
        ),
        media_pb.MEDIA_RUNTIME_ERROR_CODE_POLICY_REJECTED: "policy_rejected",
        media_pb.MEDIA_RUNTIME_ERROR_CODE_OUTCOME_UNKNOWN: "outcome_unknown",
    }
    code = codes.get(value.code)
    if code is None:
        raise ValueError
    return MediaRuntimeSafeError(code=code, message=value.safe_message)


def _recovery_action(value: int) -> MediaRecoveryAction:
    actions: dict[int, MediaRecoveryAction] = {
        media_pb.MEDIA_COMMAND_RECOVERY_ACTION_GET_OPERATION: "get_operation",
        media_pb.MEDIA_COMMAND_RECOVERY_ACTION_RECOVER_COMMAND: "recover_command",
        media_pb.MEDIA_COMMAND_RECOVERY_ACTION_CONTACT_SUPPORT: "contact_support",
    }
    action = actions.get(value)
    if action is None:
        raise ValueError
    return action


def _transport_unknown(command_ref: str) -> MediaCommandUnknown:
    return MediaCommandUnknown(
        outcome="outcome_unknown",
        media_command_ref=command_ref,
        recovery_action="recover_command",
        error=MediaRuntimeSafeError(
            code="transport_outcome_unknown",
            message="Media owner outcome is not yet known; recover the same command.",
        ),
    )


def _media_address(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
    ):
        raise ValueError("MEDIA_RUNTIME_URL_INVALID")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _tls_file(value: str, kind: str, *, private: bool = False) -> bytes:
    path = Path(value)
    try:
        before = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise OSError("changed")
            material = os.read(descriptor, _MAX_TLS_BYTES + 1)
        finally:
            os.close(descriptor)
    except OSError as error:
        raise ValueError(f"MEDIA_RUNTIME_TLS_{kind}_INVALID") from error
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or not 1 <= before.st_size <= _MAX_TLS_BYTES
        or len(material) != before.st_size
        or (private and before.st_mode & 0o077 != 0)
    ):
        raise ValueError(f"MEDIA_RUNTIME_TLS_{kind}_INVALID")
    return material
