"""Typed outbound ports owned by Platform services."""

from kokoro_agent.platform.media import (
    AgentImageCreateCommand,
    ArtifactVersionHandle,
    ConnectMediaOperationPort,
    ImageCreateIntent,
    MediaCommandAccepted,
    MediaCommandRejected,
    MediaCommandResult,
    MediaCommandUnknown,
    MediaOperationPort,
    MediaOperationProtocolError,
    MediaOperationSafeView,
    MediaRuntimeSafeError,
    MediaRuntimeSettings,
)

__all__ = [
    "AgentImageCreateCommand",
    "ArtifactVersionHandle",
    "ConnectMediaOperationPort",
    "ImageCreateIntent",
    "MediaCommandAccepted",
    "MediaCommandRejected",
    "MediaCommandResult",
    "MediaCommandUnknown",
    "MediaOperationPort",
    "MediaOperationProtocolError",
    "MediaOperationSafeView",
    "MediaRuntimeSafeError",
    "MediaRuntimeSettings",
]
