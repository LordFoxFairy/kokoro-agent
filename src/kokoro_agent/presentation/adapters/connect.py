"""Generated Connect adapter for the Presentation delivery application service."""

from __future__ import annotations

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.request import RequestContext

from kokoro.agent.presentation.v1 import presentation_pb2 as wire
from kokoro_agent.presentation.delivery import (
    DeliveryService,
    PresentationProviderStore,
)


def _translate(error: ValueError) -> ConnectError:
    if str(error) == "PRESENTATION_PRODUCER_FENCED":
        return ConnectError(
            Code.FAILED_PRECONDITION,
            str(error),
            details=[
                wire.PermanentErrorDetail(
                    kind=wire.PERMANENT_ERROR_KIND_PRODUCER_FENCED,
                    retryable=False,
                    correlation_ref="agent-presentation-provider",
                )
            ],
        )
    return ConnectError(Code.INVALID_ARGUMENT, str(error))


class PresentationConnectService:
    """Translate Connect requests/errors only; delivery semantics live in DeliveryService."""

    def __init__(self, store: PresentationProviderStore) -> None:
        self._delivery = DeliveryService(store)

    async def check_active(
        self,
        request: wire.CheckActiveRequest,
        ctx: RequestContext[wire.CheckActiveRequest, wire.CheckActiveResponse] | None,
    ) -> wire.CheckActiveResponse:
        del ctx
        try:
            return await self._delivery.check_active(request)
        except ConnectError:
            raise
        except Exception as error:
            raise ConnectError(
                Code.UNAVAILABLE, "PRESENTATION_PROVIDER_NOT_ACTIVE"
            ) from error

    async def pull_records(
        self,
        request: wire.PullRecordsRequest,
        ctx: RequestContext[wire.PullRecordsRequest, wire.PullRecordsResponse] | None,
    ) -> wire.PullRecordsResponse:
        del ctx
        try:
            return await self._delivery.pull_records(request)
        except ConnectError:
            raise
        except ValueError as error:
            raise _translate(error) from error

    async def acknowledge_admissions(
        self,
        request: wire.AcknowledgeAdmissionsRequest,
        ctx: RequestContext[
            wire.AcknowledgeAdmissionsRequest, wire.AcknowledgeAdmissionsResponse
        ]
        | None,
    ) -> wire.AcknowledgeAdmissionsResponse:
        del ctx
        try:
            return await self._delivery.acknowledge_admissions(request)
        except ConnectError:
            raise
        except ValueError as error:
            raise _translate(error) from error

    async def quarantine_submission(
        self,
        request: wire.QuarantineSubmissionRequest,
        ctx: RequestContext[
            wire.QuarantineSubmissionRequest, wire.QuarantineSubmissionResponse
        ]
        | None,
    ) -> wire.QuarantineSubmissionResponse:
        del ctx
        try:
            return await self._delivery.quarantine_submission(request)
        except ConnectError:
            raise
        except ValueError as error:
            raise _translate(error) from error

    async def get_delivery_status(
        self,
        request: wire.GetDeliveryStatusRequest,
        ctx: RequestContext[
            wire.GetDeliveryStatusRequest, wire.GetDeliveryStatusResponse
        ]
        | None,
    ) -> wire.GetDeliveryStatusResponse:
        del ctx
        try:
            return await self._delivery.get_delivery_status(request)
        except ConnectError:
            raise
        except ValueError as error:
            raise _translate(error) from error


__all__ = ["PresentationConnectService"]
