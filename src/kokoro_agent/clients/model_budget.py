"""Single-send BFF model-budget transport, independent of browser credentials."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import asdict
import json
import math
import re
from typing import Self, TypeVar

import httpx
from pydantic import BaseModel, SecretStr, TypeAdapter

from kokoro_agent.clients.bff_admission_generated import (
    AgentModelAdmissionRequest,
    AgentModelAdmissionResponse,
    AgentModelAdmissionError,
    AgentModelSettlementRequest,
    AgentModelAdmissionId,
    AgentModelAttemptId,
    AgentModelSettlementResponse,
    AgentModelSettlementError,
)
from kokoro_agent.model.budget import ModelBudgetCommand, ModelBudgetGrant
from kokoro_agent.model.settlement import ModelSettlementCommand, ModelSettlementReceipt

_T = TypeVar("_T")
_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}", re.ASCII)
_OBJECT = TypeAdapter(dict[str, object])
_MAX_RESPONSE = 32768
_ERROR_STATUS = {
    "model_admission_invalid_request": 400,
    "model_admission_service_denied": 403,
    "model_admission_forbidden": 403,
    "model_admission_not_found": 404,
    "model_admission_conflict": 409,
    "model_admission_insufficient_credit": 422,
    "model_admission_price_unavailable": 422,
    "model_admission_ineligible": 422,
    "model_admission_upstream_invalid": 502,
    "model_admission_unavailable": 503,
}


class ModelBudgetError(RuntimeError):
    """Safe failure; unavailable/invalid ACK never proves no reservation occurred."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate budget member")
        result[key] = value
    return result


def _constant(_: str) -> object:
    raise ValueError("nonfinite budget value")


def _decode(raw: bytes) -> dict[str, object]:
    value: dict[str, object] | None = None
    try:
        if raw.startswith(b"\xef\xbb\xbf"):
            raise ValueError
        depth = 0
        for token in re.findall(
            r'"(?:[^"\\]|\\[\s\S])*"|[{}\[\]]', raw.decode("utf-8", errors="strict")
        ):
            if token in {"[", "{"}:
                depth += 1
                if depth > 12:
                    raise ValueError
            elif token in {"]", "}"}:
                depth -= 1
        value = _OBJECT.validate_python(
            json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant),
            strict=True,
        )
    except (ValueError, TypeError, RecursionError):
        pass
    if value is None:
        raise ModelBudgetError("MODEL_BUDGET_RESPONSE_INVALID")
    return value


class BffModelBudgetClient:
    """One explicit owner origin/pool; a fresh workload credential per tenant call."""

    def __init__(
        self,
        base_url: str,
        credentials: Callable[[str], SecretStr],
        *,
        timeout_s: float = 5,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        valid = False
        try:
            url = httpx.URL(base_url)
            valid = (
                bool(url.host)
                and url.scheme in {"http", "https"}
                and not url.userinfo
                and not url.query
                and not url.fragment
                and url.path == "/"
                and callable(credentials)
                and type(timeout_s) in {int, float}
                and math.isfinite(timeout_s)
                and 0 < timeout_s <= 10
            )
        except (ValueError, TypeError, httpx.InvalidURL):
            pass
        if not valid:
            raise ValueError("invalid BFF budget configuration")
        self._credentials = credentials
        self._timeout_s = timeout_s
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_s),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        )

    async def __aenter__(self) -> Self:
        await self._http.__aenter__()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._http.aclose()

    async def reserve(
        self,
        *,
        tenant_id: str,
        command: ModelBudgetCommand,
        proof: SecretStr,
        key: str,
        request_id: str,
    ) -> ModelBudgetGrant:
        failure: ModelBudgetError | None = None
        try:
            async with asyncio.timeout(self._timeout_s):
                return await self._reserve(
                    tenant_id=tenant_id,
                    command=command,
                    proof=proof,
                    key=key,
                    request_id=request_id,
                )
        except ModelBudgetError:
            raise
        except Exception:
            # Leave the except block before constructing the safe error: no credential-bearing cause.
            failure = ModelBudgetError("MODEL_BUDGET_UNAVAILABLE")
        raise failure

    async def _reserve(
        self,
        *,
        tenant_id: str,
        command: ModelBudgetCommand,
        proof: SecretStr,
        key: str,
        request_id: str,
    ) -> ModelBudgetGrant:
        if (
            not re.fullmatch(r"[^\s\x00-\x1f\x7f]{1,191}", tenant_id)
            or not _ID.fullmatch(request_id)
            or not re.fullmatch(r"[!-~]{8,128}", key)
            or not re.fullmatch(r"[!-~]{1,16384}", proof.get_secret_value())
        ):
            raise ModelBudgetError("MODEL_BUDGET_REQUEST_INVALID")
        body: bytes | None = None
        try:
            parsed = AgentModelAdmissionRequest.model_validate(asdict(command))
            body = json.dumps(
                parsed.model_dump(mode="json"),
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8", errors="strict")
        except (ValueError, TypeError):
            pass
        if body is None or len(body) > 16384:
            raise ModelBudgetError("MODEL_BUDGET_REQUEST_INVALID")
        return await self._post(
            tenant_id=tenant_id,
            path="/internal/v1/model-admissions",
            body=body,
            key=key,
            request_id=request_id,
            proof=proof,
            decode=lambda status, raw: self._grant(status, raw, command),
        )

    async def _post(
        self,
        *,
        tenant_id: str,
        path: str,
        body: bytes,
        key: str,
        request_id: str,
        decode: Callable[[int, dict[str, object]], _T],
        proof: SecretStr | None = None,
    ) -> _T:
        failed = False
        try:
            async with asyncio.timeout(self._timeout_s):
                if (
                    not re.fullmatch(r"[^\s\x00-\x1f\x7f]{1,191}", tenant_id)
                    or not _ID.fullmatch(request_id)
                    or not re.fullmatch(r"[!-~]{8,128}", key)
                    or len(body) > 16384
                ):
                    raise ModelBudgetError("MODEL_BUDGET_REQUEST_INVALID")
                credential = self._credentials(tenant_id).get_secret_value()
                if not re.fullmatch(r"[!-~]{32,4096}", credential):
                    raise ModelBudgetError("MODEL_BUDGET_UNAVAILABLE")
                headers = {
                    "authorization": "Bearer " + credential,
                    "idempotency-key": key,
                    "x-request-id": request_id,
                    "content-type": "application/json",
                    "accept-encoding": "identity",
                }
                if proof is not None:
                    headers["x-billing-execution-proof"] = proof.get_secret_value()
                request = self._http.build_request(
                    "POST", path, content=body, headers=headers
                )
                request.headers.pop("cookie", None)
                response = await self._http.send(request, stream=True)
                decoded = False
                try:
                    raw = await self._read(response, request_id)
                    result = decode(response.status_code, _decode(raw))
                    decoded = True
                    return result
                finally:
                    closed = await self._close_response(response)
                    if decoded and not closed:
                        raise ModelBudgetError("MODEL_BUDGET_UNAVAILABLE")
        except ModelBudgetError:
            raise
        except Exception:
            failed = True
        if failed:
            raise ModelBudgetError("MODEL_BUDGET_UNAVAILABLE")
        raise AssertionError("unreachable financial transport state")

    @staticmethod
    def _encode(model: type[BaseModel], values: dict[str, object]) -> bytes:
        body: bytes | None = None
        try:
            body = json.dumps(
                model.model_validate(values).model_dump(mode="json"),
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8", errors="strict")
        except (ValueError, TypeError):
            pass
        if body is None or len(body) > 16384:
            raise ModelBudgetError("MODEL_BUDGET_REQUEST_INVALID")
        return body

    async def recover(
        self,
        *,
        tenant_id: str,
        command: ModelBudgetCommand,
        key: str,
        request_id: str,
    ) -> ModelBudgetGrant | None:
        """Read only the original command ACK; a miss never triggers reserve."""
        body = self._encode(AgentModelAdmissionRequest, asdict(command))
        return await self._post(
            tenant_id=tenant_id,
            path="/internal/v1/model-admissions/recover",
            body=body,
            key=key,
            request_id=request_id,
            decode=lambda status, raw: self._recovered(status, raw, command),
        )

    @staticmethod
    def _recovered(
        status: int, raw: dict[str, object], command: ModelBudgetCommand
    ) -> ModelBudgetGrant | None:
        missing = False
        try:
            if status == 404:
                error = AgentModelAdmissionError.model_validate(raw).error
                missing = (
                    error.code == "model_admission_not_found"
                    and error.retryable is False
                )
        except (ValueError, TypeError):
            pass
        if missing:
            return None
        return BffModelBudgetClient._grant(status, raw, command, success_status=200)

    async def settle(
        self,
        *,
        tenant_id: str,
        admission_ref: str,
        command: ModelSettlementCommand,
        key: str,
        request_id: str,
    ) -> ModelSettlementReceipt:
        """Notify one original sealed event; no proof, guessed amount, or provider retry."""
        invalid = False
        try:
            TypeAdapter(AgentModelAdmissionId).validate_python(
                admission_ref, strict=True
            )
            TypeAdapter(AgentModelAttemptId).validate_python(
                command.attempt_id, strict=True
            )
        except (ValueError, TypeError):
            invalid = True
        if invalid:
            raise ModelBudgetError("MODEL_BUDGET_REQUEST_INVALID")
        body = self._encode(
            AgentModelSettlementRequest,
            dict(
                run_id=command.run_id,
                evidence_event_id=command.event_id,
                evidence_revision=command.revision,
                evidence_digest=command.digest,
            ),
        )
        return await self._post(
            tenant_id=tenant_id,
            path=f"/internal/v1/model-admissions/{admission_ref}/settle",
            body=body,
            key=key,
            request_id=request_id,
            decode=lambda status, raw: self._settled(
                status, raw, admission_ref, command
            ),
        )

    @staticmethod
    def _settled(
        status: int,
        raw: dict[str, object],
        admission_ref: str,
        command: ModelSettlementCommand,
    ) -> ModelSettlementReceipt:
        result: AgentModelSettlementResponse | None = None
        error: AgentModelSettlementError | None = None
        try:
            if status == 200:
                result = AgentModelSettlementResponse.model_validate(raw)
            else:
                error = AgentModelSettlementError.model_validate(raw)
        except (ValueError, TypeError):
            pass
        if result is not None:
            data = result.data
            if (
                data.admission_id,
                data.run_id,
                data.attempt_id,
                data.evidence_event_id,
                data.evidence_revision,
                data.evidence_digest,
            ) == (
                admission_ref,
                command.run_id,
                command.attempt_id,
                command.event_id,
                command.revision,
                command.digest,
            ):
                return ModelSettlementReceipt(
                    admission_ref=data.admission_id,
                    run_id=data.run_id,
                    attempt_id=data.attempt_id,
                    event_id=data.evidence_event_id,
                    revision=data.evidence_revision,
                    digest=data.evidence_digest,
                    status=data.status,
                    reason=data.reason,
                    captured_credits=data.captured_credits,
                    released_credits=data.released_credits,
                    usage_event_ref=data.usage_event_id,
                    settlement_ref=data.settlement_id,
                    updated_at=data.updated_at,
                )
        if (
            error is not None
            and _ERROR_STATUS.get(
                error.error.code.replace("model_settlement_", "model_admission_")
            )
            == status
            and error.error.retryable is (status == 503)
        ):
            raise ModelBudgetError(
                "MODEL_SETTLEMENT_"
                + error.error.code.removeprefix("model_settlement_").upper()
            )
        raise ModelBudgetError("MODEL_BUDGET_RESPONSE_INVALID")

    @staticmethod
    async def _close_response(response: httpx.Response) -> bool:
        # Cleanup has its own finite allowance after the request deadline/caller cancellation.
        # A failed close is not evidence that the owner did not reserve funds.
        try:
            async with asyncio.timeout(0.1):
                await response.aclose()
        except Exception:
            return False
        return True

    async def _read(self, response: httpx.Response, request_id: str) -> bytes:
        def single(name: str) -> str | None:
            values = response.headers.get_list(name)
            return values[0] if len(values) == 1 else None

        content_type = single("content-type")
        if (
            content_type is None
            or content_type.split(";", 1)[0].strip().lower() != "application/json"
            or single("x-request-id") != request_id
            or "no-store"
            not in [
                p.strip().lower() for p in (single("cache-control") or "").split(",")
            ]
            or response.headers.get("content-encoding", "identity").lower()
            != "identity"
            or "set-cookie" in response.headers
        ):
            raise ModelBudgetError("MODEL_BUDGET_RESPONSE_INVALID")
        size = sum(len(k) + len(v) + 4 for k, v in response.headers.raw)
        length = single("content-length")
        if size > _MAX_RESPONSE or (
            "content-length" in response.headers
            and (
                length is None
                or not re.fullmatch(r"0|[1-9][0-9]{0,7}", length)
                or int(length) > _MAX_RESPONSE - size
            )
        ):
            raise ModelBudgetError("MODEL_BUDGET_RESPONSE_INVALID")
        chunks = bytearray()
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > _MAX_RESPONSE:
                raise ModelBudgetError("MODEL_BUDGET_RESPONSE_INVALID")
            chunks.extend(chunk)
        return bytes(chunks)

    @staticmethod
    def _grant(
        status: int,
        raw: dict[str, object],
        command: ModelBudgetCommand,
        *,
        success_status: int = 201,
    ) -> ModelBudgetGrant:
        result: AgentModelAdmissionResponse | None = None
        error: AgentModelAdmissionError | None = None
        try:
            if status == success_status:
                result = AgentModelAdmissionResponse.model_validate(raw)
            else:
                error = AgentModelAdmissionError.model_validate(raw)
        except (ValueError, TypeError):
            pass
        if result is not None:
            data = result.data
            if (
                all(
                    getattr(data, key) == value
                    for key, value in asdict(command).items()
                )
                and data.status == "held"
                and data.created_at == data.updated_at
            ):
                return ModelBudgetGrant(admission_ref=data.admission_id)
        if (
            error is not None
            and _ERROR_STATUS.get(error.error.code) == status
            and error.error.retryable is (status == 503)
        ):
            raise ModelBudgetError(
                "MODEL_BUDGET_"
                + error.error.code.removeprefix("model_admission_").upper()
            )
        raise ModelBudgetError("MODEL_BUDGET_RESPONSE_INVALID")
