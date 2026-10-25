"""Canonical result of one native summary, bound to its original model attempt."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Annotated, Literal, Mapping

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints

MAX_SUMMARY_RESULT_BYTES = 4_194_304
Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Count = Annotated[str, StringConstraints(pattern=r"^(0|[1-9][0-9]{0,18})$")]


class _Value(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )


class SummaryCallBinding(_Value):
    tenant_ref: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    call_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    peer_path: tuple[str, ...] = Field(min_length=1, max_length=32)
    checkpoint_id: str = Field(min_length=1)
    checkpoint_ns: str
    task_id: str = Field(min_length=1)
    call_kind: Literal["summarization"] = "summarization"
    recipe_digest: Digest
    planned_digest: Digest
    prompt_digest: Digest


class SummaryMessage(_Value):
    role: Literal["assistant"] = "assistant"
    content: str = Field(repr=False)


class SummaryUsage(_Value):
    input_tokens: Count | None
    output_tokens: Count | None
    total_tokens: Count | None

    @property
    def known_total(self) -> int | None:
        present = tuple(
            int(value)
            for value in (self.input_tokens, self.output_tokens, self.total_tokens)
            if value is not None
        )
        if any(value > (1 << 63) - 1 for value in present):
            raise ValueError("summary result usage conflicts")
        if (
            self.input_tokens is None
            or self.output_tokens is None
            or self.total_tokens is None
        ):
            return None
        values = tuple(
            int(value)
            for value in (self.input_tokens, self.output_tokens, self.total_tokens)
        )
        if (
            any(value > (1 << 63) - 1 for value in values)
            or values[0] + values[1] != values[2]
        ):
            raise ValueError("summary result usage conflicts")
        return values[2]

    @property
    def reported_total(self) -> int | None:
        self.known_total
        return None if self.total_tokens is None else int(self.total_tokens)


class SummaryResult(_Value):
    version: Literal[1] = 1
    domain: Literal["kokoro-agent:native-summary-result:1"] = (
        "kokoro-agent:native-summary-result:1"
    )
    binding: SummaryCallBinding
    attempt_binding_digest: Digest
    final_request_digest: Digest
    event_id: str = Field(min_length=1)
    event_digest: Digest
    usage: SummaryUsage
    message: SummaryMessage = Field(repr=False)


class SummarySealInput(_Value):
    binding: SummaryCallBinding
    text: str | None = Field(repr=False)


@dataclass(frozen=True, slots=True, kw_only=True)
class SummaryReplayReceipt:
    result: SummaryResult
    token_total: int
    state: Literal["captured", "pending"]


def encode_summary_result(value: SummaryResult) -> tuple[bytes, str]:
    try:
        # Bound before canonical encoding as well as after JSON escaping/metadata.
        if len(value.message.content.encode("utf-8")) > MAX_SUMMARY_RESULT_BYTES:
            raise ValueError
        value.usage.known_total
        raw = rfc8785.dumps(value.model_dump(mode="json"))
        if not 1 <= len(raw) <= MAX_SUMMARY_RESULT_BYTES:
            raise ValueError
        return raw, hashlib.sha256(raw).hexdigest()
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ValueError("summary result is invalid or exceeds its bound") from None


def decode_summary_result(raw: bytes, digest: str) -> SummaryResult:
    try:
        if type(raw) is not bytes or not 1 <= len(raw) <= MAX_SUMMARY_RESULT_BYTES:
            raise ValueError
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError
        value = SummaryResult.model_validate_json(raw, strict=True)
        if encode_summary_result(value) != (raw, digest):
            raise ValueError
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ValueError("summary result violates canonical bindings") from None


def summary_prompt_digest(
    prompt: str,
    stop: list[str] | None,
    parameters: Mapping[str, JsonValue],
    profile: str,
) -> str:
    """Only the digest survives; raw prompt/parameters never enter the capsule."""
    try:
        raw = rfc8785.dumps(
            {
                "version": 1,
                "domain": "kokoro-agent:native-summary-input:1",
                "profile": profile,
                "prompt": prompt,
                "stop": stop,
                "parameters": dict(parameters),
            }
        )
        return hashlib.sha256(raw).hexdigest()
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ValueError("summary result input binding is invalid") from None
