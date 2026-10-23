"""Generated from the fixed BFF internal1.1 publication. Do not edit."""

from __future__ import annotations

from collections.abc import Callable
import re
from typing import Annotated, Literal, TypeAlias

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

SOURCE_SHA256 = "695901b23f1f0322fd18ec2a11be27c5b6efcce07b04f57839498eab402d17c6"


def _patterns(*patterns: str) -> Callable[[str], str]:
    def validate(value: str) -> str:
        if not all(re.fullmatch(pattern, value, re.ASCII) for pattern in patterns):
            raise ValueError("invalid BFF budget field")
        return value

    return validate


Field0: TypeAlias = Annotated[
    str,
    Field(
        min_length=1,
        max_length=512,
    ),
]


Field1: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field2: TypeAlias = Annotated[
    str,
    Field(
        min_length=1,
        max_length=512,
    ),
]


Field3: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field4: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field5: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field6: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field7: TypeAlias = Annotated[
    str,
    Field(
        min_length=1,
        max_length=512,
    ),
]


Field8: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field9: TypeAlias = Annotated[
    str,
    Field(
        min_length=1,
        max_length=512,
    ),
]


Field10: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field11: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field12: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field13: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field14: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^(?:[1-9][0-9]{0,11}|[1-8][0-9]{12}|9[0-1][0-9]{11}|92[0-1][0-9]{10}|922[0-2][0-9]{9}|9223[0-2][0-9]{8}|92233[0-6][0-9]{7}|922337[0-1][0-9]{6}|92233720[0-2][0-9]{4}|922337203[0-5][0-9]{3}|9223372036[0-7][0-9]{2}|92233720368[0-4][0-9]|922337203685[0-3]|9223372036854)$(?![\\s\\S])",
        ),
    ),
]


Field15: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field16: TypeAlias = Literal[
    "held",
    "captured",
    "released",
    "pending_reconciliation",
]


Field17: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))T(?:(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d+)?(?:Z))$",
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$(?![\\s\\S])",
        ),
    ),
]


Field18: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))T(?:(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d+)?(?:Z))$",
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$(?![\\s\\S])",
        ),
    ),
]


Field19: TypeAlias = Literal[
    "model_admission_invalid_request",
    "model_admission_service_denied",
    "model_admission_not_found",
    "model_admission_forbidden",
    "model_admission_conflict",
    "model_admission_insufficient_credit",
    "model_admission_price_unavailable",
    "model_admission_ineligible",
    "model_admission_upstream_invalid",
    "model_admission_unavailable",
]


class AgentModelAdmissionRequest(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    run_id: Field0
    attempt_id: Field1
    request_ref: Field2
    attempt_binding_digest: Field3
    planned_digest: Field4
    final_request_digest: Field5


class AgentModelAdmissionResponseData(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    admission_id: Field6
    run_id: Field7
    attempt_id: Field8
    request_ref: Field9
    attempt_binding_digest: Field10
    planned_digest: Field11
    final_request_digest: Field12
    hold_id: Field13
    budget_credits: Field14
    pricing_snapshot_digest: Field15
    status: Field16
    created_at: Field17
    updated_at: Field18


class AgentModelAdmissionResponse(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    data: AgentModelAdmissionResponseData


class AgentModelAdmissionErrorError(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    code: Field19
    message: str
    retryable: bool


class AgentModelAdmissionError(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    error: AgentModelAdmissionErrorError
