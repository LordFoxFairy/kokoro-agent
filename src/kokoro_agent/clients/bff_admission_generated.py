"""Generated from the fixed BFF internal1.3 publication. Do not edit."""

from __future__ import annotations

from collections.abc import Callable
import re
from typing import Annotated, Literal, TypeAlias

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

SOURCE_SHA256 = "4de1a6c7564f3a994d879cfc93588021c5ef4aeebbcc89959fa574d5b6f16d39"


def _patterns(*patterns: str, forbidden: tuple[str, ...] = ()) -> Callable[[str], str]:
    def validate(value: str) -> str:
        if not all(
            re.fullmatch(pattern, value, re.ASCII) for pattern in patterns
        ) or any(re.search(pattern, value, re.ASCII) for pattern in forbidden):
            raise ValueError("invalid BFF budget field")
        return value

    return validate


Field0: TypeAlias = Annotated[
    str,
    Field(min_length=1, max_length=512),
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
    Field(min_length=1, max_length=512),
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
    Field(min_length=1, max_length=512),
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
    Field(min_length=1, max_length=512),
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


Field20: TypeAlias = Annotated[
    str,
    Field(min_length=1, max_length=512),
]


Field21: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field22: TypeAlias = Annotated[
    str,
    Field(max_length=19),
    AfterValidator(
        _patterns(
            "^[1-9][0-9]*$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{17}|92[3-9][0-9]{16}|922[4-9][0-9]{15}|9223[4-9][0-9]{14}|92233[8-9][0-9]{13}|922337[3-9][0-9]{12}|9223372[1-9][0-9]{11}|92233720[4-9][0-9]{10}|922337203[7-9][0-9]{9}|92233720369[0-9]{8}|92233720368[6-9][0-9]{7}|922337203685[5-9][0-9]{6}|9223372036854[8-9][0-9]{5}|92233720368547[8-9][0-9]{4}|922337203685477[6-9][0-9]{3}|92233720368547759[0-9]{2}|92233720368547758[1-9][0-9]{1}|922337203685477580[8-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field23: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field24: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field25: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field26: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field27: TypeAlias = Annotated[
    str,
    Field(min_length=1, max_length=512),
]


Field28: TypeAlias = Annotated[
    str,
    Field(max_length=19),
    AfterValidator(
        _patterns(
            "^[1-9][0-9]*$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{17}|92[3-9][0-9]{16}|922[4-9][0-9]{15}|9223[4-9][0-9]{14}|92233[8-9][0-9]{13}|922337[3-9][0-9]{12}|9223372[1-9][0-9]{11}|92233720[4-9][0-9]{10}|922337203[7-9][0-9]{9}|92233720369[0-9]{8}|92233720368[6-9][0-9]{7}|922337203685[5-9][0-9]{6}|9223372036854[8-9][0-9]{5}|92233720368547[8-9][0-9]{4}|922337203685477[6-9][0-9]{3}|92233720368547759[0-9]{2}|92233720368547758[1-9][0-9]{1}|922337203685477580[8-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field29: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field30: TypeAlias = Literal["captured"]


Field31: TypeAlias = Annotated[
    str,
    Field(max_length=13),
    AfterValidator(
        _patterns(
            "^(?:0|[1-9][0-9]*)$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{11}|92[3-9][0-9]{10}|922[4-9][0-9]{9}|9223[4-9][0-9]{8}|92233[8-9][0-9]{7}|922337[3-9][0-9]{6}|9223372[1-9][0-9]{5}|92233720[4-9][0-9]{4}|922337203[7-9][0-9]{3}|92233720369[0-9]{2}|92233720368[6-9][0-9]{1}|922337203685[5-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field32: TypeAlias = Annotated[
    str,
    Field(max_length=13),
    AfterValidator(
        _patterns(
            "^(?:0|[1-9][0-9]*)$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{11}|92[3-9][0-9]{10}|922[4-9][0-9]{9}|9223[4-9][0-9]{8}|92233[8-9][0-9]{7}|922337[3-9][0-9]{6}|9223372[1-9][0-9]{5}|92233720[4-9][0-9]{4}|922337203[7-9][0-9]{3}|92233720369[0-9]{2}|92233720368[6-9][0-9]{1}|922337203685[5-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field33: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field34: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field35: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))T(?:(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d+)?(?:Z))$",
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$(?![\\s\\S])",
        ),
    ),
]


Field36: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field37: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field38: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field39: TypeAlias = Annotated[
    str,
    Field(min_length=1, max_length=512),
]


Field40: TypeAlias = Annotated[
    str,
    Field(max_length=19),
    AfterValidator(
        _patterns(
            "^[1-9][0-9]*$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{17}|92[3-9][0-9]{16}|922[4-9][0-9]{15}|9223[4-9][0-9]{14}|92233[8-9][0-9]{13}|922337[3-9][0-9]{12}|9223372[1-9][0-9]{11}|92233720[4-9][0-9]{10}|922337203[7-9][0-9]{9}|92233720369[0-9]{8}|92233720368[6-9][0-9]{7}|922337203685[5-9][0-9]{6}|9223372036854[8-9][0-9]{5}|92233720368547[8-9][0-9]{4}|922337203685477[6-9][0-9]{3}|92233720368547759[0-9]{2}|92233720368547758[1-9][0-9]{1}|922337203685477580[8-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field41: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field42: TypeAlias = Literal["released"]


Field43: TypeAlias = Literal["0"]


Field44: TypeAlias = Annotated[
    Field43,
    Field(max_length=13),
    AfterValidator(
        _patterns(
            "^(?:0|[1-9][0-9]*)$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{11}|92[3-9][0-9]{10}|922[4-9][0-9]{9}|9223[4-9][0-9]{8}|92233[8-9][0-9]{7}|922337[3-9][0-9]{6}|9223372[1-9][0-9]{5}|92233720[4-9][0-9]{4}|922337203[7-9][0-9]{3}|92233720369[0-9]{2}|92233720368[6-9][0-9]{1}|922337203685[5-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field45: TypeAlias = Annotated[
    str,
    Field(max_length=13),
    AfterValidator(
        _patterns(
            "^(?:0|[1-9][0-9]*)$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{11}|92[3-9][0-9]{10}|922[4-9][0-9]{9}|9223[4-9][0-9]{8}|92233[8-9][0-9]{7}|922337[3-9][0-9]{6}|9223372[1-9][0-9]{5}|92233720[4-9][0-9]{4}|922337203[7-9][0-9]{3}|92233720369[0-9]{2}|92233720368[6-9][0-9]{1}|922337203685[5-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field46: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))T(?:(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d+)?(?:Z))$",
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$(?![\\s\\S])",
        ),
    ),
]


Field47: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field48: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field49: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


Field50: TypeAlias = Annotated[
    str,
    Field(min_length=1, max_length=512),
]


Field51: TypeAlias = Annotated[
    str,
    Field(max_length=19),
    AfterValidator(
        _patterns(
            "^[1-9][0-9]*$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{17}|92[3-9][0-9]{16}|922[4-9][0-9]{15}|9223[4-9][0-9]{14}|92233[8-9][0-9]{13}|922337[3-9][0-9]{12}|9223372[1-9][0-9]{11}|92233720[4-9][0-9]{10}|922337203[7-9][0-9]{9}|92233720369[0-9]{8}|92233720368[6-9][0-9]{7}|922337203685[5-9][0-9]{6}|9223372036854[8-9][0-9]{5}|92233720368547[8-9][0-9]{4}|922337203685477[6-9][0-9]{3}|92233720368547759[0-9]{2}|92233720368547758[1-9][0-9]{1}|922337203685477580[8-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field52: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^[0-9a-f]{64}$(?![\\s\\S])",
        ),
    ),
]


Field53: TypeAlias = Literal["pending_reconciliation"]


Field54: TypeAlias = Literal[
    "unknown_usage_or_outcome",
    "budget_exceeded",
]


Field55: TypeAlias = Literal["0"]


Field56: TypeAlias = Annotated[
    Field55,
    Field(max_length=13),
    AfterValidator(
        _patterns(
            "^(?:0|[1-9][0-9]*)$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{11}|92[3-9][0-9]{10}|922[4-9][0-9]{9}|9223[4-9][0-9]{8}|92233[8-9][0-9]{7}|922337[3-9][0-9]{6}|9223372[1-9][0-9]{5}|92233720[4-9][0-9]{4}|922337203[7-9][0-9]{3}|92233720369[0-9]{2}|92233720368[6-9][0-9]{1}|922337203685[5-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field57: TypeAlias = Literal["0"]


Field58: TypeAlias = Annotated[
    Field57,
    Field(max_length=13),
    AfterValidator(
        _patterns(
            "^(?:0|[1-9][0-9]*)$(?![\\s\\S])",
            forbidden=(
                "^(?:9[3-9][0-9]{11}|92[3-9][0-9]{10}|922[4-9][0-9]{9}|9223[4-9][0-9]{8}|92233[8-9][0-9]{7}|922337[3-9][0-9]{6}|9223372[1-9][0-9]{5}|92233720[4-9][0-9]{4}|922337203[7-9][0-9]{3}|92233720369[0-9]{2}|92233720368[6-9][0-9]{1}|922337203685[5-9])$(?![\\s\\S])",
            ),
        ),
    ),
]


Field59: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))T(?:(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d+)?(?:Z))$",
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$(?![\\s\\S])",
        ),
    ),
]


Field60: TypeAlias = Literal[
    "model_settlement_invalid_request",
    "model_settlement_service_denied",
    "model_settlement_not_found",
    "model_settlement_forbidden",
    "model_settlement_conflict",
    "model_settlement_insufficient_credit",
    "model_settlement_price_unavailable",
    "model_settlement_ineligible",
    "model_settlement_upstream_invalid",
    "model_settlement_unavailable",
]


Field61: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


AgentModelAdmissionId: TypeAlias = Field61


Field62: TypeAlias = Annotated[
    str,
    AfterValidator(
        _patterns(
            "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\\s\\S])",
        ),
    ),
]


AgentModelAttemptId: TypeAlias = Field62


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


class AgentModelSettlementRequest(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    run_id: Field20
    evidence_event_id: Field21
    evidence_revision: Field22
    evidence_digest: Field23


class AgentModelSettlementResponseDataVariant0(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    admission_id: Field24
    attempt_id: Field25
    evidence_event_id: Field26
    run_id: Field27
    evidence_revision: Field28
    evidence_digest: Field29
    status: Field30
    reason: None
    captured_credits: Field31
    released_credits: Field32
    usage_event_id: Field33
    settlement_id: Field34
    updated_at: Field35


class AgentModelSettlementResponseDataVariant1(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    admission_id: Field36
    attempt_id: Field37
    evidence_event_id: Field38
    run_id: Field39
    evidence_revision: Field40
    evidence_digest: Field41
    status: Field42
    reason: None
    captured_credits: Field44
    released_credits: Field45
    usage_event_id: None
    settlement_id: None
    updated_at: Field46


class AgentModelSettlementResponseDataVariant2(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    admission_id: Field47
    attempt_id: Field48
    evidence_event_id: Field49
    run_id: Field50
    evidence_revision: Field51
    evidence_digest: Field52
    status: Field53
    reason: Field54
    captured_credits: Field56
    released_credits: Field58
    usage_event_id: None
    settlement_id: None
    updated_at: Field59


class AgentModelSettlementResponse(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    data: (
        AgentModelSettlementResponseDataVariant0
        | AgentModelSettlementResponseDataVariant1
        | AgentModelSettlementResponseDataVariant2
    )


class AgentModelSettlementErrorError(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    code: Field60
    message: str
    retryable: bool


class AgentModelSettlementError(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    error: AgentModelSettlementErrorError
