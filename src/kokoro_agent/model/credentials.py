"""Strict ephemeral System-owned credential; no file reader or local key registry."""

from __future__ import annotations

from typing import Annotated
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    field_validator,
)

_Reference = Annotated[
    str,
    StringConstraints(
        min_length=1, max_length=255, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$"
    ),
]


class ModelCredential(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    provider_id: Annotated[str, StringConstraints(min_length=1, max_length=255)] = (
        Field(repr=False)
    )
    secret_handle_ref: _Reference = Field(repr=False)
    api_key: SecretStr = Field(min_length=1, max_length=4096, repr=False)

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, value: SecretStr) -> SecretStr:
        if any(not 33 <= ord(c) <= 126 for c in value.get_secret_value()):
            raise ValueError("invalid model credential")
        return value
