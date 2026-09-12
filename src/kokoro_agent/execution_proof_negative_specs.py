"""Immutable named-negative policy for execution-proof conformance vectors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, TypeAlias


Stage: TypeAlias = Literal[
    "json_parse",
    "integer_token",
    "schema",
    "canonicalization",
    "base64url",
    "pair",
]
Component: TypeAlias = Literal["decoded_profile", "protected_header", "claims", "jti"]
ErrorKind: TypeAlias = Literal[
    "duplicate_member",
    "non_integer_token",
    "schema_validation",
    "noncanonical_json",
    "padded_base64url",
    "noncanonical_base64url",
    "claim_pair",
]
JsonScalar: TypeAlias = None | bool | int | float | str


@dataclass(frozen=True, slots=True)
class _JsonObject:
    members: tuple[tuple[str, FrozenJson], ...] = ()


@dataclass(frozen=True, slots=True)
class _JsonArray:
    items: tuple[FrozenJson, ...] = ()


FrozenJson: TypeAlias = JsonScalar | _JsonObject | _JsonArray


@dataclass(frozen=True, slots=True)
class ValueDifference:
    pointer: str
    value: FrozenJson


@dataclass(frozen=True, slots=True)
class TokenDifference:
    pointer: str
    from_token: str
    to_token: str


@dataclass(frozen=True, slots=True)
class PairChange:
    pointer: str
    value: FrozenJson


@dataclass(frozen=True, slots=True)
class PairDifference:
    changes: tuple[PairChange, ...]


@dataclass(frozen=True, slots=True)
class PaddingDifference:
    base: str
    suffix: str


@dataclass(frozen=True, slots=True)
class CharacterDifference:
    character_index: int
    from_character: str
    to_character: str


Difference: TypeAlias = (
    ValueDifference
    | TokenDifference
    | PairDifference
    | PaddingDifference
    | CharacterDifference
)


@dataclass(frozen=True, slots=True, kw_only=True)
class NegativeSpec:
    name: str
    stage: Stage
    component: Component
    error_kind: ErrorKind
    expected_member: str | None = None
    expected_pointer: str | None = None
    expected_keyword: str | None = None
    expected_decoded: str | None = None
    canonical_segment: str | None = None
    difference: Difference | None = None

    def metadata(self) -> dict[str, object]:
        result: dict[str, object] = {
            "stage": self.stage,
            "component": self.component,
            "error_kind": self.error_kind,
        }
        optional = (
            ("expected_member", self.expected_member),
            ("expected_pointer", self.expected_pointer),
            ("expected_keyword", self.expected_keyword),
            ("expected_decoded", self.expected_decoded),
            ("canonical_segment", self.canonical_segment),
        )
        result.update((name, value) for name, value in optional if value is not None)
        if self.difference is not None:
            result["difference"] = _difference_json(self.difference)
        return result


def _frozen_json(value: FrozenJson) -> object:
    if isinstance(value, _JsonObject):
        return {name: _frozen_json(item) for name, item in value.members}
    if isinstance(value, _JsonArray):
        return [_frozen_json(item) for item in value.items]
    return value


def _difference_json(difference: Difference) -> dict[str, object]:
    if isinstance(difference, ValueDifference):
        return {
            "pointer": difference.pointer,
            "value": _frozen_json(difference.value),
        }
    if isinstance(difference, TokenDifference):
        return {
            "pointer": difference.pointer,
            "from_token": difference.from_token,
            "to_token": difference.to_token,
        }
    if isinstance(difference, PairDifference):
        return {
            "changes": [
                {"pointer": change.pointer, "value": _frozen_json(change.value)}
                for change in difference.changes
            ]
        }
    if isinstance(difference, PaddingDifference):
        return {"base": difference.base, "suffix": difference.suffix}
    return {
        "character_index": difference.character_index,
        "from": difference.from_character,
        "to": difference.to_character,
    }


def _duplicate(name: str, component: Component, expected_member: str) -> NegativeSpec:
    return NegativeSpec(
        name=name,
        stage="json_parse",
        component=component,
        error_kind="duplicate_member",
        expected_member=expected_member,
    )


def _integer_token(
    name: str, member: str, from_token: str, to_token: str
) -> NegativeSpec:
    return NegativeSpec(
        name=name,
        stage="integer_token",
        component="claims",
        error_kind="non_integer_token",
        expected_member=member,
        difference=TokenDifference(
            pointer=f"/claims/{member}",
            from_token=from_token,
            to_token=to_token,
        ),
    )


def _schema(
    name: str,
    *,
    component: Component,
    expected_pointer: str,
    expected_keyword: str,
    pointer: str,
    value: FrozenJson,
) -> NegativeSpec:
    return NegativeSpec(
        name=name,
        stage="schema",
        component=component,
        error_kind="schema_validation",
        expected_pointer=expected_pointer,
        expected_keyword=expected_keyword,
        difference=ValueDifference(pointer=pointer, value=value),
    )


def _claim_schema(
    name: str, member: str, keyword: str, value: FrozenJson
) -> NegativeSpec:
    return _schema(
        name,
        component="claims",
        expected_pointer=f"/claims/{member}",
        expected_keyword=keyword,
        pointer=f"/claims/{member}",
        value=value,
    )


def _forbidden_header(name: str, member: str, value: FrozenJson) -> NegativeSpec:
    return _schema(
        name,
        component="protected_header",
        expected_pointer="/protected_header",
        expected_keyword="additionalProperties",
        pointer=f"/protected_header/{member}",
        value=value,
    )


NEGATIVE_SPECS = (
    _duplicate("duplicate_root_member", "decoded_profile", "claims"),
    _duplicate("duplicate_header_member", "protected_header", "alg"),
    _duplicate("duplicate_actor_member", "claims", "opaque_ref"),
    _duplicate("duplicate_claim_member", "claims", "jti"),
    _integer_token(
        "lease_generation_float", "lease_generation", "9007199254740991", "1.0"
    ),
    _claim_schema("lease_generation_bool", "lease_generation", "type", True),
    _claim_schema("lease_generation_zero", "lease_generation", "minimum", 0),
    _claim_schema("lease_generation_negative", "lease_generation", "minimum", -1),
    _claim_schema(
        "lease_generation_2pow53", "lease_generation", "maximum", 9_007_199_254_740_992
    ),
    _claim_schema(
        "lease_generation_2pow53_plus_1",
        "lease_generation",
        "maximum",
        9_007_199_254_740_993,
    ),
    _integer_token("iat_float", "iat", "1789236000", "1789236000.0"),
    _claim_schema("iat_bool", "iat", "type", True),
    _claim_schema("iat_negative", "iat", "minimum", -1),
    _claim_schema("iat_2pow53", "iat", "maximum", 9_007_199_254_740_992),
    _claim_schema("iat_2pow53_plus_1", "iat", "maximum", 9_007_199_254_740_993),
    _integer_token("exp_float", "exp", "1789236060", "1789236060.0"),
    _claim_schema("exp_bool", "exp", "type", True),
    _claim_schema("exp_negative", "exp", "minimum", -1),
    _claim_schema("exp_2pow53", "exp", "maximum", 9_007_199_254_740_992),
    _claim_schema("exp_2pow53_plus_1", "exp", "maximum", 9_007_199_254_740_993),
    _schema(
        "unknown_claim",
        component="claims",
        expected_pointer="/claims",
        expected_keyword="additionalProperties",
        pointer="/claims/scope",
        value="skills.read",
    ),
    _forbidden_header("forbidden_header_jku", "jku", "https://keys.invalid/jwks"),
    _forbidden_header("forbidden_header_x5u", "x5u", "https://keys.invalid/cert"),
    _forbidden_header("forbidden_header_jwk", "jwk", _JsonObject()),
    _forbidden_header("forbidden_header_x5c", "x5c", _JsonArray()),
    _forbidden_header("forbidden_header_crit", "crit", _JsonArray(items=("jku",))),
    NegativeSpec(
        name="noncanonical_claims",
        stage="canonicalization",
        component="claims",
        error_kind="noncanonical_json",
        expected_decoded="positive_claims",
    ),
    NegativeSpec(
        name="base64url_padding",
        stage="base64url",
        component="claims",
        error_kind="padded_base64url",
        difference=PaddingDifference(base="positive_claims_segment", suffix="="),
    ),
    NegativeSpec(
        name="jti_trailing_pad_bits_alias",
        stage="base64url",
        component="jti",
        error_kind="noncanonical_base64url",
        canonical_segment="AAECAwQFBgcICQoLDA0ODw",
        difference=CharacterDifference(
            character_index=21,
            from_character="w",
            to_character="x",
        ),
    ),
    NegativeSpec(
        name="ttl_zero",
        stage="pair",
        component="claims",
        error_kind="claim_pair",
        difference=PairDifference(
            changes=(
                PairChange(pointer="/claims/iat", value=0),
                PairChange(pointer="/claims/exp", value=0),
            )
        ),
    ),
    NegativeSpec(
        name="ttl_over_60",
        stage="pair",
        component="claims",
        error_kind="claim_pair",
        difference=PairDifference(
            changes=(PairChange(pointer="/claims/exp", value=1_789_236_061),)
        ),
    ),
)


def build_negative_spec_index(
    specs: tuple[NegativeSpec, ...],
) -> Mapping[str, NegativeSpec]:
    index: dict[str, NegativeSpec] = {}
    for spec in specs:
        if spec.name in index:
            raise ValueError(f"duplicate negative specification: {spec.name}")
        index[spec.name] = spec
    return MappingProxyType(index)


NEGATIVE_SPEC_INDEX = build_negative_spec_index(NEGATIVE_SPECS)

__all__ = [
    "NEGATIVE_SPECS",
    "NEGATIVE_SPEC_INDEX",
    "NegativeSpec",
    "build_negative_spec_index",
]
