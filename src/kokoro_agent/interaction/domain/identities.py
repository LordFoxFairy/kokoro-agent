"""Pure ADR-014 interaction identity derivation.

The domain intentionally accepts only the restricted Root canonical profile:
objects, arrays and strings; decimal integers are strings.  Session-owned
identity planes are available only through the corpus-shaped generic method,
never through Agent runtime helpers.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import TypeAlias


CanonicalInput: TypeAlias = (
    str | Sequence["CanonicalInput"] | Mapping[str, "CanonicalInput"]
)
CanonicalValue: TypeAlias = (
    str | tuple["CanonicalValue", ...] | Mapping[str, "CanonicalValue"]
)
CanonicalJsonValue: TypeAlias = (
    str | list["CanonicalJsonValue"] | dict[str, "CanonicalJsonValue"]
)

_POSITIVE_DECIMAL = re.compile(r"^[1-9][0-9]*$")
_INTERACTION_KINDS = frozenset(
    {"approval", "question", "structured_input", "result_review", "plan"}
)


class IdentityContractError(ValueError):
    """Identity material violates the frozen Root canonical contract."""


@dataclass(frozen=True, slots=True)
class DerivedIdentity:
    value: str
    canonical_json: str


@dataclass(frozen=True, slots=True)
class _IdentityPlane:
    domain: str
    prefix: str
    fields: frozenset[str]


_PLANES = MappingProxyType(
    {
        "application_request": _IdentityPlane(
            "kokoro.application-request.v2",
            "areq_",
            frozenset(
                {
                    "run_id",
                    "stable_task_path",
                    "origin_tool_call_ref",
                    "interaction_kind",
                    "elicitation_ordinal",
                }
            ),
        ),
        "interaction_owner": _IdentityPlane(
            "kokoro.interaction-owner.v2",
            "iown_",
            frozenset(
                {
                    "run_id",
                    "stable_task_path",
                    "origin_tool_call_ref",
                    "interaction_kind",
                    "application_request_ref",
                }
            ),
        ),
        "projection_event": _IdentityPlane(
            "agent-execution-evidence@v2",
            "ipev_",
            frozenset({"run_id", "interaction_owner_ref", "owner_revision"}),
        ),
        "group_projection": _IdentityPlane(
            "agent-execution-evidence@v2",
            "igpev_",
            frozenset({"run_id", "decision_group_ref", "decision_group_revision"}),
        ),
        "human_decision": _IdentityPlane(
            "kokoro.human-decision.v2",
            "dec_",
            frozenset(
                {
                    "site_id",
                    "session_id",
                    "run_id",
                    "interaction_owner_ref",
                    "owner_revision",
                    "projection_event_ref",
                    "command_id",
                    "actor_subject_ref",
                    "actor_subject_generation",
                    "request_digest",
                }
            ),
        ),
        "decision_receipt": _IdentityPlane(
            "kokoro.human-decision-receipt.v2",
            "drcpt_",
            frozenset(
                {
                    "site_id",
                    "session_id",
                    "run_id",
                    "interaction_owner_ref",
                    "owner_revision",
                    "projection_event_ref",
                    "decision_id",
                    "actor_subject_ref",
                    "actor_subject_generation",
                }
            ),
        ),
        "run_resume": _IdentityPlane(
            "kokoro.run-resume.v2",
            "rsm_",
            frozenset(
                {
                    "run_id",
                    "decision_group_ref",
                    "decision_group_revision",
                    "pending_frame_digest",
                    "members",
                }
            ),
        ),
        "resume_receipt": _IdentityPlane(
            "kokoro.run-resume-receipt.v2",
            "rrcpt_",
            frozenset({"run_id", "resume_ref"}),
        ),
        "resume_receipt_event": _IdentityPlane(
            "kokoro.run-resume-receipt-event.v2",
            "rrcev_",
            frozenset({"run_id", "resume_ref", "resume_receipt_revision"}),
        ),
    }
)

_DECIMAL_FIELDS = frozenset(
    {
        "elicitation_ordinal",
        "owner_revision",
        "decision_group_revision",
        "actor_subject_generation",
        "member_ordinal",
        "resume_receipt_revision",
    }
)
_RESUME_MEMBER_FIELDS = frozenset(
    {
        "decision_payload_digest",
        "decision_receipt_ref",
        "member_ordinal",
        "projection_event_ref",
    }
)


def _validate_string(value: str) -> None:
    if not value:
        raise IdentityContractError("identity strings must be non-empty")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise IdentityContractError(
            "identity strings must be valid Unicode scalars"
        ) from exc


def _freeze_value(
    value: CanonicalInput, *, field_name: str | None = None
) -> CanonicalValue:
    if isinstance(value, str):
        _validate_string(value)
        if field_name in _DECIMAL_FIELDS and _POSITIVE_DECIMAL.fullmatch(value) is None:
            raise IdentityContractError(
                f"{field_name} must be a positive decimal string"
            )
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, CanonicalValue] = {}
        for key, child in value.items():
            try:
                ascii_key = key.isascii()
            except AttributeError as exc:
                raise IdentityContractError(
                    "identity object keys must be ASCII strings"
                ) from exc
            if not ascii_key:
                raise IdentityContractError(
                    "identity object keys must be ASCII strings"
                )
            if key in frozen:
                raise IdentityContractError(f"duplicate identity key: {key}")
            frozen[key] = _freeze_value(child, field_name=key)
        return MappingProxyType(frozen)
    try:
        return tuple(_freeze_value(child) for child in value)
    except TypeError as exc:
        raise IdentityContractError(
            "identity values must be objects, arrays, or strings"
        ) from exc


def _json_value(value: CanonicalValue) -> CanonicalJsonValue:
    if isinstance(value, str):
        return value
    if isinstance(value, tuple):
        return [_json_value(child) for child in value]
    return {key: _json_value(value[key]) for key in sorted(value)}


def _canonical_json(material: Mapping[str, CanonicalInput]) -> str:
    frozen = _freeze_value(material)
    if isinstance(frozen, str) or isinstance(frozen, tuple):
        raise IdentityContractError("identity material must be an object")
    return json.dumps(
        _json_value(frozen),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=False,
    )


def _positive_decimal(value: int) -> str:
    if isinstance(value, bool) or value < 1:
        raise IdentityContractError(
            "identity revision/ordinal must be a positive integer"
        )
    return str(value)


class InteractionIdentityFactory:
    """Closed, deterministic identity factory for ADR-014 V2."""

    def derive_contract_ref(
        self, *, kind: str, material: Mapping[str, CanonicalInput]
    ) -> DerivedIdentity:
        try:
            plane = _PLANES[kind]
        except KeyError as exc:
            raise IdentityContractError(f"unknown identity plane: {kind}") from exc
        if frozenset(material) != plane.fields:
            raise IdentityContractError(
                f"{kind} material fields do not match Root contract"
            )
        if kind == "run_resume":
            members = material.get("members")
            if not isinstance(members, Sequence) or isinstance(members, (str, bytes)):
                raise IdentityContractError(
                    "run_resume members must be an ordered array"
                )
            for member in members:
                if (
                    not isinstance(member, Mapping)
                    or frozenset(member) != _RESUME_MEMBER_FIELDS
                ):
                    raise IdentityContractError(
                        "run_resume member fields do not match Root contract"
                    )
        canonical_json = _canonical_json(material)
        digest = hashlib.sha256(
            plane.domain.encode("ascii") + b"\0" + canonical_json.encode("utf-8")
        ).hexdigest()
        return DerivedIdentity(
            value=f"{plane.prefix}{digest}", canonical_json=canonical_json
        )

    def application_request(
        self,
        *,
        run_id: str,
        stable_task_path: str,
        origin_tool_call_ref: str,
        interaction_kind: str,
        elicitation_ordinal: int,
    ) -> DerivedIdentity:
        if interaction_kind not in _INTERACTION_KINDS:
            raise IdentityContractError("interaction_kind is outside the V2 closed set")
        return self.derive_contract_ref(
            kind="application_request",
            material={
                "run_id": run_id,
                "stable_task_path": stable_task_path,
                "origin_tool_call_ref": origin_tool_call_ref,
                "interaction_kind": interaction_kind,
                "elicitation_ordinal": _positive_decimal(elicitation_ordinal),
            },
        )

    def interaction_owner(
        self,
        *,
        run_id: str,
        stable_task_path: str,
        origin_tool_call_ref: str,
        interaction_kind: str,
        application_request_ref: str,
    ) -> DerivedIdentity:
        if interaction_kind not in _INTERACTION_KINDS:
            raise IdentityContractError("interaction_kind is outside the V2 closed set")
        return self.derive_contract_ref(
            kind="interaction_owner",
            material={
                "run_id": run_id,
                "stable_task_path": stable_task_path,
                "origin_tool_call_ref": origin_tool_call_ref,
                "interaction_kind": interaction_kind,
                "application_request_ref": application_request_ref,
            },
        )

    def projection_event(
        self, *, run_id: str, interaction_owner_ref: str, owner_revision: int
    ) -> DerivedIdentity:
        return self.derive_contract_ref(
            kind="projection_event",
            material={
                "run_id": run_id,
                "interaction_owner_ref": interaction_owner_ref,
                "owner_revision": _positive_decimal(owner_revision),
            },
        )

    def group_projection(
        self, *, run_id: str, decision_group_ref: str, decision_group_revision: int
    ) -> DerivedIdentity:
        return self.derive_contract_ref(
            kind="group_projection",
            material={
                "run_id": run_id,
                "decision_group_ref": decision_group_ref,
                "decision_group_revision": _positive_decimal(decision_group_revision),
            },
        )
