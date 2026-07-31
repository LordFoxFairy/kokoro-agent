"""Transactional Mongo adapter for ADR-014 origin and whole-frame facts.

The adapter cannot be constructed without a same-session evidence committer.
That committer is the activation seam for the existing durable sequence,
execution-evidence, and critical outbox authorities; this package deliberately
does not create a second counter or outbox.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, TypeAdapter
from pymongo import ReadPreference
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import DuplicateKeyError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from kokoro_agent.interaction.domain.models import (
    GroupRevisionCandidate,
    OriginCandidate,
    OwnerRevisionCandidate,
    PublishedFrame,
    RunWriteFence,
)


AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION = "agent_interaction_origin_journal"
AGENT_INTERACTION_OWNER_HEADS_COLLECTION = "agent_interaction_owner_heads"
AGENT_INTERACTION_REVISIONS_COLLECTION = "agent_interaction_revisions"
AGENT_INTERACTION_GROUP_HEADS_COLLECTION = "agent_interaction_group_heads"
AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION = "agent_interaction_group_revisions"
AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION = "agent_interaction_group_members"
AGENT_INTERACTION_TRANSITIONS_COLLECTION = "agent_interaction_transitions"

_T = TypeVar("_T")


class _MongoIndexInfo(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")

    key: list[tuple[str, int | str]]
    unique: bool = False
    sparse: bool = False
    partialFilterExpression: dict[str, object] | None = None
    collation: dict[str, object] | None = None


_INDEX_INFORMATION_ADAPTER: TypeAdapter[dict[str, _MongoIndexInfo]] = TypeAdapter(
    dict[str, _MongoIndexInfo]
)


class InteractionRepositoryConflict(ValueError):
    """Immutable replay, predecessor, or run fence did not match exactly."""


class InteractionFoundationNotReady(RuntimeError):
    """Replica-set/index/canonical activation prerequisites are not certified."""


@dataclass(frozen=True, slots=True)
class MongoEvidenceCommit:
    evidence_ref: str
    durable_seq: int
    event_id: str
    run_fence_cas_succeeded: bool

    def __post_init__(self) -> None:
        if not self.evidence_ref or not self.event_id or self.durable_seq < 1:
            raise InteractionRepositoryConflict("INTERACTION_EVIDENCE_COMMIT_INVALID")
        if not self.run_fence_cas_succeeded:
            raise InteractionRepositoryConflict("INTERACTION_RUN_FENCE_LOST")


class MongoWholeFrameEvidenceCommitter(Protocol):
    """Adapter-local seam that must use the supplied Mongo transaction session.

    Implementations must re-CAS the exact RunWriteFence while atomically
    allocating the existing durable sequence and writing the V2 evidence plus
    existing critical outbox row.  No default implementation exists while the
    V1 evidence reader and V2 activation topology remain unresolved.
    """

    async def commit_whole_frame(
        self,
        candidate: GroupRevisionCandidate,
        fence: RunWriteFence,
        *,
        session: AsyncClientSession,
    ) -> MongoEvidenceCommit: ...

    async def verify_existing_whole_frame(
        self,
        candidate: GroupRevisionCandidate,
        fence: RunWriteFence,
        commit: MongoEvidenceCommit,
        *,
        session: AsyncClientSession,
    ) -> bool: ...


class InteractionCanonicalDigestVerifier(Protocol):
    """Future Root-generated verifier for the four unresolved digest planes."""

    def verify_whole_frame(self, candidate: GroupRevisionCandidate) -> None: ...


_REQUIRED_UNIQUE_INDEXES: dict[str, tuple[tuple[str, ...], ...]] = {
    AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION: (
        ("run_id", "stable_task_path", "origin_tool_call_ref", "elicitation_ordinal"),
        ("application_request_ref",),
        ("run_id", "origin_key_digest"),
    ),
    AGENT_INTERACTION_OWNER_HEADS_COLLECTION: (
        ("run_id", "interaction_owner_ref"),
        ("run_id", "origin_key_digest"),
    ),
    AGENT_INTERACTION_REVISIONS_COLLECTION: (
        ("run_id", "interaction_owner_ref", "owner_revision"),
        ("projection_event_ref",),
    ),
    AGENT_INTERACTION_GROUP_HEADS_COLLECTION: (("run_id", "decision_group_ref"),),
    AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION: (
        ("run_id", "decision_group_ref", "decision_group_revision"),
        ("group_projection_ref",),
    ),
    AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION: (
        (
            "run_id",
            "decision_group_ref",
            "decision_group_revision",
            "group_member_ordinal",
        ),
        ("run_id", "interaction_owner_ref", "owner_revision"),
    ),
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _same_fields(
    existing: Mapping[str, object], expected: Mapping[str, object]
) -> bool:
    return all(existing.get(name) == value for name, value in expected.items())


def _origin_document(candidate: OriginCandidate) -> dict[str, object]:
    return {
        "_id": candidate.application_request_ref,
        "run_id": candidate.run_id,
        "stable_task_path": candidate.stable_task_path,
        "origin_tool_call_ref": candidate.origin_tool_call_ref,
        "elicitation_ordinal": candidate.elicitation_ordinal,
        "interaction_kind": candidate.interaction_kind.value,
        "application_request_ref": candidate.application_request_ref,
        "interaction_owner_ref": candidate.interaction_owner_ref,
        "origin_key_digest": candidate.origin_key_digest,
        "base_descriptor_sha256": candidate.base_descriptor_sha256,
        "base_schema_sha256": candidate.base_schema_sha256,
        "continuation_ref": candidate.continuation_ref,
        "continuation_sha256": candidate.continuation_sha256,
        "effect_idempotency_ref": candidate.effect_idempotency_ref,
        "effect_idempotency_sha256": candidate.effect_idempotency_sha256,
    }


def _owner_revision_document(
    run_id: str, group_ref: str, group_revision: int, member: OwnerRevisionCandidate
) -> dict[str, object]:
    return {
        "_id": member.projection_event_ref,
        "run_id": run_id,
        "decision_group_ref": group_ref,
        "decision_group_revision": group_revision,
        "interaction_owner_ref": member.interaction_owner_ref,
        "origin_key_digest": member.origin_key_digest,
        "owner_revision": member.owner_revision,
        "projection_event_ref": member.projection_event_ref,
        "predecessor_projection_event_ref": member.predecessor_projection_event_ref,
        "predecessor_evidence_sha256": member.predecessor_evidence_sha256,
        "member_evidence_sha256": member.member_evidence_sha256,
        "canonical_member_evidence": member.canonical_member_evidence,
        "projection_payload_sha256": member.projection_payload_sha256,
        "application_request_ref": member.application_request_ref,
        "interaction_kind": member.interaction_kind.value,
        "group_member_ordinal": member.group_member_ordinal,
        "required_owner_revision_refs": [
            {
                "interaction_owner_ref": ref.interaction_owner_ref,
                "owner_revision": ref.owner_revision,
            }
            for ref in member.required_owner_revision_refs
        ],
        "initial_state": member.state.value,
    }


def _group_revision_document(candidate: GroupRevisionCandidate) -> dict[str, object]:
    return {
        "_id": candidate.group_projection_ref,
        "run_id": candidate.run_id,
        "decision_group_ref": candidate.decision_group_ref,
        "decision_group_revision": candidate.decision_group_revision,
        "group_projection_ref": candidate.group_projection_ref,
        "predecessor_group_projection_ref": candidate.predecessor_group_projection_ref,
        "predecessor_group_evidence_sha256": candidate.predecessor_group_evidence_sha256,
        "group_evidence_sha256": candidate.group_evidence_sha256,
        "canonical_group_evidence": candidate.canonical_group_evidence,
        "pending_frame_digest": candidate.pending_frame_digest,
        "member_vector_sha256": candidate.member_vector_sha256,
        "ordered_owner_refs": [
            member.interaction_owner_ref for member in candidate.members
        ],
        "ordered_owner_revisions": [
            member.owner_revision for member in candidate.members
        ],
        "successor_proof_ref": candidate.successor_proof_ref,
        "successor_proof_sha256": candidate.successor_proof_sha256,
        "initial_state": "pending",
    }


class MongoInteractionRepository:
    """Replica-set-only Mongo UoW; intentionally not composed into runtime."""

    def __init__(
        self,
        run_collection: AsyncCollection[dict[str, object]],
        *,
        evidence_committer: MongoWholeFrameEvidenceCommitter,
        canonical_digest_verifier: InteractionCanonicalDigestVerifier,
        clock: Callable[[], int] = _now_ms,
    ) -> None:
        self._runs = run_collection
        self._origins = run_collection.database[
            AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION
        ]
        self._owner_heads = run_collection.database[
            AGENT_INTERACTION_OWNER_HEADS_COLLECTION
        ]
        self._revisions = run_collection.database[
            AGENT_INTERACTION_REVISIONS_COLLECTION
        ]
        self._group_heads = run_collection.database[
            AGENT_INTERACTION_GROUP_HEADS_COLLECTION
        ]
        self._group_revisions = run_collection.database[
            AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION
        ]
        self._group_members = run_collection.database[
            AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION
        ]
        self._transitions = run_collection.database[
            AGENT_INTERACTION_TRANSITIONS_COLLECTION
        ]
        self._evidence_committer = evidence_committer
        self._canonical_digest_verifier = canonical_digest_verifier
        self._clock = clock
        self._storage_ready = False

    async def assert_storage_ready(self) -> None:
        """Fail closed unless topology and every ADR-014 unique index exist."""
        hello = await self._runs.database.command({"hello": 1})
        if not isinstance(hello.get("setName"), str) and hello.get("msg") != "isdbgrid":
            raise InteractionFoundationNotReady(
                "INTERACTION_MONGO_TRANSACTION_UNAVAILABLE"
            )
        for collection_name, required_indexes in _REQUIRED_UNIQUE_INDEXES.items():
            collection = self._runs.database[collection_name]
            options = await collection.options()
            if options.get("collation") is not None:
                raise InteractionFoundationNotReady(
                    f"INTERACTION_COLLECTION_COLLATION_UNSAFE:{collection_name}"
                )
            raw_indexes = _INDEX_INFORMATION_ADAPTER.validate_python(
                await collection.index_information()
            )
            available: set[tuple[str, ...]] = set()
            for raw_index in raw_indexes.values():
                if (
                    raw_index.unique
                    and not raw_index.sparse
                    and raw_index.partialFilterExpression is None
                    and raw_index.collation is None
                ):
                    available.add(tuple(key for key, _direction in raw_index.key))
            missing = set(required_indexes) - available
            if missing:
                raise InteractionFoundationNotReady(
                    f"INTERACTION_UNIQUE_INDEX_MISSING:{collection_name}:{sorted(missing)!r}"
                )
        self._storage_ready = True

    def _assert_storage_ready(self) -> None:
        if not self._storage_ready:
            raise InteractionFoundationNotReady(
                "INTERACTION_STORAGE_READINESS_NOT_PROVEN"
            )

    async def _transaction(
        self, callback: Callable[[AsyncClientSession], Awaitable[_T]]
    ) -> _T:
        async with self._runs.database.client.start_session() as session:
            return await session.with_transaction(
                callback,
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern("majority"),
                read_preference=ReadPreference.PRIMARY,
            )

    def _run_fence_query(self, fence: RunWriteFence) -> dict[str, object]:
        now = max(self._clock(), fence.lease_valid_at_ms)
        return {
            "_id": fence.run_id,
            "terminal": {"$ne": True},
            "$or": [
                {"terminal_fence_seq": {"$exists": False}},
                {"terminal_fence_seq": None},
            ],
            "owner": fence.lease_owner_ref,
            "lease_expires_ms": {"$gt": now},
            "interaction_v2_fence.producer_instance_ref": fence.producer_instance_ref,
            "interaction_v2_fence.producer_generation": fence.producer_generation,
            "interaction_v2_fence.checkpoint_ref": fence.checkpoint_ref,
            "interaction_v2_fence.checkpoint_sha256": fence.checkpoint_sha256,
            "interaction_v2_fence.checkpoint_generation": fence.checkpoint_generation,
        }

    async def _assert_run_fence(
        self, fence: RunWriteFence, session: AsyncClientSession
    ) -> None:
        matched = await self._runs.find_one(
            self._run_fence_query(fence), {"_id": 1}, session=session
        )
        if matched is None:
            raise InteractionRepositoryConflict("INTERACTION_RUN_FENCE_LOST")

    async def prepare_origin(
        self, candidate: OriginCandidate, fence: RunWriteFence
    ) -> OriginCandidate:
        self._assert_storage_ready()
        if candidate.run_id != fence.run_id:
            raise InteractionRepositoryConflict("INTERACTION_RUN_ID_CONFLICT")
        exact_key = {
            "run_id": candidate.run_id,
            "stable_task_path": candidate.stable_task_path,
            "origin_tool_call_ref": candidate.origin_tool_call_ref,
            "elicitation_ordinal": candidate.elicitation_ordinal,
        }
        expected = _origin_document(candidate)

        async def prepare(session: AsyncClientSession) -> OriginCandidate:
            await self._assert_run_fence(fence, session)
            existing = await self._origins.find_one(exact_key, session=session)
            if existing is not None:
                if not _same_fields(existing, expected):
                    raise InteractionRepositoryConflict("INTERACTION_ORIGIN_CONFLICT")
                return candidate
            later = await self._origins.find_one(
                {
                    **{
                        key: value
                        for key, value in exact_key.items()
                        if key != "elicitation_ordinal"
                    },
                    "elicitation_ordinal": {"$gt": candidate.elicitation_ordinal},
                },
                {"_id": 1},
                session=session,
            )
            if later is not None:
                raise InteractionRepositoryConflict(
                    "INTERACTION_ORIGIN_ORDINAL_CONFLICT"
                )
            if candidate.elicitation_ordinal > 1:
                predecessor = await self._origins.find_one(
                    {
                        **{
                            key: value
                            for key, value in exact_key.items()
                            if key != "elicitation_ordinal"
                        },
                        "elicitation_ordinal": candidate.elicitation_ordinal - 1,
                    },
                    {"_id": 1},
                    session=session,
                )
                if predecessor is None:
                    raise InteractionRepositoryConflict(
                        "INTERACTION_ORIGIN_ORDINAL_GAP"
                    )
            await self._origins.insert_one(expected, session=session)
            return candidate

        try:
            return await self._transaction(prepare)
        except DuplicateKeyError:
            return await self._transaction(prepare)

    async def _assert_origin(
        self,
        candidate: GroupRevisionCandidate,
        member: OwnerRevisionCandidate,
        session: AsyncClientSession,
    ) -> None:
        origin = await self._origins.find_one(
            {
                "run_id": candidate.run_id,
                "application_request_ref": member.application_request_ref,
                "interaction_owner_ref": member.interaction_owner_ref,
                "origin_key_digest": member.origin_key_digest,
            },
            {"_id": 1},
            session=session,
        )
        if origin is None:
            raise InteractionRepositoryConflict("INTERACTION_ORIGIN_MISSING")

    async def _assert_predecessors(
        self, candidate: GroupRevisionCandidate, session: AsyncClientSession
    ) -> None:
        group_head = await self._group_heads.find_one(
            {
                "run_id": candidate.run_id,
                "decision_group_ref": candidate.decision_group_ref,
            },
            session=session,
        )
        if candidate.decision_group_revision == 1:
            if group_head is not None:
                raise InteractionRepositoryConflict("INTERACTION_GROUP_CONFLICT")
            for member in candidate.members:
                await self._assert_origin(candidate, member, session)
                owner_head = await self._owner_heads.find_one(
                    {
                        "run_id": candidate.run_id,
                        "interaction_owner_ref": member.interaction_owner_ref,
                    },
                    {"_id": 1},
                    session=session,
                )
                if owner_head is not None or member.owner_revision != 1:
                    raise InteractionRepositoryConflict("INTERACTION_OWNER_CONFLICT")
            return
        if group_head is None or not _same_fields(
            group_head,
            {
                "current_revision": candidate.decision_group_revision - 1,
                "group_projection_ref": candidate.predecessor_group_projection_ref,
                "head_evidence_sha256": candidate.predecessor_group_evidence_sha256,
                "state": "applied",
            },
        ):
            raise InteractionRepositoryConflict(
                "INTERACTION_GROUP_PREDECESSOR_CONFLICT"
            )
        cursor = self._group_members.find(
            {
                "run_id": candidate.run_id,
                "decision_group_ref": candidate.decision_group_ref,
                "decision_group_revision": candidate.decision_group_revision - 1,
            },
            {
                "_id": 0,
                "interaction_owner_ref": 1,
                "owner_revision": 1,
                "group_member_ordinal": 1,
            },
            session=session,
        ).sort("group_member_ordinal", 1)
        previous_members = [
            {
                "interaction_owner_ref": row["interaction_owner_ref"],
                "owner_revision": row["owner_revision"],
            }
            async for row in cursor
        ]
        expected_previous = [
            {
                "interaction_owner_ref": member.interaction_owner_ref,
                "owner_revision": member.owner_revision - 1,
            }
            for member in candidate.members
        ]
        if previous_members != expected_previous:
            raise InteractionRepositoryConflict(
                "INTERACTION_GROUP_MEMBER_VECTOR_CONFLICT"
            )
        for member in candidate.members:
            await self._assert_origin(candidate, member, session)
            owner_head = await self._owner_heads.find_one(
                {
                    "run_id": candidate.run_id,
                    "interaction_owner_ref": member.interaction_owner_ref,
                },
                session=session,
            )
            if owner_head is None or not _same_fields(
                owner_head,
                {
                    "current_revision": member.owner_revision - 1,
                    "projection_event_ref": member.predecessor_projection_event_ref,
                    "head_evidence_sha256": member.predecessor_evidence_sha256,
                    "state": "applied",
                },
            ):
                raise InteractionRepositoryConflict(
                    "INTERACTION_OWNER_PREDECESSOR_CONFLICT"
                )

    async def _write_heads(
        self, candidate: GroupRevisionCandidate, session: AsyncClientSession
    ) -> None:
        if candidate.decision_group_revision == 1:
            await self._group_heads.insert_one(
                {
                    "_id": f"{candidate.run_id}:{candidate.decision_group_ref}",
                    "run_id": candidate.run_id,
                    "decision_group_ref": candidate.decision_group_ref,
                    "current_revision": 1,
                    "group_projection_ref": candidate.group_projection_ref,
                    "head_evidence_sha256": candidate.group_evidence_sha256,
                    "pending_frame_digest": candidate.pending_frame_digest,
                    "state": "pending",
                },
                session=session,
            )
            await self._owner_heads.insert_many(
                [
                    {
                        "_id": f"{candidate.run_id}:{member.interaction_owner_ref}",
                        "run_id": candidate.run_id,
                        "interaction_owner_ref": member.interaction_owner_ref,
                        "origin_key_digest": member.origin_key_digest,
                        "current_revision": 1,
                        "projection_event_ref": member.projection_event_ref,
                        "head_evidence_sha256": member.member_evidence_sha256,
                        "state": "pending",
                    }
                    for member in candidate.members
                ],
                ordered=True,
                session=session,
            )
            return
        group_advanced = await self._group_heads.update_one(
            {
                "run_id": candidate.run_id,
                "decision_group_ref": candidate.decision_group_ref,
                "current_revision": candidate.decision_group_revision - 1,
                "group_projection_ref": candidate.predecessor_group_projection_ref,
                "head_evidence_sha256": candidate.predecessor_group_evidence_sha256,
                "state": "applied",
            },
            {
                "$set": {
                    "current_revision": candidate.decision_group_revision,
                    "group_projection_ref": candidate.group_projection_ref,
                    "head_evidence_sha256": candidate.group_evidence_sha256,
                    "pending_frame_digest": candidate.pending_frame_digest,
                    "state": "pending",
                }
            },
            session=session,
        )
        if group_advanced.modified_count != 1:
            raise InteractionRepositoryConflict("INTERACTION_GROUP_CAS_LOST")
        for member in candidate.members:
            owner_advanced = await self._owner_heads.update_one(
                {
                    "run_id": candidate.run_id,
                    "interaction_owner_ref": member.interaction_owner_ref,
                    "current_revision": member.owner_revision - 1,
                    "projection_event_ref": member.predecessor_projection_event_ref,
                    "head_evidence_sha256": member.predecessor_evidence_sha256,
                    "state": "applied",
                },
                {
                    "$set": {
                        "current_revision": member.owner_revision,
                        "projection_event_ref": member.projection_event_ref,
                        "head_evidence_sha256": member.member_evidence_sha256,
                        "state": "pending",
                    }
                },
                session=session,
            )
            if owner_advanced.modified_count != 1:
                raise InteractionRepositoryConflict("INTERACTION_OWNER_CAS_LOST")

    async def publish_whole_frame(
        self, candidate: GroupRevisionCandidate, fence: RunWriteFence
    ) -> PublishedFrame:
        self._assert_storage_ready()
        self._canonical_digest_verifier.verify_whole_frame(candidate)
        if candidate.run_id != fence.run_id:
            raise InteractionRepositoryConflict("INTERACTION_RUN_ID_CONFLICT")
        group_document = _group_revision_document(candidate)

        async def publish(session: AsyncClientSession) -> PublishedFrame:
            await self._assert_run_fence(fence, session)
            existing = await self._group_revisions.find_one(
                {"_id": candidate.group_projection_ref}, session=session
            )
            if existing is not None:
                if not _same_fields(existing, group_document):
                    raise InteractionRepositoryConflict(
                        "PROJECTION_EVENT_IDENTITY_CONFLICT"
                    )
                evidence_ref = existing.get("evidence_ref")
                durable_seq = existing.get("durable_seq")
                event_id = existing.get("event_id")
                if (
                    not isinstance(evidence_ref, str)
                    or not isinstance(durable_seq, int)
                    or not isinstance(event_id, str)
                ):
                    raise InteractionRepositoryConflict("INTERACTION_FRAME_PARTIAL")
                existing_commit = MongoEvidenceCommit(
                    evidence_ref=evidence_ref,
                    durable_seq=durable_seq,
                    event_id=event_id,
                    run_fence_cas_succeeded=True,
                )
                if not await self._evidence_committer.verify_existing_whole_frame(
                    candidate,
                    fence,
                    existing_commit,
                    session=session,
                ):
                    raise InteractionRepositoryConflict(
                        "INTERACTION_EVIDENCE_LINK_CONFLICT"
                    )
                return PublishedFrame(
                    group_projection_ref=candidate.group_projection_ref,
                    evidence_ref=evidence_ref,
                    durable_seq=durable_seq,
                    event_id=event_id,
                    created=False,
                )
            await self._assert_predecessors(candidate, session)
            await self._group_revisions.insert_one(group_document, session=session)
            await self._revisions.insert_many(
                [
                    _owner_revision_document(
                        candidate.run_id,
                        candidate.decision_group_ref,
                        candidate.decision_group_revision,
                        member,
                    )
                    for member in candidate.members
                ],
                ordered=True,
                session=session,
            )
            await self._group_members.insert_many(
                [
                    {
                        "_id": (
                            f"{candidate.run_id}:{candidate.decision_group_ref}:"
                            f"{candidate.decision_group_revision}:{member.group_member_ordinal}"
                        ),
                        "run_id": candidate.run_id,
                        "decision_group_ref": candidate.decision_group_ref,
                        "decision_group_revision": candidate.decision_group_revision,
                        "group_member_ordinal": member.group_member_ordinal,
                        "interaction_owner_ref": member.interaction_owner_ref,
                        "owner_revision": member.owner_revision,
                        "projection_event_ref": member.projection_event_ref,
                    }
                    for member in candidate.members
                ],
                ordered=True,
                session=session,
            )
            if candidate.decision_group_revision > 1:
                transition_seed = (
                    f"{candidate.run_id}\0{candidate.group_projection_ref}\0"
                    f"{candidate.successor_proof_ref}"
                )
                await self._transitions.insert_many(
                    [
                        {
                            "_id": "itrans_"
                            + hashlib.sha256(
                                f"{transition_seed}\0group".encode()
                            ).hexdigest(),
                            "run_id": candidate.run_id,
                            "decision_group_ref": candidate.decision_group_ref,
                            "decision_group_revision": candidate.decision_group_revision
                            - 1,
                            "entity_kind": "group",
                            "from_state": "applied",
                            "to_state": "superseded_by_revision",
                            "cause_ref": candidate.successor_proof_ref,
                            "cause_sha256": candidate.successor_proof_sha256,
                        },
                        *[
                            {
                                "_id": "itrans_"
                                + hashlib.sha256(
                                    f"{transition_seed}\0{member.interaction_owner_ref}".encode()
                                ).hexdigest(),
                                "run_id": candidate.run_id,
                                "interaction_owner_ref": member.interaction_owner_ref,
                                "owner_revision": member.owner_revision - 1,
                                "entity_kind": "owner",
                                "from_state": "applied",
                                "to_state": "superseded_by_revision",
                                "cause_ref": candidate.successor_proof_ref,
                                "cause_sha256": candidate.successor_proof_sha256,
                            }
                            for member in candidate.members
                        ],
                    ],
                    ordered=True,
                    session=session,
                )
            await self._write_heads(candidate, session)
            committed = await self._evidence_committer.commit_whole_frame(
                candidate, fence, session=session
            )
            linked = await self._group_revisions.update_one(
                {"_id": candidate.group_projection_ref},
                {
                    "$set": {
                        "evidence_ref": committed.evidence_ref,
                        "durable_seq": committed.durable_seq,
                        "event_id": committed.event_id,
                    }
                },
                session=session,
            )
            if linked.modified_count != 1:
                raise InteractionRepositoryConflict("INTERACTION_FRAME_LINK_CAS_LOST")
            return PublishedFrame(
                group_projection_ref=candidate.group_projection_ref,
                evidence_ref=committed.evidence_ref,
                durable_seq=committed.durable_seq,
                event_id=committed.event_id,
                created=True,
            )

        try:
            return await self._transaction(publish)
        except DuplicateKeyError:
            return await self._transaction(publish)
